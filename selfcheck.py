"""
Self-check against a REAL MongoDB. Not part of the app; run it once yourself:

    MONGO_URI="mongodb://localhost:27017" python selfcheck.py            # correctness + explain + concurrency
    MONGO_URI="mongodb://localhost:27017" python selfcheck.py --big      # also seeds 100k logs and times endpoints

It uses its own throw-away database (attendance_selfcheck), starts the app on port 8765, seeds awkward data
(no-log employee, mid-month joiner, overnight shift, weekend records, ties, half days, a missing weekday, a
log of an unknown employee), and compares every analytics endpoint with an independent pure-Python calculation.
"""
import concurrent.futures as cf
import json
import os
import random
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

import httpx
from pymongo import MongoClient

URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB = "attendance_selfcheck"
PORT = 8765
BASE = f"http://127.0.0.1:{PORT}"
PRES = {"PRESENT", "WFH", "ON_DUTY"}
UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
failures = []


def check(name, got, want):
    if got != want:
        failures.append(name)
        print(f"  FAIL {name}\n    got : {got}\n    want: {want}")
    else:
        print(f"  ok   {name}")


def r4(x):
    return float(Decimal(x).quantize(Decimal("0.0001"), ROUND_HALF_UP))


def r2(x):
    return float(Decimal(str(x)).quantize(Decimal("0.01"), ROUND_HALF_UP))


def mk(emp, d, status="PRESENT", late=0, ot=0, wh=9.0, half=None, legacy=False):
    day = date.fromisoformat(d)
    pin = datetime(day.year, day.month, day.day, 9, 30, tzinfo=IST).astimezone(UTC) if status in PRES else None
    pout = pin + timedelta(hours=wh) if (pin and wh is not None) else None
    doc = {"emp_code": emp, "date": d, "status": status, "punch_in": pin, "punch_out": pout,
           "work_hours": wh if pout else None, "late_minutes": late, "overtime_minutes": ot,
           "half_day": bool(half) if half is not None else (wh is not None and wh < 4.5), "history": []}
    if legacy:
        doc.pop("half_day"); doc.pop("history")
    return doc


def seed(db):
    for c in ("employees", "attendance_logs"):
        db[c].drop()
    E = lambda code, dep, joined, ss="09:30", se="18:30": {
        "emp_code": code, "name": f"Name {code}", "email": f"{code}@x.com", "department": dep,
        "shift_start": ss, "shift_end": se, "joined_on": joined, "created_at": datetime(2026, 1, 1, tzinfo=UTC)}
    db.employees.insert_many([
        E("EMP0001", "Eng", "2026-01-05"), E("EMP0002", "Eng", "2026-01-05"), E("EMP0003", "Eng", "2026-07-15"),
        E("EMP0004", "Eng", "2026-01-05"),
        E("EMP0005", "Ops", "2026-01-05", "22:00", "06:00"),
        E("EMP0006", "Ops", "2026-01-05"), E("EMP0007", "Ops", "2026-08-01"),
        E("EMP0008", "HR", "2026-09-01"),
    ])
    logs = []
    for emp in ("EMP0001", "EMP0002", "EMP0005", "EMP0006"):
        for i in range(1, 32):
            d = date(2026, 7, i)
            if d.weekday() >= 5 or (emp == "EMP0002" and i == 8):
                continue
            if i % 11 == 0:
                logs.append(mk(emp, d.isoformat(), "LEAVE", wh=None)); continue
            if i % 13 == 0:
                logs.append(mk(emp, d.isoformat(), "ABSENT", wh=None)); continue
            logs.append(mk(emp, d.isoformat(), "WFH" if i % 7 == 0 else "ON_DUTY" if i % 9 == 0 else "PRESENT",
                           late=(i % 4) * 15 if emp in ("EMP0001", "EMP0006") else 0, ot=40 if i % 5 == 0 else 0,
                           wh=3.5 if i % 6 == 0 else 8.125, legacy=(i % 8 == 0)))
    logs += [mk("EMP0001", "2026-07-04", late=45, ot=60),
             mk("EMP0001", "2026-07-05", late=20, wh=None),
             mk("EMP0003", "2026-07-20", late=30), mk("EMP0003", "2026-07-21", late=30),
             mk("EMP0002", "2026-06-30", late=99),
             mk("GHOST01", "2026-07-06", late=500)]
    for emp in ("EMP0002", "EMP0006"):
        logs = [l for l in logs if not (l["emp_code"] == emp and "2026-07-01" <= l["date"] <= "2026-07-31")]
        logs += [mk(emp, "2026-07-01", late=100), mk(emp, "2026-07-02", late=50)]
    db.attendance_logs.insert_many(logs)


class Ref:
    def __init__(self, db):
        self.emps = list(db.employees.find({}, {"_id": 0}))
        self.logs = list(db.attendance_logs.find({}, {"_id": 0}))

    def month_logs(self, month):
        return [l for l in self.logs if l["date"].startswith(month)]

    @staticmethod
    def weight(l):
        return (0.5 if l.get("half_day") else 1) if l["status"] in PRES else 0

    @staticmethod
    def weekday(l):
        return date.fromisoformat(l["date"]).weekday() < 5

    def monthly(self, code, month):
        y, m = map(int, month.split("-"))
        e = next(e for e in self.emps if e["emp_code"] == code)
        last = (date(y + (m == 12), m % 12 + 1, 1) - timedelta(days=1))
        first = max(date(y, m, 1), date.fromisoformat(e["joined_on"]))
        wd = sum(1 for i in range((last - first).days + 1) if (first + timedelta(days=i)).weekday() < 5) if first <= last else 0
        ls = [l for l in self.month_logs(month) if l["emp_code"] == code]
        present = sum(self.weight(l) for l in ls if self.weekday(l))
        return {"emp_code": code, "month": month, "working_days": wd, "present_days": r2(present),
                "leave_days": sum(l["status"] == "LEAVE" for l in ls),
                "late_count": sum(l.get("late_minutes", 0) > 0 for l in ls),
                "total_late_minutes": sum(l.get("late_minutes", 0) for l in ls),
                "total_overtime_minutes": sum(l.get("overtime_minutes", 0) for l in ls),
                "attendance_pct": None if wd == 0 else r2(Decimal(str(present)) / wd * 100)}

    def summary(self, month, dep=None):
        y, m = map(int, month.split("-"))
        end = (date(y + (m == 12), m % 12 + 1, 1) - timedelta(days=1)).isoformat()
        out = []
        for d in sorted({e["department"] for e in self.emps}):
            if dep and d != dep:
                continue
            codes = {e["emp_code"] for e in self.emps if e["department"] == d and e["joined_on"] <= end}
            if not codes:
                continue
            ls = [l for l in self.month_logs(month) if l["emp_code"] in codes]
            hrs = [Decimal(str(l["work_hours"])) for l in ls if l["status"] in PRES and l.get("work_hours") is not None]
            out.append({"department": d, "headcount": len(codes),
                        "present_days": r2(sum(self.weight(l) for l in ls if self.weekday(l))),
                        "avg_work_hours": float((sum(hrs) / len(hrs)).quantize(Decimal("0.01"), ROUND_HALF_UP)) if hrs else None,
                        "late_count": sum(l.get("late_minutes", 0) > 0 for l in ls),
                        "total_late_minutes": sum(l.get("late_minutes", 0) for l in ls),
                        "leave_count": sum(l["status"] == "LEAVE" for l in ls),
                        "on_duty_count": sum(l["status"] == "ON_DUTY" for l in ls)})
        return {"month": month, "items": out}

    def leaderboard(self, month, limit=10, dep=None):
        info = {e["emp_code"]: e for e in self.emps}
        tot = {}
        for l in self.month_logs(month):
            if l["emp_code"] in info and l.get("late_minutes", 0) > 0:
                t = tot.setdefault(l["emp_code"], [0, 0])
                t[0] += l["late_minutes"]; t[1] += 1
        if dep:
            tot = {k: v for k, v in tot.items() if info[k]["department"] == dep}
        rows = sorted(tot.items(), key=lambda kv: (-kv[1][0], kv[0]))
        items = []
        for code, (t, n) in rows:
            rank = 1 + sum(1 for _, (t2, _) in rows if t2 > t)
            if rank <= limit:
                items.append({"rank": rank, "emp_code": code, "name": info[code]["name"],
                              "department": info[code]["department"], "total_late_minutes": t, "late_count": n})
        return {"month": month, "items": items}

    def trend(self, dep, d_from, d_to):
        emps = [e for e in self.emps if e["department"] == dep]
        codes = {e["emp_code"] for e in emps}
        rows, rates = [], []
        for i in range((d_to - d_from).days + 1):
            d = d_from + timedelta(days=i)
            ds = d.isoformat()
            ls = [l for l in self.logs if l["date"] == ds and l["emp_code"] in codes]
            head = sum(e["joined_on"] <= ds for e in emps)
            pc = sum(self.weight(l) for l in ls)
            work = d.weekday() < 5
            rate = r4(Decimal(str(pc)) / head) if work and head > 0 else None
            rates.append(rate)
            win = [x for x in rates[-7:] if x is not None]
            rows.append({"date": ds, "is_working_day": work, "headcount": head, "present_count": r2(pc),
                         "late_count": sum(l.get("late_minutes", 0) > 0 for l in ls), "attendance_rate": rate,
                         "moving_avg_7d": r4(sum(Decimal(str(x)) for x in win) / len(win)) if win else None})
        return {"department": dep, "items": rows}


def correctness(c, ref):
    print("analytics vs reference")
    for code in ("EMP0001", "EMP0002", "EMP0003", "EMP0004", "EMP0005", "EMP0007"):
        for month in ("2026-07", "2026-08"):
            check(f"monthly {code} {month}", c.get(f"/analytics/employees/{code}/monthly", params={"month": month}).json(),
                  ref.monthly(code, month))
    for month in ("2026-06", "2026-07", "2026-08", "2026-09"):
        check(f"summary {month}", c.get("/analytics/departments/summary", params={"month": month}).json(), ref.summary(month))
    check("summary dept filter", c.get("/analytics/departments/summary", params={"month": "2026-07", "department": "Ops"}).json(),
          ref.summary("2026-07", "Ops"))
    for limit in (1, 2, 3, 10):
        check(f"leaderboard limit={limit}", c.get("/analytics/leaderboard/late", params={"month": "2026-07", "limit": limit}).json(),
              ref.leaderboard("2026-07", limit))
    check("leaderboard dept", c.get("/analytics/leaderboard/late", params={"month": "2026-07", "department": "Eng"}).json(),
          ref.leaderboard("2026-07", 10, "Eng"))
    for dep, a, b in (("Eng", "2026-06-25", "2026-07-31"), ("Ops", "2026-07-01", "2026-07-14"), ("HR", "2026-08-25", "2026-09-10")):
        got = c.get(f"/analytics/departments/{dep}/trend", params={"from": a, "to": b}).json()
        check(f"trend {dep} {a}..{b}", got, ref.trend(dep, date.fromisoformat(a), date.fromisoformat(b)))
    check("trend unknown dept 404", c.get("/analytics/departments/Nope/trend", params={"from": "2026-07-01", "to": "2026-07-02"}).status_code, 404)
    check("trend range > 92 days 422", c.get("/analytics/departments/Eng/trend", params={"from": "2026-01-01", "to": "2026-06-01"}).status_code, 422)
    check("monthly unknown employee 404", c.get("/analytics/employees/EMP9999/monthly", params={"month": "2026-07"}).status_code, 404)


def explains(c):
    print("explain plans")
    cases = {"attendance_list": {}, "employee_monthly": {"emp_code": "EMP0001", "month": "2026-07"},
             "department_summary": {"month": "2026-07"}, "late_leaderboard": {"month": "2026-07"},
             "department_trend": {"department": "Eng", "from": "2026-07-01", "to": "2026-07-31"}}
    extra = {"attendance_list": [{"emp_code": "EMP0001", "date_from": "2026-07-01", "date_to": "2026-07-31"},
                                 {"status": "LEAVE"}, {"page": 3, "page_size": 5}]}
    for name, params in cases.items():
        for p in [params] + extra.get(name, []):
            r = c.get(f"/admin/explain/{name}", params=p)
            text = json.dumps(r.json())
            check(f"explain {name} {p}", (r.status_code, "IXSCAN" in text, "COLLSCAN" in text), (200, True, False))
    check("explain missing param 422", c.get("/admin/explain/employee_monthly").status_code, 422)


def concurrency(c, db):
    print("concurrency")
    c.post("/employees", json={"emp_code": "EMP0900", "name": "Race", "email": "r@x.com", "department": "Eng", "joined_on": "2026-01-01"})
    ts = int(datetime(2026, 7, 6, 9, 30, tzinfo=IST).timestamp()) * 1000
    with cf.ThreadPoolExecutor(20) as ex:
        codes = list(ex.map(lambda _: httpx.post(f"{BASE}/attendance/punch-in", json={"emp_code": "EMP0900", "punched_at": ts}).status_code, range(20)))
    check("20 parallel punch-ins -> one 201", (codes.count(201), codes.count(409)), (1, 19))
    out = ts + 9 * 3600 * 1000
    with cf.ThreadPoolExecutor(20) as ex:
        codes = list(ex.map(lambda _: httpx.post(f"{BASE}/attendance/punch-out", json={"emp_code": "EMP0900", "punched_at": out}).status_code, range(20)))
    check("20 parallel punch-outs -> one 200", (codes.count(200), codes.count(409)), (1, 19))
    def patch(i):
        return httpx.patch(f"{BASE}/attendance/EMP0900/2026-07-06",
                           json={"punch_out": out + (i + 1) * 60000, "reason": f"edit number {i}", "regularized_by": "t"}).status_code
    with cf.ThreadPoolExecutor(10) as ex:
        codes = list(ex.map(patch, range(10)))
    hist = len(db.attendance_logs.find_one({"emp_code": "EMP0900", "date": "2026-07-06"}).get("history", []))
    check("parallel PATCH: history entries == successful PATCHes", hist, codes.count(200))
    with cf.ThreadPoolExecutor(10) as ex:
        codes = list(ex.map(lambda _: httpx.post(f"{BASE}/employees", json={"emp_code": "EMP0901", "name": "D", "email": "d@x.com", "department": "Eng", "joined_on": "2026-01-01"}).status_code, range(10)))
    check("parallel employee create -> one 201", (codes.count(201), codes.count(409)), (1, 9))


def big(db, c):
    print("100k logs: timing (seconds)")
    db.attendance_logs.delete_many({})
    rnd = random.Random(1)
    emps = [f"EMP{i:05d}" for i in range(1, 501)]
    db.employees.delete_many({})
    db.employees.insert_many([{"emp_code": e, "name": e, "email": "e@x.com", "department": f"D{i % 8}", "shift_start": "09:30",
                               "shift_end": "18:30", "joined_on": "2025-01-01", "created_at": datetime(2025, 1, 1, tzinfo=UTC)} for i, e in enumerate(emps)])
    docs = []
    for e in emps:
        for k in range(200):
            d = (date(2026, 1, 1) + timedelta(days=k)).isoformat()
            docs.append(mk(e, d, late=rnd.choice([0, 0, 15, 30]), ot=rnd.choice([0, 40])))
    db.attendance_logs.insert_many(docs)
    for name, url, p in [("list", "/attendance", {"page": 50}), ("list emp", "/attendance", {"emp_code": "EMP00007"}),
                         ("monthly", "/analytics/employees/EMP00007/monthly", {"month": "2026-03"}),
                         ("summary", "/analytics/departments/summary", {"month": "2026-03"}),
                         ("leaderboard", "/analytics/leaderboard/late", {"month": "2026-03"}),
                         ("trend", "/analytics/departments/D3/trend", {"from": "2026-03-01", "to": "2026-05-31"})]:
        t = time.time(); r = c.get(url, params=p); print(f"  {name:12} {time.time() - t:6.3f}s  status {r.status_code}")
    explains(c)


def main():
    env = dict(os.environ, MONGO_URI=URI, MONGO_DB=DB)
    db = MongoClient(URI, tz_aware=True)[DB]
    seed(db)
    srv = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(PORT)], env=env,
                           cwd=os.path.dirname(os.path.abspath(__file__)), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        for _ in range(40):
            try:
                if httpx.get(f"{BASE}/health").status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.5)
        else:
            sys.exit("app did not start: " + srv.stderr.read().decode()[-2000:])
        c = httpx.Client(base_url=BASE, timeout=30)
        correctness(c, Ref(db))
        explains(c)
        concurrency(c, db)
        if "--big" in sys.argv:
            big(db, c)
    finally:
        srv.terminate()
    print("\nFAILED:" if failures else "\nALL CHECKS PASSED", *failures, sep="\n  ")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()