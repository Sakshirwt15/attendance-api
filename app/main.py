"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB  (a local .env is loaded for convenience; real environment variables win)

Layout of this file
  1. configuration, database, indexes
  2. small helpers: time, dates, rounding
  3. business rules R1-R5 (pure functions)
  4. request models and (de)serialisation
  5. endpoints: system, employees, attendance
  6. aggregation pipelines + analytics endpoints
  7. explain endpoint
"""
import calendar
import json
import logging
import os
import re
import threading
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError, ServerSelectionTimeoutError

load_dotenv()

log = logging.getLogger("attendance")

client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]
NON_PRESENCE = ["ABSENT", "LEAVE"]
STATUSES = ["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

GRACE_SECONDS = 10 * 60
MIN_OVERTIME_MINUTES = 30
HALF_DAY_BELOW = Decimal("4.50")
MAX_SHIFT_SECONDS = 24 * 3600
MAX_TREND_DAYS = 92
MAX_PATCH_RETRIES = 10

MIN_MS, MAX_MS = 100_000_000_000, 4_102_444_800_000
HHMM = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"
MONTH_RE = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")

INDEXES = [
    ("employees", [("emp_code", 1)], {"name": "uniq_emp_code", "unique": True}),
    ("employees", [("department", 1), ("emp_code", 1)], {"name": "dept_emp_code"}),
    ("employees", [("joined_on", 1)], {"name": "joined_on"}),
    ("attendance_logs", [("emp_code", 1), ("date", 1)], {"name": "uniq_emp_date", "unique": True}),
    ("attendance_logs", [("date", -1), ("emp_code", 1)], {"name": "date_desc_emp"}),
    ("attendance_logs", [("status", 1), ("date", -1), ("emp_code", 1)], {"name": "status_date_emp"}),
    ("attendance_logs", [("emp_code", 1), ("punch_in", -1)], {"name": "emp_punch_in"}),
]
_indexes_ready = False
_index_lock = threading.Lock()


def ensure_indexes() -> None:
    """Create every index (idempotent). Safe to call repeatedly and from several threads."""
    global _indexes_ready
    if _indexes_ready:
        return
    with _index_lock:
        if _indexes_ready:
            return
        ok = True
        for coll, keys, opts in INDEXES:
            try:
                db[coll].create_index(keys, **opts)
            except PyMongoError as exc:
                ok = False
                log.error("could not create index %s on %s: %s", opts["name"], coll, exc)
                if isinstance(exc, ServerSelectionTimeoutError):
                    break
        _indexes_ready = ok


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except Exception as exc:
        log.error("index creation failed at startup: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_: Request, exc: PyMongoError):
    log.error("database error: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


def unprocessable(msg: str, *loc: str) -> HTTPException:
    """422 with the same body shape FastAPI uses for validation errors."""
    return HTTPException(422, detail=[{"loc": list(loc) or ["body"], "msg": msg, "type": "value_error"}])


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def ms_to_dt(ms: int) -> datetime:
    """Epoch milliseconds -> aware UTC datetime, truncated to whole seconds (R1)."""
    return datetime.fromtimestamp(ms // 1000, tz=UTC)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def round_half_up(value: Any, places: int) -> float:
    """Half-up rounding (R4/R8). Python's round() is banker's rounding on binary floats."""
    q = Decimal(1).scaleb(-places)
    return float(Decimal(repr(value) if isinstance(value, float) else str(value)).quantize(q, ROUND_HALF_UP))


def parse_date(value: str, *loc: str) -> date:
    if not DATE_RE.fullmatch(value):
        raise unprocessable("date must be YYYY-MM-DD", *loc)
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise unprocessable("not a valid calendar date", *loc)


def month_bounds(month: str) -> tuple[date, date]:
    y, m = int(month[:4]), int(month[5:7])
    if y < 1:
        raise unprocessable("not a valid month", "query", "month")
    return date(y, m, 1), date(y, m, calendar.monthrange(y, m)[1])


def count_weekdays(first: date, last: date) -> int:
    """Number of Mon-Fri days in [first, last] (R7). Pure calendar arithmetic."""
    n, d = 0, first
    while d <= last:
        n += d.weekday() < 5
        d += timedelta(days=1)
    return n


def parse_hhmm(value: str) -> time:
    h, m = value.split(":")
    return time(int(h), int(m))


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return parse_hhmm(shift_end) <= parse_hhmm(shift_start)


def at_ist(day: date, hhmm: str, add_days: int = 0) -> datetime:
    return datetime.combine(day + timedelta(days=add_days), parse_hhmm(hhmm), tzinfo=IST)


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> date:
    """R1: IST calendar date of the punch-in; an early-morning punch-in of an overnight shift
    belongs to the shift that started the previous day."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if is_overnight(shift_start, shift_end) and local.time() < parse_hhmm(shift_end):
        day -= timedelta(days=1)
    return day


def compute_late_minutes(punch_in: datetime, att_date: date, shift_start: str) -> int:
    """R2: late only when strictly more than 10:00 after shift start; then whole minutes since shift start."""
    seconds = int((punch_in - at_ist(att_date, shift_start)).total_seconds())
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def compute_overtime(punch_out: datetime, att_date: date, shift_start: str, shift_end: str) -> int:
    """R3: whole minutes after shift end, counted only when at least 30. Overnight shifts end the next day."""
    end = at_ist(att_date, shift_end, 1 if is_overnight(shift_start, shift_end) else 0)
    minutes = int((punch_out - end).total_seconds()) // 60
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, rounded half-up to 2 decimals (exact Decimal arithmetic)."""
    seconds = int((punch_out - punch_in).total_seconds())
    return float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), ROUND_HALF_UP))


def derive_fields(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime],
                  emp: dict, att_date: date) -> dict:
    """The four API-computed fields for a record in its final state (see DATA_MODEL.md section 3)."""
    if status not in PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = compute_late_minutes(punch_in, att_date, emp["shift_start"])
    if punch_out is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    hours = compute_work_hours(punch_in, punch_out)
    return {
        "work_hours": hours,
        "late_minutes": late,
        "overtime_minutes": compute_overtime(punch_out, att_date, emp["shift_start"], emp["shift_end"]),
        "half_day": Decimal(str(hours)) < HALF_DAY_BELOW,
    }


EpochMs = Annotated[int, Field(strict=True, ge=MIN_MS, le=MAX_MS)]


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field("09:30", pattern=HHMM)
    shift_end: str = Field("18:30", pattern=HHMM)
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _valid_date(cls, v: str) -> str:
        if not DATE_RE.fullmatch(v):
            raise ValueError("joined_on must be YYYY-MM-DD")
        date.fromisoformat(v)
        return v

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start", "09:30"),
        "shift_end": doc.get("shift_end", "18:30"),
        "joined_on": doc.get("joined_on"),
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def _history_value(v: Any) -> Any:
    return dt_to_ms(v) if isinstance(v, datetime) else v


def history_out(entry: dict) -> dict:
    return {
        "at": dt_to_ms(entry.get("at")),
        "by": entry.get("by"),
        "reason": entry.get("reason"),
        "changes": {
            k: {"from": _history_value(c.get("from")), "to": _history_value(c.get("to"))}
            for k, c in (entry.get("changes") or {}).items()
        },
    }


def record_out(doc: dict) -> dict:
    """Stored attendance document -> API shape. Tolerates legacy documents (missing fields)."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": [history_out(h) for h in (doc.get("history") or [])],
    }


def get_employee(emp_code: str) -> dict:
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "MongoDB is not reachable")
    ensure_indexes()
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        db.employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1, le=10**9),
    page_size: int = Query(20, ge=1, le=100),
):
    q = {} if department is None else {"department": department}
    total = db.employees.count_documents(q)
    cursor = db.employees.find(q, {"_id": 0}).sort("emp_code", 1).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    att_date = attendance_date(ts, emp["shift_start"], emp["shift_end"])
    doc = {
        "emp_code": body.emp_code,
        "date": att_date.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        "history": [],
        **derive_fields(body.status, ts, None, emp, att_date),
    }
    try:
        db.attendance_logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}}, sort=[("punch_in", -1)]
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    if ts <= rec["punch_in"] or (ts - rec["punch_in"]).total_seconds() > MAX_SHIFT_SECONDS:
        raise unprocessable("punched_at must be after punch-in and within 24 hours of it", "body", "punched_at")
    derived = derive_fields(rec["status"], rec["punch_in"], ts, emp, date.fromisoformat(rec["date"]))
    update = {"punch_out": ts, **{k: derived[k] for k in ("work_hours", "overtime_minutes", "half_day")}}
    res = db.attendance_logs.update_one({"_id": rec["_id"], "punch_out": None}, {"$set": update})
    if res.modified_count != 1:
        raise HTTPException(409, "already punched out")
    rec.update(update)
    return record_out(rec)


def _attendance_query(emp_code, date_from, date_to, status) -> dict:
    q: dict = {}
    if emp_code is not None:
        q["emp_code"] = emp_code
    if date_from is not None or date_to is not None:
        q["date"] = {}
        if date_from is not None:
            q["date"]["$gte"] = date_from
        if date_to is not None:
            q["date"]["$lte"] = date_to
    if status is not None:
        q["status"] = status
    return q


def _checked_attendance_filters(emp_code, date_from, date_to, status) -> dict:
    d1 = parse_date(date_from, "query", "date_from") if date_from is not None else None
    d2 = parse_date(date_to, "query", "date_to") if date_to is not None else None
    if d1 and d2 and d1 > d2:
        raise unprocessable("date_from must not be after date_to", "query", "date_from")
    return _attendance_query(emp_code, d1.isoformat() if d1 else None, d2.isoformat() if d2 else None, status)


ATTENDANCE_SORT = [("date", -1), ("emp_code", 1)]


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1, le=10**9),
    page_size: int = Query(20, ge=1, le=100),
):
    q = _checked_attendance_filters(emp_code, date_from, date_to, status)
    total = db.attendance_logs.count_documents(q)
    cursor = (
        db.attendance_logs.find(q).sort(ATTENDANCE_SORT).skip((page - 1) * page_size).limit(page_size)
    )
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


def plan_regularization(rec: dict, emp: dict, body: RegularizeIn) -> tuple[dict, dict]:
    """Validate a correction against the stored record and work out the new state.
    Returns (fields to $set, changes for the history entry). Raises 422 on any rule violation."""
    status = body.status or rec["status"]
    in_given, out_given = body.punch_in is not None, body.punch_out is not None
    if status in NON_PRESENCE:
        if in_given or out_given:
            raise unprocessable("ABSENT/LEAVE clear the punch times; do not supply punch_in/punch_out", "body")
        new_in = new_out = None
    else:
        new_in = ms_to_dt(body.punch_in) if in_given else rec.get("punch_in")
        new_out = ms_to_dt(body.punch_out) if out_given else rec.get("punch_out")
        if new_in is None:
            raise unprocessable("a presence status needs a punch_in", "body", "punch_in")
        if in_given and attendance_date(new_in, emp["shift_start"], emp["shift_end"]).isoformat() != rec["date"]:
            raise unprocessable("punch_in must stay on the record's attendance date", "body", "punch_in")
        if new_out is not None and (new_out <= new_in or (new_out - new_in).total_seconds() > MAX_SHIFT_SECONDS):
            raise unprocessable("punch_out must be after punch_in and within 24 hours", "body", "punch_out")
    new = {
        "status": status,
        "punch_in": new_in,
        "punch_out": new_out,
        **derive_fields(status, new_in, new_out, emp, date.fromisoformat(rec["date"])),
    }
    defaults = {"late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    changes = {}
    for key, value in new.items():
        old = rec.get(key, defaults.get(key))
        if old is None and key in defaults:
            old = defaults[key]
        if old != value:
            changes[key] = {"from": old, "to": value}
    if not changes:
        raise unprocessable("the request changes nothing", "body")
    return new, changes


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):
    day = parse_date(date, "path", "date").isoformat()
    emp = get_employee(emp_code)
    for _ in range(MAX_PATCH_RETRIES):
        rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": day})
        if rec is None:
            raise HTTPException(404, "no attendance record for that date")
        new, changes = plan_regularization(rec, emp, body)
        entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
        history = rec.get("history") or []
        guard = {
            "_id": rec["_id"],
            "status": rec["status"],
            "punch_in": rec.get("punch_in"),
            "punch_out": rec.get("punch_out"),
            "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(history)]},
        }
        res = db.attendance_logs.update_one(guard, {"$set": new, "$push": {"history": entry}})
        if res.modified_count == 1:
            rec.update(new)
            rec["history"] = history + [entry]
            return record_out(rec)
    raise HTTPException(409, "record is being modified concurrently, please retry")


def _present_weight() -> dict:
    """Aggregation expression: 1 for a present record, 0.5 for a present half day, 0 otherwise (R5, R6)."""
    return {"$cond": [
        {"$in": ["$status", PRESENCE]},
        {"$cond": [{"$eq": [{"$ifNull": ["$half_day", False]}, True]}, 0.5, 1]},
        0,
    ]}


def _is_weekday() -> dict:
    """Aggregation expression: is the record's `date` string a Monday-Friday?"""
    return {"$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": "$date"}}}, 5]}


def _late_minutes() -> dict:
    return {"$ifNull": ["$late_minutes", 0]}


def _is_late() -> dict:
    return {"$cond": [{"$gt": [_late_minutes(), 0]}, 1, 0]}


def _month_match(start: date, end: date) -> dict:
    return {"$gte": start.isoformat(), "$lte": end.isoformat()}


def employee_monthly_pipeline(emp_code: str, start: date, end: date) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": _month_match(start, end)}},
        {"$group": {
            "_id": None,
            "present_days": {"$sum": {"$cond": [_is_weekday(), _present_weight(), 0]}},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": _is_late()},
            "total_late_minutes": {"$sum": _late_minutes()},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        }},
    ]


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(..., pattern=MONTH_RE)):
    start, end = month_bounds(month)
    emp = get_employee(emp_code)
    row = next(db.attendance_logs.aggregate(employee_monthly_pipeline(emp_code, start, end)), {})
    first = max(start, date.fromisoformat(emp["joined_on"]))
    working_days = count_weekdays(first, end)
    present = row.get("present_days", 0)
    pct = None if working_days == 0 else round_half_up(Decimal(str(present)) / working_days * 100, 2)
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": round_half_up(present, 2),
        "leave_days": int(row.get("leave_days", 0)),
        "late_count": int(row.get("late_count", 0)),
        "total_late_minutes": int(row.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(row.get("total_overtime_minutes", 0)),
        "attendance_pct": pct,
    }


def department_summary_pipeline(start: date, end: date, department: Optional[str]) -> list:
    """Starts from `employees` (so people without any log still count, R9) and looks each one's
    month of logs up through the (emp_code, date) index. One row per employee, then one per department."""
    emp_match: dict = {"joined_on": {"$lte": end.isoformat()}}
    if department is not None:
        emp_match["department"] = department
    has_hours = {"$and": [{"$in": ["$status", PRESENCE]}, {"$isNumber": "$work_hours"}]}
    return [
        {"$match": emp_match},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": _month_match(start, end)}},
                {"$group": {
                    "_id": None,
                    "present_days": {"$sum": {"$cond": [_is_weekday(), _present_weight(), 0]}},
                    "hours_sum": {"$sum": {"$cond": [has_hours, "$work_hours", 0]}},
                    "hours_cnt": {"$sum": {"$cond": [has_hours, 1, 0]}},
                    "late_count": {"$sum": _is_late()},
                    "late_minutes": {"$sum": _late_minutes()},
                    "leave_count": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                    "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}},
                }},
            ],
            "as": "s",
        }},
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            **{
                f: {"$sum": {"$ifNull": [{"$arrayElemAt": [f"$s.{f}", 0]}, 0]}}
                for f in ("present_days", "hours_sum", "hours_cnt", "late_count", "late_minutes",
                          "leave_count", "on_duty_count")
            },
        }},
        {"$sort": {"_id": 1}},
    ]


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(..., pattern=MONTH_RE), department: Optional[str] = None):
    start, end = month_bounds(month)
    items = []
    for r in db.employees.aggregate(department_summary_pipeline(start, end, department)):
        cnt = int(r["hours_cnt"])
        avg = None
        if cnt:
            total = Decimal(repr(float(r["hours_sum"]))).quantize(Decimal("0.000001"), ROUND_HALF_UP)
            avg = round_half_up(total / cnt, 2)
        items.append({
            "department": r["_id"],
            "headcount": int(r["headcount"]),
            "present_days": round_half_up(r["present_days"], 2),
            "avg_work_hours": avg,
            "late_count": int(r["late_count"]),
            "total_late_minutes": int(r["late_minutes"]),
            "leave_count": int(r["leave_count"]),
            "on_duty_count": int(r["on_duty_count"]),
        })
    return {"month": month, "items": items}


def late_leaderboard_pipeline(start: date, end: date, limit: int, department: Optional[str]) -> list:
    pipeline: list = [
        {"$match": {"date": _month_match(start, end), "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})
    pipeline += [
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {
            "_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name", "department": "$emp.department",
            "total_late_minutes": 1, "late_count": 1,
        }},
    ]
    return pipeline


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=MONTH_RE),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    start, end = month_bounds(month)
    rows = db.attendance_logs.aggregate(late_leaderboard_pipeline(start, end, limit, department))
    items = [
        {
            "rank": int(r["rank"]), "emp_code": r["emp_code"], "name": r["name"], "department": r["department"],
            "total_late_minutes": int(r["total_late_minutes"]), "late_count": int(r["late_count"]),
        }
        for r in rows
    ]
    return {"month": month, "items": items}


def department_trend_pipeline(department: str, d_from: date, d_to: date) -> list:
    """One output row per calendar day. The day grid is generated inside MongoDB with $range, so days
    without any record still appear; the 7-day moving average is a $setWindowFields window.
    Starts from `employees` (index: department) which also gives the per-day headcount."""
    days = (d_to - d_from).days + 1
    start_dt = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    day_date = {"$add": [start_dt, {"$multiply": ["$days", 86_400_000]}]}
    window = {"documents": [-6, 0]}
    return [
        {"$match": {"department": department}},
        {"$group": {"_id": None, "codes": {"$push": "$emp_code"}, "joined": {"$push": "$joined_on"}}},
        {"$project": {"_id": 0, "codes": 1, "joined": 1, "days": {"$range": [0, days]}}},
        {"$unwind": {"path": "$days", "includeArrayIndex": "idx"}},
        {"$addFields": {
            "ds": {"$dateToString": {"format": "%Y-%m-%d", "date": day_date}},
            "dow": {"$isoDayOfWeek": day_date},
        }},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "ds",
            "foreignField": "date",
            "let": {"codes": "$codes"},
            "pipeline": [
                {"$match": {"$expr": {"$in": ["$emp_code", "$$codes"]}}},
                {"$group": {"_id": None, "present": {"$sum": _present_weight()}, "late": {"$sum": _is_late()}}},
            ],
            "as": "agg",
        }},
        {"$addFields": {
            "is_working_day": {"$lte": ["$dow", 5]},
            "headcount": {"$size": {"$filter": {"input": "$joined", "cond": {"$lte": ["$$this", "$ds"]}}}},
            "present_count": {"$ifNull": [{"$arrayElemAt": ["$agg.present", 0]}, 0]},
            "late_count": {"$ifNull": [{"$arrayElemAt": ["$agg.late", 0]}, 0]},
        }},
        {"$addFields": {"rate_int": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
            {"$floor": {"$divide": [
                {"$add": [{"$multiply": ["$present_count", 20_000]}, "$headcount"]},
                {"$multiply": [2, "$headcount"]},
            ]}},
            None,
        ]}}},
        {"$project": {"ds": 1, "idx": 1, "is_working_day": 1, "headcount": 1, "present_count": 1,
                      "late_count": 1, "rate_int": 1}},
        {"$setWindowFields": {
            "sortBy": {"idx": 1},
            "output": {
                "win_sum": {"$sum": "$rate_int", "window": window},
                "win_n": {"$sum": {"$cond": [{"$eq": ["$rate_int", None]}, 0, 1]}, "window": window},
            },
        }},
        {"$project": {
            "_id": 0,
            "date": "$ds",
            "is_working_day": 1,
            "headcount": 1,
            "present_count": 1,
            "late_count": 1,
            "attendance_rate": {"$cond": [{"$eq": ["$rate_int", None]}, None, {"$divide": ["$rate_int", 10_000]}]},
            "moving_avg_7d": {"$cond": [
                {"$gt": ["$win_n", 0]},
                {"$divide": [{"$floor": {"$divide": [
                    {"$add": [{"$multiply": ["$win_sum", 2]}, "$win_n"]},
                    {"$multiply": [2, "$win_n"]},
                ]}}, 10_000]},
                None,
            ]},
        }},
        {"$sort": {"date": 1}},
    ]


def _checked_trend_range(from_: str, to: str) -> tuple[date, date]:
    d_from = parse_date(from_, "query", "from")
    d_to = parse_date(to, "query", "to")
    if d_to < d_from:
        raise unprocessable("to must not be before from", "query", "to")
    if (d_to - d_from).days + 1 > MAX_TREND_DAYS:
        raise unprocessable(f"range must not exceed {MAX_TREND_DAYS} days", "query", "to")
    return d_from, d_to


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, from_: str = Query(..., alias="from"), to: str = Query(...)):
    d_from, d_to = _checked_trend_range(from_, to)
    if db.employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(404, "unknown department")
    items = [
        {
            "date": r["date"],
            "is_working_day": bool(r["is_working_day"]),
            "headcount": int(r["headcount"]),
            "present_count": round_half_up(r["present_count"], 2),
            "late_count": int(r["late_count"]),
            "attendance_rate": r["attendance_rate"],
            "moving_avg_7d": r["moving_avg_7d"],
        }
        for r in db.employees.aggregate(department_trend_pipeline(department, d_from, d_to))
    ]
    return {"department": department, "items": items}


def _need(**params) -> None:
    for name, value in params.items():
        if value is None:
            raise unprocessable("field required", "query", name)


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard",
                      "department_trend"],
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=MONTH_RE),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1, le=10**9),
    page_size: int = Query(20, ge=1, le=100),
):
    """Runs explain(executionStats) on the same query / pipeline the real endpoint builds."""
    if endpoint == "attendance_list":
        coll = "attendance_logs"
        q = _checked_attendance_filters(emp_code, date_from, date_to, status)
        command = {"find": coll, "filter": q, "sort": dict(ATTENDANCE_SORT),
                   "skip": (page - 1) * page_size, "limit": page_size}
    else:
        if endpoint == "employee_monthly":
            _need(emp_code=emp_code, month=month)
            coll, pipeline = "attendance_logs", employee_monthly_pipeline(emp_code, *month_bounds(month))
        elif endpoint == "department_summary":
            _need(month=month)
            coll, pipeline = "employees", department_summary_pipeline(*month_bounds(month), department)
        elif endpoint == "late_leaderboard":
            _need(month=month)
            coll, pipeline = "attendance_logs", late_leaderboard_pipeline(*month_bounds(month), limit, department)
        else:
            _need(department=department, **{"from": from_, "to": to})
            coll, pipeline = "employees", department_trend_pipeline(department, *_checked_trend_range(from_, to))
        command = {"aggregate": coll, "pipeline": pipeline, "cursor": {}}
    raw = db.command({"explain": command, "verbosity": "executionStats"})
    return {
        "endpoint": endpoint,
        "collection": coll,
        "explain": json.loads(json_util.dumps(raw, json_options=json_util.RELAXED_JSON_OPTIONS)),
    }