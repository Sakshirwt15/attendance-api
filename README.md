# Employee Attendance & Analytics API

A small attendance API built with FastAPI and MongoDB. All the code is in `app/main.py`.

## How to run
```bash
python -m venv .venv
source .venv/bin/activate        # on Windows: .venv\Scripts\activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017"   # or your Atlas URI
export MONGO_DB="attendance_db"
uvicorn app.main:app --port 8000
```
On Windows PowerShell, set the variables with `$env:MONGO_URI="..."` instead of `export`.

`MONGO_URI` and `MONGO_DB` are read from environment variables. Indexes are created automatically when the app starts. MongoDB 6.0 or newer is needed.

## Notes
- Review findings (Part A) are in `REVIEW.md` and the answers for Part C are in `DECISIONS.md`.
- Times are stored in MongoDB as UTC dates and sent in the API as epoch milliseconds. Dates, months and shift times are IST strings.
- The four analytics endpoints (monthly, department summary, late leaderboard, department trend) are each one MongoDB aggregation pipeline.
- `GET /admin/explain/{endpoint}` uses the same query-building functions as the real endpoints.
- Logs of an employee that does not exist are ignored in analytics that need employee data.
- Logs dated before an employee's `joined_on` are still counted in that employee's monthly totals. Working days start from `joined_on`.

## Testing
I tested it against a local MongoDB with my own sample data: edge cases (employee with no logs, mid-month joiner, overnight shift, weekend records, ties, half days), explain plans, parallel requests, and 100,000 records for speed.

## Missing / limitations
Nothing known is missing.