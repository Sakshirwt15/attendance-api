# DECISIONS.md

1. **Indexes.**
   - `attendance_logs`:
     - unique `(emp_code, date)`: gives one record per day, a race-safe punch-in, and fast PATCH and monthly lookups.
     - `(date desc, emp_code)`: for the sorted list, leaderboard and trend.
     - `(status, date desc, emp_code)`: for the status filter.
     - `(emp_code, punch_in desc)`: to find the open record at punch-out.
   - `employees`:
     - unique `emp_code`
     - `(department, emp_code)`
     - `(joined_on)`
   - I rejected copying `department` into every log. It would save a `$lookup`, but the data would become wrong when someone changes department.

2. **Punch-in race.** Both requests call `insert_one`. The unique index lets one insert succeed, and the other raises `DuplicateKeyError`, which I return as 409. I do not check anything before the insert.

3. **Ties.** `$rank` gives ranks like 1, 2, 2, 4, and `limit` is applied after ranking. If there is a tie at the cutoff, all tied employees are returned, so there can be more than `limit` rows. They are ordered by total late minutes, then `emp_code`.

4. **Headcount.** The summary starts from `employees` (joined on or before month end) and looks up each person's logs. A person with no logs gets an empty array but is still one row, so the department `$group` still counts them.

5. **If the data becomes 100x bigger.** I would keep a per-employee, per-month summary (rollup) that is updated on punch-out and PATCH, instead of recalculating from raw logs every time. I would also use keyset pagination instead of `skip`.