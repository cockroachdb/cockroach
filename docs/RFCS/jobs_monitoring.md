- Feature Name: Syntax for monitoring jobs
- Status: draft
- Start Date: 2017-06-22
- Authors: Julian Gilyadov
- RFC PR: to be added
- Cockroach Issue: None

# Motivation
Recent syntax additions such as `SHOW [QUERIES|SESSIONS]` syntax provide a convenient interface to monitor queries and sessions, but for a consistent and symmetric interface it is important to add `SHOW JOBS` syntax too, it is even more encouraged due to the more recent syntax additions of `CANCEL [QUERY/JOB]` and such...

Although it adds additional SQL syntax with no direct basis in another SQL dialect, it is better to have this implemented rather than have a more confusing interface where it is possible to monitor queries and sessions but not jobs.

In addition to consistent interface, the current interface to jobs monitoring via a [`system.jobs`](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/system_jobs.md) table has [drawbacks](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/system_jobs.md#drawbacks) which the new syntax `SHOW JOBS` can resolve, one drawback is that users cannot retrieve fields stored in the protobuf from SQL directly which contains fields that may be useful to users such as `fraction_completed` and `creator`.

It is also worthy to note that at least one current customer has requested the ability to query job status from SQL directly.

# Design
For consistency, it is best to follow the `SHOW [LOCAL|CLUSTER] [QUERIES|SESSIONS]` syntax ([RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/monitoring_queries_and_sessions.md)):  
`SHOW [LOCAL|CLUSTER] JOBS`  
Where `CLUSTER` is the default when unspecified.

# Drawbacks
Additional SQL syntax with no direct basis in another SQL dialect.

# Alternatives
Not to introduce a new syntax for jobs monitoring and instead, keep the `system.jobs` table as the way to track the status of these long-running backup, restore, and schema change jobs.  
Perhaps, the need for introducing a special syntax has not arisen yet, although at least one current customer has requested the ability to query job status from SQL directly, basic status information is already available directly through SQL without introducing new syntax.

# Unresolved questions
The future of the `system.jobs` table.  
This RFC discusses if a new syntax for jobs monitoring should be introduced, how it will actually be implemented is out of scope.
