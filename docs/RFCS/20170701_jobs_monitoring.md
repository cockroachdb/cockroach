- Feature Name: Jobs Monitoring
- Status: completed ([#16733](https://github.com/cockroachdb/cockroach/pull/16733))
- Start Date: 2017-06-22
- Author: Julian Gilyadov
- RFC PR: [#16688](https://github.com/cockroachdb/cockroach/pull/16688)
- Cockroach Issue: None

# Motivation

Recent syntax additions such as `SHOW [QUERIES|SESSIONS]` syntax provide a
convenient interface for monitoring queries and sessions, but for a consistent
and symmetric interface it is important to add `SHOW JOBS` statement too,
especially considering the forthcoming `CANCEL [QUERY|JOB]` statement.

Although a `SHOW JOBS` statement is additional SQL syntax with no direct basis
in another SQL dialect, it is better to have this implemented rather than have
an inconsistent interface where it is possible to monitor queries and sessions
but not jobs.

It is also worth noting that at least one current customer has requested the
ability to query job status from SQL directly.

# Design

For consistency, it is best to mimic the `SHOW [QUERIES|SESSIONS]` syntax
([RFC](20170505_monitoring_queries_and_sessions.md)):

```sql
SHOW JOBS
```

Since all jobs are cluster jobs, there's no need to specify `LOCAL` or `CLUSTER`
(like in the RFC above). Users who want more fine-grained queries can use `SHOW
JOBS` as a data source in a `FROM` clause, thanks to [#15590]:

```sql
SELECT fraction_completed FROM [SHOW JOBS] WHERE type = 'RESTORE';
```

Though the [`system.jobs`](20170215_system_jobs.md) table is the source for
`SHOW JOBS` and can be queried by users (`SELECT * FROM system.jobs`),
`system.jobs` cannot be used for monitoring. Users cannot retrieve any of
several important fields stored in the `payload` protobuf, like 
`fraction_completed` and `username`, from SQL directly.

The `crdb_internal.jobs` table wraps the `system.jobs` table and exposes the
important information by decoding the `payload` protobuf. The columns returned
from `crdb_internal.jobs` are exactly the columns we intend intend to return
from `SHOW JOBS`. (In v1.0, we weren't ready to make `crdb_internal.jobs` part
of the public API.) Since we have two months before v1.1 is considered
stable, we feel comfortable making `SHOW JOBS` part of the public API.

For reference, the columns are:

* `id` — the ID of the job
* `type` — the type of the job (currently BACKUP or RESTORE)
* `description` — the query used to create the job
* `username` — the user who created the job
* `descriptor_ids` — the IDs of the tables impacted by the job
* `status` — pending, running, succeeded, failed (aborted soon)
* `created, started, finished` — the time of state transitions
* `modified` — the last time this row was updated
* `fraction_completed` — the approximate progress of this job
* `error` — the error that caused the job to fail, if it failed

# Implementation

The implementation of `SHOW JOBS` could essentially copy/paste what the internal
`crdb_internal.jobs` table does, with the intention of removing
`crdb_internal.jobs`; however, by keeping the virtual table, we can take
advantage of a forthcoming optimization to push filtering down to the virtual
table constructors, which avoids populating all jobs in memory if the user
specifies a `WHERE` clause.

Because the internal `crdb_internal.jobs` table is here to stay, the
implementation of `SHOW JOBS` should be simply parsing the internal jobs table,
similarly how `SHOW DATABASES` is implemented:

```go
// ShowJobs returns all jobs, past and present, on the cluster.
// Privileges: None.
func (p *planner) ShowJobs(ctx context.Context, n *parser.ShowJobs) (planNode, error) {
  const getJobs = `TABLE crdb_internal.jobs`
  stmt, err := parser.ParseOne(getJobs)
  if err != nil {
    return nil, err
  }
  return p.newPlan(ctx, stmt, nil)
}
```

Tests for `SHOW JOBS` should be added in `jobs_test.go` by wrapping the contents
of the `verifyJobRecord` in a function which takes a `source` string. The
function is simply called twice, once with `crdb_internal.jobs` as `source` and
once with `[SHOW JOBS]` as `source`.

``` go
func verifyJobRecord(...) error {
  testSource := func(source string) error {
    var typ string
    var created pq.NullTime
    // Additional fields...
    db.QueryRow(fmt.Sprintf(
      `SELECT type, created, ...
      FROM %s WHERE created >= $1 ORDER BY created LIMIT 1`, source),
      expected.Before,
    ).Scan(
      &typ, &created
    )
    // Verification...
  }
  if err := testSource(`crdb_internal.jobs`); err != nil {
    return err
  }
  return testSource(`[SHOW JOBS]`)
}
```

*NB: The implementation of this RFC landed before the RFC itself in [#16733].*

# Drawbacks

`SHOW JOBS` is additional SQL syntax with no direct basis in another SQL
dialect.

# Alternatives

We could avoid introducing a new syntax for jobs monitoring and simply document
the `crdb_internal.jobs` table as the way to track the status of these
long-running backup, restore, and schema change jobs. The consensus during the
comment period, however, was that we'll be ready to commit to `SHOW JOBS` as a
public API for the v1.1 release.

# Unresolved questions

None.

[#15590]: https://github.com/cockroachdb/cockroach/pull/15590
[#16733]: https://github.com/cockroachdb/cockroach/pull/16733
