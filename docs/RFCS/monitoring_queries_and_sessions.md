- Feature Name: Monitoring Queries and Sessions
- Status: in-progress
- Start Date: 2017-05-04
- Authors: Bilal Akhtar
- RFC PR: https://github.com/cockroachdb/cockroach/pull/15761
- Cockroach Issue: [#7003](https://github.com/cockroachdb/cockroach/issues/7003)

# Summary

This feature would add a mechanism to list currently active sessions and currently executing queries
in the admin UI, along with their start timestamps  / durations

# Motivation

Currently, there's no visibility into what queries/sessions are running on the cluster at any
given point of time. Adding visibility into this has been a common customer request for
a while. This functionality would also be useful for determining which queries and sessions
to cancel or terminate; however, the cancellation mechanism is out of scope for this RFC.

# Detailed design

The inspiration behind some of the design comes from
[this PR from Raphael](https://github.com/cockroachdb/cockroach/pull/10317) which implemented
node-local session registries exposed via a virtual table.

## New unique UUID identifiers for sessions and queries

Currently we don't have any unique identifiers for sessions or queries. Having UUID strings
for this purpose should guarantee uniqueness. Later on, when cancellation is implemented,
the user would only have to specify the gateway node ID, and the session/query ID in the RPC call
for cancelling that query or session.

These two IDs (gateway node ID, session/query ID) can be combined in one with a delimeter
separating them, or they can be presented individually.

Sessions are currently identified in logs with log tags that consist of remote IP address
and port. Replacing these with session UUIDs can be considered, but since the remote IP/port
combination has a more direct meaning to the DBA, it is best to leave it as-is for now.

## Node-local session and query registries

The session registry will be an in-memory data structure that stores a copy of a subset
of fields of every session on that node; particularly, only those fields that
need to be exposed through virtual tables/RPC. Addition and deletion of new
sessions to this slice would require acquiring a mutex lock, but none of these
fields should ever be updated during the lifetime of a session - so session-specific
locks are unnecessary.

These fields would be stored in the shadow session object:
- Session ID (defined earlier)
- User
- Start timestamp
- Any contexts/object references useful for cancellation

This addition/deletion into the slice can be made in `NewSession(...)` and `Session.Finish(...)`
in `pkg/sql/session.go`.

Similarly, this in-memory slice of query objects will store a copy of these
fields about each query:

- Query ID (defined earlier)
- Query string (output of `planToString()`)
- Session ID (for JOINs with session table)
- Start timestamp
- Any contexts/object references useful for cancellation 

There would be one addition/deletion to this slice per query; handled in `execStmtInOpenTxn` in
`pkg/sql/executor.go`. 

## Virtual tables in crdb_internal

Two new tables, `crdb_internal.sessions` and `crdb_internal.queries`, would list active
sessions and queries respectively. Only sessions and queries from the node where the query
is being run, would be listed - this continues our convention of only exposing node-local
data through virtual tables.

## Fan-out gRPC call to get cluster-wide sessions/queries

Two new RPC endpoints would be created for sessions: `Sessions` and `LocalSessions`.
The former would iterate over all known nodes and collect the responses together,
while the latter would return the contents of its local session registry.

Similarly, `Queries` and `LocalQueries` would be the RPC endpoints for queries.

# Drawbacks

## Locking/synchronization

Additions, accesses and deletions to session and query registries would require mutex locks.
This has the potential to cause a slight decrease in performance when active sessions and queries
are being requested, but since that action would be present in only a fraction of requests,
the most common use-cases shouldn't see an impact in performance.

To ensure that we don't unintentionally cause a performance regression, the included benchmarks
in `pkg/sql/bench_test.go` will be run before and after the change, and the results posted in the PR
for this feature.

# Alternatives

## Generating identifiers from hashing object addresses

One alternative to a UUID-based identification that was suggested, was hashing object pointers.
For instance, a hash of the session object address could be its identifier. Since cluster IDs
are currently UUIDs, it's better to continue the UUID convention than to introduce pointer
hashes as another one.

Furthermore, it's possible, though unlikely, for a new session/query object to be created at the same
address as a previous one - violating our uniqueness constraint.

Note that, with either approach, both a node ID and a session/query ID would be necessary to
look up that object in the cluster.

# Unresolved questions

## Should we store ApplicationName in session shadow object?

Currently, this proposed solution doesn't include the Session `ApplicationName`  in the
shadow object and virtual table, however it might be useful information for the DBA to identify
sessions.

Adding this field to the registry would involve adding mutex locks on each session shadow object in
that registry - which would be acquired every time that object is read, and every time the
`ApplicationName` is updated in `resetApplicationName()`.

## Populate virtual table with cluster-wide info or let RPC be the only interface for that

Instead of having the virtual table serve only sessions/queries
on that particular node, it could serve cluster-wide sessions and queries.
This would allow the DBA to use SQL syntax for filtering/joining on the result
set from the entire cluster, rather than be limited to the node where the query is being run.

There could be two different tables for each kind of entity (session and queries):
one for local sessions/queries, and one for cluster ones. That way, queries for local
sessions/queries could run faster since that vtable population wouldn't involve
communicating with other nodes.
