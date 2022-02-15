- Feature Name: Graceful Draining
- Status: Draft
- Start Date: 2021-11-01
- Authors: Jane Xing
- RFC PR: https://github.com/cockroachdb/cockroach/pull/72991
- Cockroach Issue: [66319](https://github.com/cockroachdb/cockroach/issues/66319)

# Summary
This RFC proposes to optimize the draining process to be more legible for
customers, and introduce a new step, `connection_wait`, to the draining
process, which allows the server to early exit when all SQL connections are
closed.

# Motivation
Customers encountered intermittent blips and failed requests when they were
performing operations that are related to restarting nodes, such as rolling
updates. This is because CockroachDB’s current draining mode is
incompatible with how many 3rd connection pools work against PostgreSQL.
This issue is prominent for customers relying on Cockroach Cloud, which does
minor upgrades with 7 days between CockroachDB releases. Customers are also
looking for less downtime when going through these upgrades.

Specifically, to close a server, in PostgreSQL’s case, customers are given
the option to use the [“Smart Shutdown” mode][postgres_shutdown],
in which the server disallows new connections and waits until all sessions
are closed by clients to shut down. While in CockroachDB’s case, the
draining process consists of two consecutive periods, `drain_wait` and
`query_wait`. Their duration is configured via two
[cluster settings][cockroach cluster settings],`server.shutdown.drain_wait`
and `server.shutdown.query_wait`, with the former “controlling the amount of
time a server waits in an unready state before proceeding with the rest of the
shutdown process”, and the latter specifying “the amount of time a server waits
for existent queries to finish”. CockroachDB expects all SQL connections to be
closed by clients before the end of `drain_wait`. After `drain_wait` ends and
`query_wait` starts, the server shuts down all SQL connections that are not with
inflight queries (such as open transaction sessions). After the end of
`query_wait`, all SQL connections will be forced to shut down, regardless of
active SQL queries.

As part of CockroachDB’s draining process, the clients are not notified of
the server’s current state (i.e. which step in the draining algorithm it is
undergoing) and when their SQL connections are going to be closed.
That is to say, if they are in the middle of using a connection when the
server is shutting down, they may receive confusing error messages such as
`io timeout` or `ERROR: server is shutting down`. More specific use cases
will be discussed in the Background section.

The following shows the timeline for the current draining process of CRDB,
and the workaround setting for `drain_wait`.

![Current Draining Process][current draining pic]
The current workaround that CockroachDB provides is to set the cluster setting
`server.shutdown.drain_wait` longer than the sum of the maximum lifetime of a
connection in the connection pool (`MaxLifeTime`), the time for the load
balancer to stop routing new SQL connections, and the max transaction latency.
This setting will let connections reach `MaxLifeTime` and be recycled by
the connection pool before the node is being shut down (drained). Active
SQL connections will stay alive until the inflight query completes. The
connection pool would then recycle the connection and open a new connection,
from a different node.

The downsides of this workaround are:

  1. The setting `server.shutdown.drain_wait` has to be adjusted based on
     clients’ configuration of the connection pooling and load balancer.
  2. The server is doing a hard wait till the end of `server.shutdown.drain_wait`.
     So though setting a long `drain_wait` can help ensure all SQL connections
     are closed before the (true) draining starts, it brings a long downtime
     for this node.

To solve these issues, this design proposes to optimize the waiting stages
during the draining process, and implement an early exit when all
SQL connections are closed by the client.

Note that our ultimate goal is to provide clear guidance to avoid having
similar problems again, but since they are encountered by a small number of
customers, we don’t want to change the default draining workflow too much,
and bring the risk of a longer draining process. Instead, we're aiming at giving
the user more configuration options.

# Background
This section will focus on CockroachDB’s current draining process.
Here are some facts about the current draining process:

1. From the start of `drain_wait`, the health checkpoint /health?ready=1 returns
   ``` shell
   {
   "error": "node is shutting down",
   "code": 14,
   "message": "node is shutting down",
   "details": [
   ]
   }%
   ```
   but new SQL connections are **always** allowed to be established during the
   whole span of `drain_wait`. This is to give a grace period for the load
   balancer to detect that this node has started draining, and then
   stop routing new SQL connections to this node.

   But if during `drain_wait`, the user tries to connect to this node without a
   load balancer, they still can. (And this means the
   `s.sqlServer.acceptingClients.Set(false)` function call before `drain_wait`
   is confusing. We propose to rename this function to
   `s.sqlServer.isHealthcheckReady`.)

2. New SQL connections and queries are not allowed from the start of
   `query_wait`.

3. The draining process is reported periodically in `cockroach-data/logs/cockroach.log`,
   but it does not shows the draining phases (e.g. `query_wait`).

4. If all SQL connections are idle or there are no SQL connections during
   `drain_wait`, the server still does a hard wait till the end of `drain_wait`.

5. If all SQL connections become query-free at a timestamp within `query_wait`,
   the server performs an early exit.

6. If a user acquires a connection before `query_wait` starts, but sends the
   server a query on this connection after `query_wait` starts, this user will
   receive `ERROR: server is shutting down`.

7. Throughout the whole draining process, HTTP connections are still allowed.
   The restrictions are only for *SQL* connection.

In summary, the features of the two periods in the current draining process,
`drain_wait` and `query_wait`, are as below:
![Summary of features for drain_wait and query_wait][summary pic]
To better illustrate the behavior of the current draining process, and the
problem it brings along, we made an experiment with the [HirakiCP][HirakiCP]
connection pool is built [here][demo code],
and a demo recording can be found [here][demo recording current draining].

# Technical Design

This design proposes to optimize the draining process by adding a new
waiting period between `drain_wait` and `query_wait` called `connection_wait`,
during which the server waits for clients to close SQL connections.
New SQL connections are not allowed, and any SQL connections that are not pulled
by the user and expire the `MaxLifeTime` will be closed by the connection pool.
i.e. **no more client queries will be allowed to this draining node within this
period, and the connection pools are expected to recycle idle SQL connections.**

If at a certain timestamp all SQL connections are closed, the server performs an
early exit and proceeds to the lease transfer stage. Note that if a connection
is pulled by a user and waiting for query input, this connection is not
considered idle, and will not be recycled until the user puts it back in the
pool. The timeout is finite. Clients can notice from the log that
the existing SQL connections that are not executing queries will be closed.
The following is the proposed draining process with optimized waiting stages,
and early exit.

![Proposed draining process][proposed draining pic]

Most of the changes should be made in the server and pgwire package.
Similar to existent `drain_wait` and `query_wait`, the new wait period will be
declared as a `*settings.DurationSetting` as follows:

```go
connectionWait = settings.RegisterDurationSetting(
    settings.TenantReadOnly,
    "server.shutdown.connection_wait",
    "the amount of time a server waits for clients to close existing SQL
    connections. After the start of connection_wait, no new SQL connections are
    allowed. If all SQL connections are closed by the client, the server will
    continue with the rest of the shutdown process before the end of
    connection_wait.”,
    0*time.Second,
).WithPublic()

```

The default duration length of `connection_wait` is set to be 0 seconds.
This is because the draining error mentioned above only happened to a small
number of users, and we don’t want this workaround to implicitly prolong the
existing draining process for all customers. We leave the users or operators to
better configure the duration of `connection_wait` if they hit an error during
draining.

The main functionalities of `connection_wait` will be implemented with a
function
`func (s *Server) DrainConnections(ctx context.Context, connectionWait time.Duration`)
at `/pkg/sql/pgwire/server.go`.

It mainly consists of three parts:
- **Disallow new SQL connections**:
  we move the calling of `func (s *Server) setDrainingLocked` from `func (s
  *Server) drainImpl`, which is used during `query_wait`, to the
  `connection_wait`. `setDrainingLocked` modifies the server attribute
  `s.mu.draining`, which is checked when the server registers a new connection
  at `func (s *Server) ServeConn` in `pkg/sql/pgwire/server.go`.
- **Early exit if all SQL connections are closed**:
  we let the server keep listening to the registered SQL connections
  (`s.mu.connCancelMap`).
  If the length of this list reaches 0, the function returns.
- **Optimize the Reporting Systems**:
  To provide better observability for an operator in case the draining takes a
  longer-than-normal time, we propose to enhance three reporting systems:
  - **cockroach-data/logs/cockroach.log**:
    - At the start of each draining phase, print the current timestamp,
      draining phase name and its duration. E.g. `I220117 05:27:26.716731
      connection_wait starts, no new SQL connections are allowed from now,
      connections will all be closed within 1m20s`.
    - From the start of the whole draining process, print the number of alive
      SQL connections and in-flight SQL queries.
    This will also help customers reconfigure the length of each draining phase
    once they receive a related error message.
  - **stdout/stderr**: This is for customers / operators that drain a node
    interactively (e.g. through pressing `ctrl-c` to interrupt a
    `cockroach start` process in the cli). At the start of each draining phase,
    the server print the current timestamp, draining phase name and its
    duration in the stdout/stderr.
  - **`cockroach node status`**: When this command is called during a draining
    process, the output shows a `draining_status` field that contains the
    current draining phase and its duration setting, along with the number of
    currently active SQL connections and queries. This is for the case where
    the operator doesn't have access to the logs, but they want to query the
    draining progress through network. Note that both the HTTP API and RPC API
    for `cockroach node status` will be modified.

For an infinite `connection_wait`, there are concerns that "it may introduce
an upgrade failure mode where SQL connections aren't closed by the customer,
eventually leading the upgrade to fail (as we need some timeout); this will
frustrate the customer and maybe, even more, it will frustrate CRL eng
including SRE who own patching fleet (instead of the customer)."
Therefore, we propose to pause allowing an infinite
`connection_wait` until we confirm that customers have to apply this feature.

In summary, the desired final state of this design is to have the following
timeline for the draining:
- `drain_wait` (unchanged): `health?ready=1` starts to return the draining
  state of the node, and load balancer listening to this http for health check
  stops routing new SQL connections to this draining node. But new SQL
  connections from other channels are still allowed, and existing SQL
  connections still work.
- `connection_wait`: No new SQL connections are allowed. Existing SQL
  connections still work, but those without queries in-flight will be given a
  timeout to be recycled by connection pool or closed by user. Idle SQL
  connections whose lifetime is longer than the connection pool’s maximum
  lifetime will be recycled.
  SQL connections not returned to the pool won’t be recycled. If all SQL
  connections are closed, it does an early exit before the timeout, and proceed
  to the lease transfer stage.
- `query_wait` (unchanged): Any SQL connections without open queries in-flight
  will be closed. Any SQL connection, once its query is finished, will be
  closed. If all SQL connections are closed, the server does an early exit
  before the timeout, and proceed to the lease transfer stage. Otherwise, the
  server waits till `query_wait` times out, and then force into the
  the lease transfer phase.
  In this case, all on-going queries will be left uncommitted.
- lease transfer stage (unchanged): The server keeps retrying until all range
  leases and raft leaderships on this draining node have been tranfered.
  **In each iteration**, the kvserver transfers the leases with timeout
  specified by `server.shutdown.lease_transfer_wait`.
  (Which means the total duration of the lease transfer stage is not totally
  determined by the value of `server.shutdown.lease_transfer_wait"`.
  We [propose][Lease Transfer Issue] to rename this cluster setting with
  `server.shutdown.lease_transfer_timeout"`, and document it seperately with
  the other three waiting periods.)

# Demo with changes from Technical Design
[A 4-min demo recording][improvement recording] with the same Hikari connection
pool setting can be found here, where we did manual testing.

To add an automated test, we will refer to `pkg/sql/conn_executor_test.go`
and `pkg/sql/pgwire/pgwire_test.go` to set up a connection pool, and apply
a timeline similar to the one in the demo.

Note that in the demo we only manually tested with a single-node server. A
multi-node server with a load balancer involved should also be considered.
The `drain_wait` should still be set longer than the time that the load
balance needs to stop routing new SQL connections for this draining node, and
users just need to set the length of `connection_wait` longer than the
maximum lifetime of the connection pool.

Specifically, `drain_wait` should be longer than the `check inter` in the
[http health check of HAProxy][HAProxy health check],
and the `periodSeconds` in [k8s’s readinessProbe setting][k8s health check].

# Possible Stretch
We may also want to enable the server to only allow new SQL connections of
certain types during `drain_wait`, or even allow admins to login
during `connection_wait`. This is because some customers mentioned that they’d
like to reserve access to the node when the draining process started.
The possible solution is to add a gate in the connection register that
looks at a connection’s metadata (e.g. if it’s from admin or not) during
`drain_wait` and `connection_wait`.


# Survey: configuration for C-Pool and Load Balancer
We made [a survey][survey link] (note: this is an internal doc)
to collect information about uses’ setting for connection pool and load balancer.
This would help us provide better guidance if a user encounters similar issues
during draining.

# Edge Cases
- We leave the user to set the exact duration of each wait period because
  they should be configured based on their load balancer, connection
  pooling, and inflight query settings. Users are expected to be clear
  about how to customize the process based on their needs. If wrongly
  configured, the draining process can lead to existing issues, such as
  unexpected disconnection or uncommitted queries.

# Questions not fully answered
- How does this work in a multi-tenant deployment?
  - [Issue 74412][Issue 74412] is about supporting the drain protocol in a
    multi-tenant server started with `mt-start-sql`, with which this design will
    be compatible with the multi-tenant scenario. The draining process with
    multi-tenancy should not differ much from servers started via `start` or
    `start-single-node`, since in the
    [multi-tenant architecture][multi-tenant architecture], each tenant has
    independent SQL layer.
- How does the change behave in mixed-version deployments? During a version
  upgrade? Which migrations are needed?
  - If certain nodes in the cluster are not updated to support this feature,
    it may lead to confusion. We may need a version gate for
    `server.shutdown.connection_wait`.
- Is the result/usage of this change different for CC end-users than for
  on-prem deployments? How?
  - The new functionality in this design is expected to be used in multitenent
    SQL proxy.
- Does the change impact performance? How?
  - It will likely increase the draining process’s duration since we now
  wait for the users to close SQL connections. But given an early exit
  functionality, the wait can be less painful.
- How is resource usage affected for “large” loads?
  - Users are very more likely to wait for a long time since the server
    needs to wait for all SQL connections to be stopped or returned to the pool
    after queries are finished. A possible workaround is to set a smaller
    connection pool size, and also avoid running long queries.
- Can the new functionality be disabled? Can a user opt out? How?
  - Set `connection_wait` to 0 seconds, then it should be identical to the
    current draining process.
    But users will still see the newly added logs and will have access to
    the newly added endpoints for the draining status.
- Can the new functionality affect clusters which are not explicitly using it?
  - It can prolong the draining process by a default timeout set to the
    `connection_wait`.
- Does the change concern authentication or authorization logic? If so,
  mention this explicitly tag the relevant security-minded reviewer as
  reviewer to the RFC.
  - So far no, since we are following the logic of existing `drain_wait` and
    `query_wait`.
- Does the change create a new way to communicate data over the network?
  What rules are in place to ensure that this cannot be used by a malicious
  user to extract confidential data?
  - No, it does not create a new communication way.
- Is there telemetry or crash reporting? What mechanisms are used to ensure
  no sensitive data is accidentally exposed?
  - Don’t think it is very related.
- Is the change visible to users of CockroachDB or operators who run
  CockroachDB clusters?
  - Yes, they will notice it from the changes of the reporting system.
- Is usage of the new feature observable in telemetry? If so, mention where
  in the code telemetry counters or metrics would be added.
  - No, it won’t affect telemetry.
- How to accomodate the use case of changefeed?
  - Per [Issue 73274][Issue 73274], in the case of changefeed, when the traffic
    is down to zero (i.e. no ongoing queries), the CDC jobs still needs the server
    to be alive for a specific period, until it finally signals that it’s “done”
    and it’s safe to suspend. Such “signal” mechanism is not implemented yet in
    CDC, but once it’s finished we need to leverage it for the design of
    `query_wait`.

# Docs changes
  Since we expect the user to approriately set the length of these three waiting
  period during the draining process, sufficient tutorial is needed in the
  documentations.

  Specifically, we should add the following in
  [Cluster Settings Page][Cluster Setting Page]:
  - The CockroachDB server's draining procedures. i.e. the behavior of the
    system during each waiting period. We can refer to the `final state of this
    design` in this RFC.
  - How to correctly tune the cluster settings
      - `server.shutdown.drain_wait` (the wait for the load balancer to detect
        the draining status of the node) should be in coordination with the load
        balancer's settings, such as the health check interval.
      - `server.shutdown.connection_wait` (the timeout to wait for all SQL
        connections to be closed) should be longer than the maximum life
        time of a connection as specified in the connection pool.
      - `server.shutdown.query_wait` (the timeout to wait all SQL queries to
        finish) should be greater than any of the followings:
        - The longest possible transaction in the workload expected to complete
          successfully under normal operating conditions.
        - The `sql.defaults.idle_in_transaction_session_timeout` cluster setting.
        - The `sql.defaults.statement_timeout` cluster setting.

    Note that the cluster setting `server.shutdown.drain_wait` is of different
    meaning of the `--drain-wait` flag under the `cockroach node drain` command.
    The latter specifies the "_amount of time to wait for the node to drain
    before returning to the client_", and it is usually set with several
    minutes. While the former only determines the duration of a subprocess, and
    it is usally set with couple seconds.

# Drawbacks
- Customer will still receive error messages if their connection pool
  configuration is not compatible with their wait period settings, e.g.
  connection pool’s maximum lifetime > `connection_wait`.

# Rationale and Alternative
The design of a `connection_wait` that allows early exit provides the most
similar functionality as
[Postgres’s smart shutdown service][postgres_shutdown].

Note that in the current design, we only support a finite `connection_wait`
duration, which means that in some cases, setting a long
(`drain_wait` + `connection_wait`) can be
the same as setting a long `drain_wait` without the implementation of
`connection_wait`, and it may not solve all the existing issues.

For example, if we set `drain_wait` to 30s, `connection_wait` to 30s, and the
customer grabs a SQL connection at the 20s from the start of draining, but
inputs the query until 70s. Then the customer will still receive the
“server is shutting down” error. In this case, the situation is not
fundamentally changed, but we hope that with the improved reporting system,
clients or operators will be clearer about the status of the draining
process, and take corresponding actions (in the above example, abort remaining
SQL connections before the `connection_wait` timeout). And also the early exit
in `connection_wait` can help shorten the downtime if the user sets a long
duration.

# Explain it to folk outside of your team
See the motivation part.
# Unsolved Questions
See “questions not fully answered” in the Technical Design section.

[postgres_shutdown]: https://www.postgresql.org/docs/current/server-shutdown.html
[cockroach cluster settings]: https://www.cockroachlabs.com/docs/stable/cluster-settings.html
[HirakiCP]: https://github.com/brettwooldridge/HikariCP
[demo code]: https://github.com/janeCockroachDB/hikari_demo
[demo recording current draining]: https://www.dropbox.com/s/6333e4x6yzwswfu/CurrentDraining.mp4?dl=0
[draft pr]: https://github.com/cockroachdb/cockroach/pull/72991
[current draining pic]: images/graceful_draining_current_draining_process.png?raw=true "Current Draining Process"
[summary pic]: images/graceful_draining_summary_drain_query_wait.png?raw=true "Summary of features for drain_wait and query_wait"
[proposed draining pic]: images/graceful_draining_connection_wait.png?raw=true "Proposed draining process"
[multi-tenant architecture]: https://www.cockroachlabs.com/blog/how-we-built-cockroachdb-serverless/#multi-tenant-architecture
[HAProxy health check]: https://www.haproxy.com/documentation/hapee/latest/load-balancing/health-checking/active-health-checks/
[k8s health check]: https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/
[survey link]: https://docs.google.com/forms/d/1_BWgza8n5iYBwVR7WSrKT4jL8RClE-PMoOmzlUWvnl8/edit?usp=sharing
[Issue 74412]: https://github.com/cockroachdb/cockroach/issues/74412
[Issue 73274]: https://github.com/cockroachdb/cockroach/issues/73274
[Cluster Setting Page]: https://www.cockroachlabs.com/docs/v21.2/cluster-settings.html
[improvement recording]: https://www.dropbox.com/s/j8o81htifdi0nfw/ImprovedDrainingWMp3.mp4?dl=0
[Lease Transfer Issue]: https://github.com/cockroachdb/cockroach/issues/75520
