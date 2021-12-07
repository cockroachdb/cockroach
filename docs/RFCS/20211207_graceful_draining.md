- Feature Name: Graceful Draining
- Status: Draft
- Start Date: 2021-11-01
- Authors: Jane Xing
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [66319](https://github.com/cockroachdb/cockroach/issues/66319)

# Summary
This RFC proposes to optimize the draining process to be more legible for 
customers, and introduce a new step, connection_wait, to the draining 
process, which allows the server to early exit when all connections are closed.

# Motivation

Customers encountered intermittent blips and failed requests when they were 
performing operations that are related to restarting nodes, such as rolling 
updates. This is because CockroachDB’s current draining mode is 
incompatible with how many 3rd connection pools work against PostgreSQL.  
This issue is more prominent for customers relying on Cockroach Cloud, 
which does minor upgrades with 7 days between CockroachDB releases. 
Customers are also looking for less downtime when going through these upgrades.

Specifically, to close a server, in PostgreSQL’s case, customers are given 
the option to use the [“Smart Shutdown” mode](https://www.postgresql.org/docs/current/server-shutdown.html), 
in which the server disallows new connections and waits until all sessions 
are closed by clients to shut down.  While in CockroachDB’s case, the 
draining process consists of two consecutive periods, drain_wait and query_wait. 
Their duration is configured via two [cluster settings](https://www.cockroachlabs.com/docs/stable/cluster-settings.html), 
`server.shutdown.drain_wait` and `server.shutdown.query_wait`, with the former 
“controlling the amount of time a server waits in an unready state before 
proceeding with the rest of the shutdown process”, and the latter 
specifying “the amount of time a server waits for existent queries to finish”. 
CockroachDB expects all connections to be closed by clients before the end 
of drain_wait. After drain_wait ends and query_wait starts, the server 
shuts down all connections that are not with inflight queries (such as open 
transaction sessions). After the end of query_wait, all connections will be 
forced to shut down, regardless of active queries.

As part of CockroachDB’s draining process, the clients are not notified of 
the server’s current state (i.e. which step in the draining algorithm it is 
undergoing) and when their connections are going to be closed. 
That is to say, if they are in the middle of using a connection when the 
server is shutting down, they may receive confusing error messages such as 
`io timeout` or `ERROR: server is shutting down`.  More specific use cases 
will be discussed in the Background section.

The following shows the timeline for the current draining process of CRDB, 
and the workaround setting for drain_wait.

![Current Draining Process](images/graceful_draining_current_draining_process.png?raw=true "Current Draining Process")

The current workaround that CockroachDB provides is to set the 
`server.shutdown.drain_wait` longer than the sum of the maximum lifetime of a 
connection in the connection pool (`MaxLifeTime`), the time for the load 
balancer to stop routing new connections, and the max transaction latency. 
This setting will let connections to reach `MaxLifeTime` and be recycled by 
the connection pool before the node is being shutdown (drained). Active 
connections will stay connected until the inflight query completes. The 
connection pool would then recycle the connection and open a new connection,
from a different node. 

The current workaround that CockroachDB provides is to set the 
`server.shutdown.drain_wait` longer than the sum of the maximum lifetime of 
a connection in the connection pool (`MaxLifeTime`), the time for the load 
balancer to stop routing new connections, and the max transaction latency. 
This setting will let connections to reach `MaxLifeTime` and be recycled by 
the connection pool before the node is being shutdown (drained). Active 
connections will stay connected until the inflight query completes. 
The connection pool would then recycle the connection and open a new 
connection, from a different node. 


The downsides of this workaround are:
  1. The `server.shutdown.drain_wait` has to be adjusted based on clients’ 
     configuration of the connection pooling.

  2. The server is doing a hard wait till the end of `server.shutdown.
     drain_wait`. So though setting a long drain_wait can help ensure all 
     connections are closed before the (true) draining starts, it brings a 
     long downtime for this node.

To solve these issues, this design proposes to optimize the waiting stages 
during the draining process, and implement an early exit when all 
connections are closed by the client. 

Note that our ultimate goal is to provide clear guidance to avoid having 
similar problems again, but since they are encountered by a small number of 
customers, we don’t want to change too much the default draining workflow, 
and bring the risk of a longer draining process. Just aiming at giving more 
configs to tune if users found they are in trouble.

# Background

This section will focus on CockroachDB’s current draining process. 
A test with the [HirakiCP](https://github.com/brettwooldridge/HikariCP) 
connection pool is built [here](https://github.com/janeCockroachDB/hikari_demo), 
and a demo recording can be found [here](https://www.dropbox.com/s/6333e4x6yzwswfu/CurrentDraining.mp4?dl=0). 

As shown in the demo, here are some facts about the current draining process:

1. From the start of drain_wait, the health checkpoint /health?ready=1 returns
  ``` shell
  {
  "error": "node is shutting down",
  "code": 14,
  "message": "node is shutting down",
  "details": [
  ]
  }%
  ```
  but new connections are **always** allowed to be established during the 
  whole span of drain_wait. This is because the load balancer, which is 
  used in a multi-node cluster, needs to make new connections and ping this 
  endpoint to check the node health. Once the LB receives that error message,
  it will stop routing new connections to this draining node. But if during 
  drain_wait users try to connect to this node without a load balancer, 
  they still can.(Which means the `s.sqlServer.acceptingClients.Set(false)` 
  function call before drain_wait is confusing. We propose to rename this 
  function to `s.sqlServer.isHealthcheckReady`.) New connections and queries 
  are not allowed during query_wait.

2. If all connections are idle or there are no connections during 
   drain_wait, the server still does a hard wait till the end of drain_wait.  

3. If all connections become query-free at a timestamp within query_wait, 
   the server performs an early exit.

In summary, the features of the two periods in the current draining process,
drain_wait and query_wait, are as below:
![Summary of features for drain_wait and query_wait](images/graceful_draining_summary_drain_query_wait.png?raw=true "Summary of features for drain_wait and query_wait")

# Technical Design
([A draft PR can be found here.](https://github.com/cockroachdb/cockroach/pull/72991))

This design proposes to optimize the draining process by adding a new 
waiting period between drain_wait and query_wait called connection_wait, 
during which the server waits for clients to close connections. 
New connections are not allowed, and any connections without query 
in-flight will be killed by the connection pool once their lifespans are 
longer than the pool’s `MaxLifeTime`. I.e. **clients are not encouraged to 
start new queries within this period, and the connection pools are expected 
to recycle idle connections.** If at a certain timestamp all connections 
are closed, the server performs an early exit and proceeds to 
lease_transfer_wait. Note that if a connection is pulled by a user and 
waiting for query input, this connection is not considered idle, and will 
not be recycled until the user puts it back in the pool. The timeout can be 
set to be infinitely long. Clients should be notified that the existing 
connections that are not executing queries will be closed.

The following is the proposed draining process with optimized waiting stages, 
and early exit
![Proposed draining process](images/graceful_draining_connection_wait.png?raw=true "Proposed draining process")

Most of the changes should be made in the server and pgwire package. 
Similar to existent drain_wait and query_wait, the new wait period will be 
declared as a `*settings.DurationSetting` as follows:

```
connectionWait = settings.RegisterDurationSetting(
"server.shutdown.connection_wait",
"the amount of time a server waits for clients to close existing connections. 
After the start of connection_wait, no new connections are allowed. 
If all connections are closed by the client, the server will continue with the 
rest of the shutdown before the end of connection_wait.”,
0*time.Second,
).WithPublic()
)
```
The default duration length of connection_wait is set to be 0 second. 
This is because we don’t want to prolong the existing draining process for 
all customers. 

The main functionalities of connection_wait will be implemented with a 
function 
`func (s *Server) DrainConnections(ctx context.Context, connectionWait time.Duration`) 
at `/pkg/server/drain.go`. 
It mainly consists of three parts:

- **Disallow new connections**:
  we move the calling of `func (s *Server) setDrainingLocked` from `func (s 
  *Server) drainImpl`, which is used during query_wait, to the connection_wait. 
  `setDrainingLocked` modifies the server attribute s.mu.draining, which is 
  checked when the server registers a new connection at `func (s *Server) 
  ServeConn` in `pkg/sql/pgwire/server.go`. 

- **Early exit if all connections are closed**:
  we let the server keeps listening to the registered connections (`s.mu.
  connCancelMap`). If the length of this list reaches 0, the function returns. 
  Small channel tricks will enable an infinite timeout:
  ```go
    var timeout <-chan time.Time
    // If connectionWait is a negative period, we wait for infinitely long time
    // till all connections are stopped by the user.
    if connectionWait >= 0 {
      timeout = time.After(connectionWait)
    }
  
    select {
    case <-timeout:
      fmt.Println("connection wait ended at ", time.Now().Format(pgTimeFormat))
      close(timeoutChan)
    case <-connAllClosed:
      fmt.Println("all connections are closed by clients at ", time.Now().Format(pgTimeFormat))
      return
    }
  ```

- **Notify customers of the connection status**:

  we propose to enhance two notification channels:

  - **Log**: print the timestamp of the start and end of each draining 
    stage in the log file. This will help customers to reconfigure their 
    connection pools or load balancer if they receive an error message.

  - **`health?ready=1` endpoint**: print the current draining stage when 
    users are calling this endpoint. This helps users to take instant 
    actions on their current connections (e.g. hurry up queries with 
    remaining connections and close them by the connection_wait timeout.)

For an infinite connection_wait, there are concerns that it may introduce 
an upgrade failure mode where connections aren't closed by the customer, 
eventually leading the upgrade to fail (as we need some timeout); this will 
frustrate the customer and maybe, even more, it will frustrate CRL eng 
including SRE who own patching fleet (instead of the customer)._ ” With 
this in consideration, we propose to only allow an infinite connection_wait 
for the `cockroach node drain` cli command by adding a 
`--infinite-connection-wait` boolean flag. For cluster settings, we set an 
upper limit (e.g. 1 hour) for the duration 
length of `server.shutdown.connection_wait`.

In summary, the desired final state of this design is to have the following 
timeline for the draining:

- drain_wait: health?ready=1 starts to return error, and load balancer 
  listening to this http for health check stops routing new connections to 
  this draining node. But other new connections are still allowed.

- connection_wait: No new connections are allowed. Idle connections whose 
  lifetime is longer than the connection pool’s maximum lifetime setting 
  will be recycled. Connections not returned to the pool won’t be recycled. 
  If all connections are closed by the user, it does an early exit before 
  the timeout, and proceed to lease_transfer_wait.

- query_wait: Any connections without open queries in-flight will be closed.
  Any connection, once its query is finished, will be closed. If all 
  connections are closed, the server does an early exit before the timeout, 
  and proceed to lease_transfer_wait.

- lease_transfer_wait: the server waits to transfer range leases.

# Demo
A 4-min demo recording with the same Hikari connection pool setting can be 
found here, where we did manual testing. 
To add an automated test, we will refer to `pkg/sql/conn_executor_test.go` 
and `pkg/sql/pgwire/pgwire_test.go` to set up a connection pool, and apply 
a timeline similar to the one in the demo.

Note that in the demo we only manually tested with a single-node server. A 
multi-node server with a load balancer involved should also be considered. 
The drain_wait should still be set longer than the time that the load 
balance needs to stop routing new connections for this draining node, and 
users just need to set the length of connection_wait longer than the 
maximum lifetime of the connection pool. 
Specifically, drain_wait should be longer than the `check inter` in the 
[http health check of HAProxy](https://www.haproxy.com/documentation/hapee/latest/load-balancing/health-checking/active-health-checks/), and the `periodSeconds` in [k8s’s readinessProbe setting](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/).

# Possible Stretch
We may also want to enable the server to only allow new connections of 
certain types during drain_wait, since some customers mentioned that they’d 
like to reserve access to the node when the draining process started. 
The possible solution is to add a gate in the connection register that 
looks at a connection’s metadata (e.g. if it’s from admin or not) during 
drain_wait.

# Survey: configuration for C-Pool and Load Balancer
We made [a survey](https://docs.google.com/forms/d/1_BWgza8n5iYBwVR7WSrKT4jL8RClE-PMoOmzlUWvnl8/edit?usp=sharing) 
to collect information about uses’ setting for connection pool and load balancer. 
This would help us provide better guidance if user encounter similar issues 
during draining.

# Edge Cases
- If the user calls the `Undrain()` method during draining, the server 
  needs to stop the whole draining process immediately, and reset the 
  related attribute of the server. A channel can help with that.

- We leave the users to set the exact duration of each wait period because 
  they should be configured based on their load balancer, connection 
  pooling, and inflight query settings. Users are expected to be clear 
  about how to customize the process based on their needs. If wrongly 
  configured, the draining process can lead to existing issues, such as 
  unexpected disconnection or uncommitted queries.


# Questions not fully answered
- How does this work in a multi-tenant deployment?

- How does the change behave in mixed-version deployments? During a version 
  upgrade? Which migrations are needed?
  - If certain nodes in the cluster are not updated to support this feature,
    it may lead to confusion. We may need a version gate for 
    `server.shutdown.connection_wait`.

- Is the result/usage of this change different for CC end-users than for 
  on-prem deployments? How?

- What is the effect of possible mistakes by other CockroachDB team members 
  trying to use the feature in their own code? How does the change impact 
  how they will troubleshoot things?
  - More informative log messages of the whole draining process should be 
    added, especially the monitoring of alive connections.

- Does the change impact performance? How?
  -It will likely increase the draining process’s duration since we now 
  wait for the users to close connections. But given an early exit 
  functionality, the wait can be less painful.

- How is resource usage affected for “large” loads?
  - Users are very more likely to wait for a long time since the server 
    needs to wait for all connections to be stopped or returned to the pool 
    after queries are finished. A possible workaround is to set a smaller 
    connection pool size, and also avoid running long queries.
  
- Can this new functionality affect the stability of a node or the entire 
  cluster? How does the behavior of a node or a cluster degrade if there is 
  an error in the implementation?
  - It will not affect the node and cluster stability as long as multiple 
    nodes are not draining at the same time.

- Can the new functionality be disabled? Can a user opt out? How?
  - Set connection_wait to 0 seconds, and set `--infinite-connection-wait` 
    to false, then it should be identical to the current draining process. 
    But users will still see the newly added logs and will have access to 
    the newly added endpoints for the draining status.

- Can the new functionality affect clusters which are not explicitly using it?
  - It can prolong the draining process by a default timeout set to the 
    connection_wait.

- Does the change concern authentication or authorization logic? If so, 
  mention this explicitly tag the relevant security-minded reviewer as 
  reviewer to the RFC.
  - So far no, since we are following the logic of existing drain_wait and 
    query_wait.

- Does the change create a new way to communicate data over the network? 
  What rules are in place to ensure that this cannot be used by a malicious 
  user to extract confidential data?
  - No, it doesn’t create a new communication way.

- Is there telemetry or crash reporting? What mechanisms are used to ensure 
  no sensitive data is accidentally exposed?
  - Don’t think it is very related.

- Is the change affecting asynchronous / background subsystems? Are there 
  new APIs, or API changes (either internal or external)?
  - We may add a new endpoint to allow users to query their draining status.
    This endpoint should be external.

- Is the change visible to users of CockroachDB or operators who run 
  CockroachDB clusters?
  - We may print the draining status to the cli os as well.

- Is usage of the new feature observable in telemetry? If so, mention where 
  in the code telemetry counters or metrics would be added.
  - No, it won’t affect telemetry.


# Drawbacks
- We now set the server attribute `s.mu.draining` to disallow all new 
  connections. Should we instead send a more connection-pool-dedicated 
  message to stop new connections from a specific pool?

- Customer will still receive error messages if their connection pool 
  configuration is not compatible with their wait period settings, e.g. 
  connection pool’s max lifetime > connection_wait.

# Rationale and Alternative
The design of a connection_wait that allows early exit and infinite waiting 
provides the most similar functionality as [Postgres’s smart shutdown service](https://www.postgresql.org/docs/current/server-shutdown.html).

Note that in the current design, we do support an unlimited connection_wait 
duration. But this may lead to some problems as mentioned in the Edge Cases 
section. If we limit its duration to finite time, then setting a long 
(drain_wait + connection_wait) can be the same as setting a long drain_wait 
without the implementation of connection_wait in some cases. 
For example, if we set drain_wait to 30s, connection_wait to 30s, and the 
customer grabs a connection at the 20s from the start of draining, but 
inputs the query until 70s. Then the customer will still receive the 
“server is shutting down” error. In this case, the situation is not 
fundamentally changed, but we hope that with the improved notifications / 
endpoints, our users will be clearer about the status of the draining 
process, and take corresponding actions (in the above example, abort the 
connection before the connection_wait timeout). And also the early exit in 
connection_wait can help shorten the downtime if the user sets a long duration.

# Explain it to folk outside of your team
See the motivation part.

# Unsolved Questions
See “questions not fully answered” in technical design.

