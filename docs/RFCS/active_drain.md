- Feature Name: Actively draining nodes
- Status: draft
- Start Date: 2016-11-17
- Authors: Alfonso Subiotto Marqués
- RFC PR:
- Cockroach Issues: [#9541](https://github.com/cockroachdb/cockroach/issues/9541), [#9493](https://github.com/cockroachdb/cockroach/issues/9493), [#6295](https://github.com/cockroachdb/cockroach/issues/6295)

# Summary
Propose a more active way of draining nodes.

Specifically:
- Actively transfer range leases away from the node.
- Close client SQL connections. This is broken down as:
  - Close connections with no ongoing transactions.
  - Allow some time for clients to finish transactions. After a timeout (10s), abort ongoing transactions and clean up.
  - Actively release SQL table leases.

# Motivation
The two reasons for actively draining nodes are avoiding unnecessary wait time during shutdown and performing adequate clean up.

We drain client connections and leases in the same way: stop accepting new ones and wait for current ones to expire. This leads to an undesirably slow node shutdown. This RFC proposes a more active way of releasing the node of responsibilities that doesn't compromise on correctness, thus avoiding unnecessary waiting time.

Cleaning up after a draining node is both good practice and can avoid undesirable behavior. Cleaning up ongoing transactions and transferring range leases away from the draining node are examples of the former and ([#9493](https://github.com/cockroachdb/cockroach/issues/9493)) is a good example of an issue caused by not actively releasing SQL table leases on shutdown.

# Detailed design
## `drain-leases` mode
Any leases that the node currently holds will be transferred away using `AdminTransferLease(target)` where target will be the first `StoreID` found in `replica.mu.state.Desc` not equal to the `StoreID` currently draining.

Moving ranges to another node is also an option. Not doing this could affect QPS as the draining node's removal from the raft group could block writes. However, this healing happens down the road anyway and trying to do this could end up adding too much complexity. Without a certainty that this is necessary, it would be best to defer this until it becomes an issue.
## `drain-clients` mode
Open pgwire v3 connections will be kept around in the `pgwire.Server` as a linked list. A mutex and a `newTxnsAllowed` boolean will have to be added to the `sql.txnState` struct to avoid scenarios in which transactions start after we have checked for open transactions. This mutex and boolean will be used in `txnState.resetForNewSQLTxn(...)` and when draining.

When the server is set to `drain-clients`, each connection's session will be disallowed from creating new transactions. Open transactions will be checked for using
```
conn.session.TxnState.State.kvTxnIsOpen()
```
If no transaction is open, an [appropriate error code](#note-on-closing-sql-connections) will be written to the client followed by a call to `conn.finish(...)`, leading to `session.Finish(...)`. Note that this will lead to decrementing table lease reference counts. Once the end of the list is reached and there are still open connections, a timeout of 10s will be applied, after which open connections will be closed in the same way. `session.Finish(...)` will take care of aborting transactions and cleaning up.

At this stage we have no more client connections open. However, sessions might still exist from entities like the admin UI. These sessions will be found and closed through the upcoming session registry ([#10317](https://github.com/cockroachdb/cockroach/pull/10317/)).

The final stage is to execute `DELETE FROM system.lease WHERE nodeID = nodeID`. When closing sessions, the refcount of held leases was decremented. However, the leases themselves are not deleted from the lease table unless the lease is for an outdated version of a table.

### Note on closing SQL connections
[#6283](https://github.com/cockroachdb/cockroach/pull/6283) proposed politely refusing new connections and closing existing connections with

> 08004  SQLSERVER REJECTED ESTABLISHMENT OF SQLCONNECTION
> sqlserver_rejected_establishment_of_sqlconnection").

but mentioned that more research should be done into how widely this error code is supported by load balancers.

The load balancing solutions for Postgres that seem to be the most popular are [PGPool](http://www.pgpool.net/docs/latest/pgpool-en.html) and [HAProxy](https://www.haproxy.com/). Both systems have health check mechanisms that open a connection to known servers and try to establish a SQL connection[[1]](http://www.pgpool.net/docs/latest/pgpool-en.html#HEALTH_CHECK_USER)[[2]](https://www.haproxy.com/doc/aloha/7.0/haproxy/healthchecks.html#checking-a-pgsql-service). However, outside of these health check procedures, SQL errors when establishing a connection are simply forwarded back to the client. Additionally, closing existing connections to the draining server will not be handled by PGPool[[3]](http://www.pgpool.net/pipermail/pgpool-general/2016-October/005110.html) or HAProxy (SQL session information is outside the scope of a TCP/HTTP load balancer).

We should therefore stick to returning error 08004 to refuse new connections, close existing connections with the same error and inform clients to retry when encountering this error.

# Drawbacks

# Alternatives
- Don't offer a 10s timeout to ongoing transactions. The idea of the timeout is to allow clients a grace period in which to complete work. The issue is that this timeout is arbitrary and it might make more sense to forcibly close these connections. It might also make sense to offer clients to set this timeout via an environment variable.
- Reacquire table leases when the node goes back up. This was suggested in [#9493](https://github.com/cockroachdb/cockroach/issues/9493) but does not fix the issue of having to wait for the lease to expire if the node does not come back up.

# Unresolved questions
