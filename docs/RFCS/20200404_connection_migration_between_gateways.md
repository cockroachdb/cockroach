- Feature Name: connection migration between gateways
- Status: draft
- Start Date: 2020-04-05
- RFC PR: [#47060](https://github.com/cockroachdb/cockroach/pull/47060)
- Cockroach Issue:

# Summary

This RFC discusses the implementation of saving/restoring of SQL connection
state such that connections can survive a gateway shutdown. This is generally
useful for rolling restarts or node decommissioning, and in particular useful in
conjunction with ideas about ephemeral SQL gateways.

The idea is to introduce a pgwire load balancer (called proxy thereafter) that
cooperates with gateways shutting down in order to be able to continue serving
the client's connection by establishing a new connection to another gateway. The
outgoing gateway would serialize all of the state needed in the database such
that the new gateway could re-load it. From the client's perspective, the
handoff from one gateway to another is transparent.

The state that needs to be persisted include SQL level info (session vars,
prepared statements, temp tables) and also kvclient txn info (txn proto, read
spans, lock spans, inflight writes). We also discuss limited support for
resuming in-flight queries. In order to keep transactions alive without a
gateway heartbeating them, the heartbeating of transactions records changes - we
add a level of indirection by tying transaction liveness to the liveness of a
new node-level record. A scheme is proposed through which a proxy can adopt
transactions that were previously coordinated by an outgoing transaction. Thus,
the proxy will become responsible for keeping these transactions alive until it
eventually passes the ownership forward to an incoming gateway.

# Motivation

Today, a gateway shutdown/restart causes the failure of any queries running at
the time, the rollback or abandonment of any SQL txn running at the time, and
the closing of any pgwire connection to that node. This RFC proposes greatly
improving the situation for clean gateway shutdowns: we'll support keeping
client connections and transactions alive, and we'll support restarting an
in-flight query if it either hadn't returned any results at the time of the
gateway shutdown, or it the query's execution is deterministic (results come
back in the same order on the 2nd attempt).

There's two related motivations for doing this:
1. Improving our node rolling restart story. At the moment, nodes undergoing a
clean shutdown perform a draining procedure the involves waiting for in-flight
queries and transactions for a while before closing their connections. However,
queries/transactions that don't terminate within the timeout are aborted.
Alternatively, if there was no timeout, such queries/transactions would block
the restart arbitrarily.
2. Support for emerging architecture proposals that want to treat SQL gateways
as ephemeral processes - for example for scaling SQL processing capacity up and
down on demand. In such an architecture, gw stopping would be particularly
common. Having client connections be affected by these events would be
unfortunate.
3. There's potentially different others benefits to having a load balancer that
we control in the CRDB architecture. For example smart query routing or
eliminating the cost of opening TLS connections after a gw restart.

There's three layers of proposals being discussed in this RFC, with diminishing
returns for client impact of a gw restart: dealing with connections without an
open transaction, connections with an open transaction but no in-flight query,
and in-flight queries. Generally speaking, ideally we'd decouple the lifetimes
of all client operations from the lifetime of a particular gw. For example, in a
ephemeral gw architecture, the amount of time idle connections or transactions
are allowed to stay alive should be a matter of policy, divorced from the
scaling down of gw capacity. The implementation of the three stages will be
gradual, though.


# Guide-level explanation

The big idea here is that we'd introduce an optional component to the CRDB
architecture - a protocol-level proxy for Postgres connections. This proxy will
connect to SQL gateways and forward client traffic. The proxy will synchronize
with gateways shutting down, such that client connections whose backend
connections are terminating will buffer the incoming client commands on the
proxy until the commands can be forwarded to a new gateway; this can include
spinning up a new gateway at some later point if no client commands are
currently pending.

The proxy has to be protocol-aware, since it'll have to understand an extension
of pgwire used by a gateway signaling that it's about to shut down. As such, the
proxy has to decrypt the traffic, so it'll terminate the client TLS connections
and perform certificate-based authentication. The connections to the gateways
will be encrypted in some cheaper way, or perhaps still with TLS but with node
certs instead of client certs.

When a gateway shuts down, it'll perform the usual draining procedure (i.e.
waiting for in-flight queries to finish and open-transactions to complete)
subject to the draining timeouts policy. After the draining procedure, if there
are still open connections coming from our enlightened proxies, the gw will
coordinate with each proxy for saving the state of its respective connections
into the database. The proxy receives a token for every connection, which can be used to
restore the connection's state on another gw. The proxy can chose to restore
that connection immediately or at an arbitrary later time, depending on client
traffic and gw availability. The proxy takes temporary ownership of the
connection's state; it is in charge of: a) keeping transactions alive by
heartbeating them (or equivalent, see further) b) cleaning up transactions and
deleting the saved state if the corresponding client conn goes away

Once the proxy designates another gw as the one to handle a specific client
conn, a different handshake is performed when establishing a backend connection
to the gw. As part of the handshake, the token identifying the previously saved
state is passed to the gw, which uses it to load it up. The state is deleted
from the database, being now held in memory by the new gw. The new gw takes
responsibility for coordinating the conn's open txn (if any).

The amount of state required to be saved for a connection depends on its situation:
1. No open transaction: 
    - session variables that have been updated from their defaults
    - default values for session variables that are different from the global defaults
    - stored procedures
    - portals?
    - temp tables
    - pgwire commands that are queued up for execution (see [Gateway shutdown
    handshake](gateway-shutdown-handshake))
2. Open transactions, additionally:
    - transaction record
    - savepoint information (read spans, lock spans, in-flight writes)
3. In-flight query, additionally:
    - savepoint info, but with the note that the savepoint is taken before the
    query started running
    - the in-flight query
    - information on the query's results that were already sent to the client
    (or rather, to the proxy): the number of result rows and a hash of all the
    result bytes. This information will be used when the query is re-run on a
    different gw in order to check that the query's execution is
    "deterministic": we'll verify that the first results generated on the new gw
    correspond to what was sent to the client already. If they don't, then the
    query needs to be aborted.
    
 # Detailed design
 
### Gateway shutdown handshake
 
We need a way for a gw to announce to a proxy that its about to shutdown, a way
for a proxy to acknowledge to the gw that it has stopped forwarding traffic on a
specific connection and thus it's safe to save the conn's state, and we need a
way for the gw to let the proxy know that the state for a specific conn has been
saved. The proposal is the following scheme:

1. When a gateway wants to shut down, it enters its existing draining state. New
connections are not accepted any more. Proxies are signaled to stop sending
traffic on existing connections that don't currently have a transaction open.
Connections with no txn to non-proxied clients are closed now. Connections to
proxies are left open, but are not expected to receive more traffic (modulo
races). A timer starts ticking for open transactions (including running
queries). When the timer fires, the respective connections will be closed.

2. Upon hearing about a gw draining, a proxy sends a special `no_more_traffic`
pgwire packet on every connection to that gw, signaling the fact that it will
not send any more traffic on that connection. Any incoming traffic will now be
buffered. The proxy will still read results from the backend conn and send them
to the client until the gw signals that the connection state has been persisted.

3. Upon receiving the signal that the proxy is not sending any more traffic on a
conn, the gw will try to finish responding to all the commands that have
preceded that signal. Upon the expiry of the draining timer, the gw will make
sure that the transaction's record was written (if there is a transaction) and
persist the conn's state into a `connection state record` and send the gw a
token indicating:
a) that no more results are coming
b) whether or not there are pending commands whose results have not been sent.
If there are, the gw will serialize them in the persisted state, and the proxy
should try to find a new gateway for this connection ASAP, as the client is
waiting for result (this is similar to the proxy having buffered some commands
after sending the no_more_traffic signal; an alternative is to have the proxy
always buffer commands until it sees the respective results flowing back to the
client).
c) whether the connection's state was successfully saved and thus the proxy
needs to take ownership of it. An optimization here is to identify connections
that have no state whatsoever and, instead of persisting their empty state,
signal the proxy that it should just open a new backend connection. This vanilla
connection, btw, is the only case supported by [Aurora RDS
Proxy](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/rds-proxy.html#rds-proxy-pinning)
for failovers.

The point of persisting the state in the database instead of simply passing it
to the proxy is to save RAM on the proxy. The assumption here is that we want to
run a lot less proxies than gateways (which gateways had all this state in RAM),
so RAM is at more of a premium. Also, in multi-tenant setups, some connections
might be idle for long periods of time, so we'd rather pay storage than RAM for
them. But I'm curious of opinions.

### Proxy resource ownership

When receiving a connection token from a gw, the proxy will take ownership of
it. If it fails to do so within a timeout, the token becomes eligible for
garbage collection and the respective open transaction (if any) becomes eligible
for aborting and cleanup.

Since the tokens are passed individually on each connection, the proxy needs to
take ownership of each one individually (or, at least I don't yet see a scheme
by which the tokens can safely be passed in bulk, since synchronization with
individual connections is required). In order to take ownership, the proxy will
update the connection state records, changing their liveness from an expiration
to a redirection to a "proxy liveness record". The point of the liveness record
is that, instead of heartbeating the connection state records individually, the
proxy could hearbeat a single central record.

The proxy also needs to take ownership of all the transactions corresponding to
the adopted connections. This means either heartbeating their transaction
records individually or, better yet, switching the transaction liveness also to
an indirect scheme based on a gateway/proxy liveness record. This is something
we've been considering doing independently of this RFC, for efficiency purposes
(e.g. there's currently a performance cliff when transactions go from a mostly
below 1s regime to mostly over 1s - the point at which heartbeating and txn
record writing starts happening).

### Backend connection re-establishment

The whole point of the game is to have proxies be able to move the serving of a
client connection from a gateway to another. Once a proxy has adopted a client
conn, it will eventually delegate it to another gw (unless the client closes its
side of the connection, or the proxy dies). For each adopted connection, the
proxy will look for another backend gateway as soon as there's traffic from the
client (or immediately, if there had been pending commands at the time of the
adoption). If there is no gw available to delegate the connection to, we
envision the proxy being involved in spinning one up, but that's beyond the
scope of this RFC.

Once a gateway has been designated, the proxy will open a connection to it and,
through a special pgwire package, ask the gw to load up the respective
connection state record and delete it. The proxy will then try to explicitly
relinquish ownership of the record by racing an update with the gw's deletion.

The gateway will load up the state, initialize a `connExecutor` with it, and
then start serving the network connection normally. If the connection state had
been saved while a query was ongoing, the savepoint that had been created before
that query is restored. If there were outstanding pgwire commands recorded in
the connection state record, they are processed first. If the first such command
refers to a query for which some (but not all) results had already been
delivered to the proxy by the old gw, then, as the query is re-executed
normally, the first *n* results are not delivered to the connection. As soon as
the first *n* results are produced, their hash is computed and, if it doesn't
match with the hash of the results that were previously produced, it's tough
luck. It must be the case that the order of the results is non-deterministic,
and it seems very hard to recover from that. An error is reported to the client.
Since we're creating savepoints automatically before each statement, the client
can be given the opportunity to handle this specific error by rolling back to
such a savepoint (assuming that the client has been coded such that it can
ignore the results that have been already delivered). A particular case that
will just work is the case of queries that haven't returned any results to the
client yet. A lot of queries buffer their results up to a size limit before
writing them to the network. For smaller queries it's common that most of the
queries time is spent before even producing any results. For long running
queries this is also common: for example queries blocked on locks.

### User authentication

If the client uses TLS, the proxy will terminate the client TLS connection as it
needs to read and even change traffic. The proxy thus needs to do the validation
of the client cert. If that succeeds, the gateway will not do further
authentication (technically, the gateway will use the `trust` PG auth method for
connections coming from the proxy).
