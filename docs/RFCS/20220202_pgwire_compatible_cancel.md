- Feature Name: Postgres-Compatible Cancel Protocol
- Status: draft
- Start Date: 2022-02-02
- Authors: Rafi Shamim
- RFC PR: https://github.com/cockroachdb/cockroach/pull/75870
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/41335

## Dedication

The proposal here is entirely dependent on many thoughtful discussions and prior
work with Jordan Lewis, knz, Andrew Werner, Andy Kimball, Peter Mattis, and
several others going back to 2019 all the way until now.

## Summary

The Postgres query cancel protocol is hard to implement since it only uses 64
bits of data as an identifier, and is sent over a separate (unauthenticated)
connection, different from the SQL connection. For dedicated clusters, these 64
bits of data need to identify a node and query to cancel. We can use 32 bits to
identify which node is running the query, so that when any node receives a
cancellation request, it can forward the request to the correct node in a
cluster. We use the other 32 bits to identify a session running on that node.
Finally, we add a semaphore to guard the cancel logic so that an attacker cannot
spam guesses for 32-bit session IDs.

In CockroachDB Serverless, a SQL proxy instance also needs to identify which
tenant to send the cancel request to. To solve this, each SQL proxy will save
the 64-bit cancel keys returned by the SQL node, then send a different 64-bit
key back to the client. This proxy-client key will contain the IP address of the
proxy that is able to handle this key. (Or, if the proxies are all on the same
subnet, fewer bits can be used to identify the proxy.) The remaining bits are
random. When any proxy receives a cancel request, it can look at the key to
figure out which other proxy to forward the request to. Then when the correct
proxy receives the request, it checks that the random bits of the key are in its
in-memory map of cancel keys, and forwards the original cancel key to the tenant
where it came from.

Serverless support can be implemented entirely separately from
dedicated/self-hosted support.


## Motivation

Nearly all Postgres drivers support the Postgres query cancellation protocol.
For example, the PGJDBC
[setQueryTimeout](https://jdbc.postgresql.org/documentation/publicapi/org/postgresql/jdbc/PgStatement.html#setQueryTimeout-int-)
setting
[uses](https://github.com/pgjdbc/pgjdbc/blob/3a54d28e0b416a84353d85e73a23180a6719435e/pgjdbc/src/main/java/org/postgresql/core/QueryExecutorBase.java#L171)
it. Currently, when a client sends a cancellation request using this protocol,
CockroachDB simply ignores it. Implementing it would mean that applications
using drivers like this would immediately benefit. Specifically, it would allow
CockroachDB to stop executing queries that the client is no longer waiting for,
thereby reducing load on the cluster.

This protocol is the top unimplemented feature in our telemetry data. According
to our [Looker dashboard](https://cockroachlabs.looker.com/looks/47), 3,430
long-running clusters have attempted to use it (compared to 456 clusters for the
next unimplemented feature).


## Background

The [Postgres
documentation](https://www.postgresql.org/docs/14/protocol-flow.html#id-1.10.5.7.9)
describes how the protocol works. During connection startup, the server returns
a BackendKeyData message to the client. This is a 64-bit value; in Postgres 32
bits are used for a process ID, and 32 bits are used for a random secret that is
generated when the connection starts.

To issue a cancel request, the client opens a new unencrypted connection to the
server and sends a CancelRequest message. For security reasons, the server never
replies to this message. If the data in the request matches the BackendKeyData
that was generated earlier, then the query is cancelled.

The Postgres documentation also mentions that this protocol is best-effort, and
specifically says, "Issuing a cancel simply improves the odds that the current
query will finish soon, and improves the odds that it will fail with an error
message instead of succeeding."

There have been internal discussions about this in [April
2020](https://github.com/cockroachdb/cockroach/pull/34520#discussion_r407414290),
[July 2021](https://github.com/cockroachdb/cockroach/pull/67501), and [January
2022](https://cockroachlabs.slack.com/archives/CGA9F858R/p1643382222564939).


## Technical Design


#### SQL Node Changes

The connExecutor will be updated to generate a random 32-bit integer (secretID)
when it is initialized, and register it with the server’s
[SessionRegistry](https://github.com/cockroachdb/cockroach/blob/a434c8418c36dbeb64e73588bcd4dd5b248c3238/pkg/sql/conn_executor.go#L1692).
The 32-bit SQLInstanceID and the random 32 bit secretID will be used as the
BackendKeyData that is sent back to the client.

The status server will have a new endpoint named PGWireCancelQuery, analogous to
the existing CancelQuery endpoint. The main difference is that it just has a
32-bit secretID in the request body. It will also use the node ID to forward the
request to the correct node. Also, this endpoint will only be called by the
pgwire/server code that handles a CancelRequest – the endpoint is not meant to
be called directly by a client.

PGWireCancelQuery will call the server’s SessionRegistry using the secretID. The
SessionRegistry will use a semaphore to guard the number of concurrent
cancellation requests. If a cancel request fails, which is likely to only happen
if an attacker is spamming requests, it will be penalized by holding onto the
semaphore for an extra second. If we set the concurrency of the semaphore to 256
(2^8), then that means it would take an attacker 2^24 seconds to guess all
possible 32-bit secrets and be guaranteed to cancel something. If we suppose
there are 256 concurrent queries running on the node, then on average it would
take 2^16 seconds (18 hours) to cancel any one of the queries.

An attacker could still spam cancel requests and use up the entire quota of the
semaphore, and therefore starve legitimate cancel requests from being handled.
We consider this risk acceptable in the short-term, since the protocol is
best-effort, and this behavior is no worse than the status quo.


#### SQL Proxy Changes

The proxy code will be updated to intercept BackendKeyData messages that are
sent to the client as well as CancelRequest messages that are sent by the client
to the server.

When a proxy sees a BackendKeyData, it will generate a new random 32-bit secret
named proxySecretID. (NB: This proxySecretID could be larger depending on how
many bits are needed to identify a proxy instance.) The proxySecretID will be
used to key a map whose values are structs containing (1) the original
BackendKeyData, (2) the address of the SQL node, and (3) the remote client
address that initiated this connection. The proxy will then return a new
BackendKeyData consisting of 32 bits for the proxy’s IP address, and 32 bits for
the proxySecretID. If the proxies are all on a subnet, then fewer bits are
needed for the IP address.

When a proxy sees a CancelRequest, it first extracts the IP address component of
the message. If needed, it will forward the request to the proxy with that
address using an RPC that is only for proxy-proxy communication. This RPC
request will include the remote address of the client that sent the
CancelRequest. If the proxy is the intended recipient, then it will extract the
proxySecretID and check if it exists in the BackendKeyData map. If so, it will
check that the remote client address in the map matches the address that sent
the CancelRequest. If that matches, then the proxy will send a CancelRequest
with the original BackendKeyData to the SQL node using the address that is
stored in the map.

If the proxy migrates a session from one SQL node to another, that will make the
BackendKeyData it had previously saved obsolete. When the migration occurs, the
proxy will need to update the contents of its BackendKeyData map and replace the
old data with the new BackendKeyData provided by the session on the new SQL
node.

If the proxy crashes unexpectedly, then all the BackendKeyData entries will be
lost. From the clients’ perspective, the connection will be broken when the
proxy crashes, so they will not be able to interact with any in-flight queries,
so it’s not a huge problem that the queries can no longer be cancelled.


### Alternatives


#### Make SQL nodes verify the remote address

Instead of using a semaphore, the SQL nodes could also make sure the remote
client address that received the BackendKeyData matches the remote address that
sent the CancelRequest. This would work well in dedicated clusters. But in
serverless clusters, this would mean that the SQL proxy needs to propagate the
remote address of the client while sending the CancelRequest. For normal SQL
sessions, this is done using the **crdb:remote_addr** StartupMessage, but the
cancel protocol does not have a similar way of sending data like this.


#### Obfuscate the SQL proxy identifiers

Using 32 bits of the proxy-client BackendKeyData for the proxy IP address means
that an attacker could more easily guess an address and spam it. Instead, we
could obfuscate the address by hashing it along with a salt that is shared by
all the proxy instances. This would require additional secret management at the
proxy layer. To avoid this complexity, the proposal instead is to validate the
address sending the CancelRequest. This still allows an attacker to cause
additional network traffic, but prevents them from doing any functional damage.

### Possible Future Extensions

#### Custom protocol for SQL node to SQL proxy communication

The SQL node to SQL proxy part of the protocol described above could be changed
to be entirely custom. This would allow us to use more bits to identify the
session to cancel, and would eliminate the need for a semaphore. However, this
custom protocol could only be used in deployments where there is a SQL proxy in
front of the SQL nodes.

#### IP-based rate limiting

Another way of preventing an attacker from spamming cancel requests is to rate
limit these requests by IP. This also would eliminate the need for a semaphore,
and additionally, would prevent an attacker from causing extra network traffic
and starving legitimate cancel requests. However, we cannot add an IP-based rate
limiter at the SQL node layer, unless we also develop a custom protocol for
propagating the remote client address from the proxy to the SQL node.

It is possible that in the future, _all_ clusters might live behind a SQL proxy
process (possibly an embedded process). If we wait until that is the case, then
we can add IP-based rate limiting at the proxy layer exclusively.
