- Feature Name: CockroachDB-specific query cancel protocol
- Status: rejected
- Start Date: 2022-01-20
- Authors: knz
- RFC PR: [#75307](https://github.com/cockroachdb/cockroach/pull/75307)
- Cockroach Issue: [#75203](https://github.com/cockroachdb/cockroach/issues/75203) [#41335](https://github.com/cockroachdb/cockroach/issues/41335) [#67501](https://github.com/cockroachdb/cockroach/pull/67501)

# Dedication

The proposal here is largely inspired by the fantastic
and thorough previous efforts of Rafi Shamim in the same area. None of
this proposal could have even been formulated if it was not for Rafi's
previous insights.

# Summary

This RFC proposes to implement a CockroachDB-specific query cancel
protocol, inspired by PostgreSQL's own protocol, and without requiring
low-level (“pgwire”) protocol extensions.

The primary application is to support query interruption in
CockroachDB's native SQL shell (`cockroach sql`), with an eye for
supporting a SQL shell in the CC console and client-side SDKs which we
will likely propose to end-users at a later date.

The proposal preserves compatibility with existing pg connection pools
/ forwarders. For example, it does not require any change to the CC
infrastructure. Client-side, it does not require pg driver
customizations. It was designed such that it could be used in SQL
client apps with very limited effort, even before CRL starts
delivering crdb-specific SDKs that would encapsulate this logic.

A [draft
implementation](https://github.com/cockroachdb/cockroach/pull/75211)
is already available.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Dedication](#dedication)
- [Summary](#summary)
- [Motivation](#motivation)
    - [Compatibility with CockroachCloud](#compatibility-with-cockroachcloud)
    - [Support for non-interactive apps](#support-for-non-interactive-apps)
    - [Remain with pgwire and decouple from crdb-specific SDK plans](#remain-with-pgwire-and-decouple-from-crdb-specific-sdk-plans)
    - [Stay friendly to the ecosystem of SQL shells](#stay-friendly-to-the-ecosystem-of-sql-shells)
- [Technical design](#technical-design)
    - [Design constraints](#design-constraints)
        - [Self-imposed from research: no full authentication](#self-imposed-from-research-no-full-authentication)
        - [Security-related: tamper resistance and non-replayability](#security-related-tamper-resistance-and-non-replayability)
        - [Engineering objective: extensibility](#engineering-objective-extensibility)
    - [Solution overview](#solution-overview)
    - [Request authentication](#request-authentication)
        - [Shared secret delivery](#shared-secret-delivery)
            - [This proposal: inband shared key delivery](#this-proposal-inband-shared-key-delivery)
            - [Alternative: out-of-band shared secret](#alternative-out-of-band-shared-secret)
        - [Structure of cancel requests](#structure-of-cancel-requests)
        - [Authentication protocols](#authentication-protocols)
            - [Simple protocol](#simple-protocol)
            - [HMAC-based protocol](#hmac-based-protocol)
    - [Implementation outline](#implementation-outline)
    - [Authenticator API](#authenticator-api)
    - [Possible optimizations](#possible-optimizations)
    - [Drawbacks](#drawbacks)
    - [Alternatives](#alternatives)
        - [Plain SQL](#plain-sql)
        - [PosgreSQL cancel requests](#posgresql-cancel-requests)
        - [TOTP-based protocol](#totp-based-protocol)
        - [Target the query in the request, not the session](#target-the-query-in-the-request-not-the-session)
- [Explain it to folk outside of your team](#explain-it-to-folk-outside-of-your-team)
- [Unresolved questions](#unresolved-questions)

<!-- markdown-toc end -->


# Motivation

*Inband query cancellation* is the second most request feature in
CockroachDB's interactive shell (second only to tab completion). Users
often encounter queries that “run for too long” and wish to stop their
execution.

The word “inband” is important. It reflects both the following:

- the user wants the ability to cancel the query “from within” the
  current session, without first looking up what they need to do
  elsewhere.

  Without this constraint, CockroachDB [already has this capability](#plain-sql): a
  user can run SHOW SESSIONS in a 2nd session, then CANCEL QUERY from
  there.

- the user wants to preserve their current session at the remote
  server, including all current session customizations, prepared
  statements, etc.

  Without this constraint, a SQL shell could recognize Ctrl+C, close
  the connection and start a new one, which would be very easy to
  implement and not require any product changes.


## Compatibility with CockroachCloud

The solution we're aiming for should be easy to make work with
CockroachCloud, including CC serverless where the public SQL
connection server is potentially responsible for 100s/1000s of
separate tenants running simultaneously.

Note that this motivation alone is why we are aiming for a custom
solution, instead of supporting PostgreSQL's own cancellation protocol
natively. This is discussed further in the [“Alternatives”
section](#postgresql-cancel-requests) at the end.

## Support for non-interactive apps

In non-interactive applications, inband query cancellation does not
see as much demand. Certain client drivers provide an API for it
(e.g. in Go, `pgx` supports it) but others do not (e.g. `lib/pq`
doesn't).

Experimentally, we see that non-interactive apps generally do not find
themselves in a situation where queries run for “too long”. Instead,
excessive query execution time is optimized away during application
design before it happens, with careful query plans, index selection,
and if at all necessary, a custom `statement_timeout`.

This is not to say that inband cancellation could not possibly see a
place inside non-interactive apps; however, by all intents of purpose,
it's a feature that matters most to human users during interactive
sessions.

Therefore, drop-in compatibility with existing non-interactive pg apps
is not considered in-scope for this proposal.

## Remain with pgwire and decouple from crdb-specific SDK plans

Since mid-2021, Cockroach Labs has started seriously considering
shipping CockroachDB-specific (or, at least, CockroachCloud-specific)
client-side tooling, collectively called “SDK”.

In the abstract, this means that Cockroach Labs is potentially on a
path to eventually delivering custom SQL drivers to app
developers. Still in the abstract, this means that we could envision a
future where CockroachDB uses its own low-level wire protocol and
query cancellation requests can be sent in-band through the same TCP
connection.

However, in practice, a custom CockroachDB-specific SQL driver may
remain elusive even if Cockroach Labs provides custom SDKs to
end-users. It is possible, likely even, that in a first phase CRL SDKs
will be wrappers around off-the-shelf low-level PostgreSQL client
drivers. The work required to develop and maintain low-level SQL
drivers is non-trivial, especially compounded across all the target
programming languages.

SDK adoption will also be incremental among end-users, with many
preferring to continue using the existing pg-compatible tooling for as
long as possible.

Finally, even with custom SDKs, there is also an ecosystem of SQL
connection pools, forwarders etc which know about PostgreSQL but not
CockroachDB yet. We would need replacement for those too.

In short, our ability to customize the low-level wire protocol and
expect good support across our ecosystem of SQL shells will likely be
delayed by a few more years, if at all possible. In the meantime, the
lack of inband query cancellation hurts our users a lot, today.

Therefore, we should deem it worthwhile to explore a solution that can
run on top of PostgreSQL tooling: low-level SQL drivers client-side,
and SQL connection pools / forwarders between client and server.

## Stay friendly to the ecosystem of SQL shells

Finally, we should acknowledge that while CockroachDB's current SQL
shell (`cockroach sql` / `cockroach demo`) is where the demand for
this is most pressingly expressed, there is an ecosystem of SQL shells
besides it.

We have already plans in CockroachCloud to provide an interactive
shell to run in web browsers. Some of our DB exploration tools provide
an interactive shell as well.

Therefore, we should aim to pave a road for these alternate shells to
provide query cancellation too. The envisioned solution, if any,
should not be specific to CockroachDB's own `cockroach sql`.

# Technical design

## Design constraints

The main design constraint immediately flows from the motivation
section: the solution should not require pgwire custom messages.

Just this constraint alone sets a general technical direction: the
cancellation request has to be sent over *a new TCP connection*
alongside the main one. There is simply no space in the pgwire
protocol to send a cancellation “inline” on the same TCP connection
while a query is running, and we wish to not need to customize pg
client drivers to do so.

### Self-imposed from research: no full authentication

We also choose to adopt another design constraint: the
solution should not require a fully-fledged SQL authentication
handshake.

This constraint does not naturally flow from the motivation section,
and instead stems from research into the PostgreSQL ecosystem:

- a full authentication cycle can be expensive in terms of latency.

  Both the pg folk and us wish cancellation requests to be sent and
  processed quickly. This includes avoiding the overhead of a mTLS
  handshake if possible.

- we do not wish to request users to re-enter their passwords to
  process a cancellation.

  Note that it's customary in the pg ecosystem to omit the password
  from connection URLs, and instead rely on interactive entry when the
  session connects initially (or when there's a need to reconnect
  after a server failure). Query cancellation does not customarily
  require password reentry. We want to preserve this user journey.

  However, we also don't want to retain passwords in-memory if there
  was no need to do so previously, as in-memory password preservation
  is tricky to achieve properly (needs locked memory regions) and poor
  implementations can create security vulnerabilities.

- with Kerberos/GSS deployments, token use can be metered and
  operator-users interacting with a server should not be pushed to
  explain why their rate of login is abnormally higher.

- in a close future where we support OAuth-derived authentication
  flows (DB SSO logins), we wish to not consume an authentication
  token to process a cancellation, because cancellations can be
  frequent and irregular and this could flag the client as suspicious
  in automated intrusion detection systems.

- authentication events are often monitored and stored in auditing
  systems, so incurring authn events for every cancellation (which,
  during interactive use, are orders of magniture more frequent than
  new connections) would bloat up the audit logs in a surprising way
  for operators or require the configuration of custom filters to
  avoid this bloat. We wish to avoid this monitoring and deployment
  overhead.

With this additional design constraint, we acknowledge the wisdom of
PostgreSQL's own cancellation protocol, which was also designed to
bypass a full authentication cycle.


### Security-related: tamper resistance and non-replayability

The design should match the following obvious security objectives:

- a bug in a SQL client should not result in cancelling a query in an
  unrelated session by accident.
- a bug in a SQL client wrt cancellation queries should not result in
  an accidental DoS of the CockroachDB server or cluster.
- a malicious user should not be able to cancel queries for sessions
  they don't own, just by having access to the same SQL server.

  This includes:
    - it should be hard for a malicious user to bruteforce
      query cancellation, to cancel queries they don't own.
    - it should not be possible to overload a CockroachDB cluster with
      query cancellation requests.

The objectives above are collectively part of the *tamper resistance*
category of security protections: a legitimate user should not be
subject to unwanted interference due to the cancel protocol.

Note: PostgreSQL's own cancellation protocol is already somewhat
resistant on these points, and we should aim to do even better.

Additionally, PostgreSQL's own protocol has a property that can be
considered both an advantage and a liability: cancellation can be sent
without a mTLS handshake.

The advantage is that it makes it extremely lightweight to
process. The liability is that it makes cancellation messages
vulnerable to *replayability*: a malicious user observing the network
traffic in-between the client and server can capture one cancellation
message and use it to cancel subsequent queries on the same session.

If at all possible, we could aim to preserve the advantage (low-cost
of avoiding the mTLS handshake) while avoiding the liability (by preventing
replayability).

Note that the proposal here is to possibly make TLS *optional*, not
require that cancel request always be sent in clear. For example, in
TLS-required deployments, `StartupMessage` is never sent in the
clear and it may even be rejected by proxies. Case in point: the proxy
we built for CC Serverless would need to be modified to pass this
message along.

### Engineering objective: extensibility

The shortcomings in pg's own protocol have been discovered long after
it was implemented, and by that time most of the low-level pgwire
implementations in SQL proxies / load balancers had baked in the
particular structure of cancellation requests, so it was not possible
to fix the shortcomings after the fact.

Our solution should instead aim to be replaceable in the future, as we
discover new and better ways to achieve the same.

## Solution overview

The proposed solution is to send cancellation requests by opening
another TCP connection to the same server and sending a single
pg `StartupMessage` using a client parameter `crdb:cancel_request` containing:

- the session ID of the main session whose current query needs to be cancelled.
- a cryptographically secure, non-replayable cancel request payload that
  is sufficient to authenticate the request as being “owned” by the original
  session.
- a protocol identifier, to provide cross-version compatibility if we ever
  need/want to change the protocol.

This `StartupMessage` can be sent in the clear without mTLS and does not
need to set the username. With direct database access, it does not
even need to set the database field or any other field really. Only in
the CC serverless case, we will need to continue to set at least the
additional field needed to route the request to the appropriate
tenant.

By recycling pg's standard `StartupMessage` and simply relying on a
client parameter, we make it possible to issue cancellation requests
by constructing a custom connection URL and feeding it to
off-the-shelf SQL drivers. There is no driver customization
needed. Also, because the `StartupMessage` remains standardized, it
will be forwarded perfectly by off-the-shelf pg connection pools and
forwarders.

Server-side, when a `StartupMessage` containing the field
`crdb:cancel_request` is received, the remainder of the connection
logic is bypassed entirely, including authentication, session
establishment, etc. Instead:

1. the cancel request is funneled through a semaphore, to limit
   concurrency and thus the risk of DoS.
2. the target node ID is extracted from the session ID value.
   (this will be a SQL instance ID in the multi-tenant case.)
3. the request is forwarded to the target node / SQL server via
   a single inter-node RPC.
4. at the target node, the session object is looked up by
   ID. This loads the session-specific secret state necessary to
   finalize authentication.
5. the request is authenticated, using the session-specific secret
   state.
6. if authentication succeeds, the current query is cancelled.

Finally, a standard PostgreSQL error payload is returned to the client
over the same cancellation TCP connection, and the connection is closed.

Note that if there is any error encountered during the steps 1-6, we
hide this error from the client by sending a standardized payload back
in any case. This ensures that a malicious attacker (or legit
pentester) cannot learn details about the infrastructure by blasting
randomly crafted cancellation request at a server.

Finally, note that the RPC implemented for step 3 is also available
over (authenticated) HTTP. This gives access to web-based
shell to the cancellation protocol.

## Request authentication

From a cryptographic perspective, the security of the authentication protocol uses
the following properties across all variants of the proposal, detailed below:

- there is a shared secret between the client and the server, different
  for every session.
- cancellation requests are *derived* from the session ID and the shared
  secret, but the shared secret itself is not included in the requests.

The first point is akin to PostgreSQL's use of session-specific
"cancel keys". The second point differs from PostgreSQL's and is what
makes our protocol non-replayable.

### Shared secret delivery

Conceptually, the shared secret between the client and the server
could be exchanged in two ways: inband (during the initial session
establishment) or out-of-band (using some 3rd party mechanism,
e.g. when the client receives their login credentials).

#### This proposal: inband shared key delivery

In this proposal, we propose to deliver the shared secret in-band, via
a new session variable `cancel_key`.

The secret is retrieved by the SQL shell after it has authenticated
its main session successfully.

This is solution is similar to PostgreSQL's own protocol, although we
need a session variable and cannot reuse the standard "cancel key"
protocol message, because we want to keep the ability to change the
format and size of cancel keys over time.

The in-band solution also requires confidentiality on the main SQL
connection, either using TLS or some other means.

Finally, the cancel key is also prefixed by a protocol identifier,
which tells the client which cancel protocol to use when multiple
protocols are supported side-by-side.

#### Alternative: out-of-band shared secret

We are not exploring out-of-band shared secret delivery for
cancellation at this time, as it would yield a more complex
implementation and the benefits are unclear.

(An idea could be to use properties of the TLS client certificate used
to authenticate the connection. But this idea was not explored further.)

### Structure of cancel requests

The payload passed via `crdb:cancel_request` by a client contains multiple parts separated by `:`:

- the session ID,
- a session-specific cancel request, composed of:
  - a protocol identifier,
  - one or more additional protocol-specific parts.

Since it's possible for a single cluster to support multiple cancel
request protocols, we make the protocol identifier relative to the
session.

### Authentication protocols

We propose 2 different low-level protocols to authenticate cancellation requests:

- a “simple” protocol without replay protection, where the shared
  secret is used directly as cancel request. We would use this in unit tests.
- a HMAC-based protocol with nonces for replay protection, enabled by
  default in deployments.

Note that we also have considered (but are not proposing) a TOTP-based
protocol that uses shared time between the client and the server for
replay protection. This is detailed further in the [alternatives
section](#totp-based-protocol) below. We mention this now because it
motivates the particular choice of API in the implementation, as a
hint of what possible future protocols could need.

#### Simple protocol

In this protocol, the client copies the `client_key` received on the
main connection into the cancel requests as-is.

The server authenticates by simply comparing the string received
from the client with the one attached to the session. The cancel
request is accepted if they are equal.

This protocol can only be made safe against replayability if we require
the delivery of cancel requests over TLS. Since we would prefer to avoid
the TLS requirement, we do not make this protocol the default. Instead,
it is implemented for unit testing.

#### HMAC-based protocol

In this protocol, the server maintains an integer counter alongside
the shared secret. The counter is incremented at every authentication
attempt and serves as nonce.

A cancel request is then computed as a HMAC of the nonce and session
ID using the shared secret.

The client then needs to preserve up-to-date knowledge of the nonce.

In the common case, both client and server start with the same value
and increment the nonce at the same pace, so they should match by
default.

However, if a malicious user (or a bug in an unrelated client) sends
an invalid cancel request, the server will increment its nonce but the
legitimate client will not, causing them to become out of sync. We
need a protocol to re-sync them in the legitimate client.

For this, we rely on the standard PostgreSQL error payload structure:
this provides an optional "Detail" field. For cancel requests,
the error returned by the server to the client contains a copy of the new
server-side nonce in the "Detail" field. We also choose a particular
value of SQLSTATE in the error payload to tell the legitimate client
that it should *retry the cancellation* with a different nonce.

The client SQL shell updates its local copy of the nonce from that
error payload and retries as necessary.

## Implementation outline

The following PR contains a MVP of the above proposal: https://github.com/cockroachdb/cockroach/pull/75211

The outline of the implementation is thus:

1. we abstract the cancellation protocol though Go interfaces, so that
   it can be implemented in multiple variants, in the `security`
   package. This API is described in a sub-section later.

2. we implement the two variants of the protocol as instances of the
   common interface: `SimpleCancelProtocol` and `HMACCancelProtocol`.

3. server-side, we instantiate the `CancelRequestAuthenticator` object in every
   new SQL session, in `(*connExecutor) run()`. This takes
   care of minting a new shared secret.

   We also define a new SQL session var `cancel_key` to retrieve
   the shared secret.

4. client-side, we retrieve the `session_id` (previously implemented)
   and `cancel_key` (new) when sessions are established in the `clisqlclient.Conn` object.

   We instantiate the `CancelRequestClient` object of the protocol
   and attach it to the connection object.

5. client-side, we implement a new method `CancelCurrentQuery()` on the
   `clisqlclient.Conn` object, which uses the new
   `CancelRequestClient` to mint cancel requests, inject them in a
   connection URL via the `crdb:cancel_request` parameter, and open a
   new SQL connection with that.

   This tricks any off-the-shelf SQL driver to mint an appropriate
   `StartupMessage` and send it to the server.

   We also handle the error received back from the server, and deliver
   the error "Detail" payload, if any, to the `CancelRequestClient`.

6. client-side, we implement a signal handler for SIGHUP (Ctrl+C) in
   the interactive shell, to call the new `CancelCurrentQuery()`
   method on the client connection object.

   At this point, notice how all the client-side logic has become
   independent of the particular authentication protocol. This makes
   us able to introduce new protocols in the future without
   much engineering effort.

7. server-side, in the `pgwire` package we add a special case when parsing client parameters
   in `StartupMessage`, to recognize `crdb:cancel_request` and interrupt
   the normal connection logic when that is seen.

   We send this request off to the `sql.DoCancelRequest()` function
   for processing.

8. server-side, in `sql.DoCancelRequest()` we first use a semaphore (from `quotapool`) to
   rate-limit cancellation requests.

   Then we forward the request to the local `server.SQLStatusServer` instance
   that can handle the routing of the query to the appropriate target node.

9. server-side, we implement a new method on `SQLStatusServer` called `CancelQueryByKey()`
   which takes a cancel request composed of a `SessionID` and `CancelRequest` strings.

   We implement both multitenant (`tenant_status.go`) and
   non-multitenant (`status.go`) variants of this method.

   This extracts the target node ID from the `SessionID` field, and routes
   the request either locally (same node) or via an inter-node RPC.

   We also provide a HTTP alias for this method
   (e.g. `/_status/cancel_query_by_key`, send arguments as JSON in a
   POST message), so that a web-based SQL shell can also issue
   cancellation requests.

10. server-side, for the target node in the handler for the RPC,
    we route the request to the local `SessionRegistry`
    via a similarly named `CancelQueryByKey()` method.

11. server-side, we implement `(*SessionRegistry).CancelQueryByKey()`
    by a lookup of the target session using the `SessionID`, and then
    delegating the final step to a `cancelQueryByKey()` method on the
    `registrySession` interface.

12. server-side, in the implementation of `registrySession` by `sql.connExecutor`,
    we use the `Authenticate()` method of the `CancelRequestAuthenticator` interface
    to first validate the cancel request, and if that succeeds we effectively
    cancel the query.

13. server-side, we flow the following results from step 12 back to
    the custom pgwire code from step 7 above, via a `CancelQueryByKeyResponse` payload:

    - whether a query was canceled.
    - whether the client should retry the cancel request.
    - an optional authenticator update string, used e.g in the HMAC protocol.
    - an optional error.

14. server-side, in the `pgwire` package, we *log the detailed result
    of the cancel operation* if logging is enabled (via
    `server.auth_log.sql_cancellations.enabled`). This is important for
    troubleshooting.

    Then we mint a custom client error payload:
    - regardless of whether an error happened or not, the client
      always receives the same error string. This limits the ability
      of a malicious attacker to explore the infrastructure.
    - only if a retry was requested by the authenticator, a custom
      SQLSTATE is used to indicate this fact.
    - the authenticator update message, if any, is included
      in the "Detail" field of the payload.

    Then we send this message back to the client.

## Authenticator API

We define the following Go API to make our cancellation protocol extensible:

```go
// CancelRequestAuthenticator is the object stored per-session on the
// server, which is able to authenticate incoming query cancellation
// requests.
type CancelRequestAuthenticator interface {
    // Initialize resets and prepares the authenticator.
    // sessionID is the session for which this authenticator
    // is validating cancel requests.
    // (We are not using the sql.ClusterWideID type in order
    // to avoid a dependency cycle.)
    Initialize(sessionID string) error

    // GetClientKey retrieves the client key to provide
    // to the client when it initially opens the session.
    GetClientKey() CancelClientKey

    // ParseCancelRequest turns a string converted from
    // CancelRequest.String() back into a CancelRequest.
    ParseCancelRequest(string) (CancelRequest, error)

    // Authenticate is the method to call when a cancel request is
    // received by a server, to determine whether the cancel request
    // is valid. The sessionID argument is the target session requested
    // by the client.
    Authenticate(ctx context.Context, sessionID string, request CancelRequest) (ok, shouldRetry bool, update ClientMessage, err error)
}

// CancelClientKey is the type of a cancel client key.
type CancelClientKey interface {
    fmt.Stringer
}

// CancelRequest is the type of a cancel request.
type CancelRequest interface {
    fmt.Stringer
}

// ClientMessage is the type of the payload sent
// back from the authenticator running on the server,
// to the CancelRequestClient running on the client.
// It should be provided to the CancelRequestClient via
// the UpdateFromServer() method.
// CancelRequest is the type of a cancel request.
type ClientMessage interface {
    fmt.Stringer
}

// CancelRequestClient is the object that should be instantiated
// by SQL clients that wish to issue cancel requests to a server.
type CancelRequestClient interface {
    // MakeRequest creates a new cancel request for the current session.
    MakeRequest() (sessionID string, req CancelRequest)

    // Parse converts the result of ClientMessage.String() on the server
    // back into a ClientMessage on the client.
    ParseClientMessage(s string) (ClientMessage, error)

    // UpdateFromServer should be called by the client when
    // receiving a client message in response to an Authenticate
    // call server-side.
    UpdateFromServer(srvMsg ClientMessage) error
}

// CancelRequestProtocol is the type of protocol to use
// to validate cancel requests. Must match between server and client.
type CancelRequestProtocol string


// NewCancelRequestAuthenticator returns a CancelRequestAuthenticator
// for the given protocol.
func NewCancelRequestAuthenticator(proto CancelRequestProtocol)

// NewCancelRequestClient returns a CanceRequestClient
// suitable for minting cancel requests given a client
// key representation provided by a server.
// sessionID is the session for which this client operates.
// It should match the sessionID provided to the server-side
// authenticator.
// clientKey is the client key received from the server
// when the session was opened. It should be the
// same as returned by GetClientKey() on the authenticator.
func NewCancelRequestClient(sessionID string, cancelKeyRepr string) (CancelRequestClient, error)
```

## Possible optimizations

- when using a CockroachDB-specific pg connection pool, e.g. SQL proxy
  in the CC infrastructure, it's possible to use the node ID bits in
  the session ID in the cancel request to route the request directly
  to the right server, so as to avoid the cross-node RPC.

## Drawbacks

- Does not enable the pg standard `psql` client to cancel CockroachDB queries.
- Needs customization in other SQL shells in the ecosystem. For this, we can
  work with our partners (e.g. we already have a relationship with DBeaver developers and others).

## Alternatives

The following alternatives were considered.

### Plain SQL

CockroachDB already supports `SHOW SESSIONS` and `CANCEL QUERY`.

So we can implement inband query cancellation in a SQL shell as follows:

1. generate a random `application_name` every time the SQL shell logs
   in, or use some other custom session var that is supported by `SHOW
   SESSIONS`.
2. when the user wishes to interrupt a query, make the SQL shell do the following:
   - open and authenticate a 2nd session.
   - run `SHOW SESSIONS`
   - filter the output to identify the main session using the identification
     string generated at step 1.
   - take the query ID from the output of `SHOW SESSIONS`, and pass it
     into a `CANCEL QUERY` statement.
   - close the 2nd session.

This approach seems advantageous at first because it does not require
any server-side changes.

This approach was rejected for three reasons:

- its latency is very large. Its network overhead is large too.
- it would pollute any monitoring system used to gauge SQL session
  usage and other observability screens about running queries, by
  introducing transient "control" sessions used just for cancellation.
- it forces the client to go through an authentication cycle, and we
  have [identified
  previously](#self-imposed-from-research-no-full-authentication) that
  we would prefer to avoid this.

### PosgreSQL cancel requests

We could try to implement PostgreSQL's own cancellation request protocol,
described here: https://www.postgresql.org/docs/13/protocol-flow.html#id-1.10.5.7.9

This has a long history at Cockroach Labs:

- GH [sql: support pgwire query cancellation #41335](https://github.com/cockroachdb/cockroach/issues/41335),

  This GH issue ID is incidentally reported to telemetry every time a
  pg-native cancel message is received by CockroachDB. This has helped
  us recognize that many folk attempt to use this via non-CockroachDB
  SQL shells and probably experience frustration when they see it is ineffective.

- Rafi's previous work at [sql/pgwire: implement pgwire query cancellation #67501](https://github.com/cockroachdb/cockroach/pull/67501)
  and the subsequent discussion.

This alternative is largely explored and discussed already in the
linked PR thread. In a nutshell, **pg's protocol does not give us
enough room for safe cancel requests that would work well with
CockroachCloud**.

It only provides a non-extensible 64 bits of payload, without any
other authentication header nor space for more client
parameters. Within that space, we would need to specify securely the
target CC cluster (currently identified by 128-bit UUIDs), and the
target session (currently identified by 96 bit per-node session IDs +
32 bits node ID).

There are multiple compromises possible within the 64-bit constraint,
but any combination of them make us profoundly unhappy:

- we'd need to mandate mTLS, which adds overhead and latency.
- we'd need to give up on non-replayability.
- we'd need to broadcast received cancel requests across all nodes in
  a cluster, to discover which server should handle it, incurring
  non-trivial traffic within the cluster.
- we'd need to coordinate shared state across CC SQL proxies to
  remember how cancellation keys map to target clusters, including
  possibly across regions, in turn a complex and previously-unneeded
  development of the CC infrastructure.

Note that even in the non-CockroachCloud case, when SQL clients have
direct access to a SQL load balancer in front of CockroachDB nodes for
a single cluster, we *still do not have enough space*. This is because
we still need to identify the target node (32 bits) and the session
(currently 96 bits, could _perhaps_ be reduced to just 64 or even 32),
*and also a unique cancellation key* of sufficient size to [ensure
that the protocol remains
tamper-proof](#security-related-tamper-resistance-and-non-replayability).

Our industry has established that any cancellation key smaller than
16-32 bits would be so small as to make sessions vulnerable to
unwanted interference. That authentication key is simply not optional!
So we need to identify sessions *across the entire cluster* with just
the 32-48 bits that remain.

Assuming we continue to aim supporting clusters with
hundreds/thounsands of nodes (8-11 bits node IDs), and tens of
thousands of sessions per node (16+ bits session IDs), we would need
to implement extremely aggressive reuse of identifiers, which in turn
would create new bottlenecks in our architecture: we'd likely need to
persist session IDs and/or introduce broadcasts to look them up across
the entire cluster. These are major architectural changes that we
cannot consider lightly.

The step forward from there, which enables the present RFC, is the
realization that we may not need to use pg's native cancel message at
all, and so we may not be restricted on the size of the cancel requests.

### TOTP-based protocol

This is an alternative to the nonce-based protocol described in the main proposal.

In this proposal, we assume that both the SQL shell and the remote
server have synchronized clocks (with less than 1-3 second drift).

To craft a cancel request, we compute a HMAC of the shared secret and
the current time rounded to a target precision (say 1s precision if
that's the drift we want to tolerate).

To verify a cancel request server-side, we compute both HMACs for the
current time rounded up and down, and then accept the request if the
client HMAC matches either of them.

The advantage of this approach is that it avoids the nonce synchronization
via a `ClientMessage` back to the client in the error payload.

The drawback is that it requires the client to have synchronized
clocks.  Our experience in the field suggests this is a fraught
assumption, as end-users of SQL shells may use them on desktop
computers where clock synchronization is not always guaranteed. TOTP
failures due to wrongly aligned clocks are often non-deterministic and
harder to troubleshoot than crisp mismatches in the HMAC protocol.

### Target the query in the request, not the session

This proposal (and also PostgreSQL's own protocol) makes cancel
requests target a *session*, not a particular *query*.

This creates a theoretical failure mode: if a client sends a cancel
request while query A is running, then query A terminates *before the
server receives the cancel request*, then the client starts query B,
it's possible for the cancel request to end up cancelling query B
instead of A.

If we cared about this problem, we could imagine changing the protocol
so that the identifier in the cancel request would be that of the
*query*, not the *session*. However, that change in turn raises a
difficult question: how does the SQL client learn what the query ID of
each query is? Here we're stuck, as the pgwire protocol does not give
us "space" to transmit a query ID on the main connection. Or perhaps
should we use a sequence number? But then we have unpleasant questions
about how to keep this number in sync between server and client.

Now, we should take a step back: is this a problem in practice?

It's useful to remember how an interactive SQL shell works:

1. print a prompt
2. read the user input
3. execute the user input and print results
4. go back to step 1.

With this simple REPL structure, adding support for query cancellation looks like this:

1. start ignoring Ctrl+C, wait for any previous Ctrl+C processing to complete.
2. print a prompt
3. start handling Ctrl+C as "erase current input, put cursor at start of line"
4. read the user input
5. start handling Ctrl+C as "cancel current query"
6. execute the user input and print results
7. go back to step 1.

With this structure, a cancellation query sent as a side effect of
sending Ctrl+C for query A, would be waited on automatically by the
shell structure at step 1, before printing the prompt again. So it's
not possible for the user to issue query B before the cancellation
response is received back from the server.

The structure of an interactive shell already naturally enforces the
sequencing of queries and cancellation requests, so we don't have a
direct need to identify queries in the request.

# Explain it to folk outside of your team

The proximate benefit of the proposal is that end-users can now press
Ctrl+C in their interactive shell and see their query canceled, and
then continue to use their session.

There is not much more to say for the end-users of these shells!

For the **implementers of SQL shells** and client drivers, it's perhaps
worth producing a technical documentation that explains how they can
implement these cancellation themselves, in their code.

This documentation would explain the following steps:

1. retrieve the `session_id` and `cancel_key` when the session is
   established initially.

2. when a cancellation is needed, compute a cancel request as per
   the documented authentication protocol(s). We should publish the formulas and example code to do so.

3. craft a connection URL with the session ID + cancellation request
   in the parameter `crdb:cancel_request`. Optionally, disable TLS for
   additional performance (`sslmode=disable`), since the protocol is
   safe even without TLS.

   We should show examples on how to do this.

   Alternatively, for web-based shells, the implementer can craft a HTTP request
   to the cancellation endpoint instead.

4. open a connection with the regular SQL driver. Explain how this
   tricks the driver to send the cancellation.

5. handle the returned error, as specified in the documented
   authentication protocol, including retries as necessary.

# Unresolved questions

None known?
