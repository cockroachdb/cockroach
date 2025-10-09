- Feature Name: Always-available Users
- Status: draft
- Start Date: 2020-01-19
- Authors: knz
- RFC PR: [#44134](https://github.com/cockroachdb/cockroach/pull/44134)
- Cockroach Issue: [#43974](https://github.com/cockroachdb/cockroach/issues/43974)

# Summary

This works enables the creation of multiple accounts besides `root`
that have the same special properties: the ability to log in, monitor
and troubleshoot a cluster when system ranges are unavailable.

The reason why this is important is that "serious" deployments will
hand out separate user accounts for different people, so as to enable
auditing of "who does what". Sharing a single `root` account between
all operators is neither practical nor a security best practice.

The proposal is to achieve this by augmenting `system.users` /
`system.roles` with a gossip-supported cache. For scalability, the
cache is restricted to a subset of roles, initially including only
`admin`.

The expected impact of this change is to ease adoption by "serious"
sites (e.g., Enterprise users) and simplify the administration story
for CockroachCloud.

# Motivation

Today the list of cluster users consists of two categories:

- "Regular" users that have an entry in the KV-stored `system.users`.
  When these users access the cluster (either via the admin UI, RPC or
  CLI commands) an access to `system.users` is made to check that
  their account is valid, authenticate them (let them in) and then
  authorize them (decide which permissions they have).

- *Always-available users* which can log in and access the
  cluster without requiring a KV read to authenticate and
  authorize them.

The list of system ranges that must be available to authenticate and
authorize a regular user includes:

- for authentication: `system.namespace`, `system.users`,
  `system.role_members`, `system.web_sessions` (the latter only for
  the admin UI).
- for authorization, when they access only monitoring information:
  `system.role_members`, `system.namespace`.

When a cluster is partially unavailable, in particular when any of
these system ranges is unavailable, a regular user will find themselves
blocked trying to access, observe and manipulate the cluster. This
includes the following example operations:

- logging into the admin UI,
- running `cockroach node ls`,
- running `cockroach node status`,
- running `cockroach debug zip`.

In contrast, an always-available users can authenticate and be
authorized to perform their monitoring and administrative task without
requiring KV operations. They are thus able to do the above even when
the system ranges are unavailable.

Now, for historical reasons, the only always-available user is
currently `root`.

This is a problem, because security-conscious deployments instead wish
to delegate tasks and responsibilities to different user accounts for
auditing purposes. Additionally, when CockroachDB will be extended in
the future to support finer-grained monitoring permissions, the need
eixsts to devolve specific administration privileges to specific
users.

These requirements cannot be met if any other user than `root` "finds
themselves blocked at the door" and cannot even log in.

# Guide-level explanation

Previously, only `root` could access and monitor a
unavailable cluster. With this change enabled, all users
with the `admin` role can access and monitor when the cluster is
partially or fully unavailable.

# Reference-level explanation

The rest of this section is written with the assumption that the
cluster is partially unavailable, in particular that KV operations on
the `system` database and other SQL databases and tables are
impossible.

The RFC also works under the following assumptions:

- regular user accounts can be numerous (tens of thousands).
- only few (hundreds or less) of these accounts are meant
  to access and monitor/operate an unavailable cluster.
- small propagation delays in updates to user accounts and
  cluster-level privileges (e.g. membership to the `admin` role) can
  be tolerated.

These assumptions enable *an in-memory eventually-consistent cache for
always-available users*.

To achieve this, it creates a new system config table
`system.cached_users`, which *caches* a subset of the users and their
privileges:

```sql
CREATE TABLE system.cached_users(
  username           STRING NOT NULL,
  hashedPassword     STRING,
  roles              []STRING NOT NULL,
  -- the following fields are to be added only when the corresponding
  -- features are added to CockroachDB in the future:
  expiration         TIMESTAMPTZ, -- user expiry
  cluster_privileges []STRING,    -- fine-grained monitoring privileges
)
```

How this works:

- While the system ranges are available:

  - When a user account is added,
    removed, updated *and the user is a member of the `admin` role*,
    then the user update code now updates `system.cached_users` in
    addition to the regular places.

  - When a user account is added or removed from the `admin` role,
    entries are added and removed (respectively) from
    `system.cached_users`.

- The table is propagated via gossip and is thus available in RAM
  on each node without requiring available ranges.

- When a user accesses the cluster via SQL or the CLI, the information
  therein is looked up first (before `system.users` and the other
  tables) and, if an entry is found, is used *in lieu* of the
  information stored in the other places. This is possible even when
  the KV operations on stored data are impossible.

## Access to the admin UI

The Admin UI is special in that it does authentication via session
cookies. This works as follows:

1. when a user accesses the admin UI the first time (their browser does
   not have a cookie yet) they are redirected to the "login" page.
2. submitting their username/pw to the login form performs a
   `UserLogin` request, which accesses `system.users` to look up and
   validate the password.
3. `UserLogin` then creates and stores a *session token* into
   `system.web_sessions`, and provides a web cookie back to the client
   browser.
4. on subsequent page accesses, the client browser sends the web
   cookie alongside with its requests. For each request, the
   HTTP server looks up whether the cookie is valid by performing
   an access to `system.web_sessions`.

The solution outlined above would un-block steps 1 and 2 on
unavailable clusters, however these accesses to `system.web_sessions`
would remain blocked.

The RFC is considering either of the following three solutions.
Which one will be selected depends on experimentation and discussion.

1) a *RAM cache* for web auth tokens *alongside* `system.web_sessions`; either:

   1.1) a new gossip table `system.cached_web_sessions` alongside
        `system.cached_users`.

   1.2) a RAM-only cache of web auth tokens on each node, not
        propagated by gossip.

2) moving `system.web_sessions` to become a system config table
   propagated by gossip.

In both cases 1.1 and 1.2, the web auth cache would operate as
follows: when a user logs in to the admin UI and the auth code finds
them in `system.cached_users` instead of `system.users`, an entry is
added to the web auth cache *instead* of `system.web_sessions` so as
to avoid a KV operation entirely. Conversely, when a token is
presented by the client browser, it is looked up in the cache
before the stored table.

The difference between 1.1 and 1.2 is that 1.1 would make the Admin UI
tolerate web load balancers but is more complex, whereas 1.2 is
simpler but would force the user to log in anew when the web balancer
switches between nodes.

Solution 2 does not require change to the web login/auth code and
works through load balancers transparently but requires thinking about
a transition period for the migration.

In all three cases, putting the web tokens in RAM requires
to think carefully about scalability.

I (knz) think it is reasonable to expect that regardless
of the total number of users in the cluster, there won't be
more than hundreds or less *simultaneous* active sessions
on the admin UI (or active within the same one-hour period).

Observing that web tokens are only valid for one hour each, we can
then limit the size of the RAM cache or gossiped table by culling it
periodically to only include non-expired, non-revoked web auth tokens.

## Rationale and Alternatives

### Status quo: `root` remains the only always-available user

This is likely to block adoption by Enterprise users who wish to
devolve responsibility to monitor a cluster to multiple operators.

In a future world where CockroachDB has fine-grained cluster
monitoring permissions, this is even more problematic: we want to give
the ability to operators to log in and monitor the cluster without
granting them privileges to access and modify any SQL table, which
`root` can do.

### Trusting the TLS client certificates

Today CockroachDB already requires CLI and RPC clients to present
a valid TLS client certificate. A certificate contains:

- the username,
- an expiry date,
- arbitrary additional fields, which could be used to
  list their CockroachDB role memberships and additional
  cluster-level privileges.

Today, the certificate is only used for *authentication* (let
users in). *Authorization* (who can do what) is still done via SQL
lookups to `system.role_members`. We could change that to trust the
information inside the certificate to do authorization too.

We could also make it possible to use a client TLS certificate in a
web browser to access the Admin UI; then have the UI code recognize
the information in the cert *instead* of the login form + web auth
token mechanism.

This would not require the addition of any system or gossip-supported
table nor RAM caches.

However, any deployment of TLS certificates needs a *revocation
mechanism*: a list of certs who are forbidden from use even though
their expiration date has not passed yet.

Such a revocation mechanism needs to be globally available throughout
the cluster. To tolerate system range unavailability, its
implementation would in turn require a gossip-based solution and a RAM
cache.

So really enabling TLS certs to be used for
authentication/authorization is not truly an alternative. Enabling TLS
certs would not provide a different way to achieve always-available
users. It can be seen instead as an additional feature, which this RFC
considers to be out of scope.

### External authn/authz connectors

We could replace (or supplement) CockroachDB's `system.users` and
`system.role_members` by an external source, which does not depend on
cluster availability. There are multiple relevant industry standards:

- [LDAP](https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol)
- [RADIUS](https://en.wikipedia.org/wiki/RADIUS)
- Active Directory via LDAP

Access to any and all of these systems could be enabled by a single
module in CockroachDB able to operate with the host OS'
[PAM](https://en.wikipedia.org/wiki/Pluggable_authentication_module)
subsystem.

In such a world, upon a client connection CockroachDB would consult
these external systems *before* it tries its own. The external
system would help decide both authentication (whether to let in)
and authorization (role memberships, privileges).

A CockroachDB-specific web authentication token cache [as described
above](#Access-to-the-Admin-UI) would still be needed in this
solution for the admin UI.

The reason why this alternative is rejected is twofold:

- the aforementioned industry standard solutions require complex
  network setups, especially when they span multiple geographical
  regions. The resulting setup is brittle and any casual
  users of distributed LDAP/RADIUS deployments can attest that
  failure modes are hard to troubleshoot.

- we want new adopters of CockroachDB users, including *and
  especially* casual users who are experimenting with the tech, to
  have a good experience when they are checking "how resilient"
  CockroachDB really is. Requiring a complex setup of an external
  service would run against this use case.

## Unresolved questions

- which of the 3 solutions to adopt for web sessions
