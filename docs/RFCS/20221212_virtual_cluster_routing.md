- Feature Name: Virtual cluster routing (update)
- Status: completed
- Start Date: 2022-12-12
- Authors: jeff swenson, knz
- RFC PR: [#93537](https://github.com/cockroachdb/cockroach/pull/93537)
- Cockroach Issue: [#92526](https://github.com/cockroachdb/cockroach/issues/92526), [CRDB-22385](https://cockroachlabs.atlassian.net/browse/CRDB-22385)

# Summary

This proposal extends the rules for virtual cluster (VC, also
sometimes known as "tenant") routing in a way that restores the
ability for end-users to connect to SQL database whose name contains
periods.

It also carves out extensibility mechanisms so we can modify our
routing protocol later without interfering with client app
expectations.

In a nutshell, the proposal is to NOT attempt to find VC routing
information in the client-provided DB name unless strictly necessary.

Example URLs:
- using hostame (SNI):  `postgres://app.example.com/defaultdb` vs `postgres://system.example.com/defaultdb`
- using option: `postgres://example.com/defaultdb?option=--cluster=app` vs `postgres://example.com/defaultdb?option=--cluster=system`
- using db name: `postgres://example.com/cluster:app/defaultdb` vs `postgres://example.com/cluster:system/defaultdb`

# Motivation

VC routing was discussed most recently here:
https://github.com/cockroachlabs/managed-service/pull/7411/files

However this design does not allow for DB names containing periods, as
discussed here: https://github.com/cockroachdb/cockroach/issues/92526

Then concurrently we also want to design a mechanism coherent with the
VC routing in shared-process deployments, as per
https://github.com/cockroachdb/cockroach/pull/92580


# Technical design

Currently VC routing (as per the SQL proxy) comingles routing
details provided via the host name, the `--cluster` connection option,
and the database name. The details are extracted from all 3 sources
simultaneously, and then checked for consistency.

The proposal as detailed below proposes to _order_ the three sources
of routing details, and only check the client-provided db name when strictly necessary.

## Requirements on the shape of the solution

1. We want to preserve compatibility with existing CC serverless
   deployments, including connection strings embedded in client apps.
   All points below should be evaluated with this requirement in mind.
2. In addition to the above, we're adding the requirement that there
   must always be _some way_ for a user to connect to a database
   containing periods in its name, since those db names can be created
   in SQL ("if one can create a db with a given name, then one must
   also be able to connect to it").
3. We're also adding the requirement that we should reserve _today_ a
   mechanism for _future_ extensions to our routing protocol, in a way
   that will guarantee 1) we don't require currently developed client
   apps to change their URLs later 2) we don't create a situation that
   impede our other requirements in this list.
4. We also would like to limit the number of _documentation_
   differences between serverless and dedicated/SH, including how
   connection strings are constructed in the multi-tenant case
   (#92580). We should not aim for completely divergent mechanisms in
   SH/dedicated.
5. in the dedicated/SH case using cluster virtualization (NOT serverless), we want to:
   - support routing to a "default" VC, i.e. it must be possible
     to connect without routing details, at least to provide
     backward-compatibility with apps developed for previous versions.
   - support routing based on the _name_ alone, *without the ID*, so
     that we can use VC renaming to redirect client app traffic
     from one VC to another (this will come handy in C2C
     replication failover).
6. in the SH case (not CC dedicated/serverless), we will continue to
   support customers who cannot customize their DNS to use SNI. For
   those, a non-SNI routing method must continue to be available.

### Some useful properties we can take advantage of

- in CC serverless, regarding the ergonomics of routing:
  - we already planned to make SNI the main way to provide routing
    information. The number of client drivers that don't support SNI
    was already small in 2016 and we're expecting they will die out.
  - for all non-SNI cases, the `--cluster` option is our main
    documented fallback.
  - A database-based routing is a fallback of a fallback. **We only
    need to consider the database name for routing if neither SNI nor
    cluster option is present in the URL**.

- regarding cluster names:
  - in CC serverless, cluster names match the regexp
    `^[a-z0-9][a-z0-9-]{4,98}[a-z0-9]$`, i.e. they can't contain a
    period and never start or end with a hyphen, with a min of 6
    characters and a max of 100.
  - in SH/dedicated, as per
    [#93269](https://github.com/cockroachdb/cockroach/pull/93269),
    VC names match a similar regexp
    `^[a-z0-9]([a-z0-9-]{0,98}[a-z0-9])?$` (i.e. same overall
    structure, but minimum of 1 character instead of 6)

- regarding routing to the right host cluster:
  - we expect the main mechanism for host cluster routing to be DNS
    (different host clusters will have different proxy instances)
  - in the future use case where a VC is migrated from one host
    cluster to another, the SQL proxy will use the VC directory to
    resolve the `<cluster_name>-<tenant_id>.<original_host_id>` to a
    specific VC within the host. The VC directory
    stores the mapping in the k8s api server and the api server state
    is maintained by the Cockroach Cloud control plane.
  - **no current serverless user embeds a routing ID in their
    connection URLs other than in the hostname part** (i.e. routing ID
    is not currently included in –cluster nor db name) so as of this
    writing we still have flexibility on where/how we support
    introducing the routing ID in a URL, should we want/need to
    support a mechanism other than SNI.

### Proposal

In serverless proxy, we would change the routing logic as follows:

1. First look at provided pg options.
   - If `--cluster` is present, use that. Expect it to match the
     structure `<clustername>-<tenantid>(\.<shortHostID>)?`. Short
     host ID is optional in that case. Its separator is `.` which can
     never occur inside a cluster name. Don't look at anything else.
     The client-provided database name is preserved as-is.

    Note: the specific syntax to embed the routing ID in `--cluster`
     does not need to be finalized at this time. We can just say that
     `--cluster` has way more extension points than the db name
     because there is no prior use.

   - Otherwise, fall through.

2. Then look at provided SNI hostname. Parse it with the regexp
   `<clustername>-<tenantid>(\.<shortHostID>)?`
   - If the regexp matches, use that for routing and stop here. The
     client-provided database name is also preserved as-is.
   - Otherwise, fall through. This will incidentally also include
     connections to `free-tier.gcp.xxx` (that don't also include a
     `--cluster` option) because free tier URLs do not contain a
     number after the `free-tier` prefix.

3. Then (fall back without option nor SNI) consider the database name. At this point, because this is a fallthrough case, we *need* routing details. In that case:
   1. Extract everything up to the _first_ period in the database
      name. Strip that prefix - the remainder (including any
      additional period) will remain as client-provided database name.
   2. Inside that extracted prefix, inspect the structure:
      - If it matches the regexp for `<clustername>-<tenantid>`, use
        that (this is the free tier compatibility case).
      - _for now_ refuse anything else with an error. We could (but
        choose not to, for now) extend this to support the routing ID
        in there, as detailed in the appendix later below.

Examples for serverless:

| URL                                                                              | Resulting Configuration                                    | Notes                                           |
|----------------------------------------------------------------------------------|------------------------------------------------------------|-------------------------------------------------|
| `postgres://10.20.30.40/foo.bar?options=--cluster=sometenant-100`                | cluster=`sometenant-100`  dbname=`foo.bar`                  | Cluster option only                             |
| `postgres://10.20.30.40/foo.bar?options=--cluster=sometenant-100.xyz`            | cluster=`sometenant-100`  host=`xyz`  dbname=`foo.bar`      | Cluster option only                             |
| `postgres://10.20.30.40/blah-100.baz?options=--cluster=sometenant-100`           | cluster=`sometenant-100`  dbname=`blah-100.baz`             | Cluster option prevails over db name            |
| `postgres://free-tier.gcp/blah-100.baz?options=--cluster=sometenant-100`         | cluster=`sometenant-100`  dbname=`blah-100.baz`             | Cluster option prevails over db name            |
| `postgres://free-tier.gcp/blah-100.baz?options=--cluster=sometenant-100.xyz`     | cluster=`sometenant-100`  host=`xyz`  dbname=`blah-100.baz` | Cluster option prevails over db name            |
| `postgres://mytenant-100.gcp.xxx/blah-100.baz?options=--cluster=othertenant-100` | cluster=`othertenant-100` dbname=`blah-100.baz`             | Cluster option prevails over SNI                |
| `postgres://mytenant-100.gcp.xxx/blah-100.baz`                                   | cluster=`mytenant-100` dbname=`blah-100.baz`                | SNI prevails over db name                       |
| `postgres://mytenant-100.xyz.gcp.xxx/blah-100.baz`                               | cluster=`mytenant-100` host=`xyz` dbname=`blah-100.baz`     | SNI prevails over db name                       |
| `postgres://free-tier.gcp.xxx/mytenant-100.blah-100.baz`                         | cluster=`mytenant-100` dbname=`blah-100.baz`                | Unambiguous mandatory routing prefix in db name |
| `postgres://free-tier.gcp.xxx/mytenant-100`                                      | cluster=`mytenant-100` dbname=`defaultdb`                   | Unambiguous mandatory routing prefix in db name |


In the dedicated/SH case with cluster virtualization, we would
implement the routing logic as follows:

1. First look at provided pg options.
   - If `--cluster` is present, use that. Expect it to match the
     structure `<clustername>`. Don't look at anything else.
     Client-provided dbname is unchanged.
   - Otherwise, fall through.
2. Then, check the (NEW) `server.pre-serve.sni-routing.suffix` cluster
   setting.
   - If empty, fall through to point 3 below.
   - Otherwise, take its value and strip it from the right side of the
     SNI hostname. (There may be multiple names in the setting, to
     support DNS renames.) Then inspect the rest (the prefix) using a
     regexp for `<clustername>`. If it matches, use that. Then don't
     look at anything else. Client-provided dbname is unchanged.
   - Otherwise (`server.pre-serve.sni-routing.suffix` is non-empty but
     does not match), look at (NEW)
     `server.pre-serve.sni-routing.fallback.enabled`. If disabled,
     stop here with an error. Otherwise, fall through.
3. Then (fall back without option nor SNI) consider the database name field.
   - If it starts with `cluster:<clustername>` and either immediately
     ends afterwards, or is followed by `/` and optional more
     characters, strip that prefix and use the cluster name enclosed
     therein.
   - Otherwise, use the default VC, and keep the db name field
     as-is (including any and all periods).

To support the last rule above, we would also modify the SQL dialect
to reject the creation of any DB with a name that matches
`^cluster:[^/]*(/$)`

We find this restriction to be much less likely to be
backward-incompatible with existing deployments without cluster
virtualization than blocking the entire `<clustername>\.` prefix
namespace in db names.

This will also provide us a mechanism for extension, just like we
have several extension points with extra punctuation in `--cluster`.

Examples for dedicated/SH:

| URL                                                                              | Resulting Configuration                        | Notes                                                                                                              |
|----------------------------------------------------------------------------------|------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `postgres://10.20.30.40/foo.bar?options=--cluster=sometenant-100`                | cluster=`sometenant-100`  dbname=`foo.bar`      | Cluster option only                                                                                                |
| `postgres://10.20.30.40/foo.bar?options=--cluster=sometenant-100.xyz`            | connection error (routing ID not supported)    | Cluster option only                                                                                                |
| `postgres://10.20.30.40/blah-100.baz?options=--cluster=sometenant-100`           | cluster=`sometenant-100`  dbname=`blah-100.baz` | Cluster option prevails over db name                                                                               |
| `postgres://mytenant-100.gcp.xxx/blah-100.baz?options=--cluster=othertenant-100` | cluster=`othertenant-100` dbname=`blah-100.baz` | Cluster option prevails over SNI                                                                                   |
| `postgres://mytenant-100.gcp.xxx/blah-100.baz`                                   | cluster=`mytenant-100` dbname=`blah-100.baz`    | SNI prevails over db name (if `server.pre-serve.sni-routing.suffix` is `gcp.xxx`)                                  |
| `postgres://mytenant-100.xyz.gcp.xxx/blah-100.baz`                               | cluster=`mytenant-100` dbname=`blah-100.baz`    | SNI prevails over db name (if `server.pre-serve.sni-routing.suffix` is `xyz.gcp.xxx`, otherwise error or fallback) |
| `postgres://someaddress/cluster:mytenant-100/blah-100.baz`                       | cluster=`mytenant-100` dbname=`blah-100.baz`    | Unambiguous presence of routing prefix in db name.                                                                 |
| `postgres://someaddress/cluster:mytenant-100`                                    | cluster=`mytenant-100` dbname=`defaultdb`       | Unambiguous presence of routing prefix in db name.                                                                 |
| `postgres://someaddress/blah-100.baz`                                            | cluster=`<default>` dbname=`blah-100.baz`       | Unambiguous absence of routing prefix in db name.                                                                  |

## Appendix: Serverless-only, optional host routing ID in database name

How to find a host routing ID in the fallback of the fallback in the
database name? Remember in that case (we're in the fallback) the
routing info _must_ be present so we don't have to support the case
where it's not included.

In that case, separate with additional punctuation. This is possible
because pg db names can contain extra punctuation. Some candidates:

- `postgres://../cluster:<shortHostID>:<clustername>-<tenantid>/<dbname>`  (compatible with the proposal above)
- `postgres://../cluster:<clustername>-<tenantid>.<shortHostID>/<dbname>`
- `postgres://../<shortHostID>:<clustername>-<tenantid>.<dbname>`  (using just a colon)
- `postgres://../<clustername>-<tenantid>:<shortHostID>.<dbname>`

## Drawbacks

None known

## Rationale and Alternatives

The main alternative considered was "do nothing". Unfortunately this
is not acceptable as we are envisioning migrating customers to cluster
virtualizatoion and we need to provide a compatibility path for those
users who already use composite database names.

# Explain it to folk outside of your team

In CC serverless the target VC can be specified in the SQL connection URL using either:

- a host name.
- a `--cluster` option.
- as a prefix in the database name.

Prior to the change proposed here, a user could use one or multiple of
these mechanisms simultaneously; if they combined them, the server
would check that the various mechanisms were _coherent_.

After this change, a user can still use _any_ of the mechanisms; however,
if multiple mechanisms are user at once only one is considered and the others
are ignored (i.e. there is no coherency check). The priority order is as follows:

1. `--cluster`.
2. hostname.
3. db name.

In addition to this change, it is now possible to connect to a database whose
name contains periods. For example:

- `postgres://mytenant-100.gcp.xxx/blah-100.baz` connects to VC
  `mytenant-100` and a database named `"blah-100.baz"`.

# Unresolved questions

None known
