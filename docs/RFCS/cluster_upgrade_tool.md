- Feature Name: Cluster Upgrade Tool
- Status: draft
- Start Date: 2016-09-15
- Authors: Daniel Harrison
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

A series of hooks to perform any necessary bookkeeping before a CockroachDB
version upgrade.

# Motivation

Backward compatibility is crucial to a database, but can introduce considerable
complexity to a codebase. All else being equal, it's better to keep backward
compatibility logic siloed from the significant complexity that is inherent in
software as complex as CockroachDB.

# Detailed design

Some versions of CockroachDB will require that a "system upgrade hook" be run
before the cluster is upgraded to that version. This is expected to be
infrequent; not every release will bump the version, only ones that need a
migration hook.

A new kv entry `SystemVersion` (ClusterVersion?) is introduced in the system
keys to keep track of which of these hooks was the last to run. It stores a
string version that corresponds to the CockroachDB release version requiring the
hook. During a migration, a second kv entry is used to heartbeat and indicate
which node is coordinating the work.

A hook will migrate only from one SystemVersion to the one directly succeeding
it. This means that a CockroachDB cluster that hasn't been upgraded for a while
may require more than one of these hooks.

When upgrading a cluster past a SystemVersion bump, the administrator is expected
to run the `./cockroach cluster-upgrade` cli command using a binary of the
version being upgraded to. For ease of deployment, rolling one node will also do
this upgrade before its normal startup. Any nodes rolled after the first, but
before the cluster is upgraded, will panic to make it very clear the roll is
unhealthy.

When a migration is in progress, this should be exposed in the UI. It would be
nice if we could also display the progress in the same place.

Some examples of operations that could benefit from adding these hooks:
- We currently have a series of upgrades (see
  [MaybeUpgradeFormatVersion](https://github.com/cockroachdb/cockroach/blob/f6a8692485cb34dc148b9313cb9fca6c53eec42c/sql/sqlbase/structured.go#L304)
  and [maybeUpgradeToFamilyFormatVersion](https://github.com/cockroachdb/cockroach/blob/f6a8692485cb34dc148b9313cb9fca6c53eec42c/sql/sqlbase/structured.go#L313))
  that run whenever a `TableDescriptor` is fetched or otherwise acquired. These
  are necessary so the  SQL code knows how to map it to kv entries. A system
  upgrade hook could be used to ensure every SQL table in the system is upgraded
  to the latest FormatVersion, allowing the cleanup of this migration code.
- New system tables could be introduced.
- System tables could be changed. We'd like to change the
  `experimental_unique_bytes` function used by the `system.eventlog` table, so
  that we can delete the function.
- The `information_schema` tables could be persisted, removing the specialness
  around leasing them.

# Drawbacks

This adds administrative complexity to upgrading a CockroachDB cluster.
Requiring that one command be run before any major cluster version upgrade seems
like a small loss compared to the gained flexibility and codebase cleanliness.

# Alternatives

Currently, various upgrades are performed on an "as needed" basis: see the
`FormatVersion` example above. This has the advantage of supporting cluster
upgrades with no operational overhead, but it introduces code complexity that
will be hard to ever remove. Adding new system tables is particularly complex,
see [system.jobs](https://github.com/cockroachdb/cockroach/pull/7073) for an
example.

