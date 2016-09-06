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

Some versions of CockroachDB will require that a "system migration hook" be run
before the cluster is upgraded to that version. This is expected to be
infrequent; not every release will bump the version, only ones that need a
migration hook.

Migration hooks consist of a name, a work function, and optionally a minimum
version. In the steady state, there is a map in code between migration names and
the cockroach version they were released in. There is also a list of migrations
added since the last cockroach release.

When a node starts up, it checks that all known migrations have been run (the
amount of work involved in this can be kept bounded using the `/SystemVersion`
described below). If not, it runs them via a migration coordinator. (We should
also do this check when a node rejoins a cluster it's been partitioned from, but
I'm not sure how to do that.)

The migration coordinator starts by heartbeating a kv entry to ensure that there
is only ever one running at a time. Then for each missing migration, it runs the
work function and writes a record of the completion to kv
`/SystemVersion/<MigrationName>`. When finished, it writes its own version to
`/SystemVersion`.

The work function must be idempotent (in case a migration coordinator crashes)
and runnable on some previous version of the cluster. If there is some minimum
cluster version it needs, that is checked against the one written to
`/SystemVersion`.

The migration name to version map can be used to order migrations when a cluster
needs to catch up many versions.

We will introduce a thin cli wrapper `./cockroach cluster-upgrade` around
starting the migration coordinator. When upgrading a Cockroach version, this
command will be run and pointed at the cluster. After it returns, the cluster
can be rolled onto the new version.

The cli tool will be the recommended way of upgrading Cockroach versions for
production scenarios. For ease of small clusters and local development, rolling
one node will also do this upgrade before its normal startup. Any nodes rolled
after the first, but before the cluster is upgraded, will panic to make it very
clear the roll is unhealthy.

When a migration is in progress, this should be exposed in the UI. It would be
nice if we could also display the progress in the same place. Once we've used
a migration to add a `system.jobs` table, it will be used for this.

## Examples

Simple migrations, like `CREATE TABLE IF NOT EXISTS system.jobs (...)`, can be
accomplished with one hook and immediately used.

Altering system tables will require more than one migration. For example, we'd
like to remove the reference to `experimental_unique_bytes` from the
`system.eventlog` table so we can delete the function. A migration is run to add
a new column with a new name and the code in this Cockroach version sets both on
mutations and fetches both (preferring the new one) on reads. Once the entire
cluster is on this version, another code change stops reading and writing the
old value. Finally, a second mutation is run to remove the old column. This
could be done in fewer steps if SQL had something like the "unknown fields"
support in Protocol Buffers.

We currently have a series of upgrades (see
[MaybeUpgradeFormatVersion](https://github.com/cockroachdb/cockroach/blob/f6a8692485cb34dc148b9313cb9fca6c53eec42c/sql/sqlbase/structured.go#L304)
and [maybeUpgradeToFamilyFormatVersion](https://github.com/cockroachdb/cockroach/blob/f6a8692485cb34dc148b9313cb9fca6c53eec42c/sql/sqlbase/structured.go#L313))
that run whenever a `TableDescriptor` is fetched or otherwise acquired. These
are necessary so the  SQL code knows how to map it to kv entries. A system
migration hook could be used to ensure every SQL table in the system is upgraded
to the latest FormatVersion, allowing the cleanup of this migration code.

# Drawbacks

This adds administrative complexity to upgrading a CockroachDB cluster.
Requiring that one command be run before any major cluster version upgrade seems
like a small loss compared to the gained flexibility and codebase cleanliness.

An additional step is added at the end of the release process: moving the
unversioned migration names to the version map with the version just released.

# Alternatives

Currently, various upgrades are performed on an "as needed" basis: see the
`FormatVersion` example above. This has the advantage of supporting cluster
upgrades with no operational overhead, but it introduces code complexity that
will be hard to ever remove. Adding new system tables is particularly complex,
see [system.jobs](https://github.com/cockroachdb/cockroach/pull/7073) for an
example.

