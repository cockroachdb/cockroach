- Feature Name: Cluster Upgrade Tool
- Status: completed
- Start Date: 2016-09-15
- Authors: Daniel Harrison, Alex Robinson
- RFC PR: [#9404](https://github.com/cockroachdb/cockroach/pull/9404)
- Cockroach Issue: [#4267](https://github.com/cockroachdb/cockroach/issues/4267)

# Summary

A series of hooks to perform any necessary bookkeeping for a CockroachDB
version upgrade.

# Motivation

Backward compatibility is crucial to a database, but can introduce considerable
complexity to a codebase. All else being equal, it's better to keep backward
compatibility logic siloed from the significant complexity that is inherent in
software as complex as CockroachDB.

## Examples of migrations

* Add a `system.jobs` table (#7073)
* Add root user and authentication to the system.users table (#9877)
* Remove the reference to `experimental_unique_bytes` from the
  `system.eventlog` table (#5887)
* (maybe) Migrate `TableDescriptor`s to new `FormatVersion`s and ensure
  compatibility (#7136)
* (maybe) Switch from nanoseconds to microseconds for storing timestamps and
  intervals (#9758, #9759)
* (maybe) Change the encoding of the `RaftAppliedIndexKey` and
  `LeaseAppliedIndexKey` keys (#9306) - this can be done on a store-by-store
  basis without requiring cluster-level coordination
* Switch over to proposer-evaulated kv (#6290, #6166) - this is likely to be a
  special case, where we force a stop-the-world event to make the switch
  sometime before 1.0

# Detailed design

Some versions of CockroachDB will require that a "system migration hook" be run.
This is expected to be infrequent; not every release will require migrations.

## Jargon

A "system migration hook" is a self-contained function that can be run from one
of the CockroachDB nodes in a cluster to modify the state of the cluster in
some way.

Simple migrations can be discussed in terms of which versions of the Cockroach
binary are compatible with it: "pre-migration" versions are incompatible with
the migration, "pending-migration" versions are compatible with and without it,
and "post-migration" versions require it. More complex migrations can be
modeled by repeated simple migrations.

Example: Adding a `system.jobs` table. No versions are pre-migration, because
any unknown system tables are ignored. Versions that use the table if it is
there but can function without it are pending-migration. The first commit that
assumes that the table is present begins the post-migration range.

For simplicity, we assume that at most two versions of CockroachDB are ever
running in a cluster at once. This restriction could potentially be relaxed,
but it's out of scope for the first version of this RFC.

Some migrations most naturally match a model where the CockroachDB version with
the migration hook is a post-migration version. One example is a hook to add a
new system table and code that uses the system table. Significant complexity is
avoided if it can be assumed that the table exists. These migrations should be
run before any node starts using the post-migration version.

Other migrations work better when the hook version is pending-migration. When
changing the schema of an existing system table, it's easiest to include the
code that handles both schemas in the same version as the hook that performs the
actual migration. These migrations should be run after all nodes are rolled onto
the hook version.

Our most pressing initial motivations fall into the first model, so it will be
the primary focus of this RFC.

## Short-term design

Because handling migrations in the general case is a very broad problem
encompassing many potential types of migrations, we choose to first tackle the
simplest case, migrations that are backward-compatible, while leaving the door
open to more involved schemes that can support backward-incompatible
migrations.

In the short term, migration hooks will consist of a name and a work function.
In the steady state, there is an ordered list of migration names in the code,
ordered by when they were added.

When a node starts up, it checks that all known migrations have been run (the
amount of work involved in this can be kept bounded using the `/SystemVersion`
keys described below). If not, it runs them via a migration coordinator.

The ordered list of migrations can be used to order migrations when a cluster
needs to catch up with more than one of them.

The migration coordinator starts by grabbing and maintaining a migration lease
to ensure that there is only ever one running at a time. Other nodes that start
up and require migrations while a different node is doing migrations will have
to block until the lease is released (or expired in the case of node failure).
Then, for each missing migration, the migration coordinator runs its work
function and writes a record of the completion to kv entry
`/SystemVersion/<MigrationName>`.

Each work function must be idempotent (in case a migration coordinator crashes)
and compatible with previous versions of CockroachDB that could be running on
other nodes in the cluster. The latter restriction will be relaxed in the
[long-term design](#long-term-design).

### Examples

Simple migrations, like `CREATE TABLE IF NOT EXISTS system.jobs (...)`, can be
accomplished with one hook and immediately used. To add such a migration, you'd
create a new migration called something like "CREATE system.jobs" with a
function that creates the new system table and add it to the end of the list of
migrations. It would then automatically be run whenever a version of CockroachDB
that includes it joins an existing cluster for the first time.

The example of adding the root user to `system.users` can also be accomplished
with a single post-migration version hook, meaning that it could similarly just
be added to the list of migration hooks and run on startup without concern for
what CockroachDB versions are being used by other active nodes in the cluster.

## Long-term design

While our most immediate needs don't actually require backward-incompatible
changes, there are a number of examples of changes that we'd like to make that
do. For such hooks that have pre-migration versions, we'll have to put in place
additional infrastructure to ensure safe migrations.

There are two primary approaches for this, which we don't actually have to
choose from now, but should at least understand to ensure that we don't restrict
our options too much with what we do in the shorter term.

### Option 1: Require operator intervention

The first option to support non-backward-compatible migrations is to introduce
a new CLI command `./cockroach cluster-upgrade` that gives the DB administrator
control over when migrations happen. This command will support:

* Listing all migrations that have been run on the cluster
* Listing the available migrations, which migrations they depend on, and the
  CockroachDB versions at which it's safe to run each of them
* Running a single migration specified by the admin
* Running all available migrations whose migration dependencies are satisfied
  (note that this may be dangerous if the minimum version for a migration isn't
  satisfied by all nodes in the cluster; we may want to validate this ourselves
  or at least require a scary command line flag to avoid mistakes)

The idea is that before upgrading to a new version of CockroachDB, the admin
will look in the release notes for the version that they want to run and
ensure that they've run all the required migrations. If they haven't, they'll
need to do so before upgrading. If not all the upgrades required by the
desired version are available at the cluster's current version, they may need
to first upgrade to an intermediate version that supports the migrations so
that they can run them.

If a CockroachDB node starts up and the cluster that it joins has not run all
the migrations required by the node's version, that node will exit with an
appropriate error message.

This approach gives administrators total control of potentially destructive
migrations at the cost of adding extra manual work. On the plus side, it
enables rollbacks better than a design where non-backward-compatible changes
are made automatically by the system when starting up nodes with the new
version.

The CLI tool will be the recommended way of upgrading CockroachDB versions for
production scenarios. For ease of small clusters and local development,
one-node clusters will be able to do migrations during their normal startup
process if so desired. Also, migrations with no minimum version specified can
still be run at startup (as in the short-term design) because the lack of a
minimum version can be assumed to mean that they're backward-compatible.

#### Overview of changes needed from short-term design

This approach will require a few more details to be stored in the hard-coded
migration descriptors than for the short-term solution. For each migration,
we'll have to include a list of all migrations that it depends on and the
minimum version at which it can safely be run. The data stored in the
`/SystemVersion/<MigrationName>` keys will not have to change.

This approach will also require command-line tooling to be built for the
`./cockroach cluster-upgrade` command.

#### Example

Let's consider the example of switching the storage format of timestamps from
nanoseconds to microseconds (#9759). We can't simply change the code and
add an automatically run migration to the next release of CockroachDB because
it's not safe to change the storage format of all the timestamps in a cluster
while old nodes may not know how to read the new format.

Instead, the migration might look something like this from the perspective of
CockroachDB developers:

1. Release new version of CockroachDB with:
  1. Code that can handle both encodings
  1. A non-required migration that changes the encodings of all timestamps
     in the cluster. This migration probably won't depend on any other
     migrations.
1. Some number of releases later (could be the next version, or could be
   multiple versions down the line), remove the compatibility code and switch
   the migration to be required.

From the perspective of a DB admin, it'd look like:

1. Decide to upgrade CockroachDB cluster to new version
1. Verify that all migrations for the current version have been completed by
   running `./cockroach cluster-upgrade` with the current binary
1. Check release notes for the desired version to determine whether it's safe
   to upgrade directly from their current version
  1. If it is, just carry out the upgrade
  1. If it isn't, pick an intermediate version recommended by the docs and
     restart the process for it
1. Once the upgrade has completed and rollbacks are deemed no longer necessary,
   perform the new version's migrations by running `./cockroach cluster-upgrade`

### Option 2: Do all migrations automatically

The main alternative to manual intervention is to attempt to do the operator's
work for them automatically. This would provide the best user experience for
less sophisticated users who care more about their time and energy than about
having total control.

Unlike in the backward-compatible case, we can't necessarily run
all known migrations when the first node at a new version is started up,
because some migrations may modify the cluster state in ways that are
incompatible with the other nodes in the cluster. We can never automatically run
migrations unless we know that all the nodes in the cluster are at a recent
enough version.

We could ensure this in at least two different ways:

1. Start tracking both node version information for the entire cluster and add
   some code that understands our semantic versioning scheme sufficiently well
   to determine which node versions support which migrations.
1. Start tracking the migrations each node knows about that have yet to be run.
   A migration could then be run once all the nodes know about it.

That may be some work, but once it's in place, we can go back to doing
migrations on start-up by adding in some logic that checks whether the all the
nodes in the cluster support the required migrations. If not, the new node can
exit out with an error message. If they do, then the new node can kick off the
migrations.

#### Overview of changes needed from short-term design

This approach will require the same few additional details to be stored in the
hard-coded migration descriptors as for option 1. For each migration,
we'll have to include a list of all migrations that it depends on and the
minimum version at which it can safely be run. The data stored in the
`/SystemVersion/<MigrationName>` keys will not have to change.

The main difference for this approach is that we'll have to start tracking
the CockroachDB version (or alternatively the known migrations) for all nodes
in the cluster. We could potentially use gossip for this, but have to be sure
that we know the state of all nodes, not just most of them to be confident that
it's safe to run a migration. It has also been suggested that we [use a form of
version leases for this](https://github.com/cockroachdb/cockroach/issues/10212).

#### Example

Again, let's consider the example of switching the storage format of timestamps
from nanoseconds to microseconds (#9759).

The migration will look fairly similar from the perspective of CockroachDB
developers:

1. Release new version of CockroachDB with code that can handle both encodings.
1. Include a non-required migration that changes the encodings of all timestamps
   in the cluster. This migration probably won't depend on any other migrations.
1. Some number of releases later (could be the next version, or could be
   multiple versions down the line):
  1. Add an automatically run migration that switches the encodings
  1. Remove the compatibility code for handling the old encoding

From the perspective of a DB admin, it'd look like:

1. Decide to upgrade CockroachDB cluster to new version
1. Check release notes for the desired version to see whether it's safe to
   upgrade to from the current version
  1. If it is, just carry out the upgrade
  1. If it isn't, then repeat this process for an earlier version than the one
     initially chosen in the first step before continuing with the upgrade

## User experience

When a migration is in progress, that fact should be exposed in the UI. It would
be nice if we could also display the progress in the same place. Once we've used
added a `system.jobs` table (using a simple migration), it can be used for this.

# Drawbacks

The short-term design doesn't appear to have any obvious drawbacks, as it
solves a couple of immediate needs and can be added without limiting future
extensibility.

The long-term design adds administrative complexity to the upgrade process, no
matter which option we go with. It'll also add a little extra work to the
release process for developers due to the need to include minimum versions for
migrations -- sometimes the minimum version will be the version being released.

# Alternatives

Currently, various upgrades are performed on an "as needed" basis: see the
`FormatVersion` example mentioned above. This has the advantage of supporting
cluster upgrades with no operational overhead, but it introduces code
complexity that will be hard to ever remove. Adding new system tables is
particularly complex; see the
[attempted `system.jobs` PR](https://github.com/cockroachdb/cockroach/pull/7073)
for an example.

We've also considered requiring a command-line tool to be run using the new
binary on a currently running cluster (on an older version) before upgrading it.
This has a similar admin experience to long-term design option 1, while adding
the complexity of needing to deal with multiple versions of the binary at once.

There is also always the option of requiring clusters to be brought down for
migrations, but given the importance of uptime to most users, we consider that
something to be avoided whenever reasonably possible. It will likely be needed
to switch to proposer-evaluated kv (#6290, #6166) in the pre-1.0 time frame,
but we hope that it won't be needed after that.
