- Feature Name: Version Migration
- Status: completed
- Start Date: 2017-07-10
- Authors: Spencer Kimball, Tobias Schottdorf
- RFC PR: [#16977](https://github.com/cockroachdb/cockroach/pull/16977), [#17216](https://github.com/cockroachdb/cockroach/pull/17216), [#17411](https://github.com/cockroachdb/cockroach/pull/17411), [#17694](https://github.com/cockroachdb/cockroach/pull/17694)
- Cockroach Issue(s): [#17389](https://github.com/cockroachdb/cockroach/issues/17389)


# Summary

This RFC proposes a mechanism which allows point release upgrades (i.e. 1.1 to
1.2) of CockroachDB clusters using a rolling restart followed by an
operator-issued cluster-wide version bump that represents a promise that the old
version is no longer running on any of the nodes in the cluster, and that
backwards-incompatible features can be enabled.

This is achieved through a version which can be queried at server runtime, and
which is populated from

1. a new cluster setting `version`, and
1. a persisted version on the Store's engines.

The main concern in designing the mechanism here is operator friendlyness. We
don't want to make the process more complicated; it should be scriptable; it
should be hard to get wrong (and if you do, it shouldn't matter).

# Motivation

We have committed to supporting rolling restarts for version upgrades, but we do
not have a mechanism in place to safely enable those when backwards-incompatible
features are introduced.

The core problem is that incompatible features must not run in a mixed-version
cluster, yet this is precisely what happens if a rolling restart is used to
upgrade the versions. Yet, we expect a handful of backwards-incompatible
changes in each release.

What's needed is thus a mechanism that allows a rolling restart into the new
binary to be performed while holding back on using the new, incompatible,
features, and this is what's introduced in this RFC.

# Guide-level explanation

We'll identify the "moving parts" in this machinery first and then walk through
an example migration from 1.1 to 1.2, which contains exactly one incompatible
upgrade (which really lands in `v1.1`, but let's forget that), namely:

On splits,

- v1.1 was creating a Raft `HardState` while *proposing* the split. By the time
  it was applied, it was potentially stale, which could lead to anomalies.
- v1.2 does not write a `HardState` during the split, but it writes a "correct"
  `HardState` immediately before the right-hand side's Raft group is booted up.

This is an incompatible change because in a mixed cluster in which v1.2 issues
the split and v1.1 applies it, the node at v1.1 would end up without a
`HardState`, and immediately crash. We need a proper migration -- `v1.2` must know
when it is safe to use the new behavior; it must use the previous (incorrect) one
until it knows that, and hopes that the anomaly doesn't strike in the meantime.

You can forget about the precise change now, but it's useful to see that this
one is incompatible but absolutely necessary - we can't fix it in a way that
works around the incompatibility, and we can't keep the status quo.

Now let's do the actual update. For that, the operator

1. verify the output of `SHOW CLUSTER SETTING version` on each node, to assert
   they are using `v1.1` (it could be that the operator previously updated
   the *binary* from `v1.0` to `v1.1` but forgot to actually bump the version
   in use!)
1. performs a rolling restart into the `v1.2` binary,
1. checks that the nodes are *really* running a `v1.2` binary. This includes
   making sure auto-scalers, failover, etc, would spawn nodes at `v1.2`
1. in the meantime the nodes are running `v1.2`, but with `v1.1`-compatible
   features, until the operator
1. runs `SET CLUSTER SETTING version = '1.2'`.
1. The cluster now operates at `v1.2`.

We run in detail through what the last one does under the hood.

## Recap: how cluster settings work

Each node has a number of "settings variables" which have hard-coded default
values. To change a variable from that default value, one runs `SET CLUSTER
SETTING settingname = 'settingval'`.

Under the hood, this roughly translates to `INSERT INTO system.settings
VALUES(settingname, settingval)`, and we have special triggers in place which
gossip the contents of this table whenever they change.

When such a gossip update is received by a node, it goes through the (hard-coded
list of) setting variables, populates it with the values from the table, and
resets all unmentioned variables to their default value.

To show the values of the setting variables, one can run `SHOW ALL CLUSTER
SETTINGS`. Note that the output of that can vary based on the node, i.e. if one
hasn't gotten the most recent updates yet. The command to read from the actual
table is `SELECT ... FROM system.settings`; this shows only those commands for
which an explicit `SET CLUSTER SETTING` has been issued.

Note that the `version` variable has additional logic to be detailed in the next
section.

## Running `SET CLUSTER SETTING version = '1.2'`

What actually happens when the cluster version is bumped? There are a few things
that we want.

1. The operator should not be able to perform "illegal" version bumps. A legal
   bump is from one version to the next: 1.0 to 1.1, 1.1 to 1.2, perhaps 1.6 to
   2.0 (if 1.6 is the last release in the 1.x series), but *not* 1.0 to 1.3 and
   definitely not `v1.0` to `v5.0`.

   This immediately tells us that the `version` setting needs to take into
   account its existing value before updating to the new one, and may need
   to return an error when its validation logic fails.

   This also suggests that it should use the existing value from the
   `system.settings` table, and not from `SHOW` (which may not be
   authoritative).
2. If we use the `system.settings` table naively, we may have a problem: settings
   don't persist their default value, and in particular, a new cluster (or one
   started during 1.0) does not have a `version` setting persisted. It is hard
   to persist a setting during bootstrap since the `settings` table is only
   created later. It's tricky to correctly populate that table once the cluster
   is running because all you know about is your current node, but what if
   you're accidentally running 3.0 while the real cluster is at 1.0?

We defer the problem of populating the settings table to the detailed design
section and now assume that there *is* a `version` entry in the settings table.

Then what happens on a version bump is clear: the existing version is read, it
is checked whether the new version is a valid successor version, and if so, the
entry is transactionally updated. On error, the operator running the `SET
CLUSTER SETTING` command will receive a descriptive message such as:

```
cannot upgrade to 2.0: node running 1.1
cannot upgrade directly from 1.0 to 1.3
cannot downgrade from 1.0 to 0.9
```

Assuming success, the settings machinery picks up the new version, and populates
the in-memory version setting, from which it can be inspected by the running
node.

To probe the running version, essentially, all moving parts in the node hold on
to a variable that implements the following interface (and in the background is
hooked up to the version cluster setting appropriately):

```go
type Versioner interface {
  IsActive(version roachpb.Version) bool
  Version() cluster.ClusterVersion // ignored for now
}
```

For instance, before having run `SET CLUSTER SETTING version = '1.1'`, we have

```go
IsActive(roachpb.Version{Major: 1, Minor: 0}) == true
IsActive(roachpb.Version{Major: 1, Minor: 1}) == false
```

even though the binary is capable of running `v1.1`.

After having bumped the cluster version, we instead get

```go
IsActive(roachpb.Version{Major: 1, Minor: 0}) == true
IsActive(roachpb.Version{Major: 1, Minor: 1}) == true
```

but (still)

```go
IsActive(roachpb.Version{Major: 1, Minor: 2}) == false
```

To return back to our incompatible feature example, we
would have code like this:

```go
func proposeSplitCommand() {
  p := makeSplitProposal()
  if !v.IsActive(roachpb.Version{Major: 1, Minor: 1}) {
    // Preserve old behavior at v1.0.
    p.hardState = makePotentiallyDangerousHardState()
  }
  propose(p)
}

func applySplit(p splitProposal) {
  raftGroup := apply(p)
  if v.IsActive(roachpb.Version{Major: 1, Minor: 1}) {
    // Enable new behavior only if at v1.1 or later.
    hardState := makeHardState()
    writeHardState(hardState)
  }
  raftGroup.GoLive()
}
```

Some features may require an explicit "ping" when the version gets bumped. Such
a mechanism is easy to add once it's required; we won't talk about it any more
here.

Note that a server always accepts features that require the new version, even if
it hasn't received note that they are safe to use, when their use is prompted by
another node. For instance, if a new inter-node RPC is introduced, nodes should
always respond to it if they can (i.e. know about the RPC). A bump in the
cluster version propagates through the cluster asynchronously, so a node may
start using a new feature before others realize it is safe.

## Development versions

During the development cycle, new backwards-incompatible migrations may need to
be introduced. For this, we use "unstable" versions, which are written
`<major>.<minor>-<unstable>`; while stable releases will always have `<unstable>
== 0`, each unstable change gets a unique, strictly incrementing unstable
version component. For instance, at the time of writing (`v1.1-alpha`), we have
the following:

```go
var (
	// VersionSplitHardStateBelowRaft is https://github.com/cockroachdb/cockroach/pull/17051.
	VersionSplitHardStateBelowRaft = roachpb.Version{Major: 1, Minor: 0, Unstable: 2}

	// VersionRaftLogTruncationBelowRaft is https://github.com/cockroachdb/cockroach/pull/16993.
	VersionRaftLogTruncationBelowRaft = roachpb.Version{Major: 1, Minor: 0, Unstable: 1}

	// VersionBase corresponds to any binary older than 1.0-1,
	// though these binaries won't know anything about the mechanism in which
	// this version is used.
	VersionBase = roachpb.Version{Major: 1}
)
```

Note that there is no `v1.1` yet. This version will only exist with the stable
`v1.1.0` release.

Tagging the unstable versions individually has the advantage that we can
properly migrate our test clusters simply through a rolling restart, and then a
version bump to `<major>.<minor>-<latest_unstable>` (it's allowed to enable
multiple unstable versions at once).

## Upgrade process (close to documentation)

The upgrade process as we document it publicly will have to advise operators to
create appropriate backups. They should roughly follow this checklist:

### Optional prelude: staging dry run
- Start a **staging** cluster with the new version (e.g. 1.1).
- Restore data from most recent backup(s) to staging cluster.
- Tee production traffic or run load generators to simulate live
  traffic and verify cluster stability.
- Proceed to upgrade process described above.

### Steps in production
- Disable auto-scaling or other systems that could add a node with a conflicting
  version at an inopportune time.
- Ensure that all nodes are either running or guaranteed to not rejoin the
  cluster after it has been updated.
    - We intend to protect the cluster from mismatched nodes, but the exact
      mechanism is TBD. Check back when writing the docs.
- Create a (full or incremental) backup of the cluster.
- Rolling upgrade of nodes to next version.
- At this point, all nodes will be running the new binary, albeit with
  compatibility for the old one.
- Verify no node running the old version remains in the cluster (and no new one
  will accidentally be added).
- Verify basic cluster stability. If problems occur, a rolling downgrade is
  still an option.
- Depending on how much time has passed, another incremental backup could be
  advisable.
- `SET CLUSTER SETTING version = '<newversion>'`
- In the event of a catastrophic failure or corruption due to usage of new
  features requiring 1.1, the only option is to restore from backup. This is a
  two step process: start a new cluster using the old binary, and then restore
  from the backup(s).
- restore any orchestration settings (auto-scaling, etc) back to their normal
  production values.

![Version migrations with rolling upgrades](images/version_migration.png?raw=true "Version migrations with rolling upgrades")

# Reference-level explanation

This section runs through the moving parts involved in the implementation.

## Detailed design

### Structures and nomenclature

The fundamental straightforward structure is `roachpb.Version`:

```proto
message Version {
  optional int32 major = 1;   // the "2" in `v2.1`
  optional int32 minor = 2;   // the "1" in `v2.1`
  optional int32 patch = 3;   // placeholder; always zero
  optional int32 unstable = 4 // dev version; all stable versions have zero
}
```

The situation gets complicated by the fact that our usage of the version as the `version` cluster setting is really a "cluster-wide minimum version", which informs the use of the name `MinimumVersion` in the following `ClusterVersion` proto:

```
message ClusterVersion {
  // The minimum_version required for any node to support. This
  // value must monotonically increase.
  roachpb.Version minimum_version = 1 [(gogoproto.nullable) = false];
}
```

This should make sense so far. However, discussion in this RFC has mandated that
we include an ominous `UseVersion` as well. This emerged as a compromise after
giving up on allowing "rollbacks" of upgrades. Briefly put, `UseVersion` can be
smaller than `MinimumVersion` and advises the server to not use new features
that it has the discretion to not use. For example, assume `v1.1` contains a
performance optimization (that isn't supported in a cluster running nodes at
`v1.0`). After bumping the cluster version to `v1.1`, it turns out that the
optimization is a horrible pessimization for the cluster's workload, and things
start to break. The operator can then set `UseVersion` back to `v1.0` to advise
the cluster to not use that performance optimization (even if it could). On the
other hand, some other migrations it has performed (perhaps it rewrote some of
its on-disk state to a new format) may not support being "deactivated", so they
would continue to be in effect.

This feature has not been implemented in the initial version, though it has been
"plumbed". It will be ignored in this design from this point on, though no
effort will be made to scrub it from code samples.

```
message ClusterVersion {
  [...]
  // The version of functionality in use in the cluster. Unlike
  // minimum_version, use_version may be downgraded, which will
  // disable functionality requiring a higher version. However,
  // some functionality, once in use, can not be discontinued.
  // Support for that functionality is guaranteed by the ratchet
  // of minimum_version.
  roachpb.Version use_version = 2 [(gogoproto.nullable) = false];
}
```

The `system.settings` table entry for `version` is in fact a marshalled
`ClusterVersion` (for which `MinimumVersion == UseVersion`).

### Server Configuration

The `ServerVersion` (type `roachpb.Version`, sometimes referred to as "binary
version") is baked into the binaries we release (in which case it equals
`cluster.BinaryServerVersion`, so our `v1.1` release will have `ServerVersion
==roachpb.Version{Major: 1, Minor: 1}`). However, internally, ServerVersion` is
part of the configuration of a `*Server` and can be set freely (which we do in
tests).

Similarly a `Server` is configured with `MinimumSupportedVersion` (which in
release builds typically trails `cluster.BinaryServerVersion` by a minor
version, reflecting the fact that it can run in a compatible way with its
predecessor). If a server starts up with a store that has a persisted version
smaller than this or larger than its `ServerVersion`, it exits with an error.
We'll talk about store persistence in the corresponding subsection.

### Gossip

The `NodeDescriptor` gossips the server's configured `ServerVersion`. This isn't
used at the time of writing; see the unresolved section for discussion.

### Storage (persistence)

Once a node has received a cluster-wide minimum version from the settings table
via Gossip, it is used as the authoritative version the server is operating at
(unless the binary can't support it, in which case it commits suicide).

Typically, the server will go through the following transitions:

- `ServerVersion` is (say) `v1.1`, runs `v1.1` (`MinimumVersion == UseVersion == v1.1`),
  stores have the above `MinimumVersion` and `UseVersion` persisted
- rolling restart
- `ServerVersion` is `v1.2`, runs `v1.1`-compatible (`MinimumVersion == UseVersion == v1.1`)
- operator issues `SET CLUSTER SETTING version = '1.2'`
- gossip received: stores updated to `MinimumVersion == UseVersion == v1.2`
- new `MinimumVersion` (and equal `UseVersion`) exposed to running process:
  `ServerVersion` is (still) `v1.2`, but now `MinimumVersion == UseVersion == v1.2`.

We need to close the gap between starting the server and receiving the above
information, and we also want to prevent restarting into a too-recent version
in the first place (for example restarting straight into `v1.3` from `v1.1`).

To this end, whenever any `Node` receives a `version` from gossip, it writes it
to a store local key (`keys.StoreClusterVersionKey()`) on *all* of its stores
(as a `ClusterVersion`).

Additionally, when a cluster is bootstrapped, the store is populated with
the running server's version (this will not happen for `v1.0` binaries as
these don't know about this RFC).

When a node starts up with new stores to bootstrap, it takes precautions to
propagate the cluster version to these stores as well. See the unresolved
questions for some discussion of how an illegal version joining an existing
cluster can be prevented.

This seems simple enough, but the logic that reads from the stores has to deal
with the case in which the various stores either have no (as could happen as we
boot into them from 1.0) or conflicting information. Roughly speaking,
bootstrapping stores can afford to wait for an authoritative version from gossip
and use that, and whenever we ask a store about its persisted `MinimumVersion`
and it has none persisted, it counts as a store at
`MinimumVersion=UseVersion=v1.0`. We make sure to write the version atomically
with the bootstrap information (to make sure we don't bootstrap a store, crash,
and then misidentify as `v1.0`). We also write all versions again after
bootstrap to immunize against the case in which the cluster version was bumped
mid-bootstrap (this could cause trouble if we add one store and then remove the
original one between restarts).

The cluster version we synthesize at node start is then the one with the largest
`MinimumVersion` (and the smallest `UseVersion`).

Examples:

- one store at `<empty>`, another at `v1.1` results in `v1.1`.
- two stores, both `<empty>` results in `v1.0`.
- three stores at `v1.1`, `v1.2` and `v1.3` results in `v1.3` (but likely
  catches an error anyway because it spans versions)

### The implementer of `Versioner`: `ExposedClusterVersion`

`ExposedClusterVersion` is the central object that manages the information
bootstrapped from the stores at node start and the gossiped central version and
flips between the two at the appropriate moment. It also encapsulates the logic
that dictates which version upgrades are admissible, and for that reason
integrates fairly tightly with the `settings` subsystem. This is fairly complex
and so appropriate detail is supplied below.

We start out with the struct itself.

```go
type ExposedClusterVersion struct {
	MinSupportedVersion roachpb.Version // Server configuration, does not change
	ServerVersion       roachpb.Version // Server configuration, does not change
  // baseVersion stores a *ClusterVersion. It's very initially zero (at which
  // point any calls to check the version are fatal) and is initialized with
  // the version from the stores early in the boot sequence. Later, it gets
  // updated with gossiped updates, but only *after* each update has been
  // written back to the disks (so that we don't expose anything to callers
  // that we may not see again if the node restarted).
	baseVersion         atomic.Value
  // The cluster setting administering the `version` setting. On change,
  // invokes logic that calls `cb` and then bumps `baseVersion`.
	version             *settings.StateMachineSetting
  // Callback into the node to persist a new gossiped MinimumVersion (invoked
  // before `baseVersion` is updated)
	cb                  func(ClusterVersion)
}

// Version returns the minimum cluster version the caller may assume is in
// effect. It must not be called until the setting has been initialized.
func (ecv *ExposedClusterVersion) Version() ClusterVersion

// BootstrapVersion returns the version a newly initialized cluster should have.
func (ecv *ExposedClusterVersion) BootstrapVersion() ClusterVersion

// IsActive returns true if the features of the supplied version are active at
// the running version.
func (ecv *ExposedClusterVersion) IsActive(v roachpb.Version) bool
```

The remaining complexity lies in the transformer function for
`version *settings.StateMachineSetting`. It contains all of the update logic
(mod reading from the table: we've updated the settings framework to use the
table for all `StateMachineSettings`, of which this is the only instance at the
time of writing); `versionTransformer` takes

- the previous encoded value (i.e. a marshalled `ClusterVersion`)
- the desired transition, if any (for example "1.2").

and returns

- the new encoded value (i.e. the new marshalled `ClusterVersion`)
- an interface backed by a "user-friendly" representation of the new state
  (i.e. something that can be printed)
- an error if the input was illegal.

The most complicated bits happen inside of this function for the following
special cases:

- when no previous encoded value is given, the transformer provides the "default
  value". In this case, it's `baseVersion`. In particular, the default value
  changes! This behaviour is required because when the initial gossip update
  comes in, it needs to be validated, and we also validate what users do during
  `SET CLUSTER SETTING version = ...`, which they could do through multiple
  versions.
- validate the new state and fail if either the node itself has `ServerVersion`
  below the new `MinimumVersion` or its `MinSupportedVersion` is newer than the
  `MinimumVersion`.

## Initially populating the settings table version entry

### Populating the settings table

As outlined in the guide-level explanation, we'd like the settings table to hold
the "current" cluster version for new clusters, but we have no good way of
populating it at bootstrap time and don't have sufficient information to
populate it in a foolproof manner later. The solution is presented at the end of
this section. We first detail the "obvious" approaches and their shortcomings.

#### Approaches that don't work

We could use the "suspected" local version during cluster version when no
`version` is persisted in the table, but:

- this is incorrect when, say, adding a v3.0 node to a v1.0 cluster and
  running `SET CLUSTER SETTING version = '3.1'` on that node:
  - in the absence of any other information (and assume that we don't try any
    "polling" of everyone's version which can quickly get out of hand), the node
    assumes the cluster version is `v3.1` (or `v3.0`).
  - it manages to write `version = v3.1` to the settings table.
  - all other nodes in the cluster die when they learn about that.
- it would "work" (with `v1.1`) on all nodes running the "actual" version; the
  `v3.0` node would die instead, learning that the cluster is now at `v1.1`.
- the classic case to worry about is that of an operator "skipping a version"
  during the rolling restart. We would catch this as the new binary can't run
  from the old storage directory (i.e. `v1.7` can't be started on a `v1.5`
  store).
- it would equally be impossible to roll from `v1.5` into `v1.6` into `v1.7`
  (the storage markers would remain at `v1.5`).
- in effect, to realize the above problem in practice, an operator would
  have to add a brand new node running a too new version to a preexisting
  cluster and run the version bump on that node.
- This might be an acceptable solution in practice, but it's hard to reason
  about and explain.

An alternative (but apparently problematic) approach is adding a sql migration.
The problem with those is that it's not clear which value the migration should
insert into the table -- is it that of the running binary? That would do the
wrong thing if a `v1.0` cluster is restarted into `v1.1` (which now has the
migration); we need to insert `v1.0` in that case. On the other hand, after
bootstrapping a `v1.x` cluster for `x > 0`, we want to insert `v1.x`.

And of course there is the third approach, which is writing the settings table
during actual bootstrapping. This seems to much work to be realistic at this
point in the cycle, and it may come with its own migration concerns.

#### The combination that does work

All of the previous approaches combined suggest a more workable combination:

1. instead of populating the settings table at bootstrap, populate a new key
   `BootstrapVersion` (similar to the `ClusterIdent`). In effect, for the
   lifetime of this cluster, we can get an authoritative answer about the
   version at which it was bootstrapped.
1. change the semantics of `SET CLUSTER SETTING version = x` so that when it
   doesn't find an entry in the `system.settings` table, it fails.
1. add a sql migration that
    - reads `BootstrapVersion`
    - runs
        ```sql
        -- need this explicitly or the `SET` below would fail!
        UPSERT INTO system.settings VALUES(
          'version', marshal_appropriately('<bootstrap_version>')
        );
        -- Trigger proper gossip, etc, by doing "no-op upgrade".
        SET CLUSTER SETTING version = '<bootstrap_version>';
        ```
    - the cluster version is now set and operators can use `SET CLUSTER
      SETTING`.

This obviously works if the migration runs when the cluster is still running
the bootstrapped version, even if the operator set the version explicitly  (`SET
CLUSTER SETTING version = x` is idempotent).


### Bootstrapping new stores

When an existing node restarts, it has on-disk markers that should reflect a
reasonable version configuration to assume until gossip updates are in effect.

The situation is slightly different when a new node joins a cluster for the
first time. In this case, it'll bootstrap its stores using its binary's
`MinimumSupportedVersion`, (for that is all that it knows), which is usually
one minor version behind the cluster's active version.

This is not an issue since the node's binary can still participate in newer
features, and it will bump its version once it receives the first gossip update,
typically after seconds.

We could conceivably be smarter about finding out the active cluster version
proactively (we're connected to gossip when we bootstrap), but this is not
deemed worth the extra complexity.

## Drawbacks

- relying on the operator to promise that no old version is around opens cluster
  health up to user error.
- we can't roll back upgrades, which will make many users nervous
    - in particular, it discounts the OSS version of CockroachDB

## Rationale and Alternatives

The main concern in designing the mechanism here is operator friendlyness. We
don't want to make the process more complicated; it should be scriptable; it
should be hard to get wrong (and if you do, it shouldn't matter).

The main concessions we make in this design are

1. no support for downgrades once the version setting has been bumped. This was
   discussed early in the life of this RFC but was discarded for its inherent
   complexity and large search space that would have to be tested.

   It will be difficult to retrofit this, though a change in the upgrade process
   can itself be migrated through the upgrade process presented here (though it
   would take one release to go through the transition).

   We can at least downgrade before bumping the cluster version setting though,
   which allows all backwards-compatible changes (ideally the bulk) to be
   tested.

   Additionally, we could implement the originally envisioned `UseVersion`
   functionality so that the following conditions must be met until a backup is
   really necessary if a problem is a) severe and b) can't be "deactivated" (via
   `UseVersion`).
1. relying on the operator to guarantee that a cluster is not running mixed
   versions when the explicit version bump is issued.

   There are ways in which this could be approximately inferred, but again it
   was deemed too complex given the time frame. Besides, operators may prefer to
   have some level of control over the migration process, and it is difficult to
   make an autonomous upgrade workflow foolproof.

   If desired in the future, this can be retrofitted.

As a result, we get a design that's ergonomic but limited. The complexity
inherent with it as written indicates that it is a good choice to not add
additional complexity at this point. We are not locked into the process in the
long term.

## Unresolved questions

### Naming

`MinimumVersion` and `MinimumSupportedVersion` are similar but also different.
Perhaps the latter should be renamed, though no better name comes to mind.

### What to gossip

We make no use of the gossiped `ServerVersion`. The node's git commit hash is
already available, so this is only mildly interesting. Its `MinimumVersion`
(plus its `UseVersion` if that should ever differ) are more relevant. Likely
these should be added, even if the information isn't used today.
