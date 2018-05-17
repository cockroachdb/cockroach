- Feature Name: Finalize cluster upgrades automatically
- Status: in-progress
- Start Date: 2018 4/11
- Authors: Victor Chen
- RFC PR: [#24377](https://github.com/cockroachdb/cockroach/pull/24377)
- Cockroach Issue:
[#23912](https://github.com/cockroachdb/cockroach/issues/23912),
[#22686](https://github.com/cockroachdb/cockroach/issues/22686)

## Summary

We will add an auto-finalization feature to help operators finalize their
cluster upgrade process without manually running the set cluster version sql
command.

Meanwhile, we will also add a feature that allows operators to opt out of
auto-finalization to preserve the ability to downgrade.


## Motivation
In the past, in order to finalize the cluster upgrade process, operators must
manually run

```sql
SET CLUSTER SETTING version = crdb_internal.node_executable_version();
```

However, this step is easy to forget and without it, we cannot unlock
backwards-incompatible features. Therefore, it is necessary to add a version
check mechanism that automatically runs the finalization command once knowing
all the nodes in the cluster are running the new version.

Because this finalization step is irreversible, we want to allow
operators to make their own decisions on whether to use this auto-finalization
feature or not. For example, operators who want to opt out of auto-upgrade from
`version 2.0` to its next version should run:

```sql
SET CLUSTER SETTING cluster.preserve_downgrade_option = '2.0';
```

Please note that the opt-out feature is on a per version basis. That is to say,
if operators want to keep use manual upgrade for `version 2.1`, they have to
re-run the above command with `2.0` replaced with `2.1`. Otherwise, the cluster
version will still be upgraded automatically.

If the operators change their mind after running the manual upgrade command,
they can run the following command to change it back to auto upgrade:

```sql
RESET CLUSTER SETTING cluster.preserve_downgrade_option;
```

Finally, trying to set the cluster setting version when operators have already
set `cluster.preserve_downgrade_option` is not allowed. Operators have to reset
the `cluster.preserve_downgrade_option` to allow auto upgrade to happen.


## Detailed Design

### Add a new setting `cluster.preserve_downgrade_option`

The field should be added to `CLUSTER SETTING` in the format of:
`cluster.preserve_downgrade_option = 2.0`

By default, the value should be `""`, an empty string, to indicate we are always
using auto upgrade. We validate that the value being stored in this field matches
the current cluster version before storing it.

### On node startup, have a daemon that keeps running the following logic until exit:

- **IF**:
  - All lives nodes are running the same version as cluster version.
- **THEN**:
  - Exit.
- **ELSE IF**:
  - The preserve downgrade option for the current cluster version is not set.
  <br>**AND**
  - Look at `crdb_internal.gossip_nodes` and verify that all live nodes are
  running the new version.
  <br>**AND**
  - Pull `NodeLivenessStatus` from liveness server and verify that all
  non-decommissioned nodes are alive (no missing nodes).
- **THEN**:
  - Execute the following statement until success:
      - Do upgrade: `SET CLUSTER SETTING version = crdb_internal.node_executable_version();`.
  - Exit.


## Testing

### Roachtest `upgrade.go`:

Test Steps covered by `upgrade.go`
(sleep between each step for a certain amount of time if necessary):

1. Start a cluster of `N` nodes running `v2.0.0`.

2. Perform a rolling upgrade for node `1` - `N-1`. At every iteration, check
that the cluster version is not upgraded.

3. Stop node `N-1`.

4. Perform an upgrade for node `N`, which was running `v2.0.0`.

5. Check that the cluster version is not upgraded.

6. Decommission node `N-2` (decommissioned nodes should not affect auto upgrade).

7. Check cannot set `cluster.preserve_downgrade_option` to any value besides
`2.0`, which is the current cluster version.

8. Set `cluster.preserve_downgrade_option` to be `2.0`.

9. Restart node `N-1`, which was previously force stopped.

10. Check that the cluster version is not upgraded.

11. Check cannot set the cluster setting version until `cluster.preserve_downgrade_option`
is cleared.

12. Reset `cluster.preserve_downgrade_option`.

13. Check that the cluster version is upgraded to new version.

14. Check that `cluster.preserve_downgrade_option` has been reset.

### Unit test `TestClusterVersionUpgrade`:

`TestClusterVersionUpgrade` follows similar test steps as `upgrade.go`. Since 
it serves as a quick check for CI, it's not as comprehensive as the roachtest.


## UI
- If the user opt out of the auto upgrade, we should have a banner that alert
users when all the nodes are running a higher version than the cluster version
to instruct them to manually run the upgrade command.

- If all live nodes are running the newest version but some nodes are down and
not decommissioned, we should have a banner to alert operators to either revive
or decommission the down nodes.

- Let the operator know if the auto-upgrade is disabled or not at current version.
  - If `cluster.preserve_downgrade_option` is equal to `cluster.version`,
  auto-upgrade is disabled. Otherwise it's enabled.


## Drawbacks
- Race condition
[#24670](https://github.com/cockroachdb/cockroach/issues/24670) (future work):
  - Say an operator want to upgrade a cluster of 3 nodes from version 1.1 to
  2.0.
  - As soon as the operator just replaced the last node running v1.1 with a
  node running v2.0, the auto-finalization will start running the set cluster
  version command.
  - However, if right before the command get executed, the operator erroneously
  joined another node running 1.1 version. Then the command get executed and
  the cluster version is set to be 2.0, with new features enabled.
  - But the node running 1.1 version can not perform some of the 2.0 version
  operations and it will cause trouble.
- Unreachable Node:
  - We don't guarantee the upgrade will always be successful if there're
  unreachable nodes.
  - If a node is unreachable, we won't ever upgrade because it could be unsafe
  if the unreachable node is running in a lower version.
