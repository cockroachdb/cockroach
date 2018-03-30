- Feature Name: Finalize cluster upgrades automatically
- Status: Draft
- Start Date: 2018 4/1
- Authors: Victor Chen
- RFC PR: #24377
- Cockroach Issue: #23912, #22686 (UI)

## Summary

We will add an auto-finalization feature to help operators finalize their
cluster upgrade process without manually running the set cluster version sql
command.

Meanwhile, we will also add a feature feature that allows operators to opt out
of auto-finalization to preserve the ability to downgrade.


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

Finally, because this finalization step is irreversible, we want to allow
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

## Detailed Design

### Add a new field `cluster.preserve_downgrade_option`

The field should be added to `CLUSTER SETTING` in the format of:
`cluster.preserve_downgrade_option = 2.0`

By default, the value should be `0.0` to indicate we are always using auto
upgrade.

We should always validate the value being stored in this field. It should not
exceed the maximum version we have; nor should it fall below user's current
cluster version.

### Have a daemon that runs the following logic

- Check field `cluster.preserve_downgrade_options`
- **IF**:
  - Auto upgrade is enabled for current cluster version
  <br>**AND**
  - Look at `crdb_internal.gossip_nodes` and verify that all nodes are running
  the new version
  <br>**AND**
  - Look at `NodeStatusKeys` and verify that all non-decommissioned nodes are
  in `gossip_nodes` (no missing nodes)
- **THEN**:
  - Do upgrade
```
SET CLUSTER SETTING version = crdb_internal.node_executable_version();
```

  - Run `RESET CLUSTER SETTING cluster.preserve_downgrade_option;`
- **ELSE**
  - Abort upgrade


## Testing
Use the existing ``pkg/cmd/roachtest/version.go`` as a basis.

Test Steps:

1. Run `SET CLUSTER SETTING cluster.preserve_downgrade_option = '2.0';`

2. Perform a rolling upgrade.

3. Sleep and check `cluster version` is still the old version.

4. Run `RESET CLUSTER SETTING cluster.preserve_downgrade_option;`

5. Sleep and check `cluster version` is bumped to the new version.


## UI
- If the user opt out of the auto upgrade, we should have a banner that alert
users when all the nodes are running a higher version than the cluster version
to instruct them to manually run the upgrade command.

- (Maybe) We need a page that shows the version of each node in the cluster
along with the cluster version.


## Drawbacks
- Race condition:
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
