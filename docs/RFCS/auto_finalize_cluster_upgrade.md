- Feature Name: Finalize CockroachDB's cluster upgrade process automatically
- Status: Draft
- Start Date: 2018 4/1
- Authors: Nikhil, Nathan, Victor
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #23912

## Summary

We will add an auto-finalization feature to help operators finalize their cluster upgrade process without manually running the set cluster version sql command.

Meanwhile, we will also support a toggle auto-finalize feature that allows operators to opt-out of auto-finalization to preserve the ability to downgrade.


## Motivation
In the past, operators must manually run
```
SET CLUSTER SETTING version = crdb_internal.node_executable_version();
```
in order to finalize the cluster upgrade process.

However, this step is easy to forget and without it, operators are not able to unlock features in the latest version. Therefore, it is necessary to add a version-check mechanism that automatically runs the finalization command once knowing all the nodes in the cluster are running the new version.

In addition, because this finalization step is irreversible, we want to allow operators to make their own decisions on whether to use this auto-finalization feature or not. As a result, we will also add an opt-out feature for those who want to preserve the ability to downgrade.

## Detailed Design

We will likely add the following logic to `pkg/server/server.go`:

- When a node starts, let's call it <b>A</b>, it will poll version information of each node from <b>crdb\_internal.gossip\_nodes</b>.
- If:
   - every other server's node\_executable\_version is the same as <b>A</b>'s node\_executable\_version.
 <br><b>AND</b><br>
   - <b>A</b>'s node\_executable\_version is greater than the current cluster's version.
- Then:
   - instance <b>A</b> will attempt to automatically run
```
SET CLUSTER SETTING version = crdb_internal.node_executable_version();
```
to finalize the cluster version upgrade.

## Drawbacks
- Race condition:
  - Say an operator want to upgrade a cluster of 3 nodes from version 1.1 to 2.0.
  - As soon as the operator just replaced the last node running v1.1 with a node running v2.0, the auto-finalization will start running the set cluster version command.
  - However, if right before the command get executed, the operator erroneously joined another node running 1.1 version. Then the command get executed and the cluster version is set to be 2.0, with new features enabled.
  - But the node running 1.1 version can not perform some of the 2.0 version operations and it will cause trouble.
- Unreachable Node:
  - We don't guarantee the upgrade will always be successful if there're unreachable nodes.
  - If a node is unreachable, we won't ever upgrade because it could be unsafe if the unreachable node is running in a lower version.
