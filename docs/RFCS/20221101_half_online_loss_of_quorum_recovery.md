- Feature Name: Half-Online Loss of Quorum Recovery
- Status: in-progress
- Start Date: 2022-11-01
- Authors: Oleg Afanasyev, Erik Grinaker
- RFC PR: #92913
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC proposes to extend loss of quorum (later LOQ) recovery tools to support
half-online mode. Half-online mode will remove the need to manually collect and
distribute recovery data in the cluster and remove the need to take the whole
cluster offline to perform recovery. Nodes that contain replicas that lost
quorum will still need to be restarted, but it could be done as a rolling
restart to reduce impact.

This will make recovery operation less disruptive and make it usable for large
clusters that lost a quorum only on a small subset of ranges.

New functionality will be built on top of existing recovery algorithm and will
provide facilities to collect replica information and distribute update plans to
relevant nodes.

This change will simplify cluster operations and remove the need for
intermediate error prone steps like mounting storages of node VM to extract the
data, deploy recovery plans to stopped containers, and reduce downtime of
unaffected data.

# Motivation

Loss of quorum recovery is a last resort tool to recover cluster state in cases
restoring from backup is not possible or will incur significant unacceptable
downtime. It only provides best effort guarantees and may leave data in
inconsistent state i.e. database constraints are violated, indices are broken or
a non-committed raft log entry becomes committed and applied. Latter issue
should not be a problem as commits would result in ambiguous error which implies
unknown commit state. Even if part of the data is unavailable, there could be
other databases and tables that can still serve the data and are not affected.
Current loss of quorum recovery tools require operator to shutdown all nodes of
the cluster to perform recovery. It also needs offline access to all node
storages to process the cluster state. This is not good enough as we want to
minimize disruption and reduce blast radius to the nodes that contain replicas
of affected ranges.

# Technical design

As a part of this change we need to consider following aspects:
- how to collect replica data from the running cluster
- how to build and validate cluster wide view of ranges to be recovered in
  absence of consistent descriptor snapshot
- how to distribute recovery information to cluster nodes
- how to execute the recovery plan on affected nodes
- how to verify that recovery procedure completed

For the sake of clarity here's the summary of how the existing loss of quorum
recovery tool works. This information would be referenced later in this
document.

To restore the cluster to a workable state all replicas that lost their quorum
need to be recovered. Recovery implies selecting one of the surviving range
replicas and designating it as a source of truth for subsequent reads and
writes. This is achieved by rewriting the replica descriptor with a new replica
ID and a sole replica in its group. That restores the consensus and when node is
restarted, the replication queue adds more replicas to conform to the configured
replication factor. To pick a survivor replica in case a cluster has multiple
ones we use the one with the highest raft applied index as its state is most up
to date. Ties between replicas are resolved by picking the replica with the
highest store id. The replica GC queue removes any stray surviving replicas.

Suppose we have a cluster of nodes 1 to 5 that lost nodes 2 and 3. Some ranges
lost their quorum. To recover this cluster operator will:
1. Shutdown nodes 1, 4 and 5.
2. Mount node storages or log into hosts where surviving nodes run.
3. Run cockroach debug recover collect-info --store=/cockroach-data on every
   node/storage.
4. Get all produced json files in one place.
5. Run cockroach debug recover make-plan to generate a recovery plan.
6. Distribute plan in form of json file to all nodes (1, 4, 5).
7. Run cockroach debug recover apply-plan to modify local store on affected
   nodes (1, 4, 5).
8. Start nodes.

At this point ranges that lost their quorum should be able to progress using one
of the remaining replicas as a source of truth.

## Data collection

To simplify the collection stage CLI would use an admin RPC call to a single
live node to get necessary replica data (collector node). Network Address of the
node will be explicitly provided by the operator. E.g.

```
$ cockroach debug recover make-plan --host node0.domain.com:26657 \
            -o recover-plan.json
```

Collector node would be responsible for collecting necessary information using
following Admin streaming RPC call:

```
message RecoveryCollectReplicaInfoRequest {}

message RecoveryCollectReplicaInfoResponse {
  oneof info {
    roachpb.RangeDescriptor range_descriptor = 1;
    loqrecoverypb.ReplicaInfo replica_info = 2;
  }
}

service Admin {
  ...
  rpc RecoveryCollectReplicaInfo(RecoveryCollectReplicaInfoRequest)
      returns (stream RecoveryCollectReplicaInfoResponse) {}
  ...
}
```

In the half online approach planning stage is using optional information from
meta ranges to resolve ambiguity if replicas move between nodes. To send this
additional information back to the CLI collector node will use a single stream
for both descriptors from meta if they are available and information collected
directly from nodes.

Collector will try to first retrieve consistent snapshots of replicas from meta
ranges. If reading meta succeeds, it will stream the results back as
RangeDescriptors.

Regardless of the success of the operation it then proceeds to collect
additional replica info from all local storages directly from every cluster
node.

To collect additional replica info like metadata about raft log content,
collector node will use gossip info about all nodes of the cluster and perform a
fanout operation to retrieve local replica data. Local replica info is provided
using Admin RPC call:

```
message RecoveryCollectLocalReplicaInfoRequest {
}

message RecoveryCollectLocalReplicaInfoResponse {
  loqrecoverypb.ReplicaInfo replica_info = 1;
}

service Admin {
  ...
  rpc RecoveryCollectLocalReplicaInfo(
        RecoveryCollectLocalReplicaInfoRequest)
      returns (stream RecoveryCollectLocalReplicaInfoResponse) {}
  ...
}
```

Data received by the collector node from this call will be streamed back to CLI
as it is being received from other nodes.

When streaming data back to CLI collector node should be careful with data
accumulated in process as to not overload the node. It may need some throttling
and fanout limiting based on measured collection performance.

Individual nodes would scan local replica descriptors in their stores to provide
replica info. Data collected from individual nodes might suffer from
inconsistencies since replicas could be moved around and changed as collection
progresses. Collection process would save the data as it was presented by
replicas and defer any reconciliation to the planning stage.

Saving collected info to a file could also simplify investigations by
engineering if recovery doesn't succeed.

### Cluster connection

Server admin endpoint requires admin privileges and should be run as root with
certificate authentication to allow recovery to proceed even in cases where SQL
subsystem is not operational and can't verify user roles.

## Planning changes

Plan creation will use existing functionality that is used in offline loss of
quorum recovery since v22.1 with some modifications.

Plan creation would use additional info when meta ranges are available by first
checking if descriptors from meta cover all keyspace. If they do, then it just
needs to check which ranges lost quorum using up to date descriptors. For those
ranges it will pick survivors using ReplicaInfo collected from local storages.

If meta keyspace coverage is incomplete, then plan creation will rely on the
existing planner that uses collected local replica info with the caveat that it
could be inconsistent.

Existing planner scans key space and searches for any overlaps or gaps between
voter replicas in descriptors. Since healthy ranges are free to move around, we
can see transient inconsistency, but we are not interested in ranges that can
progress. Instead the scanning process could be changed only to verify
boundaries of ranges that lost quorum to ensure that they don't have overlaps.
There should be no overlap between ranges that lost quorum and any other ranges,
neither healthy nor ones that lost quorum as well. That would cover range
overlap checks. For range gaps checks, we could in rare cases see a gap where
the state of the replica destination node was captured before the range moved
and the state of the replica source node was captured after. That should happen
for all replicas of a range. In that case we'll first check if we have partially
collected descriptors from meta ranges that correspond to missing part of
keyspace and if the current descriptor points to existing stores. If all fails
we'll have to retry replica info collection to see if the gap is persistent and
if it is not, ignore it.

To simplify tracking of recovery operation, the plan would be augmented by a
unique identifier field that could be used to check if plan application was
processed by nodes.

To protect the cluster from old nodes that loss of quorum considered dead
rejoining the cluster, plan would also contain a set of those nodes.
```
message ReplicaUpdatePlan {
  ...
  bytes plan_id = 2 [(gogoproto.customtype) = "github.com/cockroachdb/cockroach/pkg/util/uuid.UUID"];
  repeated int32 removed_node_ids = 3 [
    (gogoproto.customname) = "RemovedNodeIDs",
    (gogoproto.casttype) = "github.com/cockroachdb/cockroach/pkg/roachpb.NodeID"];
}
```

## Distributing recovery plan

Plan distribution would use Server admin endpoint to distribute the plan to
nodes that need to apply changes. CLI command will follow the same approach as
the collection command above.

To stage the plan, command will use Admin RPC call:

```
message RecoveryStagePlanRequest {
  loqrecoverypb.ReplicaUpdatePlan plan = 1;
  bool all_nodes = 2;
  bool force_plan = 3;
}

message RecoveryStagePlanResponse {
  repeated string errors = 1;
}

service Admin {
  ...
  rpc RecoveryStagePlan(RecoveryStagePlanRequest)
      returns (RecoveryStagePlanResponse) {}
  ...
}
```

Node serving request from CLI will act as coordinator and perform a fanout to
all nodes of the cluster. This behaviour is selected by all_nodes field. If set
to true, then the request needs to be fanned out to all nodes of a cluster. If
set to false, it needs to be applied to local node only.

When doing a fan out, coordinator will verify that no other plan is already
staged on any nodes of the cluster using RecoveryNodeStatus Admin RPC call:

```
message NodeRecoveryStatus {
  bytes pending_plan_id = 1 [
    (gogoproto.customname) = "PendingPlanID",
    (gogoproto.customtype) = "github.com/cockroachdb/cockroach/pkg/util/uuid.UUID"];
  bytes applied_plan_id = 2 [
    (gogoproto.customname) = "AppliedPlanID",
    (gogoproto.customtype) = "github.com/cockroachdb/cockroach/pkg/util/uuid.UUID"];
  google.protobuf.Timestamp apply_timestamp = 3 [(gogoproto.stdtime) = true];
  string apply_error = 4;
}

message RecoveryNodeStatusRequest {
}

message RecoveryNodeStatusResponse {
  NodeRecoveryStatus status = 1;
}

service Admin {
  ...
  rpc RecoveryNodeStatus(RecoveryNodeStatusRequest)
      returns (RecoveryNodeStatusResponse) {}
  ...
}
```

If plan is already staged on any of the nodes and force_plan is not set, then
the RecoveryStagePlan call will fail. Errors will contain errors describing
problems found during application.

Coordinator will then check if any of the nodes listed in removed_node_ids in
the plan rejoined the cluster. If they did, the request will fail.

Coordinator node will proceed to execution and send the request with a plan to
all nodes in the cluster.

When called with all_nodes set to false, node will check if there's a pending
plan already that is different from the plan in request. If force_plan is not
set and plans are different then the request will fail. Otherwise first nodes
from removed_node_ids in the plan are marked as decommissioned in the local node
tombstone storage. Then if any replicas in the plan are located on the node, it
is saved for subsequent application in the first store data directory.

After plans are successfully distributed, the operator will be notified which
nodes need to be restarted.

Note that after this step is done, nodes that were found dead by planning won't
be able to rejoin the cluster.

```
$ cockroach debug recover apply-plan --host node0.domain.com:26657 \
            recover-plan.json
Range r47519:/Table/138{-139} update replica (n6,s6):8 to (n6,s6):13 with peer replica(s) removed: (n3,s3):7, (n5,s5):3
[...]

Proceed with above changes [y/N]? y

Plan staged, to complete recovery perform a rolling restart of nodes n1, n6.
```

## Applying recovery plan

To apply the plan node needs to be restarted. Whenever node restarts, it would
check the data directory for pending recovery plans to apply. If there's any,
after node initializes stores and before server is started it would:

- move plan into a designated location in the store data directory
- apply changes to local descriptors to perform recovery
- record plan application into local key space for audit purposes and to allow
  application status checks
- perform all other recovery actions to record the updates in the designated
  part of local key space as it is done in offline recovery (e.g. after node
  starts and sql subsystem initializes)
- attempt to decommission dead nodes by using info in the plan to make node
  liveness consistent with local node tombstone storage

If the plan application fails, we can still back off and record and log an
error. Error details would be recorded in the local key space and could be
retrieved by CLI verification command.

Note that the last step of decommissioning nodes may not necessarily succeed if
liveness is still unavailable, but the last node to recover should be able to
access all necessary ranges if recovery is finished successfully.

## Verifying recovery outcome

Once restart is finished, CLI could verify that recovery succeeded. It would
check recovery status using a dedicated server Admin RPC:

```
message RecoveryVerifyRequest {
  bytes plan_id = 1 [
    (gogoproto.customname) = "PendingPlanID",
    (gogoproto.customtype) = "github.com/cockroachdb/cockroach/pkg/util/uuid.UUID"];
  repeated int32 removed_node_ids = 2 [(gogoproto.customname) = "RemovedNodeIDs",
    (gogoproto.casttype) = "github.com/cockroachdb/cockroach/pkg/roachpb.NodeID"];
}

message RecoveryVerifyResponse {
  repeated NodeRecoveryStatus statuses = 1;
  repeated roachpb.RangeDescriptor unavailable_ranges = 2 [
    (gogoproto.customname) = "UnavailableRanges"];
  repeated int32 decommissioned_node_ids = 3 [
    (gogoproto.customname) = "DecommissionedNodeIDs",
    (gogoproto.casttype) = "github.com/cockroachdb/cockroach/pkg/roachpb.NodeID"];
}

service Admin {
  ...
  rpc RecoveryVerify(RecoveryVerifyRequest)
      returns (RecoveryVerifyResponse) {}
  ...
}
```

Node serving request will first perform fan out to collect NodeRecoveryStatus'es
from all nodes. Resulting statuses would be returned in the statuses field. If
some of the nodes fail to serve request entries for statuses will be created
with Error field containing the error. Then collect information about cluster
ranges from meta range and verify that ranges have no replicas on removed nodes.
Finally check the decommission status of nodes provided in removed_node_ids.
Information will be returned to CLI.

If some of the nodes still didn't recover then CLI would report nodes that are
not restarted yet. For pending restart CLI would report:

```
$ cockroach debug recover verify --host node0.domain.com:26657 \ 
            recover-plan.json
Recovery in progress for plan 123e4567-e89b-12d3-a456-426614174000
Node 3: Applied at 15:34:11 UTC
Node 5: Pending restart
```

If an application discovered errors (e.g. nodes unreachable or plan application
failed), they would be reported alongside node IDs in the report above. If all
nodes included in the plan are restarted and applied plan successfully then CLI
will report range availability and node decommission status as well.

```
$ cockroach debug recover verify --host node0.domain.com:26657 \ 
            recover-plan.json
All ranges have a live quorum.
```

## CLI Commands

To provide described functionality the operator will rely on CLI commands
extending existing subset of debug recover commands.

New commands and new functionality of existing commands will include:
- debug recover make-plan (new functionality)
- debug recover apply-plan (new functionality)
- debug recover verify

### Make Plan

Make plan command will accept host specification to distinguish between old
mode, where the operator had to provide replica info files collected from all
nodes, and new mode where replica info is collected by CLI itself. It will also
support any additional connection related options to specify certs and map certs
to admin user as needed. It would also work in insecure mode when using
--insecure flag for demo purposes.

```
$ cockroach debug recover make-plan --host <addr/host>[:<admin port>] \
     [-o <plan file name>] \
     [--dead-node-ids <comma separated list of node ids>] \
     [--certs-dir <dir>]
```

Command will provide a summary of execution with info about scanned nodes and
found replicas followed by human readable list of proposed changes for operator
to confirm:

```
Nodes scanned: 5
Total replicas analyzed: 154
Ranges without quorum:   13
Discarded live replicas: 0

Range r6:/Table/0 updating replica (n1,s1):1 to (n1,s1):14. Discarding available replicas: [], discarding dead replicas: [(n2,s3):3,(n3,s6):2].
Range r16:/Table/20 updating replica (n1,s1):1 to (n1,s1):14. Discarding available replicas: [], discarding dead replicas: [(n3,s5):3,(n2,s3):2].
...
Discovered dead nodes would be marked as decommissioned
 n2: store(s): s3, s4
 n3: store(s): s5, s6

Proceed with plan creation [y/N] y
Plan created with id 123e4567-e89b-12d3-a456-426614174000.

To complete recovery, invoke:
cockroach debug recover apply-plan --host node1.domain.com:26657 recover-plan.json
```

Complete recovery message would contain matching plan file name and any
additional arguments used to specify cluster parameters like certs dir when
make-plan was invoked to simplify usage.

## Apply Plan

Apply plan will accept host specification to distinguish between local store
application and cluster application modes. It will also support any additional
connection related options to specify certs and map certs to admin user as
needed. It would also work in insecure mode when using --insecure flag for demo
purposes.

```
$ cockroach debug recover apply-plan --host <addr/host>[:<admin port>] \
     [--certs-dir <dir>] <plan file name>
```

Command will then present operator with a list of proposed changes to confirm:

```
Range 6:/Table/0 replica (n1,s1):1 will be updated to (n1,s1):14 with peer replica(s) removed: (n2,s3):3,(n3,s6):2
Range 16:/Table/20 replica (n1,s1):1 for will be updated to (n1,s1):14 with peer replica(s) removed: (n3,s5):3,(n2,s3):2

Nodes n2, n3 will be permanently marked as decommissioned.

Proceed with above changes [y/N]? y

Plan 123e4567-e89b-12d3-a456-426614174000 staged, to complete recovery perform a rolling restart of node(s) n1.
```

If plan application is not possible due to conflicting plan already scheduled,
CLI will notify operator about conflicts and fail.

```
Conflicting plan 123e4567-e89b-12d3-a456-426614174005 is already staged on node n2.
Conflicting plan 123e4567-e89b-12d3-a456-426614174005 is already staged on node n3.

Overwrite existing plan [y/N]? y
```

If the operator chooses to overwrite an old pending plan, then CLI will proceed
with application and prompts to restart nodes as above.

## Verify

Verify reports on recovery application status across the cluster. Tool requires
connection parameters to the cluster node.

```
$ cockroach debug recover verify --host <addr/host>[:<admin port>] \ 
     [--certs-dir <dir>] [<plan name>]
```

If a plan file is provided then the verify command will check application status
for this particular plan and return an error if the plan is not fully applied.

```
Recovery in progress for plan 123e4567-e89b-12d3-a456-426614174000
Node 3: Applied
Node 5: Pending restart
```

If the plan was applied on all ranges, the verify command will report node
decommission status and range availability.

```
Decommissioned nodes: n2, n4.

All ranges have a live quorum.
```

If the plan file is not provided as an argument, then the tool will print
pending plans on all nodes.

```
Recovery in progress
Node 3: Applied plan 123e4567-e89b-12d3-a456-426614174000
Node 5: Pending restart for plan 123e4567-e89b-12d3-a456-426614174000
```

# Drawbacks

When compared to existing offline recovery we don't have a consistent snapshot
of replica states across the cluster. That makes consistency checks for the
range coverage and overlap less reliable. In practice it should not be a big
problem as ranges without quorum should not be able to progress and change their
descriptors.

If the cluster is large and serves large volumes of data, then the node
collecting and distributing data could have see and additional memory and cpu
usage. We should be careful when doing fanout and limit concurrency to avoid
retrieving all data at once and degrading cluster further.

Compared to current approach where all operations are carried out manually we
are losing ability to stop recovery after plan was distributed to nodes. While
cancellation behaviour may be desired, since we now reduce amount of time needed
for the operation, it is less likely that environment will change when operation
is in progress and decision to revert it taken.

# Rationale and Alternatives

Current design is building on existing set of tools and simplifying the process
by adding automation.

Keeping CLI + Admin service as an approach for recovery operations reduces
downtime and extends already familiar approach.

# Explain it to folk outside of your team

This RFC proposes to extend loss of quorum recovery tools. Loss of quorum
recovery tools are used to recover range data in case multiple cockroachdb nodes
are lost. Current tools only work when cluster nodes are taken offline. New
half-online mode will remove the need to manually collect and distribute
recovery data in the cluster and remove the need to take the whole cluster
offline to perform recovery. Half online means that there's still a need to
restart some nodes after the location of most up to date data is identified.

This document describes how half online recovery mode could be implemented in
the cluster that is partially unavailable and how that would reduce operator
workload when performing recovery in a high stress situation of cluster outage.

New commands will take responsibility for performing collection, planning and
distribution of range metadata and will only require operator confirmation of
proposed changes. The operator will then have to restart a subset of nodes of
the cluster which is also reducing the blast radius of failure.

# Unresolved questions

## Cancellation of recovery

Cancellation of recovery is not straightforward and was moved out of main design.

There's a number of problems with it:
- we need to mark nodes as dead in local node tombstone storage to prevent
  potential range split brain from happening
- this storage was never meant for be reversable so unmarking nodes as
  decommissioned is not necessarily safe
- without unmarking decommissioned nodes, canceling recovery have little sense
  as all replicas from those nodes are now explicitly lost
- even if unmarking is possible, once any pending node restarts, it will
  propagate node tombstones to liveness status further narrowing the case for
  cancelation

With this in mind, we should treat the application stage as a point of no return
and operators should consider application as final decision. I.e. plan should be
applied immediately before restarting nodes when all other recovery avenues are
exhausted and not like plan should be staged and eventually applied when
convenient.

## Do we need an unattended mode

For the application command do we want to support unattended mode where prompts
are driven by y/n confirmations? It was problematic in the past as there are
more than a single prompt and 'y' to everything is not ideal.

## Future work

- Provide info about affected schema objects or system ranges
- For pending descriptor changes found in raft log, we could consider removing
  them in some cases. If replica itself is not being removed or demoted from
  voter and it is not a range split or merge then the change is only with
  respect of the whole group and it can be discarded since survivor will have to
  rewrite descriptor with its own anyway.
