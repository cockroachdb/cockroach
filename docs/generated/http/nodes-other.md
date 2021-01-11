

<a name="cockroach.server.status.statuspb.NodeStatus"></a>
#### NodeStatus

NodeStatus records the most recent values of metrics for a node.

Support status: [alpha](#support-status)


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.NodeDescriptor](#cockroach.roachpb.NodeDescriptor) |  | desc is the node descriptor. | [reserved](#support-status) |
| build_info | [cockroach.build.Info](#cockroach.build.Info) |  | build_info describes the `cockroach` executable file. | [alpha](#support-status) |
| started_at | [int64](#int64) |  | started_at is the unix timestamp at which the node process was last started. | [alpha](#support-status) |
| updated_at | [int64](#int64) |  | updated_at is the unix timestamp at which the node status record was last updated. | [alpha](#support-status) |
| metrics | [NodeStatus.MetricsEntry](#cockroach.server.status.statuspb.NodeStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | [reserved](#support-status) |
| store_statuses | [StoreStatus](#cockroach.server.status.statuspb.StoreStatus) | repeated | store_statuses provides the store status payloads for all the stores on that node. | [reserved](#support-status) |
| args | [string](#string) | repeated | args is the list of command-line arguments used to last start the node. | [reserved](#support-status) |
| env | [string](#string) | repeated | env is the list of environment variables that influenced the node's configuration. | [reserved](#support-status) |
| latencies | [NodeStatus.LatenciesEntry](#cockroach.server.status.statuspb.NodeStatus.LatenciesEntry) | repeated | latencies is a map of nodeIDs to nanoseconds which is the latency between this node and the other node.<br><br>NOTE: this is deprecated and is only set if the min supported       cluster version is >= VersionRPCNetworkStats. | [reserved](#support-status) |
| activity | [NodeStatus.ActivityEntry](#cockroach.server.status.statuspb.NodeStatus.ActivityEntry) | repeated | activity is a map of nodeIDs to network statistics from this node to other nodes. | [reserved](#support-status) |
| total_system_memory | [int64](#int64) |  | total_system_memory is the total RAM available to the system (or, if detected, the memory available to the cgroup this process is in) in bytes. | [alpha](#support-status) |
| num_cpus | [int32](#int32) |  | num_cpus is the number of logical CPUs as reported by the operating system on the host where the `cockroach` process is running. Note that this does not report the number of CPUs actually used by `cockroach`; this parameter is controlled separately. | [alpha](#support-status) |




<a name="cockroach.server.status.statuspb.NodeStatus.MetricsEntry"></a>
#### NodeStatus.MetricsEntry






| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#string) |  |  |  |
| value | [double](#double) |  |  |  |




<a name="cockroach.server.status.statuspb.StoreStatus"></a>
#### StoreStatus

StoreStatus records the most recent values of metrics for a store.

Support status: [reserved](#support-status)


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.StoreDescriptor](#cockroach.roachpb.StoreDescriptor) |  | desc is the store descriptor. | [reserved](#support-status) |
| metrics | [StoreStatus.MetricsEntry](#cockroach.server.status.statuspb.StoreStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | [reserved](#support-status) |




<a name="cockroach.server.status.statuspb.StoreStatus.MetricsEntry"></a>
#### StoreStatus.MetricsEntry






| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#string) |  |  |  |
| value | [double](#double) |  |  |  |




<a name="cockroach.server.status.statuspb.NodeStatus.LatenciesEntry"></a>
#### NodeStatus.LatenciesEntry






| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#int32) |  |  |  |
| value | [int64](#int64) |  |  |  |




<a name="cockroach.server.status.statuspb.NodeStatus.ActivityEntry"></a>
#### NodeStatus.ActivityEntry






| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#int32) |  |  |  |
| value | [NodeStatus.NetworkActivity](#cockroach.server.status.statuspb.NodeStatus.NetworkActivity) |  |  |  |




<a name="cockroach.server.status.statuspb.NodeStatus.NetworkActivity"></a>
#### NodeStatus.NetworkActivity



Support status: [reserved](#support-status)


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| incoming | [int64](#int64) |  | in bytes | [reserved](#support-status) |
| outgoing | [int64](#int64) |  | in bytes | [reserved](#support-status) |
| latency | [int64](#int64) |  | in nanoseconds | [reserved](#support-status) |




<a name="cockroach.server.serverpb.NodesResponse.LivenessByNodeIdEntry"></a>
#### NodesResponse.LivenessByNodeIdEntry






| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#int32) |  |  |  |
| value | [cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus](#cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus) |  |  |  |


