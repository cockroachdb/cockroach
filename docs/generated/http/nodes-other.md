

<a name="cockroach.server.status.statuspb.NodeStatus"></a>
#### NodeStatus

NodeStatus records the most recent values of metrics for a node.

Note: this is an “alpha” API payload. It is subject to change without
advance notice in a subsequent release.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.NodeDescriptor](#cockroach.roachpb.NodeDescriptor) |  | desc is the node descriptor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| build_info | [cockroach.build.Info](#cockroach.build.Info) |  | build_info describes the 'cockroach' executable file. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| started_at | [int64](#int64) |  | started_at is the unix timestamp at which the node process was last started. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| updated_at | [int64](#int64) |  | updated_at is the unix timestamp at which the node status record was last updated. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| metrics | [NodeStatus.MetricsEntry](#cockroach.server.status.statuspb.NodeStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| store_statuses | [StoreStatus](#cockroach.server.status.statuspb.StoreStatus) | repeated | store_statuses provides the store status payloads for all the stores on that node. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| args | [string](#string) | repeated | args is the list of command-line arguments used to last start the node. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| env | [string](#string) | repeated | env is the list of environment variables that influenced the node's configuration. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| latencies | [NodeStatus.LatenciesEntry](#cockroach.server.status.statuspb.NodeStatus.LatenciesEntry) | repeated | latencies is a map of nodeIDs to nanoseconds which is the latency between this node and the other node.<br><br>NOTE: this is deprecated and is only set if the min supported       cluster version is >= VersionRPCNetworkStats. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| activity | [NodeStatus.ActivityEntry](#cockroach.server.status.statuspb.NodeStatus.ActivityEntry) | repeated | activity is a map of nodeIDs to network statistics from this node to other nodes. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| total_system_memory | [int64](#int64) |  | total_system_memory is the total RAM available to the system (or, if detected, the memory available to the cgroup this process is in) in bytes. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| num_cpus | [int32](#int32) |  | num_cpus is the number of logical CPUs as reported by the operating system on the host where the 'cockroach' process is running. Note that this does not report the number of CPUs actually used by 'cockroach'; this parameter is controlled separately. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |




<a name="cockroach.server.status.statuspb.NodeStatus.MetricsEntry"></a>
#### NodeStatus.MetricsEntry






| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#string) |  |  |  |
| value | [double](#double) |  |  |  |




<a name="cockroach.server.status.statuspb.StoreStatus"></a>
#### StoreStatus

StoreStatus records the most recent values of metrics for a store.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.StoreDescriptor](#cockroach.roachpb.StoreDescriptor) |  | desc is the store descriptor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| metrics | [StoreStatus.MetricsEntry](#cockroach.server.status.statuspb.StoreStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |




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



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| incoming | [int64](#int64) |  | in bytes | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| outgoing | [int64](#int64) |  | in bytes | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| latency | [int64](#int64) |  | in nanoseconds | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |




<a name="cockroach.server.serverpb.NodesResponse.LivenessByNodeIdEntry"></a>
#### NodesResponse.LivenessByNodeIdEntry






| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#int32) |  |  |  |
| value | [cockroach.kv.kvserver.storagepb.NodeLivenessStatus](#cockroach.kv.kvserver.storagepb.NodeLivenessStatus) |  |  |  |


