## Certificates

`GET /_status/certificates/{node_id}`

Certificates retrieves a copy of the TLS certificates.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.CertificatesRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| certificates | [CertificateDetails](#cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails"></a>
#### CertificateDetails



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| type | [CertificateDetails.CertificateType](#cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails.CertificateType) |  |  | [reserved](#support-status) |
| error_message | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  | "error_message" and "data" are mutually exclusive. | [reserved](#support-status) |
| data | [bytes](#cockroach.server.serverpb.CertificatesResponse-bytes) |  | data is the raw file contents of the certificate. This means PEM-encoded DER data. | [reserved](#support-status) |
| fields | [CertificateDetails.Fields](#cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails.Fields) | repeated |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails.Fields"></a>
#### CertificateDetails.Fields



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| issuer | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  |  | [reserved](#support-status) |
| subject | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  |  | [reserved](#support-status) |
| valid_from | [int64](#cockroach.server.serverpb.CertificatesResponse-int64) |  |  | [reserved](#support-status) |
| valid_until | [int64](#cockroach.server.serverpb.CertificatesResponse-int64) |  |  | [reserved](#support-status) |
| addresses | [string](#cockroach.server.serverpb.CertificatesResponse-string) | repeated |  | [reserved](#support-status) |
| signature_algorithm | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  |  | [reserved](#support-status) |
| public_key | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  |  | [reserved](#support-status) |
| key_usage | [string](#cockroach.server.serverpb.CertificatesResponse-string) | repeated |  | [reserved](#support-status) |
| extended_key_usage | [string](#cockroach.server.serverpb.CertificatesResponse-string) | repeated |  | [reserved](#support-status) |






## Details

`GET /_status/details/{node_id}`

Details retrieves details about the nodes in the cluster.

Support status: [reserved](#support-status)

#### Request Parameters




DetailsRequest requests a nodes details.
Note: this does *not* check readiness. Use the Health RPC for that purpose.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.DetailsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.DetailsResponse-int32) |  |  | [reserved](#support-status) |
| address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.DetailsResponse-cockroach.util.UnresolvedAddr) |  |  | [reserved](#support-status) |
| build_info | [cockroach.build.Info](#cockroach.server.serverpb.DetailsResponse-cockroach.build.Info) |  |  | [reserved](#support-status) |
| system_info | [SystemInfo](#cockroach.server.serverpb.DetailsResponse-cockroach.server.serverpb.SystemInfo) |  |  | [reserved](#support-status) |
| sql_address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.DetailsResponse-cockroach.util.UnresolvedAddr) |  |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.DetailsResponse-cockroach.server.serverpb.SystemInfo"></a>
#### SystemInfo

SystemInfo contains information about the host system.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| system_info | [string](#cockroach.server.serverpb.DetailsResponse-string) |  | system_info is the output from `uname -a` | [reserved](#support-status) |
| kernel_info | [string](#cockroach.server.serverpb.DetailsResponse-string) |  | kernel_info is the output from `uname -r`. | [reserved](#support-status) |






## Regions



RegionsRequest retrieves all available regions.

Support status: [reserved](#support-status)

#### Request Parameters




RegionsRequest requests all available regions.








#### Response Parameters




RegionsResponse describes the available regions.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| regions | [RegionsResponse.RegionsEntry](#cockroach.server.serverpb.RegionsResponse-cockroach.server.serverpb.RegionsResponse.RegionsEntry) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.RegionsResponse-cockroach.server.serverpb.RegionsResponse.RegionsEntry"></a>
#### RegionsResponse.RegionsEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.RegionsResponse-string) |  |  |  |
| value | [RegionsResponse.Region](#cockroach.server.serverpb.RegionsResponse-cockroach.server.serverpb.RegionsResponse.Region) |  |  |  |





<a name="cockroach.server.serverpb.RegionsResponse-cockroach.server.serverpb.RegionsResponse.Region"></a>
#### RegionsResponse.Region



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| zones | [string](#cockroach.server.serverpb.RegionsResponse-string) | repeated |  | [reserved](#support-status) |






## NodesList



NodesList returns all available nodes with their addresses.

Support status: [reserved](#support-status)

#### Request Parameters




NodesListRequest requests list of all nodes.
The nodes are KV nodes when the cluster is a single
tenant cluster or the host cluster in case of multi-tenant
clusters.
The nodes are SQL instances in case of multi-tenant
clusters.








#### Response Parameters




NodesListResponse contains a list of all nodes with their addresses.
The nodes are KV nodes when the cluster is a single
tenant cluster or the host cluster in case of multi-tenant
clusters.
The nodes are SQL instances in case of multi-tenant
clusters.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| nodes | [NodeDetails](#cockroach.server.serverpb.NodesListResponse-cockroach.server.serverpb.NodeDetails) | repeated | nodes contains a list of NodeDetails. Each individual node within the list is a SQL node in case of a tenant server and KV nodes in case of a KV server. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.NodesListResponse-cockroach.server.serverpb.NodeDetails"></a>
#### NodeDetails



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.NodesListResponse-int32) |  | node_id is a unique identifier for the node. This corresponds to SQL instance ID for a tenant server and KV node id for for a KV server. | [reserved](#support-status) |
| address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.NodesListResponse-cockroach.util.UnresolvedAddr) |  | address is the RPC address for a KV node. This will be set to null for a tenant server node. | [reserved](#support-status) |
| sql_address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.NodesListResponse-cockroach.util.UnresolvedAddr) |  | sql_address is the SQL address for a node. | [reserved](#support-status) |






## Nodes

`GET /_status/nodes`

Nodes returns status info for all commissioned nodes. Decommissioned nodes
are not included, except in rare cases where the node doing the
decommissioning crashed before completing the operation. In these cases,
the decommission operation can be rerun to clean up the status entry.



Don't introduce additional usages of this RPC. See #50707 for more details.
The underlying response type is something we're looking to get rid of.

Support status: [alpha](#support-status)

#### Request Parameters




NodesRequest requests a copy of the node information as known to gossip
and the KV layer.








#### Response Parameters




NodesResponse describe the nodes in the cluster.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| nodes | [cockroach.server.status.statuspb.NodeStatus](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus) | repeated | nodes carries the status payloads for all nodes in the cluster. | [alpha](#support-status) |
| liveness_by_node_id | [NodesResponse.LivenessByNodeIdEntry](#cockroach.server.serverpb.NodesResponse-cockroach.server.serverpb.NodesResponse.LivenessByNodeIdEntry) | repeated | liveness_by_node_id maps each node ID to a liveness status. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus"></a>
#### NodeStatus

NodeStatus records the most recent values of metrics for a node.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.NodeDescriptor](#cockroach.server.serverpb.NodesResponse-cockroach.roachpb.NodeDescriptor) |  | desc is the node descriptor. | [reserved](#support-status) |
| build_info | [cockroach.build.Info](#cockroach.server.serverpb.NodesResponse-cockroach.build.Info) |  | build_info describes the `cockroach` executable file. | [alpha](#support-status) |
| started_at | [int64](#cockroach.server.serverpb.NodesResponse-int64) |  | started_at is the unix timestamp at which the node process was last started. | [alpha](#support-status) |
| updated_at | [int64](#cockroach.server.serverpb.NodesResponse-int64) |  | updated_at is the unix timestamp at which the node status record was last updated. | [alpha](#support-status) |
| metrics | [NodeStatus.MetricsEntry](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | [reserved](#support-status) |
| store_statuses | [StoreStatus](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.StoreStatus) | repeated | store_statuses provides the store status payloads for all the stores on that node. | [reserved](#support-status) |
| args | [string](#cockroach.server.serverpb.NodesResponse-string) | repeated | args is the list of command-line arguments used to last start the node. | [reserved](#support-status) |
| env | [string](#cockroach.server.serverpb.NodesResponse-string) | repeated | env is the list of environment variables that influenced the node's configuration. | [reserved](#support-status) |
| latencies | [NodeStatus.LatenciesEntry](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus.LatenciesEntry) | repeated | latencies is a map of nodeIDs to nanoseconds which is the latency between this node and the other node.<br><br>NOTE: this is deprecated and is only set if the min supported       cluster version is >= VersionRPCNetworkStats. | [reserved](#support-status) |
| activity | [NodeStatus.ActivityEntry](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus.ActivityEntry) | repeated | activity is a map of nodeIDs to network statistics from this node to other nodes. | [reserved](#support-status) |
| total_system_memory | [int64](#cockroach.server.serverpb.NodesResponse-int64) |  | total_system_memory is the total RAM available to the system (or, if detected, the memory available to the cgroup this process is in) in bytes. | [alpha](#support-status) |
| num_cpus | [int32](#cockroach.server.serverpb.NodesResponse-int32) |  | num_cpus is the number of logical CPUs as reported by the operating system on the host where the `cockroach` process is running. Note that this does not report the number of CPUs actually used by `cockroach`; this parameter is controlled separately. | [alpha](#support-status) |





<a name="cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus.MetricsEntry"></a>
#### NodeStatus.MetricsEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.NodesResponse-string) |  |  |  |
| value | [double](#cockroach.server.serverpb.NodesResponse-double) |  |  |  |





<a name="cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.StoreStatus"></a>
#### StoreStatus

StoreStatus records the most recent values of metrics for a store.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.StoreDescriptor](#cockroach.server.serverpb.NodesResponse-cockroach.roachpb.StoreDescriptor) |  | desc is the store descriptor. | [reserved](#support-status) |
| metrics | [StoreStatus.MetricsEntry](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.StoreStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.StoreStatus.MetricsEntry"></a>
#### StoreStatus.MetricsEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.NodesResponse-string) |  |  |  |
| value | [double](#cockroach.server.serverpb.NodesResponse-double) |  |  |  |





<a name="cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus.LatenciesEntry"></a>
#### NodeStatus.LatenciesEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.NodesResponse-int32) |  |  |  |
| value | [int64](#cockroach.server.serverpb.NodesResponse-int64) |  |  |  |





<a name="cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus.ActivityEntry"></a>
#### NodeStatus.ActivityEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.NodesResponse-int32) |  |  |  |
| value | [NodeStatus.NetworkActivity](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus.NetworkActivity) |  |  |  |





<a name="cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus.NetworkActivity"></a>
#### NodeStatus.NetworkActivity



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| incoming | [int64](#cockroach.server.serverpb.NodesResponse-int64) |  | in bytes | [reserved](#support-status) |
| outgoing | [int64](#cockroach.server.serverpb.NodesResponse-int64) |  | in bytes | [reserved](#support-status) |
| latency | [int64](#cockroach.server.serverpb.NodesResponse-int64) |  | in nanoseconds | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodesResponse-cockroach.server.serverpb.NodesResponse.LivenessByNodeIdEntry"></a>
#### NodesResponse.LivenessByNodeIdEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.NodesResponse-int32) |  |  |  |
| value | [cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus](#cockroach.server.serverpb.NodesResponse-cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus) |  |  |  |






## Node

`GET /_status/nodes/{node_id}`

Node retrieves details about a single node.

Support status: [alpha](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.NodeRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |







#### Response Parameters




NodeStatus records the most recent values of metrics for a node.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.NodeDescriptor](#cockroach.server.status.statuspb.NodeStatus-cockroach.roachpb.NodeDescriptor) |  | desc is the node descriptor. | [reserved](#support-status) |
| build_info | [cockroach.build.Info](#cockroach.server.status.statuspb.NodeStatus-cockroach.build.Info) |  | build_info describes the `cockroach` executable file. | [alpha](#support-status) |
| started_at | [int64](#cockroach.server.status.statuspb.NodeStatus-int64) |  | started_at is the unix timestamp at which the node process was last started. | [alpha](#support-status) |
| updated_at | [int64](#cockroach.server.status.statuspb.NodeStatus-int64) |  | updated_at is the unix timestamp at which the node status record was last updated. | [alpha](#support-status) |
| metrics | [NodeStatus.MetricsEntry](#cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.NodeStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | [reserved](#support-status) |
| store_statuses | [StoreStatus](#cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.StoreStatus) | repeated | store_statuses provides the store status payloads for all the stores on that node. | [reserved](#support-status) |
| args | [string](#cockroach.server.status.statuspb.NodeStatus-string) | repeated | args is the list of command-line arguments used to last start the node. | [reserved](#support-status) |
| env | [string](#cockroach.server.status.statuspb.NodeStatus-string) | repeated | env is the list of environment variables that influenced the node's configuration. | [reserved](#support-status) |
| latencies | [NodeStatus.LatenciesEntry](#cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.NodeStatus.LatenciesEntry) | repeated | latencies is a map of nodeIDs to nanoseconds which is the latency between this node and the other node.<br><br>NOTE: this is deprecated and is only set if the min supported       cluster version is >= VersionRPCNetworkStats. | [reserved](#support-status) |
| activity | [NodeStatus.ActivityEntry](#cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.NodeStatus.ActivityEntry) | repeated | activity is a map of nodeIDs to network statistics from this node to other nodes. | [reserved](#support-status) |
| total_system_memory | [int64](#cockroach.server.status.statuspb.NodeStatus-int64) |  | total_system_memory is the total RAM available to the system (or, if detected, the memory available to the cgroup this process is in) in bytes. | [alpha](#support-status) |
| num_cpus | [int32](#cockroach.server.status.statuspb.NodeStatus-int32) |  | num_cpus is the number of logical CPUs as reported by the operating system on the host where the `cockroach` process is running. Note that this does not report the number of CPUs actually used by `cockroach`; this parameter is controlled separately. | [alpha](#support-status) |






<a name="cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.NodeStatus.MetricsEntry"></a>
#### NodeStatus.MetricsEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.status.statuspb.NodeStatus-string) |  |  |  |
| value | [double](#cockroach.server.status.statuspb.NodeStatus-double) |  |  |  |





<a name="cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.StoreStatus"></a>
#### StoreStatus

StoreStatus records the most recent values of metrics for a store.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.StoreDescriptor](#cockroach.server.status.statuspb.NodeStatus-cockroach.roachpb.StoreDescriptor) |  | desc is the store descriptor. | [reserved](#support-status) |
| metrics | [StoreStatus.MetricsEntry](#cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.StoreStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | [reserved](#support-status) |





<a name="cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.StoreStatus.MetricsEntry"></a>
#### StoreStatus.MetricsEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.status.statuspb.NodeStatus-string) |  |  |  |
| value | [double](#cockroach.server.status.statuspb.NodeStatus-double) |  |  |  |





<a name="cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.NodeStatus.LatenciesEntry"></a>
#### NodeStatus.LatenciesEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.status.statuspb.NodeStatus-int32) |  |  |  |
| value | [int64](#cockroach.server.status.statuspb.NodeStatus-int64) |  |  |  |





<a name="cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.NodeStatus.ActivityEntry"></a>
#### NodeStatus.ActivityEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.status.statuspb.NodeStatus-int32) |  |  |  |
| value | [NodeStatus.NetworkActivity](#cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.NodeStatus.NetworkActivity) |  |  |  |





<a name="cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.NodeStatus.NetworkActivity"></a>
#### NodeStatus.NetworkActivity



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| incoming | [int64](#cockroach.server.status.statuspb.NodeStatus-int64) |  | in bytes | [reserved](#support-status) |
| outgoing | [int64](#cockroach.server.status.statuspb.NodeStatus-int64) |  | in bytes | [reserved](#support-status) |
| latency | [int64](#cockroach.server.status.statuspb.NodeStatus-int64) |  | in nanoseconds | [reserved](#support-status) |






## NodesUI

`GET /_status/nodes_ui`



Support status: [reserved](#support-status)

#### Request Parameters




NodesRequest requests a copy of the node information as known to gossip
and the KV layer.








#### Response Parameters




NodesResponse describe the nodes in the cluster.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| nodes | [NodeResponse](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeResponse) | repeated | nodes carries the status payloads for all nodes in the cluster. | [alpha](#support-status) |
| liveness_by_node_id | [NodesResponseExternal.LivenessByNodeIdEntry](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodesResponseExternal.LivenessByNodeIdEntry) | repeated | liveness_by_node_id maps each node ID to a liveness status. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeResponse"></a>
#### NodeResponse

NodeStatus records the most recent values of metrics for a node.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [NodeDescriptor](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeDescriptor) |  | desc is the node descriptor. | [reserved](#support-status) |
| build_info | [cockroach.build.Info](#cockroach.server.serverpb.NodesResponseExternal-cockroach.build.Info) |  | build_info describes the `cockroach` executable file. | [alpha](#support-status) |
| started_at | [int64](#cockroach.server.serverpb.NodesResponseExternal-int64) |  | started_at is the unix timestamp at which the node process was last started. | [alpha](#support-status) |
| updated_at | [int64](#cockroach.server.serverpb.NodesResponseExternal-int64) |  | updated_at is the unix timestamp at which the node status record was last updated. | [alpha](#support-status) |
| metrics | [NodeResponse.MetricsEntry](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeResponse.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | [reserved](#support-status) |
| store_statuses | [StoreStatus](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.StoreStatus) | repeated | store_statuses provides the store status payloads for all the stores on that node. | [reserved](#support-status) |
| args | [string](#cockroach.server.serverpb.NodesResponseExternal-string) | repeated | args is the list of command-line arguments used to last start the node. args should only be visible by the Admin role. | [reserved](#support-status) |
| env | [string](#cockroach.server.serverpb.NodesResponseExternal-string) | repeated | env is the list of environment variables that influenced the node's configuration. env should only be visible by the Admin role. | [reserved](#support-status) |
| latencies | [NodeResponse.LatenciesEntry](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeResponse.LatenciesEntry) | repeated | latencies is a map of nodeIDs to nanoseconds which is the latency between this node and the other node.<br><br>NOTE: this is deprecated and is only set if the min supported       cluster version is >= VersionRPCNetworkStats. | [reserved](#support-status) |
| activity | [NodeResponse.ActivityEntry](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeResponse.ActivityEntry) | repeated | activity is a map of nodeIDs to network statistics from this node to other nodes. | [reserved](#support-status) |
| total_system_memory | [int64](#cockroach.server.serverpb.NodesResponseExternal-int64) |  | total_system_memory is the total RAM available to the system (or, if detected, the memory available to the cgroup this process is in) in bytes. | [alpha](#support-status) |
| num_cpus | [int32](#cockroach.server.serverpb.NodesResponseExternal-int32) |  | num_cpus is the number of logical CPUs as reported by the operating system on the host where the `cockroach` process is running. Note that this does not report the number of CPUs actually used by `cockroach`; this parameter is controlled separately. | [alpha](#support-status) |





<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeDescriptor"></a>
#### NodeDescriptor

NodeDescriptor holds details on node physical/network topology.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.NodesResponseExternal-int32) |  |  | [reserved](#support-status) |
| address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.NodesResponseExternal-cockroach.util.UnresolvedAddr) |  | address should only be visible by the Admin role. | [reserved](#support-status) |
| attrs | [cockroach.roachpb.Attributes](#cockroach.server.serverpb.NodesResponseExternal-cockroach.roachpb.Attributes) |  | attrs should only be visible by the Admin role. | [reserved](#support-status) |
| locality | [Locality](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.Locality) |  |  | [reserved](#support-status) |
| ServerVersion | [Version](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.Version) |  |  | [reserved](#support-status) |
| build_tag | [string](#cockroach.server.serverpb.NodesResponseExternal-string) |  |  | [reserved](#support-status) |
| started_at | [int64](#cockroach.server.serverpb.NodesResponseExternal-int64) |  |  | [reserved](#support-status) |
| locality_address | [cockroach.roachpb.LocalityAddress](#cockroach.server.serverpb.NodesResponseExternal-cockroach.roachpb.LocalityAddress) | repeated | locality_address should only be visible by the Admin role. | [reserved](#support-status) |
| cluster_name | [string](#cockroach.server.serverpb.NodesResponseExternal-string) |  |  | [reserved](#support-status) |
| sql_address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.NodesResponseExternal-cockroach.util.UnresolvedAddr) |  | The SQL address. If empty, indicates that the base address field is also used to accept SQL connections. sql_address should only be visible by the Admin role. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.Locality"></a>
#### Locality

Locality is an ordered set of key value Tiers that describe a node's
location. The tier keys should be the same across all nodes.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| tiers | [Tier](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.Tier) | repeated |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.Tier"></a>
#### Tier

Tier represents one level of the locality hierarchy.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.NodesResponseExternal-string) |  | Key is the name of tier and should match all other nodes. | [reserved](#support-status) |
| value | [string](#cockroach.server.serverpb.NodesResponseExternal-string) |  | Value is node specific value corresponding to the key. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.Version"></a>
#### Version



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| major_val | [int32](#cockroach.server.serverpb.NodesResponseExternal-int32) |  | The names "major" and "minor" are reserved in C in some platforms (e.g. FreeBSD). | [reserved](#support-status) |
| minor_val | [int32](#cockroach.server.serverpb.NodesResponseExternal-int32) |  |  | [reserved](#support-status) |
| patch | [int32](#cockroach.server.serverpb.NodesResponseExternal-int32) |  | Note that patch is a placeholder and will always be zero. | [reserved](#support-status) |
| internal | [int32](#cockroach.server.serverpb.NodesResponseExternal-int32) |  | The internal version is used to introduce migrations during the development cycle. They are subversions that are never the end versions of a release, i.e. users of stable, public release will only use binaries with the internal version set to 0. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeResponse.MetricsEntry"></a>
#### NodeResponse.MetricsEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.NodesResponseExternal-string) |  |  |  |
| value | [double](#cockroach.server.serverpb.NodesResponseExternal-double) |  |  |  |





<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.StoreStatus"></a>
#### StoreStatus

StoreStatus records the most recent values of metrics for a store.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [StoreDescriptor](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.StoreDescriptor) |  | desc is the store descriptor. | [reserved](#support-status) |
| metrics | [StoreStatus.MetricsEntry](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.StoreStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.StoreDescriptor"></a>
#### StoreDescriptor

StoreDescriptor holds store information including store attributes, node
descriptor and store capacity.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| store_id | [int32](#cockroach.server.serverpb.NodesResponseExternal-int32) |  |  | [reserved](#support-status) |
| attrs | [cockroach.roachpb.Attributes](#cockroach.server.serverpb.NodesResponseExternal-cockroach.roachpb.Attributes) |  |  | [reserved](#support-status) |
| node | [NodeDescriptor](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeDescriptor) |  |  | [reserved](#support-status) |
| capacity | [cockroach.roachpb.StoreCapacity](#cockroach.server.serverpb.NodesResponseExternal-cockroach.roachpb.StoreCapacity) |  |  | [reserved](#support-status) |
| properties | [cockroach.roachpb.StoreProperties](#cockroach.server.serverpb.NodesResponseExternal-cockroach.roachpb.StoreProperties) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeDescriptor"></a>
#### NodeDescriptor

NodeDescriptor holds details on node physical/network topology.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.NodesResponseExternal-int32) |  |  | [reserved](#support-status) |
| address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.NodesResponseExternal-cockroach.util.UnresolvedAddr) |  | address should only be visible by the Admin role. | [reserved](#support-status) |
| attrs | [cockroach.roachpb.Attributes](#cockroach.server.serverpb.NodesResponseExternal-cockroach.roachpb.Attributes) |  | attrs should only be visible by the Admin role. | [reserved](#support-status) |
| locality | [Locality](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.Locality) |  |  | [reserved](#support-status) |
| ServerVersion | [Version](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.Version) |  |  | [reserved](#support-status) |
| build_tag | [string](#cockroach.server.serverpb.NodesResponseExternal-string) |  |  | [reserved](#support-status) |
| started_at | [int64](#cockroach.server.serverpb.NodesResponseExternal-int64) |  |  | [reserved](#support-status) |
| locality_address | [cockroach.roachpb.LocalityAddress](#cockroach.server.serverpb.NodesResponseExternal-cockroach.roachpb.LocalityAddress) | repeated | locality_address should only be visible by the Admin role. | [reserved](#support-status) |
| cluster_name | [string](#cockroach.server.serverpb.NodesResponseExternal-string) |  |  | [reserved](#support-status) |
| sql_address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.NodesResponseExternal-cockroach.util.UnresolvedAddr) |  | The SQL address. If empty, indicates that the base address field is also used to accept SQL connections. sql_address should only be visible by the Admin role. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.StoreStatus.MetricsEntry"></a>
#### StoreStatus.MetricsEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.NodesResponseExternal-string) |  |  |  |
| value | [double](#cockroach.server.serverpb.NodesResponseExternal-double) |  |  |  |





<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeResponse.LatenciesEntry"></a>
#### NodeResponse.LatenciesEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.NodesResponseExternal-int32) |  |  |  |
| value | [int64](#cockroach.server.serverpb.NodesResponseExternal-int64) |  |  |  |





<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeResponse.ActivityEntry"></a>
#### NodeResponse.ActivityEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.NodesResponseExternal-int32) |  |  |  |
| value | [NodeResponse.NetworkActivity](#cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeResponse.NetworkActivity) |  |  |  |





<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodeResponse.NetworkActivity"></a>
#### NodeResponse.NetworkActivity



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| incoming | [int64](#cockroach.server.serverpb.NodesResponseExternal-int64) |  | in bytes | [reserved](#support-status) |
| outgoing | [int64](#cockroach.server.serverpb.NodesResponseExternal-int64) |  | in bytes | [reserved](#support-status) |
| latency | [int64](#cockroach.server.serverpb.NodesResponseExternal-int64) |  | in nanoseconds | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodesResponseExternal-cockroach.server.serverpb.NodesResponseExternal.LivenessByNodeIdEntry"></a>
#### NodesResponseExternal.LivenessByNodeIdEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.NodesResponseExternal-int32) |  |  |  |
| value | [cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus](#cockroach.server.serverpb.NodesResponseExternal-cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus) |  |  |  |






## NodeUI

`GET /_status/nodes_ui/{node_id}`



Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.NodeRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |







#### Response Parameters




NodeStatus records the most recent values of metrics for a node.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [NodeDescriptor](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.NodeDescriptor) |  | desc is the node descriptor. | [reserved](#support-status) |
| build_info | [cockroach.build.Info](#cockroach.server.serverpb.NodeResponse-cockroach.build.Info) |  | build_info describes the `cockroach` executable file. | [alpha](#support-status) |
| started_at | [int64](#cockroach.server.serverpb.NodeResponse-int64) |  | started_at is the unix timestamp at which the node process was last started. | [alpha](#support-status) |
| updated_at | [int64](#cockroach.server.serverpb.NodeResponse-int64) |  | updated_at is the unix timestamp at which the node status record was last updated. | [alpha](#support-status) |
| metrics | [NodeResponse.MetricsEntry](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.NodeResponse.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | [reserved](#support-status) |
| store_statuses | [StoreStatus](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.StoreStatus) | repeated | store_statuses provides the store status payloads for all the stores on that node. | [reserved](#support-status) |
| args | [string](#cockroach.server.serverpb.NodeResponse-string) | repeated | args is the list of command-line arguments used to last start the node. args should only be visible by the Admin role. | [reserved](#support-status) |
| env | [string](#cockroach.server.serverpb.NodeResponse-string) | repeated | env is the list of environment variables that influenced the node's configuration. env should only be visible by the Admin role. | [reserved](#support-status) |
| latencies | [NodeResponse.LatenciesEntry](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.NodeResponse.LatenciesEntry) | repeated | latencies is a map of nodeIDs to nanoseconds which is the latency between this node and the other node.<br><br>NOTE: this is deprecated and is only set if the min supported       cluster version is >= VersionRPCNetworkStats. | [reserved](#support-status) |
| activity | [NodeResponse.ActivityEntry](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.NodeResponse.ActivityEntry) | repeated | activity is a map of nodeIDs to network statistics from this node to other nodes. | [reserved](#support-status) |
| total_system_memory | [int64](#cockroach.server.serverpb.NodeResponse-int64) |  | total_system_memory is the total RAM available to the system (or, if detected, the memory available to the cgroup this process is in) in bytes. | [alpha](#support-status) |
| num_cpus | [int32](#cockroach.server.serverpb.NodeResponse-int32) |  | num_cpus is the number of logical CPUs as reported by the operating system on the host where the `cockroach` process is running. Note that this does not report the number of CPUs actually used by `cockroach`; this parameter is controlled separately. | [alpha](#support-status) |






<a name="cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.NodeDescriptor"></a>
#### NodeDescriptor

NodeDescriptor holds details on node physical/network topology.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.NodeResponse-int32) |  |  | [reserved](#support-status) |
| address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.NodeResponse-cockroach.util.UnresolvedAddr) |  | address should only be visible by the Admin role. | [reserved](#support-status) |
| attrs | [cockroach.roachpb.Attributes](#cockroach.server.serverpb.NodeResponse-cockroach.roachpb.Attributes) |  | attrs should only be visible by the Admin role. | [reserved](#support-status) |
| locality | [Locality](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.Locality) |  |  | [reserved](#support-status) |
| ServerVersion | [Version](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.Version) |  |  | [reserved](#support-status) |
| build_tag | [string](#cockroach.server.serverpb.NodeResponse-string) |  |  | [reserved](#support-status) |
| started_at | [int64](#cockroach.server.serverpb.NodeResponse-int64) |  |  | [reserved](#support-status) |
| locality_address | [cockroach.roachpb.LocalityAddress](#cockroach.server.serverpb.NodeResponse-cockroach.roachpb.LocalityAddress) | repeated | locality_address should only be visible by the Admin role. | [reserved](#support-status) |
| cluster_name | [string](#cockroach.server.serverpb.NodeResponse-string) |  |  | [reserved](#support-status) |
| sql_address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.NodeResponse-cockroach.util.UnresolvedAddr) |  | The SQL address. If empty, indicates that the base address field is also used to accept SQL connections. sql_address should only be visible by the Admin role. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.Locality"></a>
#### Locality

Locality is an ordered set of key value Tiers that describe a node's
location. The tier keys should be the same across all nodes.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| tiers | [Tier](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.Tier) | repeated |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.Tier"></a>
#### Tier

Tier represents one level of the locality hierarchy.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.NodeResponse-string) |  | Key is the name of tier and should match all other nodes. | [reserved](#support-status) |
| value | [string](#cockroach.server.serverpb.NodeResponse-string) |  | Value is node specific value corresponding to the key. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.Version"></a>
#### Version



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| major_val | [int32](#cockroach.server.serverpb.NodeResponse-int32) |  | The names "major" and "minor" are reserved in C in some platforms (e.g. FreeBSD). | [reserved](#support-status) |
| minor_val | [int32](#cockroach.server.serverpb.NodeResponse-int32) |  |  | [reserved](#support-status) |
| patch | [int32](#cockroach.server.serverpb.NodeResponse-int32) |  | Note that patch is a placeholder and will always be zero. | [reserved](#support-status) |
| internal | [int32](#cockroach.server.serverpb.NodeResponse-int32) |  | The internal version is used to introduce migrations during the development cycle. They are subversions that are never the end versions of a release, i.e. users of stable, public release will only use binaries with the internal version set to 0. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.NodeResponse.MetricsEntry"></a>
#### NodeResponse.MetricsEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.NodeResponse-string) |  |  |  |
| value | [double](#cockroach.server.serverpb.NodeResponse-double) |  |  |  |





<a name="cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.StoreStatus"></a>
#### StoreStatus

StoreStatus records the most recent values of metrics for a store.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [StoreDescriptor](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.StoreDescriptor) |  | desc is the store descriptor. | [reserved](#support-status) |
| metrics | [StoreStatus.MetricsEntry](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.StoreStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.StoreDescriptor"></a>
#### StoreDescriptor

StoreDescriptor holds store information including store attributes, node
descriptor and store capacity.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| store_id | [int32](#cockroach.server.serverpb.NodeResponse-int32) |  |  | [reserved](#support-status) |
| attrs | [cockroach.roachpb.Attributes](#cockroach.server.serverpb.NodeResponse-cockroach.roachpb.Attributes) |  |  | [reserved](#support-status) |
| node | [NodeDescriptor](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.NodeDescriptor) |  |  | [reserved](#support-status) |
| capacity | [cockroach.roachpb.StoreCapacity](#cockroach.server.serverpb.NodeResponse-cockroach.roachpb.StoreCapacity) |  |  | [reserved](#support-status) |
| properties | [cockroach.roachpb.StoreProperties](#cockroach.server.serverpb.NodeResponse-cockroach.roachpb.StoreProperties) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.NodeDescriptor"></a>
#### NodeDescriptor

NodeDescriptor holds details on node physical/network topology.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.NodeResponse-int32) |  |  | [reserved](#support-status) |
| address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.NodeResponse-cockroach.util.UnresolvedAddr) |  | address should only be visible by the Admin role. | [reserved](#support-status) |
| attrs | [cockroach.roachpb.Attributes](#cockroach.server.serverpb.NodeResponse-cockroach.roachpb.Attributes) |  | attrs should only be visible by the Admin role. | [reserved](#support-status) |
| locality | [Locality](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.Locality) |  |  | [reserved](#support-status) |
| ServerVersion | [Version](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.Version) |  |  | [reserved](#support-status) |
| build_tag | [string](#cockroach.server.serverpb.NodeResponse-string) |  |  | [reserved](#support-status) |
| started_at | [int64](#cockroach.server.serverpb.NodeResponse-int64) |  |  | [reserved](#support-status) |
| locality_address | [cockroach.roachpb.LocalityAddress](#cockroach.server.serverpb.NodeResponse-cockroach.roachpb.LocalityAddress) | repeated | locality_address should only be visible by the Admin role. | [reserved](#support-status) |
| cluster_name | [string](#cockroach.server.serverpb.NodeResponse-string) |  |  | [reserved](#support-status) |
| sql_address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.NodeResponse-cockroach.util.UnresolvedAddr) |  | The SQL address. If empty, indicates that the base address field is also used to accept SQL connections. sql_address should only be visible by the Admin role. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.StoreStatus.MetricsEntry"></a>
#### StoreStatus.MetricsEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.NodeResponse-string) |  |  |  |
| value | [double](#cockroach.server.serverpb.NodeResponse-double) |  |  |  |





<a name="cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.NodeResponse.LatenciesEntry"></a>
#### NodeResponse.LatenciesEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.NodeResponse-int32) |  |  |  |
| value | [int64](#cockroach.server.serverpb.NodeResponse-int64) |  |  |  |





<a name="cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.NodeResponse.ActivityEntry"></a>
#### NodeResponse.ActivityEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.NodeResponse-int32) |  |  |  |
| value | [NodeResponse.NetworkActivity](#cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.NodeResponse.NetworkActivity) |  |  |  |





<a name="cockroach.server.serverpb.NodeResponse-cockroach.server.serverpb.NodeResponse.NetworkActivity"></a>
#### NodeResponse.NetworkActivity



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| incoming | [int64](#cockroach.server.serverpb.NodeResponse-int64) |  | in bytes | [reserved](#support-status) |
| outgoing | [int64](#cockroach.server.serverpb.NodeResponse-int64) |  | in bytes | [reserved](#support-status) |
| latency | [int64](#cockroach.server.serverpb.NodeResponse-int64) |  | in nanoseconds | [reserved](#support-status) |






## RaftDebug

`GET /_status/raft`

RaftDebug requests internal details about Raft.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_ids | [int64](#cockroach.server.serverpb.RaftDebugRequest-int64) | repeated |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| ranges | [RaftDebugResponse.RangesEntry](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftDebugResponse.RangesEntry) | repeated |  | [reserved](#support-status) |
| errors | [RaftRangeError](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeError) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftDebugResponse.RangesEntry"></a>
#### RaftDebugResponse.RangesEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  |  |
| value | [RaftRangeStatus](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeStatus) |  |  |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeStatus"></a>
#### RaftRangeStatus



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  | [reserved](#support-status) |
| errors | [RaftRangeError](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeError) | repeated |  | [reserved](#support-status) |
| nodes | [RaftRangeNode](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeNode) | repeated |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeError"></a>
#### RaftRangeError



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| message | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeNode"></a>
#### RaftRangeNode



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.RaftDebugResponse-int32) |  |  | [reserved](#support-status) |
| range | [RangeInfo](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeInfo) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeInfo"></a>
#### RangeInfo



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| span | [PrettySpan](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.PrettySpan) |  |  | [reserved](#support-status) |
| raft_state | [RaftState](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState) |  |  | [reserved](#support-status) |
| state | [cockroach.kv.kvserver.storagepb.RangeInfo](#cockroach.server.serverpb.RaftDebugResponse-cockroach.kv.kvserver.storagepb.RangeInfo) |  |  | [reserved](#support-status) |
| source_node_id | [int32](#cockroach.server.serverpb.RaftDebugResponse-int32) |  |  | [reserved](#support-status) |
| source_store_id | [int32](#cockroach.server.serverpb.RaftDebugResponse-int32) |  |  | [reserved](#support-status) |
| error_message | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  | [reserved](#support-status) |
| lease_history | [cockroach.roachpb.Lease](#cockroach.server.serverpb.RaftDebugResponse-cockroach.roachpb.Lease) | repeated |  | [reserved](#support-status) |
| problems | [RangeProblems](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeProblems) |  |  | [reserved](#support-status) |
| stats | [RangeStatistics](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeStatistics) |  |  | [reserved](#support-status) |
| lease_status | [cockroach.kv.kvserver.storagepb.LeaseStatus](#cockroach.server.serverpb.RaftDebugResponse-cockroach.kv.kvserver.storagepb.LeaseStatus) |  |  | [reserved](#support-status) |
| quiescent | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | [reserved](#support-status) |
| ticking | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | [reserved](#support-status) |
| read_latches | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  | [reserved](#support-status) |
| write_latches | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  | [reserved](#support-status) |
| locks | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  | [reserved](#support-status) |
| locks_with_wait_queues | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  | [reserved](#support-status) |
| lock_wait_queue_waiters | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  | [reserved](#support-status) |
| top_k_locks_by_wait_queue_waiters | [RangeInfo.LockInfo](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeInfo.LockInfo) | repeated |  | [reserved](#support-status) |
| locality | [Locality](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.Locality) |  |  | [reserved](#support-status) |
| is_leaseholder | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | [reserved](#support-status) |
| lease_valid | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.PrettySpan"></a>
#### PrettySpan



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| start_key | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  | [reserved](#support-status) |
| end_key | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState"></a>
#### RaftState

RaftState gives internal details about a Raft group's state.
Closely mirrors the upstream definitions in github.com/etcd-io/etcd/raft.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| replica_id | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  | [reserved](#support-status) |
| hard_state | [raftpb.HardState](#cockroach.server.serverpb.RaftDebugResponse-raftpb.HardState) |  |  | [reserved](#support-status) |
| lead | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  | Lead is part of Raft's SoftState. | [reserved](#support-status) |
| state | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  | State is part of Raft's SoftState. It's not an enum because this is primarily for ui consumption and there are issues associated with them. | [reserved](#support-status) |
| applied | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  | [reserved](#support-status) |
| progress | [RaftState.ProgressEntry](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState.ProgressEntry) | repeated |  | [reserved](#support-status) |
| lead_transferee | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState.ProgressEntry"></a>
#### RaftState.ProgressEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  |  |
| value | [RaftState.Progress](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState.Progress) |  |  |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState.Progress"></a>
#### RaftState.Progress



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| match | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  | [reserved](#support-status) |
| next | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  | [reserved](#support-status) |
| state | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  | [reserved](#support-status) |
| paused | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | [reserved](#support-status) |
| pending_snapshot | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeProblems"></a>
#### RangeProblems

RangeProblems describes issues reported by a range. For internal use only.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| unavailable | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | [reserved](#support-status) |
| leader_not_lease_holder | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | [reserved](#support-status) |
| no_raft_leader | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | [reserved](#support-status) |
| underreplicated | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | [reserved](#support-status) |
| overreplicated | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | [reserved](#support-status) |
| no_lease | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | [reserved](#support-status) |
| quiescent_equals_ticking | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  | Quiescent ranges do not tick by definition, but we track this in two different ways and suspect that they're getting out of sync. If the replica's quiescent flag doesn't agree with the store's list of replicas that are ticking, warn about it. | [reserved](#support-status) |
| raft_log_too_large | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  | When the raft log is too large, it can be a symptom of other issues. | [reserved](#support-status) |
| circuit_breaker_error | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeStatistics"></a>
#### RangeStatistics

RangeStatistics describes statistics reported by a range. For internal use
only.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| queries_per_second | [double](#cockroach.server.serverpb.RaftDebugResponse-double) |  | Queries per second served by this range.<br><br>Note that queries per second will only be known by the leaseholder. All other replicas will report it as 0. | [reserved](#support-status) |
| writes_per_second | [double](#cockroach.server.serverpb.RaftDebugResponse-double) |  | Writes per second served by this range. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeInfo.LockInfo"></a>
#### RangeInfo.LockInfo



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| pretty_key | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  | [reserved](#support-status) |
| key | [bytes](#cockroach.server.serverpb.RaftDebugResponse-bytes) |  |  | [reserved](#support-status) |
| held | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | [reserved](#support-status) |
| waiters | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  | [reserved](#support-status) |
| waiting_readers | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  | [reserved](#support-status) |
| waiting_writers | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.Locality"></a>
#### Locality

Locality is an ordered set of key value Tiers that describe a node's
location. The tier keys should be the same across all nodes.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| tiers | [Tier](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.Tier) | repeated |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.Tier"></a>
#### Tier

Tier represents one level of the locality hierarchy.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  | Key is the name of tier and should match all other nodes. | [reserved](#support-status) |
| value | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  | Value is node specific value corresponding to the key. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeError"></a>
#### RaftRangeError



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| message | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  | [reserved](#support-status) |






## Ranges

`GET /_status/ranges/{node_id}`

Ranges requests internal details about ranges on a given node.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.RangesRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |
| range_ids | [int64](#cockroach.server.serverpb.RangesRequest-int64) | repeated |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| ranges | [RangeInfo](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeInfo) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeInfo"></a>
#### RangeInfo



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| span | [PrettySpan](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.PrettySpan) |  |  | [reserved](#support-status) |
| raft_state | [RaftState](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState) |  |  | [reserved](#support-status) |
| state | [cockroach.kv.kvserver.storagepb.RangeInfo](#cockroach.server.serverpb.RangesResponse-cockroach.kv.kvserver.storagepb.RangeInfo) |  |  | [reserved](#support-status) |
| source_node_id | [int32](#cockroach.server.serverpb.RangesResponse-int32) |  |  | [reserved](#support-status) |
| source_store_id | [int32](#cockroach.server.serverpb.RangesResponse-int32) |  |  | [reserved](#support-status) |
| error_message | [string](#cockroach.server.serverpb.RangesResponse-string) |  |  | [reserved](#support-status) |
| lease_history | [cockroach.roachpb.Lease](#cockroach.server.serverpb.RangesResponse-cockroach.roachpb.Lease) | repeated |  | [reserved](#support-status) |
| problems | [RangeProblems](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeProblems) |  |  | [reserved](#support-status) |
| stats | [RangeStatistics](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeStatistics) |  |  | [reserved](#support-status) |
| lease_status | [cockroach.kv.kvserver.storagepb.LeaseStatus](#cockroach.server.serverpb.RangesResponse-cockroach.kv.kvserver.storagepb.LeaseStatus) |  |  | [reserved](#support-status) |
| quiescent | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | [reserved](#support-status) |
| ticking | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | [reserved](#support-status) |
| read_latches | [int64](#cockroach.server.serverpb.RangesResponse-int64) |  |  | [reserved](#support-status) |
| write_latches | [int64](#cockroach.server.serverpb.RangesResponse-int64) |  |  | [reserved](#support-status) |
| locks | [int64](#cockroach.server.serverpb.RangesResponse-int64) |  |  | [reserved](#support-status) |
| locks_with_wait_queues | [int64](#cockroach.server.serverpb.RangesResponse-int64) |  |  | [reserved](#support-status) |
| lock_wait_queue_waiters | [int64](#cockroach.server.serverpb.RangesResponse-int64) |  |  | [reserved](#support-status) |
| top_k_locks_by_wait_queue_waiters | [RangeInfo.LockInfo](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeInfo.LockInfo) | repeated |  | [reserved](#support-status) |
| locality | [Locality](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.Locality) |  |  | [reserved](#support-status) |
| is_leaseholder | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | [reserved](#support-status) |
| lease_valid | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.PrettySpan"></a>
#### PrettySpan



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| start_key | [string](#cockroach.server.serverpb.RangesResponse-string) |  |  | [reserved](#support-status) |
| end_key | [string](#cockroach.server.serverpb.RangesResponse-string) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState"></a>
#### RaftState

RaftState gives internal details about a Raft group's state.
Closely mirrors the upstream definitions in github.com/etcd-io/etcd/raft.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| replica_id | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  | [reserved](#support-status) |
| hard_state | [raftpb.HardState](#cockroach.server.serverpb.RangesResponse-raftpb.HardState) |  |  | [reserved](#support-status) |
| lead | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  | Lead is part of Raft's SoftState. | [reserved](#support-status) |
| state | [string](#cockroach.server.serverpb.RangesResponse-string) |  | State is part of Raft's SoftState. It's not an enum because this is primarily for ui consumption and there are issues associated with them. | [reserved](#support-status) |
| applied | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  | [reserved](#support-status) |
| progress | [RaftState.ProgressEntry](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState.ProgressEntry) | repeated |  | [reserved](#support-status) |
| lead_transferee | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState.ProgressEntry"></a>
#### RaftState.ProgressEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  |  |
| value | [RaftState.Progress](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState.Progress) |  |  |  |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState.Progress"></a>
#### RaftState.Progress



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| match | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  | [reserved](#support-status) |
| next | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  | [reserved](#support-status) |
| state | [string](#cockroach.server.serverpb.RangesResponse-string) |  |  | [reserved](#support-status) |
| paused | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | [reserved](#support-status) |
| pending_snapshot | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeProblems"></a>
#### RangeProblems

RangeProblems describes issues reported by a range. For internal use only.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| unavailable | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | [reserved](#support-status) |
| leader_not_lease_holder | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | [reserved](#support-status) |
| no_raft_leader | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | [reserved](#support-status) |
| underreplicated | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | [reserved](#support-status) |
| overreplicated | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | [reserved](#support-status) |
| no_lease | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | [reserved](#support-status) |
| quiescent_equals_ticking | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  | Quiescent ranges do not tick by definition, but we track this in two different ways and suspect that they're getting out of sync. If the replica's quiescent flag doesn't agree with the store's list of replicas that are ticking, warn about it. | [reserved](#support-status) |
| raft_log_too_large | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  | When the raft log is too large, it can be a symptom of other issues. | [reserved](#support-status) |
| circuit_breaker_error | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeStatistics"></a>
#### RangeStatistics

RangeStatistics describes statistics reported by a range. For internal use
only.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| queries_per_second | [double](#cockroach.server.serverpb.RangesResponse-double) |  | Queries per second served by this range.<br><br>Note that queries per second will only be known by the leaseholder. All other replicas will report it as 0. | [reserved](#support-status) |
| writes_per_second | [double](#cockroach.server.serverpb.RangesResponse-double) |  | Writes per second served by this range. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeInfo.LockInfo"></a>
#### RangeInfo.LockInfo



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| pretty_key | [string](#cockroach.server.serverpb.RangesResponse-string) |  |  | [reserved](#support-status) |
| key | [bytes](#cockroach.server.serverpb.RangesResponse-bytes) |  |  | [reserved](#support-status) |
| held | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | [reserved](#support-status) |
| waiters | [int64](#cockroach.server.serverpb.RangesResponse-int64) |  |  | [reserved](#support-status) |
| waiting_readers | [int64](#cockroach.server.serverpb.RangesResponse-int64) |  |  | [reserved](#support-status) |
| waiting_writers | [int64](#cockroach.server.serverpb.RangesResponse-int64) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.Locality"></a>
#### Locality

Locality is an ordered set of key value Tiers that describe a node's
location. The tier keys should be the same across all nodes.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| tiers | [Tier](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.Tier) | repeated |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.Tier"></a>
#### Tier

Tier represents one level of the locality hierarchy.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.RangesResponse-string) |  | Key is the name of tier and should match all other nodes. | [reserved](#support-status) |
| value | [string](#cockroach.server.serverpb.RangesResponse-string) |  | Value is node specific value corresponding to the key. | [reserved](#support-status) |






## TenantRanges

`GET /_status/tenant_ranges`

TenantRanges requests internal details about all range replicas within
the tenant's keyspace.

Support status: [reserved](#support-status)

#### Request Parameters













#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| ranges_by_locality | [TenantRangesResponse.RangesByLocalityEntry](#cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.TenantRangesResponse.RangesByLocalityEntry) | repeated | ranges_by_locality maps each range replica to its specified availability zone, as defined within the replica's locality metadata (default key `az`). Replicas without the default available zone key set will fall under the `locality-unset` key. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.TenantRangesResponse.RangesByLocalityEntry"></a>
#### TenantRangesResponse.RangesByLocalityEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.TenantRangesResponse-string) |  |  |  |
| value | [TenantRangesResponse.TenantRangeList](#cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.TenantRangesResponse.TenantRangeList) |  |  |  |





<a name="cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.TenantRangesResponse.TenantRangeList"></a>
#### TenantRangesResponse.TenantRangeList



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| ranges | [TenantRangeInfo](#cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.TenantRangeInfo) | repeated |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.TenantRangeInfo"></a>
#### TenantRangeInfo

TenantRangeInfo provides metadata about a specific range replica,
where concepts not considered to be relevant within the tenant
abstraction (e.g. NodeIDs) are omitted. Instead, Locality information
is used to distinguish replicas.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int64](#cockroach.server.serverpb.TenantRangesResponse-int64) |  | The ID of the Range. | [reserved](#support-status) |
| span | [PrettySpan](#cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.PrettySpan) |  | The pretty-printed key span of the range. | [reserved](#support-status) |
| locality | [Locality](#cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.Locality) |  | Any locality information associated with this specific replica. | [reserved](#support-status) |
| is_leaseholder | [bool](#cockroach.server.serverpb.TenantRangesResponse-bool) |  | Whether the range's specific replica is a leaseholder. | [reserved](#support-status) |
| lease_valid | [bool](#cockroach.server.serverpb.TenantRangesResponse-bool) |  | Whether the range's specific replica holds a valid lease. | [reserved](#support-status) |
| range_stats | [RangeStatistics](#cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.RangeStatistics) |  | Statistics about the range replica, e.g. QPS, WPS. | [reserved](#support-status) |
| mvcc_stats | [cockroach.storage.enginepb.MVCCStats](#cockroach.server.serverpb.TenantRangesResponse-cockroach.storage.enginepb.MVCCStats) |  | MVCC stats about the range replica, e.g. live_bytes. | [reserved](#support-status) |
| read_latches | [int64](#cockroach.server.serverpb.TenantRangesResponse-int64) |  | Read count as reported by the range replica's spanlatch.Manager. | [reserved](#support-status) |
| write_latches | [int64](#cockroach.server.serverpb.TenantRangesResponse-int64) |  | Write count as reported by the range replica's spanlatch.Manager. | [reserved](#support-status) |
| locks | [int64](#cockroach.server.serverpb.TenantRangesResponse-int64) |  | The number of locks as reported by the range replica's lockTable. | [reserved](#support-status) |
| locks_with_wait_queues | [int64](#cockroach.server.serverpb.TenantRangesResponse-int64) |  | The number of locks with non-empty wait-queues as reported by the range replica's lockTable | [reserved](#support-status) |
| lock_wait_queue_waiters | [int64](#cockroach.server.serverpb.TenantRangesResponse-int64) |  | The aggregate number of waiters in wait-queues across all locks as reported by the range replica's lockTable | [reserved](#support-status) |
| top_k_locks_by_wait_queue_waiters | [TenantRangeInfo.LockInfo](#cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.TenantRangeInfo.LockInfo) | repeated | The top-k locks with the most waiters (readers + writers) in their wait-queue, ordered in descending order. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.PrettySpan"></a>
#### PrettySpan



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| start_key | [string](#cockroach.server.serverpb.TenantRangesResponse-string) |  |  | [reserved](#support-status) |
| end_key | [string](#cockroach.server.serverpb.TenantRangesResponse-string) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.Locality"></a>
#### Locality

Locality is an ordered set of key value Tiers that describe a node's
location. The tier keys should be the same across all nodes.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| tiers | [Tier](#cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.Tier) | repeated |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.Tier"></a>
#### Tier

Tier represents one level of the locality hierarchy.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.TenantRangesResponse-string) |  | Key is the name of tier and should match all other nodes. | [reserved](#support-status) |
| value | [string](#cockroach.server.serverpb.TenantRangesResponse-string) |  | Value is node specific value corresponding to the key. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.RangeStatistics"></a>
#### RangeStatistics

RangeStatistics describes statistics reported by a range. For internal use
only.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| queries_per_second | [double](#cockroach.server.serverpb.TenantRangesResponse-double) |  | Queries per second served by this range.<br><br>Note that queries per second will only be known by the leaseholder. All other replicas will report it as 0. | [reserved](#support-status) |
| writes_per_second | [double](#cockroach.server.serverpb.TenantRangesResponse-double) |  | Writes per second served by this range. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.TenantRangesResponse-cockroach.server.serverpb.TenantRangeInfo.LockInfo"></a>
#### TenantRangeInfo.LockInfo

LockInfo provides metadata about the state of a single lock
in the range replica's lockTable.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| pretty_key | [string](#cockroach.server.serverpb.TenantRangesResponse-string) |  | The lock's key in pretty format. | [reserved](#support-status) |
| key | [bytes](#cockroach.server.serverpb.TenantRangesResponse-bytes) |  | The lock's key. | [reserved](#support-status) |
| held | [bool](#cockroach.server.serverpb.TenantRangesResponse-bool) |  | Is the lock actively held by a transaction, or just a reservation? | [reserved](#support-status) |
| waiters | [int64](#cockroach.server.serverpb.TenantRangesResponse-int64) |  | The number of waiters in the lock's wait queue. | [reserved](#support-status) |
| waiting_readers | [int64](#cockroach.server.serverpb.TenantRangesResponse-int64) |  | The number of waiting readers in the lock's wait queue. | [reserved](#support-status) |
| waiting_writers | [int64](#cockroach.server.serverpb.TenantRangesResponse-int64) |  | The number of waiting writers in the lock's wait queue. | [reserved](#support-status) |






## Gossip

`GET /_status/gossip/{node_id}`

Gossip retrieves gossip-level details about a given node.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.GossipRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |







#### Response Parameters



## EngineStats

`GET /_status/enginestats/{node_id}`

EngineStats retrieves statistics about a storage engine.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.EngineStatsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| stats | [EngineStatsInfo](#cockroach.server.serverpb.EngineStatsResponse-cockroach.server.serverpb.EngineStatsInfo) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.EngineStatsResponse-cockroach.server.serverpb.EngineStatsInfo"></a>
#### EngineStatsInfo



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| store_id | [int32](#cockroach.server.serverpb.EngineStatsResponse-int32) |  |  | [reserved](#support-status) |
| tickers_and_histograms | [cockroach.storage.enginepb.TickersAndHistograms](#cockroach.server.serverpb.EngineStatsResponse-cockroach.storage.enginepb.TickersAndHistograms) |  |  | [reserved](#support-status) |
| engine_type | [cockroach.storage.enginepb.EngineType](#cockroach.server.serverpb.EngineStatsResponse-cockroach.storage.enginepb.EngineType) |  |  | [reserved](#support-status) |






## Allocator

`GET /_status/allocator/node/{node_id}`

Allocator retrieves statistics about the replica allocator.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.AllocatorRequest-string) |  |  | [reserved](#support-status) |
| range_ids | [int64](#cockroach.server.serverpb.AllocatorRequest-int64) | repeated |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| dry_runs | [AllocatorDryRun](#cockroach.server.serverpb.AllocatorResponse-cockroach.server.serverpb.AllocatorDryRun) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.AllocatorResponse-cockroach.server.serverpb.AllocatorDryRun"></a>
#### AllocatorDryRun



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int64](#cockroach.server.serverpb.AllocatorResponse-int64) |  |  | [reserved](#support-status) |
| events | [TraceEvent](#cockroach.server.serverpb.AllocatorResponse-cockroach.server.serverpb.TraceEvent) | repeated |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.AllocatorResponse-cockroach.server.serverpb.TraceEvent"></a>
#### TraceEvent



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| time | [google.protobuf.Timestamp](#cockroach.server.serverpb.AllocatorResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| message | [string](#cockroach.server.serverpb.AllocatorResponse-string) |  |  | [reserved](#support-status) |






## AllocatorRange

`GET /_status/allocator/range/{range_id}`

AllocatorRange retrieves statistics about the replica allocator given
a specific range.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int64](#cockroach.server.serverpb.AllocatorRangeRequest-int64) |  |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int64](#cockroach.server.serverpb.AllocatorRangeResponse-int64) |  | The NodeID of the store whose dry run is returned. Only the leaseholder for a given range will do an allocator dry run for it. | [reserved](#support-status) |
| dry_run | [AllocatorDryRun](#cockroach.server.serverpb.AllocatorRangeResponse-cockroach.server.serverpb.AllocatorDryRun) |  |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.AllocatorRangeResponse-cockroach.server.serverpb.AllocatorDryRun"></a>
#### AllocatorDryRun



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int64](#cockroach.server.serverpb.AllocatorRangeResponse-int64) |  |  | [reserved](#support-status) |
| events | [TraceEvent](#cockroach.server.serverpb.AllocatorRangeResponse-cockroach.server.serverpb.TraceEvent) | repeated |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.AllocatorRangeResponse-cockroach.server.serverpb.TraceEvent"></a>
#### TraceEvent



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| time | [google.protobuf.Timestamp](#cockroach.server.serverpb.AllocatorRangeResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| message | [string](#cockroach.server.serverpb.AllocatorRangeResponse-string) |  |  | [reserved](#support-status) |






## ListSessions

`GET /_status/sessions`

ListSessions retrieves the SQL sessions across the entire cluster.

Support status: [reserved](#support-status)

#### Request Parameters




Request object for ListSessions and ListLocalSessions.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| username | [string](#cockroach.server.serverpb.ListSessionsRequest-string) |  | Username of the user making this request. The caller is responsible to normalize the username (= case fold and perform unicode NFC normalization). | [reserved](#support-status) |







#### Response Parameters




Response object for ListSessions and ListLocalSessions.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| sessions | [Session](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.Session) | repeated | A list of sessions on this node or cluster. | [reserved](#support-status) |
| errors | [ListSessionsError](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ListSessionsError) | repeated | Any errors that occurred during fan-out calls to other nodes. | [reserved](#support-status) |
| internal_app_name_prefix | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | If set and non-empty, indicates the prefix to application_name used for statements/queries issued internally by CockroachDB. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.Session"></a>
#### Session

Session represents one SQL session.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | ID of node where this session exists. | [reserved](#support-status) |
| username | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Username of the user for this session. | [reserved](#support-status) |
| client_address | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Connected client's IP address and port. | [reserved](#support-status) |
| application_name | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Application name specified by the client. | [reserved](#support-status) |
| active_queries | [ActiveQuery](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery) | repeated | Queries in progress on this session. | [reserved](#support-status) |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | Timestamp of session's start. | [reserved](#support-status) |
| last_active_query | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | SQL string of the last query executed on this session. | [reserved](#support-status) |
| id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  | ID of the session (uint128 represented as raw bytes). | [reserved](#support-status) |
| alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | Number of currently allocated bytes in the session memory monitor. | [reserved](#support-status) |
| max_alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | High water mark of allocated bytes in the session memory monitor. | [reserved](#support-status) |
| active_txn | [TxnInfo](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.TxnInfo) |  | Information about the txn in progress on this session. Nil if the session doesn't currently have a transaction. | [reserved](#support-status) |
| last_active_query_no_constants | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | The SQL statement fingerprint of the last query executed on this session, compatible with StatementStatisticsKey. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery"></a>
#### ActiveQuery

ActiveQuery represents a query in flight on some Session.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | ID of the query (uint128 presented as a hexadecimal string). | [reserved](#support-status) |
| txn_id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  | The UUID of the transaction this query is running in. | [reserved](#support-status) |
| sql | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | SQL query string specified by the user. | [reserved](#support-status) |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | Start timestamp of this query. | [reserved](#support-status) |
| is_distributed | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  | True if this query is distributed. | [reserved](#support-status) |
| phase | [ActiveQuery.Phase](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery.Phase) |  | phase stores the current phase of execution for this query. | [reserved](#support-status) |
| progress | [float](#cockroach.server.serverpb.ListSessionsResponse-float) |  | progress is an estimate of the fraction of this query that has been processed. | [reserved](#support-status) |
| sql_no_constants | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | The SQL statement fingerprint, compatible with StatementStatisticsKey. | [reserved](#support-status) |
| sql_summary | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | A summarized version of the sql query. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.TxnInfo"></a>
#### TxnInfo

TxnInfo represents an in flight user transaction on some Session.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  |  | [reserved](#support-status) |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | The start timestamp of the transaction. | [reserved](#support-status) |
| txn_description | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | txn_description is a text description of the underlying kv.Txn, intended for troubleshooting purposes. | [reserved](#support-status) |
| num_statements_executed | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_statements_executed is the number of statements that were executed so far on this transaction. | [reserved](#support-status) |
| num_retries | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_retries is the number of times that this transaction was retried. | [reserved](#support-status) |
| num_auto_retries | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_retries is the number of times that this transaction was automatically retried by the SQL executor. | [reserved](#support-status) |
| deadline | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | The deadline by which the transaction must be committed. | [reserved](#support-status) |
| implicit | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  | implicit is true if this transaction was an implicit SQL transaction. | [reserved](#support-status) |
| alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | Number of currently allocated bytes in the txn memory monitor. | [reserved](#support-status) |
| max_alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | High water mark of allocated bytes in the txn memory monitor. | [reserved](#support-status) |
| read_only | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  |  | [reserved](#support-status) |
| is_historical | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  |  | [reserved](#support-status) |
| priority | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  |  | [reserved](#support-status) |
| quality_of_service | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ListSessionsError"></a>
#### ListSessionsError

An error wrapper object for ListSessionsResponse.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | ID of node that was being contacted when this error occurred | [reserved](#support-status) |
| message | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Error message. | [reserved](#support-status) |






## ListLocalSessions

`GET /_status/local_sessions`

ListLocalSessions retrieves the SQL sessions on this node.

Support status: [reserved](#support-status)

#### Request Parameters




Request object for ListSessions and ListLocalSessions.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| username | [string](#cockroach.server.serverpb.ListSessionsRequest-string) |  | Username of the user making this request. The caller is responsible to normalize the username (= case fold and perform unicode NFC normalization). | [reserved](#support-status) |







#### Response Parameters




Response object for ListSessions and ListLocalSessions.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| sessions | [Session](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.Session) | repeated | A list of sessions on this node or cluster. | [reserved](#support-status) |
| errors | [ListSessionsError](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ListSessionsError) | repeated | Any errors that occurred during fan-out calls to other nodes. | [reserved](#support-status) |
| internal_app_name_prefix | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | If set and non-empty, indicates the prefix to application_name used for statements/queries issued internally by CockroachDB. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.Session"></a>
#### Session

Session represents one SQL session.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | ID of node where this session exists. | [reserved](#support-status) |
| username | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Username of the user for this session. | [reserved](#support-status) |
| client_address | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Connected client's IP address and port. | [reserved](#support-status) |
| application_name | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Application name specified by the client. | [reserved](#support-status) |
| active_queries | [ActiveQuery](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery) | repeated | Queries in progress on this session. | [reserved](#support-status) |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | Timestamp of session's start. | [reserved](#support-status) |
| last_active_query | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | SQL string of the last query executed on this session. | [reserved](#support-status) |
| id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  | ID of the session (uint128 represented as raw bytes). | [reserved](#support-status) |
| alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | Number of currently allocated bytes in the session memory monitor. | [reserved](#support-status) |
| max_alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | High water mark of allocated bytes in the session memory monitor. | [reserved](#support-status) |
| active_txn | [TxnInfo](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.TxnInfo) |  | Information about the txn in progress on this session. Nil if the session doesn't currently have a transaction. | [reserved](#support-status) |
| last_active_query_no_constants | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | The SQL statement fingerprint of the last query executed on this session, compatible with StatementStatisticsKey. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery"></a>
#### ActiveQuery

ActiveQuery represents a query in flight on some Session.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | ID of the query (uint128 presented as a hexadecimal string). | [reserved](#support-status) |
| txn_id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  | The UUID of the transaction this query is running in. | [reserved](#support-status) |
| sql | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | SQL query string specified by the user. | [reserved](#support-status) |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | Start timestamp of this query. | [reserved](#support-status) |
| is_distributed | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  | True if this query is distributed. | [reserved](#support-status) |
| phase | [ActiveQuery.Phase](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery.Phase) |  | phase stores the current phase of execution for this query. | [reserved](#support-status) |
| progress | [float](#cockroach.server.serverpb.ListSessionsResponse-float) |  | progress is an estimate of the fraction of this query that has been processed. | [reserved](#support-status) |
| sql_no_constants | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | The SQL statement fingerprint, compatible with StatementStatisticsKey. | [reserved](#support-status) |
| sql_summary | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | A summarized version of the sql query. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.TxnInfo"></a>
#### TxnInfo

TxnInfo represents an in flight user transaction on some Session.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  |  | [reserved](#support-status) |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | The start timestamp of the transaction. | [reserved](#support-status) |
| txn_description | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | txn_description is a text description of the underlying kv.Txn, intended for troubleshooting purposes. | [reserved](#support-status) |
| num_statements_executed | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_statements_executed is the number of statements that were executed so far on this transaction. | [reserved](#support-status) |
| num_retries | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_retries is the number of times that this transaction was retried. | [reserved](#support-status) |
| num_auto_retries | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_retries is the number of times that this transaction was automatically retried by the SQL executor. | [reserved](#support-status) |
| deadline | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | The deadline by which the transaction must be committed. | [reserved](#support-status) |
| implicit | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  | implicit is true if this transaction was an implicit SQL transaction. | [reserved](#support-status) |
| alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | Number of currently allocated bytes in the txn memory monitor. | [reserved](#support-status) |
| max_alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | High water mark of allocated bytes in the txn memory monitor. | [reserved](#support-status) |
| read_only | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  |  | [reserved](#support-status) |
| is_historical | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  |  | [reserved](#support-status) |
| priority | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  |  | [reserved](#support-status) |
| quality_of_service | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ListSessionsError"></a>
#### ListSessionsError

An error wrapper object for ListSessionsResponse.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | ID of node that was being contacted when this error occurred | [reserved](#support-status) |
| message | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Error message. | [reserved](#support-status) |






## CancelQuery

`POST /_status/cancel_query/{node_id}`

CancelQuery cancels a SQL query given its ID.

Support status: [reserved](#support-status)

#### Request Parameters




Request object for issuing a query cancel request.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.CancelQueryRequest-string) |  | ID of gateway node for the query to be canceled.<br><br>TODO(itsbilal): use [(gogoproto.customname) = "NodeID"] below. Need to figure out how to teach grpc-gateway about custom names.<br><br>node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |
| query_id | [string](#cockroach.server.serverpb.CancelQueryRequest-string) |  | ID of query to be canceled (converted to string). | [reserved](#support-status) |
| username | [string](#cockroach.server.serverpb.CancelQueryRequest-string) |  | Username of the user making this cancellation request. This may be omitted if the user is the same as the one issuing the CancelQueryRequest. The caller is responsible for case-folding and NFC normalization. | [reserved](#support-status) |







#### Response Parameters




Response returned by target query's gateway node.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| canceled | [bool](#cockroach.server.serverpb.CancelQueryResponse-bool) |  | Whether the cancellation request succeeded and the query was canceled. | [reserved](#support-status) |
| error | [string](#cockroach.server.serverpb.CancelQueryResponse-string) |  | Error message (accompanied with canceled = false). | [reserved](#support-status) |







## CancelLocalQuery

`POST /_status/cancel_local_query`

CancelLocalQuery cancels a SQL query running on this node given its ID.

Support status: [reserved](#support-status)

#### Request Parameters




Request object for issuing a query cancel request.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.CancelQueryRequest-string) |  | ID of gateway node for the query to be canceled.<br><br>TODO(itsbilal): use [(gogoproto.customname) = "NodeID"] below. Need to figure out how to teach grpc-gateway about custom names.<br><br>node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |
| query_id | [string](#cockroach.server.serverpb.CancelQueryRequest-string) |  | ID of query to be canceled (converted to string). | [reserved](#support-status) |
| username | [string](#cockroach.server.serverpb.CancelQueryRequest-string) |  | Username of the user making this cancellation request. This may be omitted if the user is the same as the one issuing the CancelQueryRequest. The caller is responsible for case-folding and NFC normalization. | [reserved](#support-status) |







#### Response Parameters




Response returned by target query's gateway node.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| canceled | [bool](#cockroach.server.serverpb.CancelQueryResponse-bool) |  | Whether the cancellation request succeeded and the query was canceled. | [reserved](#support-status) |
| error | [string](#cockroach.server.serverpb.CancelQueryResponse-string) |  | Error message (accompanied with canceled = false). | [reserved](#support-status) |







## CancelQueryByKey



CancelQueryByKey cancels a SQL query given its pgwire BackendKeyData.
It is invoked through the pgwire protocol, so it's not exposed as an
HTTP endpoint.

Support status: [reserved](#support-status)

#### Request Parameters




Request object for issuing a pgwire query cancel request.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| sql_instance_id | [int32](#cockroach.server.serverpb.CancelQueryByKeyRequest-int32) |  | The SQLInstanceID of the gateway node for the query to be canceled. | [reserved](#support-status) |
| cancel_query_key | [uint64](#cockroach.server.serverpb.CancelQueryByKeyRequest-uint64) |  | The key that was generated during session initialization as part of the pgwire protocol. | [reserved](#support-status) |







#### Response Parameters




Response returned by target query's gateway node for a pgwire cancel request.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| canceled | [bool](#cockroach.server.serverpb.CancelQueryByKeyResponse-bool) |  | Whether the cancellation request succeeded and the query was canceled. | [reserved](#support-status) |
| error | [string](#cockroach.server.serverpb.CancelQueryByKeyResponse-string) |  | Error message (accompanied with canceled = false). | [reserved](#support-status) |







## ListContentionEvents

`GET /_status/contention_events`

ListContentionEvents retrieves the contention events across the entire
cluster.

For SQL keys the following orderings are maintained:
- on the highest level, all IndexContentionEvents objects are ordered
  according to their importance (as defined by the number of contention
  events within each object).
- on the middle level, all SingleKeyContention objects are ordered by their
  keys lexicographically.
- on the lowest level, all SingleTxnContention objects are ordered by the
  number of times that transaction was observed to contend with other
  transactions.

For non-SQL keys the following orderings are maintained:
- on the top level, all SingleNonSQLKeyContention objects are ordered
  by their keys lexicographically.
- on the bottom level, all SingleTxnContention objects are ordered by the
  number of times that transaction was observed to contend with other
  transactions.

Support status: [reserved](#support-status)

#### Request Parameters




Request object for ListContentionEvents and ListLocalContentionEvents.








#### Response Parameters




Response object for ListContentionEvents and ListLocalContentionEvents.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| events | [cockroach.sql.contentionpb.SerializedRegistry](#cockroach.server.serverpb.ListContentionEventsResponse-cockroach.sql.contentionpb.SerializedRegistry) |  | All available contention information on this node or cluster. | [reserved](#support-status) |
| errors | [ListActivityError](#cockroach.server.serverpb.ListContentionEventsResponse-cockroach.server.serverpb.ListActivityError) | repeated | Any errors that occurred during fan-out calls to other nodes. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.ListContentionEventsResponse-cockroach.server.serverpb.ListActivityError"></a>
#### ListActivityError

An error wrapper object for ListContentionEventsResponse and
ListDistSQLFlowsResponse. Similar to the Statements endpoint, when
implemented on a tenant, the `node_id` field refers to the instanceIDs that
identify individual tenant pods.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListContentionEventsResponse-int32) |  | ID of node that was being contacted when this error occurred. | [reserved](#support-status) |
| message | [string](#cockroach.server.serverpb.ListContentionEventsResponse-string) |  | Error message. | [reserved](#support-status) |






## ListLocalContentionEvents

`GET /_status/local_contention_events`

ListLocalContentionEvents retrieves the contention events on this node.

For SQL keys the following orderings are maintained:
- on the highest level, all IndexContentionEvents objects are ordered
  according to their importance (as defined by the number of contention
  events within each object).
- on the middle level, all SingleKeyContention objects are ordered by their
  keys lexicographically.
- on the lowest level, all SingleTxnContention objects are ordered by the
  number of times that transaction was observed to contend with other
  transactions.

For non-SQL keys the following orderings are maintained:
- on the top level, all SingleNonSQLKeyContention objects are ordered
  by their keys lexicographically.
- on the bottom level, all SingleTxnContention objects are ordered by the
  number of times that transaction was observed to contend with other
  transactions.

Support status: [reserved](#support-status)

#### Request Parameters




Request object for ListContentionEvents and ListLocalContentionEvents.








#### Response Parameters




Response object for ListContentionEvents and ListLocalContentionEvents.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| events | [cockroach.sql.contentionpb.SerializedRegistry](#cockroach.server.serverpb.ListContentionEventsResponse-cockroach.sql.contentionpb.SerializedRegistry) |  | All available contention information on this node or cluster. | [reserved](#support-status) |
| errors | [ListActivityError](#cockroach.server.serverpb.ListContentionEventsResponse-cockroach.server.serverpb.ListActivityError) | repeated | Any errors that occurred during fan-out calls to other nodes. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.ListContentionEventsResponse-cockroach.server.serverpb.ListActivityError"></a>
#### ListActivityError

An error wrapper object for ListContentionEventsResponse and
ListDistSQLFlowsResponse. Similar to the Statements endpoint, when
implemented on a tenant, the `node_id` field refers to the instanceIDs that
identify individual tenant pods.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListContentionEventsResponse-int32) |  | ID of node that was being contacted when this error occurred. | [reserved](#support-status) |
| message | [string](#cockroach.server.serverpb.ListContentionEventsResponse-string) |  | Error message. | [reserved](#support-status) |






## ListDistSQLFlows

`GET /_status/distsql_flows`

ListDistSQLFlows retrieves all of the remote flows of the DistSQL execution
that are currently running or queued on any node in the cluster. The local
flows (those that are running on the same node as the query originated on)
are not included.

Flows are ordered lexicographically by the flow ID.

Support status: [reserved](#support-status)

#### Request Parameters




Request object for ListDistSQLFlows and ListLocalDistSQLFlows.








#### Response Parameters




Response object for ListDistSQLFlows and ListLocalDistSQLFlows.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| flows | [DistSQLRemoteFlows](#cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.DistSQLRemoteFlows) | repeated | Flows are ordered by FlowID lexicographically and do not contain duplicates. | [reserved](#support-status) |
| errors | [ListActivityError](#cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.ListActivityError) | repeated | Any errors that occurred during fan-out calls to other nodes. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.DistSQLRemoteFlows"></a>
#### DistSQLRemoteFlows

DistSQLRemoteFlows describes all DistSQL remote flows that are part of the
same physical plan. The gateway node that initiated the execution of the plan
isn't included.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| flow_id | [bytes](#cockroach.server.serverpb.ListDistSQLFlowsResponse-bytes) |  | FlowID is the unique identifier of the physical plan shared by all remote flows. | [reserved](#support-status) |
| infos | [DistSQLRemoteFlows.Info](#cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.DistSQLRemoteFlows.Info) | repeated | Infos contains the information about all remote flows that are part of this DistSQL physical plan and that haven't finished yet.<br><br>The slice doesn't contain duplicate NodeIDs and is sorted by NodeID in ASC order. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.DistSQLRemoteFlows.Info"></a>
#### DistSQLRemoteFlows.Info

Info contains an information about a single DistSQL remote flow.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListDistSQLFlowsResponse-int32) |  | NodeID is the node on which this remote flow is either running or queued. | [reserved](#support-status) |
| timestamp | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListDistSQLFlowsResponse-google.protobuf.Timestamp) |  | Timestamp must be in the UTC timezone. | [reserved](#support-status) |
| status | [DistSQLRemoteFlows.Status](#cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.DistSQLRemoteFlows.Status) |  | Status is the current status of this remote flow. | [reserved](#support-status) |
| stmt | [string](#cockroach.server.serverpb.ListDistSQLFlowsResponse-string) |  | Stmt is the SQL statement for which this flow is executing. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.ListActivityError"></a>
#### ListActivityError

An error wrapper object for ListContentionEventsResponse and
ListDistSQLFlowsResponse. Similar to the Statements endpoint, when
implemented on a tenant, the `node_id` field refers to the instanceIDs that
identify individual tenant pods.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListDistSQLFlowsResponse-int32) |  | ID of node that was being contacted when this error occurred. | [reserved](#support-status) |
| message | [string](#cockroach.server.serverpb.ListDistSQLFlowsResponse-string) |  | Error message. | [reserved](#support-status) |






## ListLocalDistSQLFlows

`GET /_status/local_distsql_flows`

ListLocalDistSQLFlows retrieves all of the remote flows of the DistSQL
execution that are currently running or queued on this node in the cluster.
Queries that originated on this node are not be included.

Flows are ordered lexicographically by the flow ID.

Support status: [reserved](#support-status)

#### Request Parameters




Request object for ListDistSQLFlows and ListLocalDistSQLFlows.








#### Response Parameters




Response object for ListDistSQLFlows and ListLocalDistSQLFlows.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| flows | [DistSQLRemoteFlows](#cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.DistSQLRemoteFlows) | repeated | Flows are ordered by FlowID lexicographically and do not contain duplicates. | [reserved](#support-status) |
| errors | [ListActivityError](#cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.ListActivityError) | repeated | Any errors that occurred during fan-out calls to other nodes. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.DistSQLRemoteFlows"></a>
#### DistSQLRemoteFlows

DistSQLRemoteFlows describes all DistSQL remote flows that are part of the
same physical plan. The gateway node that initiated the execution of the plan
isn't included.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| flow_id | [bytes](#cockroach.server.serverpb.ListDistSQLFlowsResponse-bytes) |  | FlowID is the unique identifier of the physical plan shared by all remote flows. | [reserved](#support-status) |
| infos | [DistSQLRemoteFlows.Info](#cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.DistSQLRemoteFlows.Info) | repeated | Infos contains the information about all remote flows that are part of this DistSQL physical plan and that haven't finished yet.<br><br>The slice doesn't contain duplicate NodeIDs and is sorted by NodeID in ASC order. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.DistSQLRemoteFlows.Info"></a>
#### DistSQLRemoteFlows.Info

Info contains an information about a single DistSQL remote flow.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListDistSQLFlowsResponse-int32) |  | NodeID is the node on which this remote flow is either running or queued. | [reserved](#support-status) |
| timestamp | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListDistSQLFlowsResponse-google.protobuf.Timestamp) |  | Timestamp must be in the UTC timezone. | [reserved](#support-status) |
| status | [DistSQLRemoteFlows.Status](#cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.DistSQLRemoteFlows.Status) |  | Status is the current status of this remote flow. | [reserved](#support-status) |
| stmt | [string](#cockroach.server.serverpb.ListDistSQLFlowsResponse-string) |  | Stmt is the SQL statement for which this flow is executing. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.ListDistSQLFlowsResponse-cockroach.server.serverpb.ListActivityError"></a>
#### ListActivityError

An error wrapper object for ListContentionEventsResponse and
ListDistSQLFlowsResponse. Similar to the Statements endpoint, when
implemented on a tenant, the `node_id` field refers to the instanceIDs that
identify individual tenant pods.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListDistSQLFlowsResponse-int32) |  | ID of node that was being contacted when this error occurred. | [reserved](#support-status) |
| message | [string](#cockroach.server.serverpb.ListDistSQLFlowsResponse-string) |  | Error message. | [reserved](#support-status) |






## CancelSession

`POST /_status/cancel_session/{node_id}`

CancelSessions forcefully terminates a SQL session given its ID.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.CancelSessionRequest-string) |  | TODO(abhimadan): use [(gogoproto.customname) = "NodeID"] below. Need to figure out how to teach grpc-gateway about custom names.<br><br>node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |
| session_id | [bytes](#cockroach.server.serverpb.CancelSessionRequest-bytes) |  |  | [reserved](#support-status) |
| username | [string](#cockroach.server.serverpb.CancelSessionRequest-string) |  | Username of the user making this cancellation request. This may be omitted if the user is the same as the one issuing the CancelSessionRequest. The caller is responsible for case-folding and NFC normalization. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| canceled | [bool](#cockroach.server.serverpb.CancelSessionResponse-bool) |  |  | [reserved](#support-status) |
| error | [string](#cockroach.server.serverpb.CancelSessionResponse-string) |  |  | [reserved](#support-status) |







## CancelLocalSession

`POST /_status/cancel_local_session`

CancelLocalSession forcefully terminates a SQL session running on this node given its ID.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.CancelSessionRequest-string) |  | TODO(abhimadan): use [(gogoproto.customname) = "NodeID"] below. Need to figure out how to teach grpc-gateway about custom names.<br><br>node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |
| session_id | [bytes](#cockroach.server.serverpb.CancelSessionRequest-bytes) |  |  | [reserved](#support-status) |
| username | [string](#cockroach.server.serverpb.CancelSessionRequest-string) |  | Username of the user making this cancellation request. This may be omitted if the user is the same as the one issuing the CancelSessionRequest. The caller is responsible for case-folding and NFC normalization. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| canceled | [bool](#cockroach.server.serverpb.CancelSessionResponse-bool) |  |  | [reserved](#support-status) |
| error | [string](#cockroach.server.serverpb.CancelSessionResponse-string) |  |  | [reserved](#support-status) |







## SpanStats

`POST /_status/span`

SpanStats accepts a key span and node ID, and returns a set of stats
summed from all ranges on the stores on that node which contain keys
in that span. This is designed to compute stats specific to a SQL table:
it will be called with the highest/lowest key for a SQL table, and return
information about the resources on a node used by that table.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.SpanStatsRequest-string) |  |  | [reserved](#support-status) |
| start_key | [bytes](#cockroach.server.serverpb.SpanStatsRequest-bytes) |  |  | [reserved](#support-status) |
| end_key | [bytes](#cockroach.server.serverpb.SpanStatsRequest-bytes) |  |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_count | [int32](#cockroach.server.serverpb.SpanStatsResponse-int32) |  |  | [reserved](#support-status) |
| approximate_disk_bytes | [uint64](#cockroach.server.serverpb.SpanStatsResponse-uint64) |  |  | [reserved](#support-status) |
| total_stats | [cockroach.storage.enginepb.MVCCStats](#cockroach.server.serverpb.SpanStatsResponse-cockroach.storage.enginepb.MVCCStats) |  |  | [reserved](#support-status) |







## Stacks

`GET /_status/stacks/{node_id}`

Stacks retrieves the stack traces of all goroutines on a given node.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.StacksRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. node_id translates to a KV node ID on a storage server and SQL instance ID on a SQL only server. | [reserved](#support-status) |
| type | [StacksType](#cockroach.server.serverpb.StacksRequest-cockroach.server.serverpb.StacksType) |  |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| data | [bytes](#cockroach.server.serverpb.JSONResponse-bytes) |  |  | [reserved](#support-status) |







## Profile

`GET /_status/profile/{node_id}`

Profile retrieves a CPU profile on a given node.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.ProfileRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. node_id translates to a KV node ID on a storage server and SQL instance ID on a SQL only server. | [reserved](#support-status) |
| type | [ProfileRequest.Type](#cockroach.server.serverpb.ProfileRequest-cockroach.server.serverpb.ProfileRequest.Type) |  | The type of profile to retrieve. | [reserved](#support-status) |
| seconds | [int32](#cockroach.server.serverpb.ProfileRequest-int32) |  | applies only to Type=CPU, defaults to 30 | [reserved](#support-status) |
| labels | [bool](#cockroach.server.serverpb.ProfileRequest-bool) |  | applies only to Type=CPU, defaults to false | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| data | [bytes](#cockroach.server.serverpb.JSONResponse-bytes) |  |  | [reserved](#support-status) |







## Metrics

`GET /_status/metrics/{node_id}`

Metrics retrieves the node metrics for a given node.

Note: this is a “reserved” API and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.MetricsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| data | [bytes](#cockroach.server.serverpb.JSONResponse-bytes) |  |  | [reserved](#support-status) |







## GetFiles

`GET /_status/files/{node_id}`

GetFiles retrieves heap or goroutine dump files from a given node.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.GetFilesRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. node_id translates to a KV node ID on a storage server and SQL instance ID on a SQL only server. | [reserved](#support-status) |
| list_only | [bool](#cockroach.server.serverpb.GetFilesRequest-bool) |  | If list_only is true then the contents of the files will not be populated in the response. Only filenames and sizes will be returned. | [reserved](#support-status) |
| type | [FileType](#cockroach.server.serverpb.GetFilesRequest-cockroach.server.serverpb.FileType) |  |  | [reserved](#support-status) |
| patterns | [string](#cockroach.server.serverpb.GetFilesRequest-string) | repeated | Each pattern given is matched with Files of the above type in the node using filepath.Glob(). The patterns only match to filenames and so path separators cannot be used. Example: * will match all files of requested type. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| files | [File](#cockroach.server.serverpb.GetFilesResponse-cockroach.server.serverpb.File) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.GetFilesResponse-cockroach.server.serverpb.File"></a>
#### File



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| name | [string](#cockroach.server.serverpb.GetFilesResponse-string) |  |  | [reserved](#support-status) |
| file_size | [int64](#cockroach.server.serverpb.GetFilesResponse-int64) |  |  | [reserved](#support-status) |
| contents | [bytes](#cockroach.server.serverpb.GetFilesResponse-bytes) |  | Contents may not be populated if only a list of Files are requested. | [reserved](#support-status) |






## LogFilesList

`GET /_status/logfiles/{node_id}`

LogFilesList retrieves a list of log files on a given node.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.LogFilesListRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| files | [cockroach.util.log.FileInfo](#cockroach.server.serverpb.LogFilesListResponse-cockroach.util.log.FileInfo) | repeated | files is the list of log files under this node's configured logging directories. Note that the response does not contain detail about which directory contains which file. The location of each file is known to the server based on its name and the logging configuration. | [reserved](#support-status) |







## LogFile

`GET /_status/logfiles/{node_id}/{file}`

LogFile retrieves a given log file.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.LogFileRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |
| file | [string](#cockroach.server.serverpb.LogFileRequest-string) |  | file is the name of the log file to retrieve. Note that it must not be prefixed by a directory name. The full path to the file is computed by the server based on the base name and the logging configuration. | [reserved](#support-status) |
| redact | [bool](#cockroach.server.serverpb.LogFileRequest-bool) |  | redact, if true, requests redaction of sensitive data away from the retrieved log entries. Only admin users can send a request with redact = false. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| entries | [cockroach.util.log.Entry](#cockroach.server.serverpb.LogEntriesResponse-cockroach.util.log.Entry) | repeated |  | [reserved](#support-status) |







## Logs

`GET /_status/logs/{node_id}`

Logs retrieves individual log entries.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.LogsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |
| level | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  | [reserved](#support-status) |
| start_time | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  | [reserved](#support-status) |
| end_time | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  | [reserved](#support-status) |
| max | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  | [reserved](#support-status) |
| pattern | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  | [reserved](#support-status) |
| redact | [bool](#cockroach.server.serverpb.LogsRequest-bool) |  | redact, if true, requests redaction of sensitive data away from the retrieved log entries. Only admin users can send a request with redact = false. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| entries | [cockroach.util.log.Entry](#cockroach.server.serverpb.LogEntriesResponse-cockroach.util.log.Entry) | repeated |  | [reserved](#support-status) |







## ProblemRanges

`GET /_status/problemranges`

ProblemRanges retrieves the list of “problem ranges”.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.ProblemRangesRequest-string) |  | If left empty, problem ranges for all nodes/stores will be returned. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ProblemRangesResponse-int32) |  | NodeID is the node that submitted all the requests. | [reserved](#support-status) |
| problems_by_node_id | [ProblemRangesResponse.ProblemsByNodeIdEntry](#cockroach.server.serverpb.ProblemRangesResponse-cockroach.server.serverpb.ProblemRangesResponse.ProblemsByNodeIdEntry) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.ProblemRangesResponse-cockroach.server.serverpb.ProblemRangesResponse.ProblemsByNodeIdEntry"></a>
#### ProblemRangesResponse.ProblemsByNodeIdEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.ProblemRangesResponse-int32) |  |  |  |
| value | [ProblemRangesResponse.NodeProblems](#cockroach.server.serverpb.ProblemRangesResponse-cockroach.server.serverpb.ProblemRangesResponse.NodeProblems) |  |  |  |





<a name="cockroach.server.serverpb.ProblemRangesResponse-cockroach.server.serverpb.ProblemRangesResponse.NodeProblems"></a>
#### ProblemRangesResponse.NodeProblems



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| error_message | [string](#cockroach.server.serverpb.ProblemRangesResponse-string) |  |  | [reserved](#support-status) |
| unavailable_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | [reserved](#support-status) |
| raft_leader_not_lease_holder_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | [reserved](#support-status) |
| no_raft_leader_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | [reserved](#support-status) |
| no_lease_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | [reserved](#support-status) |
| underreplicated_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | [reserved](#support-status) |
| overreplicated_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | [reserved](#support-status) |
| quiescent_equals_ticking_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | [reserved](#support-status) |
| raft_log_too_large_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | [reserved](#support-status) |
| circuit_breaker_error_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | [reserved](#support-status) |






## HotRanges

`GET /_status/hotranges`



Support status: [reserved](#support-status)

#### Request Parameters




HotRangesRequest queries one or more cluster nodes for a list
of ranges currently considered “hot” by the node(s).


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.HotRangesRequest-string) |  | NodeID indicates which node to query for a hot range report. It is possible to populate any node ID; if the node receiving the request is not the target node, it will forward the request to the target node.<br><br>If left empty, the request is forwarded to every node in the cluster. | [alpha](#support-status) |
| page_size | [int32](#cockroach.server.serverpb.HotRangesRequest-int32) |  |  | [reserved](#support-status) |
| page_token | [string](#cockroach.server.serverpb.HotRangesRequest-string) |  |  | [reserved](#support-status) |







#### Response Parameters




HotRangesResponse is the payload produced in response
to a HotRangesRequest.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  | NodeID is the node that received the HotRangesRequest and forwarded requests to the selected target node(s). | [alpha](#support-status) |
| hot_ranges_by_node_id | [HotRangesResponse.HotRangesByNodeIdEntry](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRangesByNodeIdEntry) | repeated | HotRangesByNodeID contains a hot range report for each selected target node ID in the HotRangesRequest. | [alpha](#support-status) |






<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRangesByNodeIdEntry"></a>
#### HotRangesResponse.HotRangesByNodeIdEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  |  |  |
| value | [HotRangesResponse.NodeResponse](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.NodeResponse) |  |  |  |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.NodeResponse"></a>
#### HotRangesResponse.NodeResponse

NodeResponse is a hot range report for a single target node.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| error_message | [string](#cockroach.server.serverpb.HotRangesResponse-string) |  | ErrorMessage is set to a non-empty string if this target node was unable to produce a hot range report.<br><br>The contents of this string indicates the cause of the failure. | [alpha](#support-status) |
| stores | [HotRangesResponse.StoreResponse](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.StoreResponse) | repeated | Stores contains the hot ranges report if no error was encountered. There is one part to the report for each store in the target node. | [alpha](#support-status) |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.StoreResponse"></a>
#### HotRangesResponse.StoreResponse

StoreResponse contains the part of a hot ranges report that
pertains to a single store on a target node.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| store_id | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  | StoreID identifies the store for which the report was produced. | [alpha](#support-status) |
| hot_ranges | [HotRangesResponse.HotRange](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRange) | repeated | HotRanges is the hot ranges report for this store on the target node. | [alpha](#support-status) |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRange"></a>
#### HotRangesResponse.HotRange

HotRange is a hot range report for a single store on one of the
target node(s) selected in a HotRangesRequest.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.RangeDescriptor](#cockroach.server.serverpb.HotRangesResponse-cockroach.roachpb.RangeDescriptor) |  | Desc is the descriptor of the range for which the report was produced.<br><br>TODO(knz): This field should be removed. See: https://github.com/cockroachdb/cockroach/issues/53212 | [reserved](#support-status) |
| queries_per_second | [double](#cockroach.server.serverpb.HotRangesResponse-double) |  | QueriesPerSecond is the recent number of queries per second on this range. | [alpha](#support-status) |
| leaseholder_node_id | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  | LeaseholderNodeID indicates the Node ID that is the current leaseholder for the given range. | [reserved](#support-status) |






## HotRangesV2

`POST /_status/v2/hotranges`



Support status: [reserved](#support-status)

#### Request Parameters




HotRangesRequest queries one or more cluster nodes for a list
of ranges currently considered “hot” by the node(s).


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.HotRangesRequest-string) |  | NodeID indicates which node to query for a hot range report. It is possible to populate any node ID; if the node receiving the request is not the target node, it will forward the request to the target node.<br><br>If left empty, the request is forwarded to every node in the cluster. | [alpha](#support-status) |
| page_size | [int32](#cockroach.server.serverpb.HotRangesRequest-int32) |  |  | [reserved](#support-status) |
| page_token | [string](#cockroach.server.serverpb.HotRangesRequest-string) |  |  | [reserved](#support-status) |







#### Response Parameters




HotRangesResponseV2 is a response payload returned by `HotRangesV2` service.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| ranges | [HotRangesResponseV2.HotRange](#cockroach.server.serverpb.HotRangesResponseV2-cockroach.server.serverpb.HotRangesResponseV2.HotRange) | repeated | Ranges contain list of hot ranges info that has highest number of QPS. | [reserved](#support-status) |
| errors_by_node_id | [HotRangesResponseV2.ErrorsByNodeIdEntry](#cockroach.server.serverpb.HotRangesResponseV2-cockroach.server.serverpb.HotRangesResponseV2.ErrorsByNodeIdEntry) | repeated | errors contains any errors that occurred during fan-out calls to other nodes. | [reserved](#support-status) |
| next_page_token | [string](#cockroach.server.serverpb.HotRangesResponseV2-string) |  | NextPageToken represents next pagination token to request next slice of data. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.HotRangesResponseV2-cockroach.server.serverpb.HotRangesResponseV2.HotRange"></a>
#### HotRangesResponseV2.HotRange

HotRange message describes a single hot range, ie its QPS, node ID it belongs to, etc.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int32](#cockroach.server.serverpb.HotRangesResponseV2-int32) |  | range_id indicates Range ID that's identified as hot range. | [reserved](#support-status) |
| node_id | [int32](#cockroach.server.serverpb.HotRangesResponseV2-int32) |  | node_id indicates the node that contains the current hot range. | [reserved](#support-status) |
| qps | [double](#cockroach.server.serverpb.HotRangesResponseV2-double) |  | qps (queries per second) shows the amount of queries that interact with current range. | [reserved](#support-status) |
| table_name | [string](#cockroach.server.serverpb.HotRangesResponseV2-string) |  | table_name indicates the SQL table that the range belongs to. | [reserved](#support-status) |
| database_name | [string](#cockroach.server.serverpb.HotRangesResponseV2-string) |  | database_name indicates on database that has current hot range. | [reserved](#support-status) |
| index_name | [string](#cockroach.server.serverpb.HotRangesResponseV2-string) |  | index_name indicates the index name for current range. | [reserved](#support-status) |
| replica_node_ids | [int32](#cockroach.server.serverpb.HotRangesResponseV2-int32) | repeated | replica_node_ids specifies the list of node ids that contain replicas with current hot range. | [reserved](#support-status) |
| leaseholder_node_id | [int32](#cockroach.server.serverpb.HotRangesResponseV2-int32) |  | leaseholder_node_id indicates the Node ID that is the current leaseholder for the given range. | [reserved](#support-status) |
| schema_name | [string](#cockroach.server.serverpb.HotRangesResponseV2-string) |  | schema_name provides the name of schema (if exists) for table in current range. | [reserved](#support-status) |
| store_id | [int32](#cockroach.server.serverpb.HotRangesResponseV2-int32) |  | store_id indicates the Store ID where range is stored. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.HotRangesResponseV2-cockroach.server.serverpb.HotRangesResponseV2.ErrorsByNodeIdEntry"></a>
#### HotRangesResponseV2.ErrorsByNodeIdEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.HotRangesResponseV2-int32) |  |  |  |
| value | [string](#cockroach.server.serverpb.HotRangesResponseV2-string) |  |  |  |






## Range

`GET /_status/range/{range_id}`



Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int64](#cockroach.server.serverpb.RangeRequest-int64) |  |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.RangeResponse-int32) |  | NodeID is the node that submitted all the requests. | [reserved](#support-status) |
| range_id | [int64](#cockroach.server.serverpb.RangeResponse-int64) |  |  | [reserved](#support-status) |
| responses_by_node_id | [RangeResponse.ResponsesByNodeIdEntry](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeResponse.ResponsesByNodeIdEntry) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeResponse.ResponsesByNodeIdEntry"></a>
#### RangeResponse.ResponsesByNodeIdEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.RangeResponse-int32) |  |  |  |
| value | [RangeResponse.NodeResponse](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeResponse.NodeResponse) |  |  |  |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeResponse.NodeResponse"></a>
#### RangeResponse.NodeResponse



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| response | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |
| error_message | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  | [reserved](#support-status) |
| infos | [RangeInfo](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeInfo) | repeated |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeInfo"></a>
#### RangeInfo



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| span | [PrettySpan](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.PrettySpan) |  |  | [reserved](#support-status) |
| raft_state | [RaftState](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState) |  |  | [reserved](#support-status) |
| state | [cockroach.kv.kvserver.storagepb.RangeInfo](#cockroach.server.serverpb.RangeResponse-cockroach.kv.kvserver.storagepb.RangeInfo) |  |  | [reserved](#support-status) |
| source_node_id | [int32](#cockroach.server.serverpb.RangeResponse-int32) |  |  | [reserved](#support-status) |
| source_store_id | [int32](#cockroach.server.serverpb.RangeResponse-int32) |  |  | [reserved](#support-status) |
| error_message | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  | [reserved](#support-status) |
| lease_history | [cockroach.roachpb.Lease](#cockroach.server.serverpb.RangeResponse-cockroach.roachpb.Lease) | repeated |  | [reserved](#support-status) |
| problems | [RangeProblems](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeProblems) |  |  | [reserved](#support-status) |
| stats | [RangeStatistics](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeStatistics) |  |  | [reserved](#support-status) |
| lease_status | [cockroach.kv.kvserver.storagepb.LeaseStatus](#cockroach.server.serverpb.RangeResponse-cockroach.kv.kvserver.storagepb.LeaseStatus) |  |  | [reserved](#support-status) |
| quiescent | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |
| ticking | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |
| read_latches | [int64](#cockroach.server.serverpb.RangeResponse-int64) |  |  | [reserved](#support-status) |
| write_latches | [int64](#cockroach.server.serverpb.RangeResponse-int64) |  |  | [reserved](#support-status) |
| locks | [int64](#cockroach.server.serverpb.RangeResponse-int64) |  |  | [reserved](#support-status) |
| locks_with_wait_queues | [int64](#cockroach.server.serverpb.RangeResponse-int64) |  |  | [reserved](#support-status) |
| lock_wait_queue_waiters | [int64](#cockroach.server.serverpb.RangeResponse-int64) |  |  | [reserved](#support-status) |
| top_k_locks_by_wait_queue_waiters | [RangeInfo.LockInfo](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeInfo.LockInfo) | repeated |  | [reserved](#support-status) |
| locality | [Locality](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.Locality) |  |  | [reserved](#support-status) |
| is_leaseholder | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |
| lease_valid | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.PrettySpan"></a>
#### PrettySpan



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| start_key | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  | [reserved](#support-status) |
| end_key | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState"></a>
#### RaftState

RaftState gives internal details about a Raft group's state.
Closely mirrors the upstream definitions in github.com/etcd-io/etcd/raft.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| replica_id | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  | [reserved](#support-status) |
| hard_state | [raftpb.HardState](#cockroach.server.serverpb.RangeResponse-raftpb.HardState) |  |  | [reserved](#support-status) |
| lead | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  | Lead is part of Raft's SoftState. | [reserved](#support-status) |
| state | [string](#cockroach.server.serverpb.RangeResponse-string) |  | State is part of Raft's SoftState. It's not an enum because this is primarily for ui consumption and there are issues associated with them. | [reserved](#support-status) |
| applied | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  | [reserved](#support-status) |
| progress | [RaftState.ProgressEntry](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState.ProgressEntry) | repeated |  | [reserved](#support-status) |
| lead_transferee | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState.ProgressEntry"></a>
#### RaftState.ProgressEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  |  |
| value | [RaftState.Progress](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState.Progress) |  |  |  |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState.Progress"></a>
#### RaftState.Progress



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| match | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  | [reserved](#support-status) |
| next | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  | [reserved](#support-status) |
| state | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  | [reserved](#support-status) |
| paused | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |
| pending_snapshot | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeProblems"></a>
#### RangeProblems

RangeProblems describes issues reported by a range. For internal use only.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| unavailable | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |
| leader_not_lease_holder | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |
| no_raft_leader | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |
| underreplicated | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |
| overreplicated | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |
| no_lease | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |
| quiescent_equals_ticking | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  | Quiescent ranges do not tick by definition, but we track this in two different ways and suspect that they're getting out of sync. If the replica's quiescent flag doesn't agree with the store's list of replicas that are ticking, warn about it. | [reserved](#support-status) |
| raft_log_too_large | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  | When the raft log is too large, it can be a symptom of other issues. | [reserved](#support-status) |
| circuit_breaker_error | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeStatistics"></a>
#### RangeStatistics

RangeStatistics describes statistics reported by a range. For internal use
only.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| queries_per_second | [double](#cockroach.server.serverpb.RangeResponse-double) |  | Queries per second served by this range.<br><br>Note that queries per second will only be known by the leaseholder. All other replicas will report it as 0. | [reserved](#support-status) |
| writes_per_second | [double](#cockroach.server.serverpb.RangeResponse-double) |  | Writes per second served by this range. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeInfo.LockInfo"></a>
#### RangeInfo.LockInfo



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| pretty_key | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  | [reserved](#support-status) |
| key | [bytes](#cockroach.server.serverpb.RangeResponse-bytes) |  |  | [reserved](#support-status) |
| held | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | [reserved](#support-status) |
| waiters | [int64](#cockroach.server.serverpb.RangeResponse-int64) |  |  | [reserved](#support-status) |
| waiting_readers | [int64](#cockroach.server.serverpb.RangeResponse-int64) |  |  | [reserved](#support-status) |
| waiting_writers | [int64](#cockroach.server.serverpb.RangeResponse-int64) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.Locality"></a>
#### Locality

Locality is an ordered set of key value Tiers that describe a node's
location. The tier keys should be the same across all nodes.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| tiers | [Tier](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.Tier) | repeated |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.Tier"></a>
#### Tier

Tier represents one level of the locality hierarchy.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.RangeResponse-string) |  | Key is the name of tier and should match all other nodes. | [reserved](#support-status) |
| value | [string](#cockroach.server.serverpb.RangeResponse-string) |  | Value is node specific value corresponding to the key. | [reserved](#support-status) |






## Diagnostics

`GET /_status/diagnostics/{node_id}`



Support status: [reserved](#support-status)

#### Request Parameters




DiagnosticsRequest requests a diagnostics report.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.DiagnosticsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |







#### Response Parameters



## Stores

`GET /_status/stores/{node_id}`



Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.StoresRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| stores | [StoreDetails](#cockroach.server.serverpb.StoresResponse-cockroach.server.serverpb.StoreDetails) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.StoresResponse-cockroach.server.serverpb.StoreDetails"></a>
#### StoreDetails



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| store_id | [int32](#cockroach.server.serverpb.StoresResponse-int32) |  |  | [reserved](#support-status) |
| encryption_status | [bytes](#cockroach.server.serverpb.StoresResponse-bytes) |  | encryption_status is a serialized ccl/storageccl/engineccl/enginepbccl/stats.go::EncryptionStatus protobuf. | [reserved](#support-status) |
| total_files | [uint64](#cockroach.server.serverpb.StoresResponse-uint64) |  | Basic file stats when encryption is enabled. Total files/bytes. | [reserved](#support-status) |
| total_bytes | [uint64](#cockroach.server.serverpb.StoresResponse-uint64) |  |  | [reserved](#support-status) |
| active_key_files | [uint64](#cockroach.server.serverpb.StoresResponse-uint64) |  | Files/bytes using the active data key. | [reserved](#support-status) |
| active_key_bytes | [uint64](#cockroach.server.serverpb.StoresResponse-uint64) |  |  | [reserved](#support-status) |






## Statements

`GET /_status/statements`



Support status: [reserved](#support-status)

#### Request Parameters




StatementsRequest is used by both tenant and node-level
implementations to serve fan-out requests across multiple nodes or
instances. When implemented on a node, the `node_id` field refers to
the cluster nodes by their nodeID. When implemented on a tenant, the
`node_id` field refers to the instanceIDs that identify individual
tenant pods.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.StatementsRequest-string) |  |  | [reserved](#support-status) |
| combined | [bool](#cockroach.server.serverpb.StatementsRequest-bool) |  | If this field is set we will use the combined statements API instead. | [reserved](#support-status) |
| start | [int64](#cockroach.server.serverpb.StatementsRequest-int64) |  | These fields are used for the combined statements API. | [reserved](#support-status) |
| end | [int64](#cockroach.server.serverpb.StatementsRequest-int64) |  |  | [reserved](#support-status) |
| fetch_mode | [StatementsRequest.FetchMode](#cockroach.server.serverpb.StatementsRequest-cockroach.server.serverpb.StatementsRequest.FetchMode) |  |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| statements | [StatementsResponse.CollectedStatementStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.CollectedStatementStatistics) | repeated |  | [reserved](#support-status) |
| last_reset | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementsResponse-google.protobuf.Timestamp) |  | Timestamp of the last stats reset. | [reserved](#support-status) |
| internal_app_name_prefix | [string](#cockroach.server.serverpb.StatementsResponse-string) |  | If set and non-empty, indicates the prefix to application_name used for statements/queries issued internally by CockroachDB. | [reserved](#support-status) |
| transactions | [StatementsResponse.ExtendedCollectedTransactionStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedCollectedTransactionStatistics) | repeated | Transactions is transaction-level statistics for the collection of statements in this response. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.CollectedStatementStatistics"></a>
#### StatementsResponse.CollectedStatementStatistics



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [StatementsResponse.ExtendedStatementStatisticsKey](#cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedStatementStatisticsKey) |  |  | [reserved](#support-status) |
| id | [uint64](#cockroach.server.serverpb.StatementsResponse-uint64) |  |  | [reserved](#support-status) |
| stats | [cockroach.sql.StatementStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.sql.StatementStatistics) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedStatementStatisticsKey"></a>
#### StatementsResponse.ExtendedStatementStatisticsKey



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key_data | [cockroach.sql.StatementStatisticsKey](#cockroach.server.serverpb.StatementsResponse-cockroach.sql.StatementStatisticsKey) |  |  | [reserved](#support-status) |
| node_id | [int32](#cockroach.server.serverpb.StatementsResponse-int32) |  |  | [reserved](#support-status) |
| aggregated_ts | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementsResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| aggregation_interval | [google.protobuf.Duration](#cockroach.server.serverpb.StatementsResponse-google.protobuf.Duration) |  | The aggregation duration. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedCollectedTransactionStatistics"></a>
#### StatementsResponse.ExtendedCollectedTransactionStatistics



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| stats_data | [cockroach.sql.CollectedTransactionStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.sql.CollectedTransactionStatistics) |  |  | [reserved](#support-status) |
| node_id | [int32](#cockroach.server.serverpb.StatementsResponse-int32) |  |  | [reserved](#support-status) |






## CombinedStatementStats

`GET /_status/combinedstmts`

Retrieve the combined in-memory and persisted statement stats by date range.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| start | [int64](#cockroach.server.serverpb.CombinedStatementsStatsRequest-int64) |  | Unix time range for aggregated statements. | [reserved](#support-status) |
| end | [int64](#cockroach.server.serverpb.CombinedStatementsStatsRequest-int64) |  |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| statements | [StatementsResponse.CollectedStatementStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.CollectedStatementStatistics) | repeated |  | [reserved](#support-status) |
| last_reset | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementsResponse-google.protobuf.Timestamp) |  | Timestamp of the last stats reset. | [reserved](#support-status) |
| internal_app_name_prefix | [string](#cockroach.server.serverpb.StatementsResponse-string) |  | If set and non-empty, indicates the prefix to application_name used for statements/queries issued internally by CockroachDB. | [reserved](#support-status) |
| transactions | [StatementsResponse.ExtendedCollectedTransactionStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedCollectedTransactionStatistics) | repeated | Transactions is transaction-level statistics for the collection of statements in this response. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.CollectedStatementStatistics"></a>
#### StatementsResponse.CollectedStatementStatistics



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [StatementsResponse.ExtendedStatementStatisticsKey](#cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedStatementStatisticsKey) |  |  | [reserved](#support-status) |
| id | [uint64](#cockroach.server.serverpb.StatementsResponse-uint64) |  |  | [reserved](#support-status) |
| stats | [cockroach.sql.StatementStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.sql.StatementStatistics) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedStatementStatisticsKey"></a>
#### StatementsResponse.ExtendedStatementStatisticsKey



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key_data | [cockroach.sql.StatementStatisticsKey](#cockroach.server.serverpb.StatementsResponse-cockroach.sql.StatementStatisticsKey) |  |  | [reserved](#support-status) |
| node_id | [int32](#cockroach.server.serverpb.StatementsResponse-int32) |  |  | [reserved](#support-status) |
| aggregated_ts | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementsResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| aggregation_interval | [google.protobuf.Duration](#cockroach.server.serverpb.StatementsResponse-google.protobuf.Duration) |  | The aggregation duration. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedCollectedTransactionStatistics"></a>
#### StatementsResponse.ExtendedCollectedTransactionStatistics



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| stats_data | [cockroach.sql.CollectedTransactionStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.sql.CollectedTransactionStatistics) |  |  | [reserved](#support-status) |
| node_id | [int32](#cockroach.server.serverpb.StatementsResponse-int32) |  |  | [reserved](#support-status) |






## StatementDetails

`GET /_status/stmtdetails/{fingerprint_id}`



Support status: [reserved](#support-status)

#### Request Parameters




StatementDetailsRequest requests the details of a Statement, based on its keys.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| fingerprint_id | [string](#cockroach.server.serverpb.StatementDetailsRequest-string) |  | fingerprint_id is generated by ConstructStatementFingerprintID using: query, failed, implicitTxn and database. So we don't need to add them to the request. | [reserved](#support-status) |
| app_names | [string](#cockroach.server.serverpb.StatementDetailsRequest-string) | repeated |  | [reserved](#support-status) |
| start | [int64](#cockroach.server.serverpb.StatementDetailsRequest-int64) |  | Unix time range for aggregated statements. | [reserved](#support-status) |
| end | [int64](#cockroach.server.serverpb.StatementDetailsRequest-int64) |  |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| statement | [StatementDetailsResponse.CollectedStatementSummary](#cockroach.server.serverpb.StatementDetailsResponse-cockroach.server.serverpb.StatementDetailsResponse.CollectedStatementSummary) |  | statement returns the total statistics for the statement. | [reserved](#support-status) |
| statements_per_aggregated_ts | [StatementDetailsResponse.CollectedStatementGroupedByAggregatedTs](#cockroach.server.serverpb.StatementDetailsResponse-cockroach.server.serverpb.StatementDetailsResponse.CollectedStatementGroupedByAggregatedTs) | repeated | statements_per_aggregated_ts returns the same statement from above, but with its statistics separated by the aggregated timestamp. | [reserved](#support-status) |
| statements_per_plan_hash | [StatementDetailsResponse.CollectedStatementGroupedByPlanHash](#cockroach.server.serverpb.StatementDetailsResponse-cockroach.server.serverpb.StatementDetailsResponse.CollectedStatementGroupedByPlanHash) | repeated | statements_per_plan_hash returns the same statement from above, but with its statistics separated by the plan hash. | [reserved](#support-status) |
| internal_app_name_prefix | [string](#cockroach.server.serverpb.StatementDetailsResponse-string) |  | If set and non-empty, indicates the prefix to application_name used for statements/queries issued internally by CockroachDB. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.StatementDetailsResponse-cockroach.server.serverpb.StatementDetailsResponse.CollectedStatementSummary"></a>
#### StatementDetailsResponse.CollectedStatementSummary



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key_data | [cockroach.sql.StatementStatisticsKey](#cockroach.server.serverpb.StatementDetailsResponse-cockroach.sql.StatementStatisticsKey) |  |  | [reserved](#support-status) |
| formatted_query | [string](#cockroach.server.serverpb.StatementDetailsResponse-string) |  | Formatted query is the return of the key_data.query after prettify_statement. The value from the key_data cannot be replaced by the formatted value, because is used as is for diagnostic bundle. | [reserved](#support-status) |
| app_names | [string](#cockroach.server.serverpb.StatementDetailsResponse-string) | repeated |  | [reserved](#support-status) |
| stats | [cockroach.sql.StatementStatistics](#cockroach.server.serverpb.StatementDetailsResponse-cockroach.sql.StatementStatistics) |  |  | [reserved](#support-status) |
| aggregation_interval | [google.protobuf.Duration](#cockroach.server.serverpb.StatementDetailsResponse-google.protobuf.Duration) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.StatementDetailsResponse-cockroach.server.serverpb.StatementDetailsResponse.CollectedStatementGroupedByAggregatedTs"></a>
#### StatementDetailsResponse.CollectedStatementGroupedByAggregatedTs



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| stats | [cockroach.sql.StatementStatistics](#cockroach.server.serverpb.StatementDetailsResponse-cockroach.sql.StatementStatistics) |  |  | [reserved](#support-status) |
| aggregation_interval | [google.protobuf.Duration](#cockroach.server.serverpb.StatementDetailsResponse-google.protobuf.Duration) |  |  | [reserved](#support-status) |
| aggregated_ts | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementDetailsResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.StatementDetailsResponse-cockroach.server.serverpb.StatementDetailsResponse.CollectedStatementGroupedByPlanHash"></a>
#### StatementDetailsResponse.CollectedStatementGroupedByPlanHash



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| stats | [cockroach.sql.StatementStatistics](#cockroach.server.serverpb.StatementDetailsResponse-cockroach.sql.StatementStatistics) |  |  | [reserved](#support-status) |
| aggregation_interval | [google.protobuf.Duration](#cockroach.server.serverpb.StatementDetailsResponse-google.protobuf.Duration) |  |  | [reserved](#support-status) |
| explain_plan | [string](#cockroach.server.serverpb.StatementDetailsResponse-string) |  |  | [reserved](#support-status) |
| plan_hash | [uint64](#cockroach.server.serverpb.StatementDetailsResponse-uint64) |  |  | [reserved](#support-status) |






## CreateStatementDiagnosticsReport

`POST /_status/stmtdiagreports`



Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| statement_fingerprint | [string](#cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest-string) |  |  | [reserved](#support-status) |
| min_execution_latency | [google.protobuf.Duration](#cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest-google.protobuf.Duration) |  | MinExecutionLatency, when non-zero, indicates the minimum execution latency of a query for which to collect the diagnostics report. In other words, if a query executes faster than this threshold, then the diagnostics report is not collected on it, and we will try to get a bundle the next time we see the query fingerprint.<br><br>NB: if MinExecutionLatency is non-zero, then all queries that match the fingerprint will be traced until a slow enough query comes along. This tracing might have some performance overhead. | [reserved](#support-status) |
| expires_after | [google.protobuf.Duration](#cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest-google.protobuf.Duration) |  | ExpiresAfter, when non-zero, sets the expiration interval of this request. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| report | [StatementDiagnosticsReport](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-cockroach.server.serverpb.StatementDiagnosticsReport) |  |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-cockroach.server.serverpb.StatementDiagnosticsReport"></a>
#### StatementDiagnosticsReport



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [int64](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-int64) |  |  | [reserved](#support-status) |
| completed | [bool](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-bool) |  |  | [reserved](#support-status) |
| statement_fingerprint | [string](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-string) |  |  | [reserved](#support-status) |
| statement_diagnostics_id | [int64](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-int64) |  |  | [reserved](#support-status) |
| requested_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| min_execution_latency | [google.protobuf.Duration](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-google.protobuf.Duration) |  |  | [reserved](#support-status) |
| expires_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |






## CancelStatementDiagnosticsReport

`POST /_status/stmtdiagreports/cancel`



Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| request_id | [int64](#cockroach.server.serverpb.CancelStatementDiagnosticsReportRequest-int64) |  |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| canceled | [bool](#cockroach.server.serverpb.CancelStatementDiagnosticsReportResponse-bool) |  | canceled indicates whether the request for the given fingerprint was actually canceled. | [reserved](#support-status) |
| error | [string](#cockroach.server.serverpb.CancelStatementDiagnosticsReportResponse-string) |  | error is set only if canceled is false. | [reserved](#support-status) |







## StatementDiagnosticsRequests

`GET /_status/stmtdiagreports`



Support status: [reserved](#support-status)

#### Request Parameters













#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| reports | [StatementDiagnosticsReport](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-cockroach.server.serverpb.StatementDiagnosticsReport) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.StatementDiagnosticsReportsResponse-cockroach.server.serverpb.StatementDiagnosticsReport"></a>
#### StatementDiagnosticsReport



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [int64](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-int64) |  |  | [reserved](#support-status) |
| completed | [bool](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-bool) |  |  | [reserved](#support-status) |
| statement_fingerprint | [string](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-string) |  |  | [reserved](#support-status) |
| statement_diagnostics_id | [int64](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-int64) |  |  | [reserved](#support-status) |
| requested_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| min_execution_latency | [google.protobuf.Duration](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-google.protobuf.Duration) |  |  | [reserved](#support-status) |
| expires_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |






## StatementDiagnostics

`GET /_status/stmtdiag/{statement_diagnostics_id}`



Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| statement_diagnostics_id | [int64](#cockroach.server.serverpb.StatementDiagnosticsRequest-int64) |  |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| diagnostics | [StatementDiagnostics](#cockroach.server.serverpb.StatementDiagnosticsResponse-cockroach.server.serverpb.StatementDiagnostics) |  |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.StatementDiagnosticsResponse-cockroach.server.serverpb.StatementDiagnostics"></a>
#### StatementDiagnostics



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [int64](#cockroach.server.serverpb.StatementDiagnosticsResponse-int64) |  |  | [reserved](#support-status) |
| statement_fingerprint | [string](#cockroach.server.serverpb.StatementDiagnosticsResponse-string) |  |  | [reserved](#support-status) |
| collected_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementDiagnosticsResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |






## JobRegistryStatus

`GET /_status/job_registry/{node_id}`



Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.JobRegistryStatusRequest-string) |  |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.JobRegistryStatusResponse-int32) |  |  | [reserved](#support-status) |
| running_jobs | [JobRegistryStatusResponse.Job](#cockroach.server.serverpb.JobRegistryStatusResponse-cockroach.server.serverpb.JobRegistryStatusResponse.Job) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.JobRegistryStatusResponse-cockroach.server.serverpb.JobRegistryStatusResponse.Job"></a>
#### JobRegistryStatusResponse.Job



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [int64](#cockroach.server.serverpb.JobRegistryStatusResponse-int64) |  |  | [reserved](#support-status) |






## JobStatus

`GET /_status/job/{job_id}`



Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| job_id | [int64](#cockroach.server.serverpb.JobStatusRequest-int64) |  |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| job | [cockroach.sql.jobs.jobspb.Job](#cockroach.server.serverpb.JobStatusResponse-cockroach.sql.jobs.jobspb.Job) |  |  | [reserved](#support-status) |







## ResetSQLStats

`POST /_status/resetsqlstats`



Support status: [reserved](#support-status)

#### Request Parameters




Request object for issuing a SQL stats reset request.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.ResetSQLStatsRequest-string) |  |  | [reserved](#support-status) |
| reset_persisted_stats | [bool](#cockroach.server.serverpb.ResetSQLStatsRequest-bool) |  | reset_persisted_stats specifies if the persisted SQL Stats will be reset along with the in-memory SQL stats. | [reserved](#support-status) |







#### Response Parameters




Response object returned by ResetSQLStats.








## IndexUsageStatistics

`GET /_status/indexusagestatistics`



Support status: [reserved](#support-status)

#### Request Parameters




Request object for issuing IndexUsageStatistics request.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.IndexUsageStatisticsRequest-string) |  | node_id is the ID of the node where the stats data shall be retrieved from. If this is left empty, the cluster-wide aggregated result will be returned. | [reserved](#support-status) |







#### Response Parameters




Response object returned by IndexUsageStatistics.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| statistics | [cockroach.sql.CollectedIndexUsageStatistics](#cockroach.server.serverpb.IndexUsageStatisticsResponse-cockroach.sql.CollectedIndexUsageStatistics) | repeated |  | [reserved](#support-status) |
| last_reset | [google.protobuf.Timestamp](#cockroach.server.serverpb.IndexUsageStatisticsResponse-google.protobuf.Timestamp) |  | Timestamp of the last index usage stats reset. | [reserved](#support-status) |







## ResetIndexUsageStats

`POST /_status/resetindexusagestats`



Support status: [reserved](#support-status)

#### Request Parameters




Request object for issuing a index usage stats reset request.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.ResetIndexUsageStatsRequest-string) |  |  | [reserved](#support-status) |







#### Response Parameters




Response object returned by ResetIndexUsageStatsRequest.








## TableIndexStats

`GET /_status/databases/{database}/tables/{table}/indexstats`

TableIndexStats retrieves index stats for a table.

Support status: [reserved](#support-status)

#### Request Parameters




Request object for issuing TableIndexStatsRequest request.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| database | [string](#cockroach.server.serverpb.TableIndexStatsRequest-string) |  | database is the name of the database that contains the table we're interested in. | [reserved](#support-status) |
| table | [string](#cockroach.server.serverpb.TableIndexStatsRequest-string) |  | table is the name of the table that we're querying. Table may be schema-qualified (schema.table) and each name component that contains sql unsafe characters such as . or uppercase letters must be surrounded in double quotes like "naughty schema".table. | [reserved](#support-status) |







#### Response Parameters




Response object returned by TableIndexStatsResponse.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| statistics | [TableIndexStatsResponse.ExtendedCollectedIndexUsageStatistics](#cockroach.server.serverpb.TableIndexStatsResponse-cockroach.server.serverpb.TableIndexStatsResponse.ExtendedCollectedIndexUsageStatistics) | repeated |  | [reserved](#support-status) |
| last_reset | [google.protobuf.Timestamp](#cockroach.server.serverpb.TableIndexStatsResponse-google.protobuf.Timestamp) |  | Timestamp of the last index usage stats reset. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.TableIndexStatsResponse-cockroach.server.serverpb.TableIndexStatsResponse.ExtendedCollectedIndexUsageStatistics"></a>
#### TableIndexStatsResponse.ExtendedCollectedIndexUsageStatistics



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| statistics | [cockroach.sql.CollectedIndexUsageStatistics](#cockroach.server.serverpb.TableIndexStatsResponse-cockroach.sql.CollectedIndexUsageStatistics) |  |  | [reserved](#support-status) |
| index_name | [string](#cockroach.server.serverpb.TableIndexStatsResponse-string) |  | index_name is the name of the index. | [reserved](#support-status) |
| index_type | [string](#cockroach.server.serverpb.TableIndexStatsResponse-string) |  | index_type is the type of the index i.e. primary, secondary. | [reserved](#support-status) |
| create_statement | [string](#cockroach.server.serverpb.TableIndexStatsResponse-string) |  | create_statement is the SQL statement that would re-create the current index if executed. | [reserved](#support-status) |
| created_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.TableIndexStatsResponse-google.protobuf.Timestamp) |  | CreatedAt is an approximate timestamp at which the index was created. Note that it may not always be populated. | [reserved](#support-status) |






## UserSQLRoles

`GET /_status/sqlroles`



Support status: [reserved](#support-status)

#### Request Parameters




UserSQLRolesRequest requests a list of roles of the logged in SQL user.








#### Response Parameters




UserSQLRolesResponse returns a list of roles for the logged SQL user.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| roles | [string](#cockroach.server.serverpb.UserSQLRolesResponse-string) | repeated | roles is a list of roles for the SQL user. | [reserved](#support-status) |







## TxnIDResolution



TxnIDResolution is used by the contention event store to resolve
transaction ID into transaction fingerprint IDs.
This RPC does not have a corresponding HTTP endpoint on purpose, since
DB Console should never directly query this endpoint.

The API contract is the following:
- if the server can resolve the transaction IDs in the RPC request, it will
  be returned in the RPC response.
- if the server is not able to resolve the transaction IDs, it will
  instructs the transaction ID cache to drain its write buffer. (Since
  transaction ID cache's write path is asynchronous, the transaction ID
  requested by the client might not be available in the cache yet).
  Client is responsible to perform retries if the requested transaction ID
  is not returned in the RPC response.

Support status: [reserved](#support-status)

#### Request Parameters




Request object for issuing Transaction ID Resolution.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| coordinator_id | [string](#cockroach.server.serverpb.TxnIDResolutionRequest-string) |  | coordinator_id is either the NodeID or SQLInstanceID depending on whether the transaction is executed on a system tenant or a regular tenant. | [reserved](#support-status) |
| txnIDs | [bytes](#cockroach.server.serverpb.TxnIDResolutionRequest-bytes) | repeated |  | [reserved](#support-status) |







#### Response Parameters




Response object for issuing Transaction ID Resolution.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| resolvedTxnIDs | [cockroach.sql.contentionpb.ResolvedTxnID](#cockroach.server.serverpb.TxnIDResolutionResponse-cockroach.sql.contentionpb.ResolvedTxnID) | repeated |  | [reserved](#support-status) |







## TransactionContentionEvents

`GET /_status/transactioncontentionevents`

TransactionContentionEvents returns a list of un-aggregated contention
events sorted by the collection timestamp.

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.TransactionContentionEventsRequest-string) |  |  | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| events | [cockroach.sql.contentionpb.ExtendedContentionEvent](#cockroach.server.serverpb.TransactionContentionEventsResponse-cockroach.sql.contentionpb.ExtendedContentionEvent) | repeated |  | [reserved](#support-status) |







## RequestCA

`GET /_join/v1/ca`



Support status: [reserved](#support-status)

#### Request Parameters




CARequest requests the CA cert anchoring this service.








#### Response Parameters




CAResponse contains a PEM encoded copy of the CA cert for this service.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| ca_cert | [bytes](#cockroach.server.serverpb.CAResponse-bytes) |  |  | [reserved](#support-status) |







## RequestCertBundle

`GET /_join/v1/requestbundle`



Support status: [reserved](#support-status)

#### Request Parameters




CertBundleRequest requests the bundle of initialization CAs for a new node.
It provides authentication in the form of a joinToken containing a
sharedSecret.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| token_id | [string](#cockroach.server.serverpb.CertBundleRequest-string) |  |  | [reserved](#support-status) |
| shared_secret | [bytes](#cockroach.server.serverpb.CertBundleRequest-bytes) |  |  | [reserved](#support-status) |







#### Response Parameters




CertBundleResponse contains a copy of all CAs needed to initialize TLS for
a new node.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| bundle | [bytes](#cockroach.server.serverpb.CertBundleResponse-bytes) |  |  | [reserved](#support-status) |







## Users

`GET /_admin/v1/users`

URL: /_admin/v1/users

Support status: [reserved](#support-status)

#### Request Parameters




UsersRequest requests a list of users.








#### Response Parameters




UsersResponse returns a list of users.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| users | [UsersResponse.User](#cockroach.server.serverpb.UsersResponse-cockroach.server.serverpb.UsersResponse.User) | repeated | usernames is a list of users for the CockroachDB cluster. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.UsersResponse-cockroach.server.serverpb.UsersResponse.User"></a>
#### UsersResponse.User

User is a CockroachDB user.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| username | [string](#cockroach.server.serverpb.UsersResponse-string) |  |  | [reserved](#support-status) |






## Databases

`GET /_admin/v1/databases`

URL: /_admin/v1/databases

Support status: [reserved](#support-status)

#### Request Parameters




DatabasesRequest requests a list of databases.








#### Response Parameters




DatabasesResponse contains a list of databases.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| databases | [string](#cockroach.server.serverpb.DatabasesResponse-string) | repeated |  | [reserved](#support-status) |







## DatabaseDetails

`GET /_admin/v1/databases/{database}`

Example URL: /_admin/v1/databases/system

Support status: [reserved](#support-status)

#### Request Parameters




DatabaseDetailsRequest requests detailed information about the specified
database


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| database | [string](#cockroach.server.serverpb.DatabaseDetailsRequest-string) |  | database is the name of the database we are querying. | [reserved](#support-status) |
| include_stats | [bool](#cockroach.server.serverpb.DatabaseDetailsRequest-bool) |  | Setting this flag includes a computationally-expensive stats field in the response. | [reserved](#support-status) |







#### Response Parameters




DatabaseDetailsResponse contains grant information, table names,
zone configuration, and size statistics for a database.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| grants | [DatabaseDetailsResponse.Grant](#cockroach.server.serverpb.DatabaseDetailsResponse-cockroach.server.serverpb.DatabaseDetailsResponse.Grant) | repeated | grants are the results of SHOW GRANTS for this database. | [reserved](#support-status) |
| table_names | [string](#cockroach.server.serverpb.DatabaseDetailsResponse-string) | repeated | table_names contains the names of all tables in this database. Note that all responses will be schema-qualified (schema.table) and that every schema or table that contains a "sql unsafe character" such as uppercase letters or dots will be surrounded with double quotes, such as "naughty schema".table. | [reserved](#support-status) |
| descriptor_id | [int64](#cockroach.server.serverpb.DatabaseDetailsResponse-int64) |  | descriptor_id is an identifier used to uniquely identify this database. It can be used to find events pertaining to this database by filtering on the 'target_id' field of events. | [reserved](#support-status) |
| zone_config | [cockroach.config.zonepb.ZoneConfig](#cockroach.server.serverpb.DatabaseDetailsResponse-cockroach.config.zonepb.ZoneConfig) |  | The zone configuration in effect for this database. | [reserved](#support-status) |
| zone_config_level | [ZoneConfigurationLevel](#cockroach.server.serverpb.DatabaseDetailsResponse-cockroach.server.serverpb.ZoneConfigurationLevel) |  | The level at which this object's zone configuration is set. | [reserved](#support-status) |
| stats | [DatabaseDetailsResponse.Stats](#cockroach.server.serverpb.DatabaseDetailsResponse-cockroach.server.serverpb.DatabaseDetailsResponse.Stats) |  | Size information about the database, present only when explicitly requested. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.DatabaseDetailsResponse-cockroach.server.serverpb.DatabaseDetailsResponse.Grant"></a>
#### DatabaseDetailsResponse.Grant



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| user | [string](#cockroach.server.serverpb.DatabaseDetailsResponse-string) |  | user is the user that this grant applies to. | [reserved](#support-status) |
| privileges | [string](#cockroach.server.serverpb.DatabaseDetailsResponse-string) | repeated | privileges are the abilities this grant gives to the user. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.DatabaseDetailsResponse-cockroach.server.serverpb.DatabaseDetailsResponse.Stats"></a>
#### DatabaseDetailsResponse.Stats



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| missing_tables | [DatabaseDetailsResponse.Stats.MissingTable](#cockroach.server.serverpb.DatabaseDetailsResponse-cockroach.server.serverpb.DatabaseDetailsResponse.Stats.MissingTable) | repeated | A list of tables that exist in the database, but for which stats could not be loaded due to failures during this request. | [reserved](#support-status) |
| range_count | [int64](#cockroach.server.serverpb.DatabaseDetailsResponse-int64) |  | The number of ranges, as determined from a query of range meta keys, across all tables. | [reserved](#support-status) |
| approximate_disk_bytes | [uint64](#cockroach.server.serverpb.DatabaseDetailsResponse-uint64) |  | An approximation of the disk space (in bytes) used for all replicas of all tables across the cluster. | [reserved](#support-status) |
| node_ids | [int32](#cockroach.server.serverpb.DatabaseDetailsResponse-int32) | repeated | node_ids is the ordered list of node ids on which data is stored. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.DatabaseDetailsResponse-cockroach.server.serverpb.DatabaseDetailsResponse.Stats.MissingTable"></a>
#### DatabaseDetailsResponse.Stats.MissingTable

A table which exists in the database, but for which we could not load stats
during this request.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| name | [string](#cockroach.server.serverpb.DatabaseDetailsResponse-string) |  | The name of the table for which we could not load stats. | [reserved](#support-status) |
| error_message | [string](#cockroach.server.serverpb.DatabaseDetailsResponse-string) |  | The error message that resulted when the request for this table failed. | [reserved](#support-status) |






## TableDetails

`GET /_admin/v1/databases/{database}/tables/{table}`

Example URL: /_admin/v1/databases/system/tables/ui

Support status: [reserved](#support-status)

#### Request Parameters




TableDetailsRequest is a request for detailed information about a table.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| database | [string](#cockroach.server.serverpb.TableDetailsRequest-string) |  | database is the name of the database that contains the table we're interested in. | [reserved](#support-status) |
| table | [string](#cockroach.server.serverpb.TableDetailsRequest-string) |  | table is the name of the table that we're querying. Table may be schema-qualified (schema.table) and each name component that contains sql unsafe characters such as . or uppercase letters must be surrounded in double quotes like "naughty schema".table. | [reserved](#support-status) |







#### Response Parameters




TableDetailsResponse contains grants, column names, and indexes for
a table.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| grants | [TableDetailsResponse.Grant](#cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.TableDetailsResponse.Grant) | repeated |  | [reserved](#support-status) |
| columns | [TableDetailsResponse.Column](#cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.TableDetailsResponse.Column) | repeated |  | [reserved](#support-status) |
| indexes | [TableDetailsResponse.Index](#cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.TableDetailsResponse.Index) | repeated |  | [reserved](#support-status) |
| range_count | [int64](#cockroach.server.serverpb.TableDetailsResponse-int64) |  | range_count is the size of the table in ranges. This provides a rough estimate of the storage requirements for the table. TODO(mrtracy): The TableStats method also returns a range_count field which is more accurate than this one; TableDetails calculates this number using a potentially faster method that is subject to cache staleness. We should consider removing or renaming this field to reflect that difference. See GitHub issue #5435 for more information. | [reserved](#support-status) |
| create_table_statement | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | create_table_statement is the output of "SHOW CREATE" for this table; it is a SQL statement that would re-create the table's current schema if executed. | [reserved](#support-status) |
| zone_config | [cockroach.config.zonepb.ZoneConfig](#cockroach.server.serverpb.TableDetailsResponse-cockroach.config.zonepb.ZoneConfig) |  | The zone configuration in effect for this table. | [reserved](#support-status) |
| zone_config_level | [ZoneConfigurationLevel](#cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.ZoneConfigurationLevel) |  | The level at which this object's zone configuration is set. | [reserved](#support-status) |
| descriptor_id | [int64](#cockroach.server.serverpb.TableDetailsResponse-int64) |  | descriptor_id is an identifier used to uniquely identify this table. It can be used to find events pertaining to this table by filtering on the 'target_id' field of events. | [reserved](#support-status) |
| configure_zone_statement | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | configure_zone_statement is the output of "SHOW ZONE CONFIGURATION FOR TABLE" for this table. It is a SQL statement that would re-configure the table's current zone if executed. | [reserved](#support-status) |
| stats_last_created_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.TableDetailsResponse-google.protobuf.Timestamp) |  | stats_last_created_at is the time at which statistics were last created. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.TableDetailsResponse.Grant"></a>
#### TableDetailsResponse.Grant

Grant is an entry from SHOW GRANTS.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| user | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | user is the user that this grant applies to. | [reserved](#support-status) |
| privileges | [string](#cockroach.server.serverpb.TableDetailsResponse-string) | repeated | privileges are the abilities this grant gives to the user. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.TableDetailsResponse.Column"></a>
#### TableDetailsResponse.Column



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| name | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | name is the name of the column. | [reserved](#support-status) |
| type | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | type is the SQL type (INT, STRING, etc.) of this column. | [reserved](#support-status) |
| nullable | [bool](#cockroach.server.serverpb.TableDetailsResponse-bool) |  | nullable is whether this column can contain NULL. | [reserved](#support-status) |
| default_value | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | default_value is the default value of this column. | [reserved](#support-status) |
| generation_expression | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | generation_expression is the generator expression if the column is computed. | [reserved](#support-status) |
| hidden | [bool](#cockroach.server.serverpb.TableDetailsResponse-bool) |  | hidden is whether this column is hidden. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.TableDetailsResponse.Index"></a>
#### TableDetailsResponse.Index



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| name | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | name is the name of this index. | [reserved](#support-status) |
| unique | [bool](#cockroach.server.serverpb.TableDetailsResponse-bool) |  | unique is whether this a unique index (i.e. CREATE UNIQUE INDEX). | [reserved](#support-status) |
| seq | [int64](#cockroach.server.serverpb.TableDetailsResponse-int64) |  | seq is an internal variable that's passed along. | [reserved](#support-status) |
| column | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | column is the column that this index indexes. | [reserved](#support-status) |
| direction | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | direction is either "ASC" (ascending) or "DESC" (descending). | [reserved](#support-status) |
| storing | [bool](#cockroach.server.serverpb.TableDetailsResponse-bool) |  | storing is an internal variable that's passed along. | [reserved](#support-status) |
| implicit | [bool](#cockroach.server.serverpb.TableDetailsResponse-bool) |  | implicit is an internal variable that's passed along. | [reserved](#support-status) |






## TableStats

`GET /_admin/v1/databases/{database}/tables/{table}/stats`

Example URL: /_admin/v1/databases/system/tables/ui/stats

Support status: [reserved](#support-status)

#### Request Parameters




TableStatsRequest is a request for detailed, computationally expensive
information about a table.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| database | [string](#cockroach.server.serverpb.TableStatsRequest-string) |  | database is the name of the database that contains the table we're interested in. | [reserved](#support-status) |
| table | [string](#cockroach.server.serverpb.TableStatsRequest-string) |  | table is the name of the table that we're querying. Table may be schema-qualified (schema.table) and each name component that contains sql unsafe characters such as . or uppercase letters must be surrounded in double quotes like "naughty schema".table. | [reserved](#support-status) |







#### Response Parameters




TableStatsResponse contains detailed, computationally expensive information
about a table.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_count | [int64](#cockroach.server.serverpb.TableStatsResponse-int64) |  | range_count is the number of ranges, as determined from a query of range meta keys. | [reserved](#support-status) |
| replica_count | [int64](#cockroach.server.serverpb.TableStatsResponse-int64) |  | replica_count is the number of replicas of any range of this table, as found by querying nodes which are known to have replicas. When compared with range_count, this can be used to estimate the current replication factor of the table. | [reserved](#support-status) |
| node_count | [int64](#cockroach.server.serverpb.TableStatsResponse-int64) |  | node_count is the number of nodes which contain data for this table, according to a query of range meta keys. | [reserved](#support-status) |
| stats | [cockroach.storage.enginepb.MVCCStats](#cockroach.server.serverpb.TableStatsResponse-cockroach.storage.enginepb.MVCCStats) |  | stats is the summation of MVCCStats for all replicas of this table across the cluster. | [reserved](#support-status) |
| approximate_disk_bytes | [uint64](#cockroach.server.serverpb.TableStatsResponse-uint64) |  | approximate_disk_bytes is an approximation of the disk space (in bytes) used for all replicas of this table across the cluster. | [reserved](#support-status) |
| missing_nodes | [TableStatsResponse.MissingNode](#cockroach.server.serverpb.TableStatsResponse-cockroach.server.serverpb.TableStatsResponse.MissingNode) | repeated | A list of nodes which should contain data for this table (according to cluster metadata), but could not be contacted during this request. | [reserved](#support-status) |
| node_ids | [int32](#cockroach.server.serverpb.TableStatsResponse-int32) | repeated | node_ids is the ordered list of node ids on which the table data is stored. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.TableStatsResponse-cockroach.server.serverpb.TableStatsResponse.MissingNode"></a>
#### TableStatsResponse.MissingNode

MissingNode represents information on a node which should contain data
for this table, but could not be contacted during this request.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.TableStatsResponse-string) |  | The ID of the missing node. | [reserved](#support-status) |
| error_message | [string](#cockroach.server.serverpb.TableStatsResponse-string) |  | The error message that resulted when the query sent to this node failed. | [reserved](#support-status) |






## NonTableStats

`GET /_admin/v1/nontablestats`

Example URL: /_admin/v1/nontablestats

Support status: [reserved](#support-status)

#### Request Parameters




NonTableStatsRequest requests statistics on cluster data ranges that do not
belong to SQL tables.








#### Response Parameters




NonTableStatsResponse returns statistics on various cluster data ranges
that do not belong to SQL tables. The statistics for each range are returned
as a TableStatsResponse.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| time_series_stats | [TableStatsResponse](#cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse) |  | Information on time series ranges. | [reserved](#support-status) |
| internal_use_stats | [TableStatsResponse](#cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse) |  | Information for remaining (non-table, non-time-series) ranges. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse"></a>
#### TableStatsResponse

TableStatsResponse contains detailed, computationally expensive information
about a table.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_count | [int64](#cockroach.server.serverpb.NonTableStatsResponse-int64) |  | range_count is the number of ranges, as determined from a query of range meta keys. | [reserved](#support-status) |
| replica_count | [int64](#cockroach.server.serverpb.NonTableStatsResponse-int64) |  | replica_count is the number of replicas of any range of this table, as found by querying nodes which are known to have replicas. When compared with range_count, this can be used to estimate the current replication factor of the table. | [reserved](#support-status) |
| node_count | [int64](#cockroach.server.serverpb.NonTableStatsResponse-int64) |  | node_count is the number of nodes which contain data for this table, according to a query of range meta keys. | [reserved](#support-status) |
| stats | [cockroach.storage.enginepb.MVCCStats](#cockroach.server.serverpb.NonTableStatsResponse-cockroach.storage.enginepb.MVCCStats) |  | stats is the summation of MVCCStats for all replicas of this table across the cluster. | [reserved](#support-status) |
| approximate_disk_bytes | [uint64](#cockroach.server.serverpb.NonTableStatsResponse-uint64) |  | approximate_disk_bytes is an approximation of the disk space (in bytes) used for all replicas of this table across the cluster. | [reserved](#support-status) |
| missing_nodes | [TableStatsResponse.MissingNode](#cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse.MissingNode) | repeated | A list of nodes which should contain data for this table (according to cluster metadata), but could not be contacted during this request. | [reserved](#support-status) |
| node_ids | [int32](#cockroach.server.serverpb.NonTableStatsResponse-int32) | repeated | node_ids is the ordered list of node ids on which the table data is stored. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse.MissingNode"></a>
#### TableStatsResponse.MissingNode

MissingNode represents information on a node which should contain data
for this table, but could not be contacted during this request.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.NonTableStatsResponse-string) |  | The ID of the missing node. | [reserved](#support-status) |
| error_message | [string](#cockroach.server.serverpb.NonTableStatsResponse-string) |  | The error message that resulted when the query sent to this node failed. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse"></a>
#### TableStatsResponse

TableStatsResponse contains detailed, computationally expensive information
about a table.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_count | [int64](#cockroach.server.serverpb.NonTableStatsResponse-int64) |  | range_count is the number of ranges, as determined from a query of range meta keys. | [reserved](#support-status) |
| replica_count | [int64](#cockroach.server.serverpb.NonTableStatsResponse-int64) |  | replica_count is the number of replicas of any range of this table, as found by querying nodes which are known to have replicas. When compared with range_count, this can be used to estimate the current replication factor of the table. | [reserved](#support-status) |
| node_count | [int64](#cockroach.server.serverpb.NonTableStatsResponse-int64) |  | node_count is the number of nodes which contain data for this table, according to a query of range meta keys. | [reserved](#support-status) |
| stats | [cockroach.storage.enginepb.MVCCStats](#cockroach.server.serverpb.NonTableStatsResponse-cockroach.storage.enginepb.MVCCStats) |  | stats is the summation of MVCCStats for all replicas of this table across the cluster. | [reserved](#support-status) |
| approximate_disk_bytes | [uint64](#cockroach.server.serverpb.NonTableStatsResponse-uint64) |  | approximate_disk_bytes is an approximation of the disk space (in bytes) used for all replicas of this table across the cluster. | [reserved](#support-status) |
| missing_nodes | [TableStatsResponse.MissingNode](#cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse.MissingNode) | repeated | A list of nodes which should contain data for this table (according to cluster metadata), but could not be contacted during this request. | [reserved](#support-status) |
| node_ids | [int32](#cockroach.server.serverpb.NonTableStatsResponse-int32) | repeated | node_ids is the ordered list of node ids on which the table data is stored. | [reserved](#support-status) |






## Events

`GET /_admin/v1/events`

Example URLs:
Example URLs:
- /_admin/v1/events
- /_admin/v1/events?limit=100
- /_admin/v1/events?type=create_table
- /_admin/v1/events?type=create_table&limit=100
- /_admin/v1/events?type=drop_table&target_id=4
- /_admin/v1/events?type=drop_table&target_id=4&limit=100

Support status: [reserved](#support-status)

#### Request Parameters




EventsRequest is a request for event log entries, optionally filtered
by the specified event type and/or target_id.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| type | [string](#cockroach.server.serverpb.EventsRequest-string) |  |  | [reserved](#support-status) |
| target_id | [int64](#cockroach.server.serverpb.EventsRequest-int64) |  |  | [reserved](#support-status) |
| limit | [int32](#cockroach.server.serverpb.EventsRequest-int32) |  | limit is the total number of results that are retrieved by the query. If this is omitted or set to 0, the default maximum number of results are returned. When set to > 0, at most only that number of results are returned. When set to < 0, an unlimited number of results are returned. | [reserved](#support-status) |
| unredacted_events | [bool](#cockroach.server.serverpb.EventsRequest-bool) |  | unredacted_events indicates that the values in the events should not be redacted. The default is to redact, so that older versions of `cockroach zip` do not see un-redacted values by default. For good security, this field is only obeyed by the server after checking that the client of the RPC is an admin user. | [reserved](#support-status) |







#### Response Parameters




EventsResponse contains a set of event log entries. This is always limited
to the latest N entries (N is enforced in the associated endpoint).


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| events | [EventsResponse.Event](#cockroach.server.serverpb.EventsResponse-cockroach.server.serverpb.EventsResponse.Event) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.EventsResponse-cockroach.server.serverpb.EventsResponse.Event"></a>
#### EventsResponse.Event



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| timestamp | [google.protobuf.Timestamp](#cockroach.server.serverpb.EventsResponse-google.protobuf.Timestamp) |  | timestamp is the time at which the event occurred. | [reserved](#support-status) |
| event_type | [string](#cockroach.server.serverpb.EventsResponse-string) |  | event_type is the type of the event (e.g. "create_table", "drop_table". | [reserved](#support-status) |
| target_id | [int64](#cockroach.server.serverpb.EventsResponse-int64) |  | target_id is the target for this event. | [reserved](#support-status) |
| reporting_id | [int64](#cockroach.server.serverpb.EventsResponse-int64) |  | reporting_id is the reporting ID for this event. | [reserved](#support-status) |
| info | [string](#cockroach.server.serverpb.EventsResponse-string) |  | info has more detailed information for the event. The contents vary depending on the event. | [reserved](#support-status) |
| unique_id | [bytes](#cockroach.server.serverpb.EventsResponse-bytes) |  | unique_id is a unique identifier for this event. | [reserved](#support-status) |






## SetUIData

`POST /_admin/v1/uidata`

This requires a POST. Because of the libraries we're using, the POST body
must be in the following format:

{"key_values":
  { "key1": "base64_encoded_value1"},
  ...
  { "keyN": "base64_encoded_valueN"},
}

Note that all keys are quoted strings and that all values are base64-
encoded.

Together, SetUIData and GetUIData provide access to a "cookie jar" for the
admin UI. The structure of the underlying data is meant to be opaque to the
server.

Support status: [reserved](#support-status)

#### Request Parameters




SetUIDataRequest stores the given key/value pairs in the system.ui table.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key_values | [SetUIDataRequest.KeyValuesEntry](#cockroach.server.serverpb.SetUIDataRequest-cockroach.server.serverpb.SetUIDataRequest.KeyValuesEntry) | repeated | key_values is a map of keys to bytes values. Each key will be stored with its corresponding value as a separate row in system.ui. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.SetUIDataRequest-cockroach.server.serverpb.SetUIDataRequest.KeyValuesEntry"></a>
#### SetUIDataRequest.KeyValuesEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.SetUIDataRequest-string) |  |  |  |
| value | [bytes](#cockroach.server.serverpb.SetUIDataRequest-bytes) |  |  |  |






#### Response Parameters




SetUIDataResponse is currently an empty response.








## GetUIData

`GET /_admin/v1/uidata`

Example URLs:
- /_admin/v1/uidata?keys=MYKEY
- /_admin/v1/uidata?keys=MYKEY1&keys=MYKEY2

Yes, it's a little odd that the query parameter is named "keys" instead of
"key". I would've preferred that the URL parameter be named "key". However,
it's clearer for the protobuf field to be named "keys," which makes the URL
parameter "keys" as well.

Support status: [reserved](#support-status)

#### Request Parameters




GETUIDataRequest requests the values for the given keys from the system.ui
table.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| keys | [string](#cockroach.server.serverpb.GetUIDataRequest-string) | repeated |  | [reserved](#support-status) |







#### Response Parameters




GetUIDataResponse contains the requested values and the times at which
the values were last updated.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key_values | [GetUIDataResponse.KeyValuesEntry](#cockroach.server.serverpb.GetUIDataResponse-cockroach.server.serverpb.GetUIDataResponse.KeyValuesEntry) | repeated | key_values maps keys to their retrieved values. If this doesn't contain a a requested key, that key was not found. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.GetUIDataResponse-cockroach.server.serverpb.GetUIDataResponse.KeyValuesEntry"></a>
#### GetUIDataResponse.KeyValuesEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.GetUIDataResponse-string) |  |  |  |
| value | [GetUIDataResponse.Value](#cockroach.server.serverpb.GetUIDataResponse-cockroach.server.serverpb.GetUIDataResponse.Value) |  |  |  |





<a name="cockroach.server.serverpb.GetUIDataResponse-cockroach.server.serverpb.GetUIDataResponse.Value"></a>
#### GetUIDataResponse.Value



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| value | [bytes](#cockroach.server.serverpb.GetUIDataResponse-bytes) |  | value is the value of the requested key. | [reserved](#support-status) |
| last_updated | [google.protobuf.Timestamp](#cockroach.server.serverpb.GetUIDataResponse-google.protobuf.Timestamp) |  | last_updated is the time at which the value was last updated. | [reserved](#support-status) |






## Cluster

`GET /_admin/v1/cluster`

Cluster returns metadata for the cluster.

Support status: [reserved](#support-status)

#### Request Parameters




ClusterRequest requests metadata for the cluster.








#### Response Parameters




ClusterResponse contains metadata for the cluster.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| cluster_id | [string](#cockroach.server.serverpb.ClusterResponse-string) |  | The unique ID used to identify this cluster. | [reserved](#support-status) |
| reporting_enabled | [bool](#cockroach.server.serverpb.ClusterResponse-bool) |  | True if diagnostics reporting is enabled for the cluster. | [reserved](#support-status) |
| enterprise_enabled | [bool](#cockroach.server.serverpb.ClusterResponse-bool) |  | True if enterprise features are enabled for the cluster. | [reserved](#support-status) |







## Settings

`GET /_admin/v1/settings`

Settings returns the cluster-wide settings for the cluster.

Support status: [reserved](#support-status)

#### Request Parameters




SettingsRequest inquires what are the current settings in the cluster.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| keys | [string](#cockroach.server.serverpb.SettingsRequest-string) | repeated | The array of setting names to retrieve. An empty keys array means "all". | [reserved](#support-status) |
| unredacted_values | [bool](#cockroach.server.serverpb.SettingsRequest-bool) |  | Indicate whether to see unredacted setting values. This is opt-in so that a previous version `cockroach zip` does not start reporting values when this becomes active. For good security, the server only obeys this after it checks that the logger-in user has admin privilege. | [reserved](#support-status) |







#### Response Parameters




SettingsResponse is the response to SettingsRequest.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key_values | [SettingsResponse.KeyValuesEntry](#cockroach.server.serverpb.SettingsResponse-cockroach.server.serverpb.SettingsResponse.KeyValuesEntry) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.SettingsResponse-cockroach.server.serverpb.SettingsResponse.KeyValuesEntry"></a>
#### SettingsResponse.KeyValuesEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.SettingsResponse-string) |  |  |  |
| value | [SettingsResponse.Value](#cockroach.server.serverpb.SettingsResponse-cockroach.server.serverpb.SettingsResponse.Value) |  |  |  |





<a name="cockroach.server.serverpb.SettingsResponse-cockroach.server.serverpb.SettingsResponse.Value"></a>
#### SettingsResponse.Value



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| value | [string](#cockroach.server.serverpb.SettingsResponse-string) |  |  | [reserved](#support-status) |
| type | [string](#cockroach.server.serverpb.SettingsResponse-string) |  |  | [reserved](#support-status) |
| description | [string](#cockroach.server.serverpb.SettingsResponse-string) |  |  | [reserved](#support-status) |
| public | [bool](#cockroach.server.serverpb.SettingsResponse-bool) |  |  | [reserved](#support-status) |






## Health

`GET /health`

Health returns liveness for the node target of the request.

Support status: [public](#support-status)

#### Request Parameters




HealthRequest requests a liveness or readiness check.

A liveness check is triggered via ready set to false. In this mode,
an empty response is returned immediately, that is, the caller merely
learns that the process is running.

A readiness check (ready == true) is suitable for determining whether
user traffic should be directed at a given node, for example by a load
balancer. In this mode, a successful response is returned only if the
node:

- is not in the process of shutting down or booting up (including
  waiting for cluster bootstrap);
- is regarded as healthy by the cluster via the recent broadcast of
  a liveness beacon. Absent either of these conditions, an error
  code will result.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| ready | [bool](#cockroach.server.serverpb.HealthRequest-bool) |  | ready specifies whether the client wants to know whether the target node is ready to receive traffic. If a node is unready, an error will be returned. | [public](#support-status) |







#### Response Parameters




HealthResponse is the response to HealthRequest. It currently does not
contain any information.








## Liveness

`GET /_admin/v1/liveness`

Liveness returns the liveness state of all nodes on the cluster.

Support status: [reserved](#support-status)

#### Request Parameters




LivenessRequest requests liveness data for all nodes on the cluster.








#### Response Parameters




LivenessResponse contains the liveness status of each node on the cluster.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| livenesses | [cockroach.kv.kvserver.liveness.livenesspb.Liveness](#cockroach.server.serverpb.LivenessResponse-cockroach.kv.kvserver.liveness.livenesspb.Liveness) | repeated |  | [reserved](#support-status) |
| statuses | [LivenessResponse.StatusesEntry](#cockroach.server.serverpb.LivenessResponse-cockroach.server.serverpb.LivenessResponse.StatusesEntry) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.LivenessResponse-cockroach.server.serverpb.LivenessResponse.StatusesEntry"></a>
#### LivenessResponse.StatusesEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.LivenessResponse-int32) |  |  |  |
| value | [cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus](#cockroach.server.serverpb.LivenessResponse-cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus) |  |  |  |






## Jobs

`GET /_admin/v1/jobs`

Jobs returns the job records for all jobs of the given status and type.

Support status: [reserved](#support-status)

#### Request Parameters




JobsRequest requests system job information of the given status and type.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| limit | [int32](#cockroach.server.serverpb.JobsRequest-int32) |  |  | [reserved](#support-status) |
| status | [string](#cockroach.server.serverpb.JobsRequest-string) |  |  | [reserved](#support-status) |
| type | [cockroach.sql.jobs.jobspb.Type](#cockroach.server.serverpb.JobsRequest-cockroach.sql.jobs.jobspb.Type) |  |  | [reserved](#support-status) |







#### Response Parameters




JobsResponse contains the job record for each matching job.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| jobs | [JobResponse](#cockroach.server.serverpb.JobsResponse-cockroach.server.serverpb.JobResponse) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.JobsResponse-cockroach.server.serverpb.JobResponse"></a>
#### JobResponse

JobResponse contains the job record for a job.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [int64](#cockroach.server.serverpb.JobsResponse-int64) |  |  | [reserved](#support-status) |
| type | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | [reserved](#support-status) |
| description | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | [reserved](#support-status) |
| statement | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | [reserved](#support-status) |
| username | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | [reserved](#support-status) |
| descriptor_ids | [uint32](#cockroach.server.serverpb.JobsResponse-uint32) | repeated |  | [reserved](#support-status) |
| status | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | [reserved](#support-status) |
| created | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| started | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| finished | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| modified | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| fraction_completed | [float](#cockroach.server.serverpb.JobsResponse-float) |  |  | [reserved](#support-status) |
| error | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | [reserved](#support-status) |
| highwater_timestamp | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  | highwater_timestamp is the highwater timestamp returned as normal timestamp. This is appropriate for display to humans. | [reserved](#support-status) |
| highwater_decimal | [string](#cockroach.server.serverpb.JobsResponse-string) |  | highwater_decimal is the highwater timestamp in the proprietary decimal form used by logical timestamps internally. This is appropriate to pass to a "AS OF SYSTEM TIME" SQL statement. | [reserved](#support-status) |
| running_status | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | [reserved](#support-status) |
| last_run | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| next_run | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| num_runs | [int64](#cockroach.server.serverpb.JobsResponse-int64) |  |  | [reserved](#support-status) |
| execution_failures | [JobResponse.ExecutionFailure](#cockroach.server.serverpb.JobsResponse-cockroach.server.serverpb.JobResponse.ExecutionFailure) | repeated | ExecutionFailures is a log of execution failures of the job. It is not guaranteed to contain all execution failures and some execution failures may not contain an error or end. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.JobsResponse-cockroach.server.serverpb.JobResponse.ExecutionFailure"></a>
#### JobResponse.ExecutionFailure

ExecutionFailure corresponds to a failure to execute the job with the
attempt starting at start and ending at end.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| status | [string](#cockroach.server.serverpb.JobsResponse-string) |  | Status is the status of the job during the execution. | [reserved](#support-status) |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  | Start is the time at which the execution started. | [reserved](#support-status) |
| end | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  | End is the time at which the error occurred. | [reserved](#support-status) |
| error | [string](#cockroach.server.serverpb.JobsResponse-string) |  | Error is the error which occurred. | [reserved](#support-status) |






## Job

`GET /_admin/v1/jobs/{job_id}`

Job returns the job record for the job of the given job_id.

Support status: [reserved](#support-status)

#### Request Parameters




JobRequest requests system job information for the given job_id.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| job_id | [int64](#cockroach.server.serverpb.JobRequest-int64) |  |  | [reserved](#support-status) |







#### Response Parameters




JobResponse contains the job record for a job.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [int64](#cockroach.server.serverpb.JobResponse-int64) |  |  | [reserved](#support-status) |
| type | [string](#cockroach.server.serverpb.JobResponse-string) |  |  | [reserved](#support-status) |
| description | [string](#cockroach.server.serverpb.JobResponse-string) |  |  | [reserved](#support-status) |
| statement | [string](#cockroach.server.serverpb.JobResponse-string) |  |  | [reserved](#support-status) |
| username | [string](#cockroach.server.serverpb.JobResponse-string) |  |  | [reserved](#support-status) |
| descriptor_ids | [uint32](#cockroach.server.serverpb.JobResponse-uint32) | repeated |  | [reserved](#support-status) |
| status | [string](#cockroach.server.serverpb.JobResponse-string) |  |  | [reserved](#support-status) |
| created | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| started | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| finished | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| modified | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| fraction_completed | [float](#cockroach.server.serverpb.JobResponse-float) |  |  | [reserved](#support-status) |
| error | [string](#cockroach.server.serverpb.JobResponse-string) |  |  | [reserved](#support-status) |
| highwater_timestamp | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobResponse-google.protobuf.Timestamp) |  | highwater_timestamp is the highwater timestamp returned as normal timestamp. This is appropriate for display to humans. | [reserved](#support-status) |
| highwater_decimal | [string](#cockroach.server.serverpb.JobResponse-string) |  | highwater_decimal is the highwater timestamp in the proprietary decimal form used by logical timestamps internally. This is appropriate to pass to a "AS OF SYSTEM TIME" SQL statement. | [reserved](#support-status) |
| running_status | [string](#cockroach.server.serverpb.JobResponse-string) |  |  | [reserved](#support-status) |
| last_run | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| next_run | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| num_runs | [int64](#cockroach.server.serverpb.JobResponse-int64) |  |  | [reserved](#support-status) |
| execution_failures | [JobResponse.ExecutionFailure](#cockroach.server.serverpb.JobResponse-cockroach.server.serverpb.JobResponse.ExecutionFailure) | repeated | ExecutionFailures is a log of execution failures of the job. It is not guaranteed to contain all execution failures and some execution failures may not contain an error or end. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.JobResponse-cockroach.server.serverpb.JobResponse.ExecutionFailure"></a>
#### JobResponse.ExecutionFailure

ExecutionFailure corresponds to a failure to execute the job with the
attempt starting at start and ending at end.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| status | [string](#cockroach.server.serverpb.JobResponse-string) |  | Status is the status of the job during the execution. | [reserved](#support-status) |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobResponse-google.protobuf.Timestamp) |  | Start is the time at which the execution started. | [reserved](#support-status) |
| end | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobResponse-google.protobuf.Timestamp) |  | End is the time at which the error occurred. | [reserved](#support-status) |
| error | [string](#cockroach.server.serverpb.JobResponse-string) |  | Error is the error which occurred. | [reserved](#support-status) |






## Locations

`GET /_admin/v1/locations`

Locations returns the locality location records.

Support status: [reserved](#support-status)

#### Request Parameters




LocationsRequest requests system locality location information.








#### Response Parameters




JobsResponse contains the job record for each matching job.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| locations | [LocationsResponse.Location](#cockroach.server.serverpb.LocationsResponse-cockroach.server.serverpb.LocationsResponse.Location) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.LocationsResponse-cockroach.server.serverpb.LocationsResponse.Location"></a>
#### LocationsResponse.Location



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| locality_key | [string](#cockroach.server.serverpb.LocationsResponse-string) |  |  | [reserved](#support-status) |
| locality_value | [string](#cockroach.server.serverpb.LocationsResponse-string) |  |  | [reserved](#support-status) |
| latitude | [double](#cockroach.server.serverpb.LocationsResponse-double) |  |  | [reserved](#support-status) |
| longitude | [double](#cockroach.server.serverpb.LocationsResponse-double) |  |  | [reserved](#support-status) |






## QueryPlan

`GET /_admin/v1/queryplan`

QueryPlan returns the query plans for a SQL string.

Support status: [reserved](#support-status)

#### Request Parameters




QueryPlanRequest requests the query plans for a SQL string.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| query | [string](#cockroach.server.serverpb.QueryPlanRequest-string) |  | query is the SQL query string. | [reserved](#support-status) |







#### Response Parameters




QueryPlanResponse contains the query plans for a SQL string (currently only
the distsql physical query plan).


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| distsql_physical_query_plan | [string](#cockroach.server.serverpb.QueryPlanResponse-string) |  |  | [reserved](#support-status) |







## Drain



Drain puts the node into the specified drain mode(s) and optionally
instructs the process to terminate.
We do not expose this via HTTP unless we have a way to authenticate
+ authorize streaming RPC connections. See #42567.

Support status: [reserved](#support-status)

#### Request Parameters




DrainRequest instructs the receiving node to drain.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| shutdown | [bool](#cockroach.server.serverpb.DrainRequest-bool) |  | When true, terminates the process after the server has started draining. Setting both shutdown and do_drain to false causes the request to only operate as a probe. Setting do_drain to false and shutdown to true causes the server to shut down immediately without first draining. | [reserved](#support-status) |
| do_drain | [bool](#cockroach.server.serverpb.DrainRequest-bool) |  | When true, perform the drain phase. See the comment above on shutdown for an explanation of the interaction between the two. do_drain is also implied by a non-nil deprecated_probe_indicator. | [reserved](#support-status) |
| node_id | [string](#cockroach.server.serverpb.DrainRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. For compatibility with v21.2 nodes, an empty node_id is interpreted as "local". This behavior might be removed in subsequent versions. | [reserved](#support-status) |
| verbose | [bool](#cockroach.server.serverpb.DrainRequest-bool) |  | When true, more detailed information is logged during the range lease drain phase. | [reserved](#support-status) |







#### Response Parameters




DrainResponse is the response to a successful DrainRequest.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| is_draining | [bool](#cockroach.server.serverpb.DrainResponse-bool) |  | is_draining is set to true iff the server is currently draining. This is set to true in response to a request where skip_drain is false; but it can also be set to true in response to a probe request (!shutdown && skip_drain) if another drain request has been issued prior or asynchronously. | [reserved](#support-status) |
| drain_remaining_indicator | [uint64](#cockroach.server.serverpb.DrainResponse-uint64) |  | drain_remaining_indicator measures, at the time of starting to process the corresponding drain request, how many actions to fully drain the node were deemed to be necessary. Some, but not all, of these actions may already have been carried out by the time this indicator is received by the client. The client should issue requests until this indicator first reaches zero, which indicates that the node is fully drained.<br><br>The API contract is the following:<br><br>- upon a first Drain call with do_drain set, the remaining   indicator will have some value >=0. If >0, it indicates that   drain is pushing state away from the node. (What this state   precisely means is left unspecified for this field. See below   for details.)<br><br>- upon a subsequent Drain call with do_drain set, the remaining   indicator should have reduced in value. The drain process does best   effort at shedding state away from the node; hopefully, all the   state is shed away upon the first call and the progress   indicator can be zero as early as the second call. However,   if there was a lot of state to shed, it is possible for   timeout to be encountered upon the first call. In that case, the   second call will do some more work and return a non-zero value   as well.<br><br>- eventually, in an iterated sequence of DrainRequests with   do_drain set, the remaining indicator should reduce to zero. At   that point the client can conclude that no state is left to   shed, and it should be safe to shut down the node with a   DrainRequest with shutdown = true.<br><br>Note that this field is left unpopulated (and thus remains at zero) for pre-20.1 nodes. A client can recognize this by observing is_draining to be false after a request with do_drain = true: the is_draining field is also left unpopulated by pre-20.1 nodes. | [reserved](#support-status) |
| drain_remaining_description | [string](#cockroach.server.serverpb.DrainResponse-string) |  | drain_remaining_description is an informal (= not machine-parsable) string that explains the progress of the drain process to human eyes. This is intended for use mainly for troubleshooting.<br><br>The field is only populated if do_drain is true in the request. | [reserved](#support-status) |







## Decommission



Decommission puts the node(s) into the specified decommissioning state.
If this ever becomes exposed via HTTP, ensure that it performs
authorization. See #42567.

Support status: [reserved](#support-status)

#### Request Parameters




DecommissionRequest requests the server to set the membership status on
all nodes specified by NodeIDs to the value of TargetMembership.

If no NodeIDs are given, it targets the recipient node.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_ids | [int32](#cockroach.server.serverpb.DecommissionRequest-int32) | repeated |  | [reserved](#support-status) |
| target_membership | [cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus](#cockroach.server.serverpb.DecommissionRequest-cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus) |  |  | [reserved](#support-status) |







#### Response Parameters




DecommissionStatusResponse lists decommissioning statuses for a number of NodeIDs.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| status | [DecommissionStatusResponse.Status](#cockroach.server.serverpb.DecommissionStatusResponse-cockroach.server.serverpb.DecommissionStatusResponse.Status) | repeated | Status of all affected nodes. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.DecommissionStatusResponse-cockroach.server.serverpb.DecommissionStatusResponse.Status"></a>
#### DecommissionStatusResponse.Status



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.DecommissionStatusResponse-int32) |  |  | [reserved](#support-status) |
| is_live | [bool](#cockroach.server.serverpb.DecommissionStatusResponse-bool) |  |  | [reserved](#support-status) |
| replica_count | [int64](#cockroach.server.serverpb.DecommissionStatusResponse-int64) |  | The number of replicas on the node, computed by scanning meta2 ranges. | [reserved](#support-status) |
| membership | [cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus](#cockroach.server.serverpb.DecommissionStatusResponse-cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus) |  | The membership status of the given node. | [reserved](#support-status) |
| draining | [bool](#cockroach.server.serverpb.DecommissionStatusResponse-bool) |  |  | [reserved](#support-status) |






## DecommissionStatus



DecommissionStatus retrieves the decommissioning status of the specified nodes.
If this ever becomes exposed via HTTP, ensure that it performs
authorization. See #42567.

Support status: [reserved](#support-status)

#### Request Parameters




DecommissionStatusRequest requests the decommissioning status for the
specified or, if none are specified, all nodes.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_ids | [int32](#cockroach.server.serverpb.DecommissionStatusRequest-int32) | repeated |  | [reserved](#support-status) |







#### Response Parameters




DecommissionStatusResponse lists decommissioning statuses for a number of NodeIDs.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| status | [DecommissionStatusResponse.Status](#cockroach.server.serverpb.DecommissionStatusResponse-cockroach.server.serverpb.DecommissionStatusResponse.Status) | repeated | Status of all affected nodes. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.DecommissionStatusResponse-cockroach.server.serverpb.DecommissionStatusResponse.Status"></a>
#### DecommissionStatusResponse.Status



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.DecommissionStatusResponse-int32) |  |  | [reserved](#support-status) |
| is_live | [bool](#cockroach.server.serverpb.DecommissionStatusResponse-bool) |  |  | [reserved](#support-status) |
| replica_count | [int64](#cockroach.server.serverpb.DecommissionStatusResponse-int64) |  | The number of replicas on the node, computed by scanning meta2 ranges. | [reserved](#support-status) |
| membership | [cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus](#cockroach.server.serverpb.DecommissionStatusResponse-cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus) |  | The membership status of the given node. | [reserved](#support-status) |
| draining | [bool](#cockroach.server.serverpb.DecommissionStatusResponse-bool) |  |  | [reserved](#support-status) |






## RangeLog

`GET /_admin/v1/rangelog/{range_id}`

URL: /_admin/v1/rangelog
URL: /_admin/v1/rangelog?limit=100
URL: /_admin/v1/rangelog/1
URL: /_admin/v1/rangelog/1?limit=100

Support status: [reserved](#support-status)

#### Request Parameters




RangeLogRequest request the history of a range from the range log.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int64](#cockroach.server.serverpb.RangeLogRequest-int64) |  | TODO(tamird): use [(gogoproto.customname) = "RangeID"] below. Need to figure out how to teach grpc-gateway about custom names. If RangeID is 0, returns range log history without filtering by range. | [reserved](#support-status) |
| limit | [int32](#cockroach.server.serverpb.RangeLogRequest-int32) |  | limit is the total number of results that are retrieved by the query. If this is omitted or set to 0, the default maximum number of results are returned. When set to > 0, at most only that number of results are returned. When set to < 0, an unlimited number of results are returned. | [reserved](#support-status) |







#### Response Parameters




RangeLogResponse contains a list of entries from the range log table.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| events | [RangeLogResponse.Event](#cockroach.server.serverpb.RangeLogResponse-cockroach.server.serverpb.RangeLogResponse.Event) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.RangeLogResponse-cockroach.server.serverpb.RangeLogResponse.Event"></a>
#### RangeLogResponse.Event



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| event | [cockroach.kv.kvserver.storagepb.RangeLogEvent](#cockroach.server.serverpb.RangeLogResponse-cockroach.kv.kvserver.storagepb.RangeLogEvent) |  |  | [reserved](#support-status) |
| pretty_info | [RangeLogResponse.PrettyInfo](#cockroach.server.serverpb.RangeLogResponse-cockroach.server.serverpb.RangeLogResponse.PrettyInfo) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.RangeLogResponse-cockroach.server.serverpb.RangeLogResponse.PrettyInfo"></a>
#### RangeLogResponse.PrettyInfo

To avoid porting the pretty printing of keys and descriptors to
javascript, they will be precomputed on the serverside.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| updated_desc | [string](#cockroach.server.serverpb.RangeLogResponse-string) |  |  | [reserved](#support-status) |
| new_desc | [string](#cockroach.server.serverpb.RangeLogResponse-string) |  |  | [reserved](#support-status) |
| added_replica | [string](#cockroach.server.serverpb.RangeLogResponse-string) |  |  | [reserved](#support-status) |
| removed_replica | [string](#cockroach.server.serverpb.RangeLogResponse-string) |  |  | [reserved](#support-status) |
| reason | [string](#cockroach.server.serverpb.RangeLogResponse-string) |  |  | [reserved](#support-status) |
| details | [string](#cockroach.server.serverpb.RangeLogResponse-string) |  |  | [reserved](#support-status) |






## DataDistribution

`GET /_admin/v1/data_distribution`



Support status: [reserved](#support-status)

#### Request Parameters













#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| database_info | [DataDistributionResponse.DatabaseInfoEntry](#cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.DatabaseInfoEntry) | repeated | By database name. | [reserved](#support-status) |
| zone_configs | [DataDistributionResponse.ZoneConfigsEntry](#cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.ZoneConfigsEntry) | repeated | By zone name. | [reserved](#support-status) |






<a name="cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.DatabaseInfoEntry"></a>
#### DataDistributionResponse.DatabaseInfoEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.DataDistributionResponse-string) |  |  |  |
| value | [DataDistributionResponse.DatabaseInfo](#cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.DatabaseInfo) |  |  |  |





<a name="cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.DatabaseInfo"></a>
#### DataDistributionResponse.DatabaseInfo



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| table_info | [DataDistributionResponse.DatabaseInfo.TableInfoEntry](#cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.DatabaseInfo.TableInfoEntry) | repeated | By table name. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.DatabaseInfo.TableInfoEntry"></a>
#### DataDistributionResponse.DatabaseInfo.TableInfoEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.DataDistributionResponse-string) |  |  |  |
| value | [DataDistributionResponse.TableInfo](#cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.TableInfo) |  |  |  |





<a name="cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.TableInfo"></a>
#### DataDistributionResponse.TableInfo



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| replica_count_by_node_id | [DataDistributionResponse.TableInfo.ReplicaCountByNodeIdEntry](#cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.TableInfo.ReplicaCountByNodeIdEntry) | repeated |  | [reserved](#support-status) |
| zone_config_id | [int64](#cockroach.server.serverpb.DataDistributionResponse-int64) |  |  | [reserved](#support-status) |
| dropped_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.DataDistributionResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |





<a name="cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.TableInfo.ReplicaCountByNodeIdEntry"></a>
#### DataDistributionResponse.TableInfo.ReplicaCountByNodeIdEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.DataDistributionResponse-int32) |  |  |  |
| value | [int64](#cockroach.server.serverpb.DataDistributionResponse-int64) |  |  |  |





<a name="cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.ZoneConfigsEntry"></a>
#### DataDistributionResponse.ZoneConfigsEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.DataDistributionResponse-string) |  |  |  |
| value | [DataDistributionResponse.ZoneConfig](#cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.ZoneConfig) |  |  |  |





<a name="cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.ZoneConfig"></a>
#### DataDistributionResponse.ZoneConfig



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| target | [string](#cockroach.server.serverpb.DataDistributionResponse-string) |  | target is the object the zone config applies to, e.g. "DATABASE db" or "PARTITION north_america OF TABLE users". | [reserved](#support-status) |
| config | [cockroach.config.zonepb.ZoneConfig](#cockroach.server.serverpb.DataDistributionResponse-cockroach.config.zonepb.ZoneConfig) |  |  | [reserved](#support-status) |
| config_sql | [string](#cockroach.server.serverpb.DataDistributionResponse-string) |  | config_sql is the SQL representation of config. | [reserved](#support-status) |






## AllMetricMetadata

`GET /_admin/v1/metricmetadata`

URL: /_admin/v1/metricmetadata

Support status: [reserved](#support-status)

#### Request Parameters




MetricMetadataRequest requests metadata for all metrics.








#### Response Parameters




MetricMetadataResponse contains the metadata for all metrics.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| metadata | [MetricMetadataResponse.MetadataEntry](#cockroach.server.serverpb.MetricMetadataResponse-cockroach.server.serverpb.MetricMetadataResponse.MetadataEntry) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.MetricMetadataResponse-cockroach.server.serverpb.MetricMetadataResponse.MetadataEntry"></a>
#### MetricMetadataResponse.MetadataEntry



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.MetricMetadataResponse-string) |  |  |  |
| value | [cockroach.util.metric.Metadata](#cockroach.server.serverpb.MetricMetadataResponse-cockroach.util.metric.Metadata) |  |  |  |






## ChartCatalog

`GET /_admin/v1/chartcatalog`

URL: /_admin/v1/chartcatalog

Support status: [reserved](#support-status)

#### Request Parameters




ChartCatalogRequest requests returns a catalog of Admin UI charts.








#### Response Parameters




ChartCatalogResponse returns a catalog of Admin UI charts useful for debugging.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| catalog | [cockroach.ts.catalog.ChartSection](#cockroach.server.serverpb.ChartCatalogResponse-cockroach.ts.catalog.ChartSection) | repeated |  | [reserved](#support-status) |







## EnqueueRange

`POST /_admin/v1/enqueue_range`

EnqueueRange runs the specified range through the specified queue on the
range's leaseholder store, returning the detailed trace and error
information from doing so. Parameters must be provided in the body of the
POST request.
For example:

{
  "queue": "raftlog",
  "rangeId": 10
}

Support status: [reserved](#support-status)

#### Request Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.EnqueueRangeRequest-int32) |  | The node on which the queue should process the range. If node_id is 0, the request will be forwarded to all other nodes. | [reserved](#support-status) |
| queue | [string](#cockroach.server.serverpb.EnqueueRangeRequest-string) |  | The name of the replica queue to run the range through. Matched against each queue's name field. See the implementation of baseQueue for details. | [reserved](#support-status) |
| range_id | [int32](#cockroach.server.serverpb.EnqueueRangeRequest-int32) |  | The ID of the range to run through the queue. | [reserved](#support-status) |
| skip_should_queue | [bool](#cockroach.server.serverpb.EnqueueRangeRequest-bool) |  | If set, run the queue's process method without first checking whether the replica should be processed by calling shouldQueue. | [reserved](#support-status) |







#### Response Parameters







| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| details | [EnqueueRangeResponse.Details](#cockroach.server.serverpb.EnqueueRangeResponse-cockroach.server.serverpb.EnqueueRangeResponse.Details) | repeated |  | [reserved](#support-status) |






<a name="cockroach.server.serverpb.EnqueueRangeResponse-cockroach.server.serverpb.EnqueueRangeResponse.Details"></a>
#### EnqueueRangeResponse.Details



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.EnqueueRangeResponse-int32) |  |  | [reserved](#support-status) |
| events | [TraceEvent](#cockroach.server.serverpb.EnqueueRangeResponse-cockroach.server.serverpb.TraceEvent) | repeated | All trace events collected while processing the range in the queue. | [reserved](#support-status) |
| error | [string](#cockroach.server.serverpb.EnqueueRangeResponse-string) |  | The error message from the queue's processing, if any. | [reserved](#support-status) |





<a name="cockroach.server.serverpb.EnqueueRangeResponse-cockroach.server.serverpb.TraceEvent"></a>
#### TraceEvent



| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| time | [google.protobuf.Timestamp](#cockroach.server.serverpb.EnqueueRangeResponse-google.protobuf.Timestamp) |  |  | [reserved](#support-status) |
| message | [string](#cockroach.server.serverpb.EnqueueRangeResponse-string) |  |  | [reserved](#support-status) |






## SendKVBatch



SendKVBatch proxies the given BatchRequest into KV, returning the
response. It is used by the CLI `debug send-kv-batch` command.

Support status: [reserved](#support-status)

#### Request Parameters



#### Response Parameters



