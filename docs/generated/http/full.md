## Certificates

`GET /_status/certificates/{node_id}`

Certificates retrieves a copy of the TLS certificates.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.CertificatesRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| certificates | [CertificateDetails](#cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails"></a>
#### CertificateDetails



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| type | [CertificateDetails.CertificateType](#cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails.CertificateType) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| error_message | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  | "error_message" and "data" are mutually exclusive. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| data | [bytes](#cockroach.server.serverpb.CertificatesResponse-bytes) |  | data is the raw file contents of the certificate. This means PEM-encoded DER data. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| fields | [CertificateDetails.Fields](#cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails.Fields) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails.Fields"></a>
#### CertificateDetails.Fields



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| issuer | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| subject | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| valid_from | [int64](#cockroach.server.serverpb.CertificatesResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| valid_until | [int64](#cockroach.server.serverpb.CertificatesResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| addresses | [string](#cockroach.server.serverpb.CertificatesResponse-string) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| signature_algorithm | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| public_key | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| key_usage | [string](#cockroach.server.serverpb.CertificatesResponse-string) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| extended_key_usage | [string](#cockroach.server.serverpb.CertificatesResponse-string) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## Details

`GET /_status/details/{node_id}`

Details retrieves details about the nodes in the cluster.

 Reference example:
 ```
 curl http://127.0.0.1:51875/_status/details/1
   ->
 {
 "nodeId": 1,
   "address": {
   "networkField": "tcp",
   "addressField": "127.0.0.1:51876"
   },
   "buildInfo": {...},
   "systemInfo": {...},
   "sqlAddress": {
   "networkField": "tcp",
   "addressField": "127.0.0.1:51877"
   }
 }
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




DetailsRequest requests a nodes details.
Note: this does *not* check readiness. Use the Health RPC for that purpose.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.DetailsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.DetailsResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.DetailsResponse-cockroach.util.UnresolvedAddr) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| build_info | [cockroach.build.Info](#cockroach.server.serverpb.DetailsResponse-cockroach.build.Info) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| system_info | [SystemInfo](#cockroach.server.serverpb.DetailsResponse-cockroach.server.serverpb.SystemInfo) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| sql_address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.DetailsResponse-cockroach.util.UnresolvedAddr) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.DetailsResponse-cockroach.server.serverpb.SystemInfo"></a>
#### SystemInfo

SystemInfo contains information about the host system.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| system_info | [string](#cockroach.server.serverpb.DetailsResponse-string) |  | system_info is the output from `uname -a` | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| kernel_info | [string](#cockroach.server.serverpb.DetailsResponse-string) |  | kernel_info is the output from `uname -r`. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## Nodes

`GET /_status/nodes`

Nodes retrieves an overview of the nodes in the cluster.


 Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/nodes   
  ->
 {
  "nodes": [
    {
      "desc": {
        "nodeId": 1,
        "address": {
          "networkField": "tcp",
          "addressField": "127.0.0.1:51876"
        },
        "attrs": {
          "attrs": []
        },
        "locality": {
          "tiers": [
            {
              "key": "region",
              "value": "us-east1"
            },
            {
              "key": "az",
              "value": "b"
            }
          ]
        },
        "ServerVersion": {
          "majorVal": 20,
          "minorVal": 2,
          "patch": 0,
          "unstable": 1
        },
        "buildTag": "v20.2.0-alpha.3-2061-gb4156e4e2c",
        "startedAt": "1606913361337496000",
        "localityAddress": [],
        "clusterName": "",
        "sqlAddress": {
          "networkField": "tcp",
          "addressField": "127.0.0.1:51877"
        }
      },
      "buildInfo": {...},
      "startedAt": "1606913361337496000",
      "updatedAt": "1606920801488818000",
      "metrics": {...},
      "storeStatuses": [
        {
          "desc": {
            "storeId": 1,
            "attrs": {
              "attrs": []
            },
            "node": {
              "nodeId": 1,
              "address": {
                "networkField": "tcp",
                "addressField": "127.0.0.1:51876"
              },
              "attrs": {
                "attrs": []
              },
              "locality": {
                "tiers": [
                  {
                    "key": "region",
                    "value": "us-east1"
                  },
                  {
                    "key": "az",
                    "value": "b"
                  }
                ]
              },
              "ServerVersion": {
                "majorVal": 20,
                "minorVal": 2,
                "patch": 0,
                "unstable": 1
              },
              "buildTag": "v20.2.0-alpha.3-2061-gb4156e4e2c",
              "startedAt": "1606913361337496000",
              "localityAddress": [],
              "clusterName": "",
              "sqlAddress": {
                "networkField": "tcp",
                "addressField": "127.0.0.1:51877"
              }
            },
            "capacity": {
              "capacity": "536870912",
              "available": "536870912",
              "used": "0",
              "logicalBytes": "21518089",
              "rangeCount": 65,
              "leaseCount": 65,
              "queriesPerSecond": 10.326122145577715,
              "writesPerSecond": 82.90851430132487,
              "bytesPerReplica": {
                "p10": 0,
                "p25": 0,
                "p50": 460,
                "p75": 10118,
                "p90": 40825,
                "pMax": 20712052
              },
              "writesPerReplica": {...}
            }
          }
          "metrics": {...}
        }
      ],
      "args": [
        "./cockroach",
        "demo",
        "--insecure"
      ],
      "env": [],
      "latencies": {},
      "activity": {
        "1": {
          "incoming": "235921",
          "outgoing": "222073",
          "latency": "677603"
        }
      },
      "totalSystemMemory": "34359738368",
      "numCpus": 16
    }
  ],
  "livenessByNodeId": {
    "1": 3
  }
}   
 ```
Don't introduce additional usages of this RPC. See #50707 for more details.
The underlying response type is something we're looking to get rid of.

Note: this is an “alpha” API endpoint. It is subject to change without
advance notice in a subsequent release.

#### Request Parameters




NodesRequest requests a copy of the node information as known to gossip
and the KV layer.

Note: this is an “alpha” API payload. It is subject to change without
advance notice in a subsequent release.








#### Response Parameters




NodesResponse describe the nodes in the cluster.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| nodes | [cockroach.server.status.statuspb.NodeStatus](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus) | repeated | nodes carries the status payloads for all nodes in the cluster. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| liveness_by_node_id | [NodesResponse.LivenessByNodeIdEntry](#cockroach.server.serverpb.NodesResponse-cockroach.server.serverpb.NodesResponse.LivenessByNodeIdEntry) | repeated | liveness_by_node_id maps each node ID to a liveness status. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus"></a>
#### NodeStatus

NodeStatus records the most recent values of metrics for a node.

Note: this is an “alpha” API payload. It is subject to change without
advance notice in a subsequent release.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.NodeDescriptor](#cockroach.server.serverpb.NodesResponse-cockroach.roachpb.NodeDescriptor) |  | desc is the node descriptor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| build_info | [cockroach.build.Info](#cockroach.server.serverpb.NodesResponse-cockroach.build.Info) |  | build_info describes the 'cockroach' executable file. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| started_at | [int64](#cockroach.server.serverpb.NodesResponse-int64) |  | started_at is the unix timestamp at which the node process was last started. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| updated_at | [int64](#cockroach.server.serverpb.NodesResponse-int64) |  | updated_at is the unix timestamp at which the node status record was last updated. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| metrics | [NodeStatus.MetricsEntry](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| store_statuses | [StoreStatus](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.StoreStatus) | repeated | store_statuses provides the store status payloads for all the stores on that node. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| args | [string](#cockroach.server.serverpb.NodesResponse-string) | repeated | args is the list of command-line arguments used to last start the node. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| env | [string](#cockroach.server.serverpb.NodesResponse-string) | repeated | env is the list of environment variables that influenced the node's configuration. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| latencies | [NodeStatus.LatenciesEntry](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus.LatenciesEntry) | repeated | latencies is a map of nodeIDs to nanoseconds which is the latency between this node and the other node.<br><br>NOTE: this is deprecated and is only set if the min supported       cluster version is >= VersionRPCNetworkStats. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| activity | [NodeStatus.ActivityEntry](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus.ActivityEntry) | repeated | activity is a map of nodeIDs to network statistics from this node to other nodes. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| total_system_memory | [int64](#cockroach.server.serverpb.NodesResponse-int64) |  | total_system_memory is the total RAM available to the system (or, if detected, the memory available to the cgroup this process is in) in bytes. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| num_cpus | [int32](#cockroach.server.serverpb.NodesResponse-int32) |  | num_cpus is the number of logical CPUs as reported by the operating system on the host where the 'cockroach' process is running. Note that this does not report the number of CPUs actually used by 'cockroach'; this parameter is controlled separately. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |





<a name="cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus.MetricsEntry"></a>
#### NodeStatus.MetricsEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.NodesResponse-string) |  |  |  |
| value | [double](#cockroach.server.serverpb.NodesResponse-double) |  |  |  |





<a name="cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.StoreStatus"></a>
#### StoreStatus

StoreStatus records the most recent values of metrics for a store.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.StoreDescriptor](#cockroach.server.serverpb.NodesResponse-cockroach.roachpb.StoreDescriptor) |  | desc is the store descriptor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| metrics | [StoreStatus.MetricsEntry](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.StoreStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





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



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| incoming | [int64](#cockroach.server.serverpb.NodesResponse-int64) |  | in bytes | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| outgoing | [int64](#cockroach.server.serverpb.NodesResponse-int64) |  | in bytes | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| latency | [int64](#cockroach.server.serverpb.NodesResponse-int64) |  | in nanoseconds | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.NodesResponse-cockroach.server.serverpb.NodesResponse.LivenessByNodeIdEntry"></a>
#### NodesResponse.LivenessByNodeIdEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.NodesResponse-int32) |  |  |  |
| value | [cockroach.kv.kvserver.storagepb.NodeLivenessStatus](#cockroach.server.serverpb.NodesResponse-cockroach.kv.kvserver.storagepb.NodeLivenessStatus) |  |  |  |






## Node

`GET /_status/nodes/{node_id}`

Node retrieves details about a single node.

Note: this is an “alpha” API endpoint. It is subject to change without
advance notice in a subsequent release.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.NodeRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




NodeStatus records the most recent values of metrics for a node.

Note: this is an “alpha” API payload. It is subject to change without
advance notice in a subsequent release.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.NodeDescriptor](#cockroach.server.status.statuspb.NodeStatus-cockroach.roachpb.NodeDescriptor) |  | desc is the node descriptor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| build_info | [cockroach.build.Info](#cockroach.server.status.statuspb.NodeStatus-cockroach.build.Info) |  | build_info describes the 'cockroach' executable file. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| started_at | [int64](#cockroach.server.status.statuspb.NodeStatus-int64) |  | started_at is the unix timestamp at which the node process was last started. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| updated_at | [int64](#cockroach.server.status.statuspb.NodeStatus-int64) |  | updated_at is the unix timestamp at which the node status record was last updated. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| metrics | [NodeStatus.MetricsEntry](#cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.NodeStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| store_statuses | [StoreStatus](#cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.StoreStatus) | repeated | store_statuses provides the store status payloads for all the stores on that node. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| args | [string](#cockroach.server.status.statuspb.NodeStatus-string) | repeated | args is the list of command-line arguments used to last start the node. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| env | [string](#cockroach.server.status.statuspb.NodeStatus-string) | repeated | env is the list of environment variables that influenced the node's configuration. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| latencies | [NodeStatus.LatenciesEntry](#cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.NodeStatus.LatenciesEntry) | repeated | latencies is a map of nodeIDs to nanoseconds which is the latency between this node and the other node.<br><br>NOTE: this is deprecated and is only set if the min supported       cluster version is >= VersionRPCNetworkStats. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| activity | [NodeStatus.ActivityEntry](#cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.NodeStatus.ActivityEntry) | repeated | activity is a map of nodeIDs to network statistics from this node to other nodes. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| total_system_memory | [int64](#cockroach.server.status.statuspb.NodeStatus-int64) |  | total_system_memory is the total RAM available to the system (or, if detected, the memory available to the cgroup this process is in) in bytes. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| num_cpus | [int32](#cockroach.server.status.statuspb.NodeStatus-int32) |  | num_cpus is the number of logical CPUs as reported by the operating system on the host where the 'cockroach' process is running. Note that this does not report the number of CPUs actually used by 'cockroach'; this parameter is controlled separately. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |






<a name="cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.NodeStatus.MetricsEntry"></a>
#### NodeStatus.MetricsEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.status.statuspb.NodeStatus-string) |  |  |  |
| value | [double](#cockroach.server.status.statuspb.NodeStatus-double) |  |  |  |





<a name="cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.StoreStatus"></a>
#### StoreStatus

StoreStatus records the most recent values of metrics for a store.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.StoreDescriptor](#cockroach.server.status.statuspb.NodeStatus-cockroach.roachpb.StoreDescriptor) |  | desc is the store descriptor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| metrics | [StoreStatus.MetricsEntry](#cockroach.server.status.statuspb.NodeStatus-cockroach.server.status.statuspb.StoreStatus.MetricsEntry) | repeated | metrics contains the last sampled values for the node metrics. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





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



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| incoming | [int64](#cockroach.server.status.statuspb.NodeStatus-int64) |  | in bytes | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| outgoing | [int64](#cockroach.server.status.statuspb.NodeStatus-int64) |  | in bytes | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| latency | [int64](#cockroach.server.status.statuspb.NodeStatus-int64) |  | in nanoseconds | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## RaftDebug

`GET /_status/raft`

RaftDebug requests internal details about Raft.
TODO DURING REVIEW: Not sure if this needs a reference example

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_ids | [int64](#cockroach.server.serverpb.RaftDebugRequest-int64) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| ranges | [RaftDebugResponse.RangesEntry](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftDebugResponse.RangesEntry) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| errors | [RaftRangeError](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeError) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftDebugResponse.RangesEntry"></a>
#### RaftDebugResponse.RangesEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  |  |
| value | [RaftRangeStatus](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeStatus) |  |  |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeStatus"></a>
#### RaftRangeStatus



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| errors | [RaftRangeError](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeError) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| nodes | [RaftRangeNode](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeNode) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeError"></a>
#### RaftRangeError



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| message | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeNode"></a>
#### RaftRangeNode



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.RaftDebugResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| range | [RangeInfo](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeInfo) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeInfo"></a>
#### RangeInfo



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| span | [PrettySpan](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.PrettySpan) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| raft_state | [RaftState](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| state | [cockroach.kv.kvserver.storagepb.RangeInfo](#cockroach.server.serverpb.RaftDebugResponse-cockroach.kv.kvserver.storagepb.RangeInfo) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| source_node_id | [int32](#cockroach.server.serverpb.RaftDebugResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| source_store_id | [int32](#cockroach.server.serverpb.RaftDebugResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| error_message | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| lease_history | [cockroach.roachpb.Lease](#cockroach.server.serverpb.RaftDebugResponse-cockroach.roachpb.Lease) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| problems | [RangeProblems](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeProblems) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| stats | [RangeStatistics](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeStatistics) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| latches_local | [cockroach.kv.kvserver.storagepb.LatchManagerInfo](#cockroach.server.serverpb.RaftDebugResponse-cockroach.kv.kvserver.storagepb.LatchManagerInfo) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| latches_global | [cockroach.kv.kvserver.storagepb.LatchManagerInfo](#cockroach.server.serverpb.RaftDebugResponse-cockroach.kv.kvserver.storagepb.LatchManagerInfo) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| lease_status | [cockroach.kv.kvserver.storagepb.LeaseStatus](#cockroach.server.serverpb.RaftDebugResponse-cockroach.kv.kvserver.storagepb.LeaseStatus) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| quiescent | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| ticking | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.PrettySpan"></a>
#### PrettySpan



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| start_key | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| end_key | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState"></a>
#### RaftState

RaftState gives internal details about a Raft group's state.
Closely mirrors the upstream definitions in github.com/etcd-io/etcd/raft.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| replica_id | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| hard_state | [raftpb.HardState](#cockroach.server.serverpb.RaftDebugResponse-raftpb.HardState) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| lead | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  | Lead is part of Raft's SoftState. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| state | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  | State is part of Raft's SoftState. It's not an enum because this is primarily for ui consumption and there are issues associated with them. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| applied | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| progress | [RaftState.ProgressEntry](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState.ProgressEntry) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| lead_transferee | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState.ProgressEntry"></a>
#### RaftState.ProgressEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  |  |
| value | [RaftState.Progress](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState.Progress) |  |  |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState.Progress"></a>
#### RaftState.Progress



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| match | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| next | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| state | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| paused | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| pending_snapshot | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeProblems"></a>
#### RangeProblems



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| unavailable | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| leader_not_lease_holder | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| no_raft_leader | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| underreplicated | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| overreplicated | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| no_lease | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| quiescent_equals_ticking | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  | Quiescent ranges do not tick by definition, but we track this in two different ways and suspect that they're getting out of sync. If the replica's quiescent flag doesn't agree with the store's list of replicas that are ticking, warn about it. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| raft_log_too_large | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  | When the raft log is too large, it can be a symptom of other issues. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeStatistics"></a>
#### RangeStatistics



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| queries_per_second | [double](#cockroach.server.serverpb.RaftDebugResponse-double) |  | Note that queries per second will only be known by the leaseholder. All other replicas will report it as 0. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| writes_per_second | [double](#cockroach.server.serverpb.RaftDebugResponse-double) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeError"></a>
#### RaftRangeError



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| message | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## Ranges

`GET /_status/ranges/{node_id}`

Ranges requests internal details about ranges on a given node.
TODO DURING REVIEW: Not sure if this needs a reference example

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.RangesRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| range_ids | [int64](#cockroach.server.serverpb.RangesRequest-int64) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| ranges | [RangeInfo](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeInfo) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeInfo"></a>
#### RangeInfo



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| span | [PrettySpan](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.PrettySpan) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| raft_state | [RaftState](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| state | [cockroach.kv.kvserver.storagepb.RangeInfo](#cockroach.server.serverpb.RangesResponse-cockroach.kv.kvserver.storagepb.RangeInfo) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| source_node_id | [int32](#cockroach.server.serverpb.RangesResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| source_store_id | [int32](#cockroach.server.serverpb.RangesResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| error_message | [string](#cockroach.server.serverpb.RangesResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| lease_history | [cockroach.roachpb.Lease](#cockroach.server.serverpb.RangesResponse-cockroach.roachpb.Lease) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| problems | [RangeProblems](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeProblems) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| stats | [RangeStatistics](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeStatistics) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| latches_local | [cockroach.kv.kvserver.storagepb.LatchManagerInfo](#cockroach.server.serverpb.RangesResponse-cockroach.kv.kvserver.storagepb.LatchManagerInfo) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| latches_global | [cockroach.kv.kvserver.storagepb.LatchManagerInfo](#cockroach.server.serverpb.RangesResponse-cockroach.kv.kvserver.storagepb.LatchManagerInfo) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| lease_status | [cockroach.kv.kvserver.storagepb.LeaseStatus](#cockroach.server.serverpb.RangesResponse-cockroach.kv.kvserver.storagepb.LeaseStatus) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| quiescent | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| ticking | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.PrettySpan"></a>
#### PrettySpan



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| start_key | [string](#cockroach.server.serverpb.RangesResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| end_key | [string](#cockroach.server.serverpb.RangesResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState"></a>
#### RaftState

RaftState gives internal details about a Raft group's state.
Closely mirrors the upstream definitions in github.com/etcd-io/etcd/raft.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| replica_id | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| hard_state | [raftpb.HardState](#cockroach.server.serverpb.RangesResponse-raftpb.HardState) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| lead | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  | Lead is part of Raft's SoftState. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| state | [string](#cockroach.server.serverpb.RangesResponse-string) |  | State is part of Raft's SoftState. It's not an enum because this is primarily for ui consumption and there are issues associated with them. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| applied | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| progress | [RaftState.ProgressEntry](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState.ProgressEntry) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| lead_transferee | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState.ProgressEntry"></a>
#### RaftState.ProgressEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  |  |
| value | [RaftState.Progress](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState.Progress) |  |  |  |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState.Progress"></a>
#### RaftState.Progress



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| match | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| next | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| state | [string](#cockroach.server.serverpb.RangesResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| paused | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| pending_snapshot | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeProblems"></a>
#### RangeProblems



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| unavailable | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| leader_not_lease_holder | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| no_raft_leader | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| underreplicated | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| overreplicated | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| no_lease | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| quiescent_equals_ticking | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  | Quiescent ranges do not tick by definition, but we track this in two different ways and suspect that they're getting out of sync. If the replica's quiescent flag doesn't agree with the store's list of replicas that are ticking, warn about it. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| raft_log_too_large | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  | When the raft log is too large, it can be a symptom of other issues. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeStatistics"></a>
#### RangeStatistics



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| queries_per_second | [double](#cockroach.server.serverpb.RangesResponse-double) |  | Note that queries per second will only be known by the leaseholder. All other replicas will report it as 0. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| writes_per_second | [double](#cockroach.server.serverpb.RangesResponse-double) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## Gossip

`GET /_status/gossip/{node_id}`

Gossip retrieves gossip-level details about a given node.
TODO DURING REVIEW: Not sure if this needs a reference example

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.GossipRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters



## EngineStats

`GET /_status/enginestats/{node_id}`

EngineStats retrieves statistics about a storage engine.

 Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/enginestats/1   
  ->
 {
  "stats": [
    {
      "storeId": 1,
      "tickersAndHistograms": {...},
      "engineType": 2
    }
  ]
}   
 ```
TODO DURING REVIEW: This looks like it was only used by RocksDB and not Pebble. 
Not sure if we still want to keep the example.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.EngineStatsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| stats | [EngineStatsInfo](#cockroach.server.serverpb.EngineStatsResponse-cockroach.server.serverpb.EngineStatsInfo) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.EngineStatsResponse-cockroach.server.serverpb.EngineStatsInfo"></a>
#### EngineStatsInfo



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| store_id | [int32](#cockroach.server.serverpb.EngineStatsResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| tickers_and_histograms | [cockroach.storage.enginepb.TickersAndHistograms](#cockroach.server.serverpb.EngineStatsResponse-cockroach.storage.enginepb.TickersAndHistograms) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| engine_type | [cockroach.storage.enginepb.EngineType](#cockroach.server.serverpb.EngineStatsResponse-cockroach.storage.enginepb.EngineType) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## Allocator

`GET /_status/allocator/node/{node_id}`

Allocator retrieves statistics about the replica allocator.

 Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/allocator/node/1   
  ->
 {
  "dryRuns": [
    {
      "rangeId": "1",
      "events": [
        {
          "time": "2020-12-02T14:53:24.252733Z",
          "message": "kv/kvserver/replicate_queue.go:343 [n1,status] next replica action: consider rebalance"
        },
        {
          "time": "2020-12-02T14:53:24.252789Z",
          "message": "kv/kvserver/replicate_queue.go:849 [n1,status] no suitable rebalance target"
        },
        {
          "time": "2020-12-02T14:53:24.252798Z",
          "message": "kv/kvserver/allocator.go:847 [n1,status] no lease transfer target found"
        }
      ]
    },
    {
      "rangeId": "2",
      "events": [
        {
          "time": "2020-12-02T14:53:24.252825Z",
          "message": "kv/kvserver/replicate_queue.go:343 [n1,status] next replica action: consider rebalance"
        },
        {
          "time": "2020-12-02T14:53:24.252840Z",
          "message": "kv/kvserver/replicate_queue.go:849 [n1,status] no suitable rebalance target"
        },
        {
          "time": "2020-12-02T14:53:24.252847Z",
          "message": "kv/kvserver/allocator.go:847 [n1,status] no lease transfer target found"
        }
      ]
    },
    ...
  ]
}   
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.AllocatorRequest-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| range_ids | [int64](#cockroach.server.serverpb.AllocatorRequest-int64) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| dry_runs | [AllocatorDryRun](#cockroach.server.serverpb.AllocatorResponse-cockroach.server.serverpb.AllocatorDryRun) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.AllocatorResponse-cockroach.server.serverpb.AllocatorDryRun"></a>
#### AllocatorDryRun



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int64](#cockroach.server.serverpb.AllocatorResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| events | [TraceEvent](#cockroach.server.serverpb.AllocatorResponse-cockroach.server.serverpb.TraceEvent) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.AllocatorResponse-cockroach.server.serverpb.TraceEvent"></a>
#### TraceEvent



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| time | [google.protobuf.Timestamp](#cockroach.server.serverpb.AllocatorResponse-google.protobuf.Timestamp) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| message | [string](#cockroach.server.serverpb.AllocatorResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## AllocatorRange

`GET /_status/allocator/range/{range_id}`

AllocatorRange retrieves statistics about the replica allocator given
a specific range.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int64](#cockroach.server.serverpb.AllocatorRangeRequest-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int64](#cockroach.server.serverpb.AllocatorRangeResponse-int64) |  | The NodeID of the store whose dry run is returned. Only the leaseholder for a given range will do an allocator dry run for it. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| dry_run | [AllocatorDryRun](#cockroach.server.serverpb.AllocatorRangeResponse-cockroach.server.serverpb.AllocatorDryRun) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.AllocatorRangeResponse-cockroach.server.serverpb.AllocatorDryRun"></a>
#### AllocatorDryRun



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int64](#cockroach.server.serverpb.AllocatorRangeResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| events | [TraceEvent](#cockroach.server.serverpb.AllocatorRangeResponse-cockroach.server.serverpb.TraceEvent) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.AllocatorRangeResponse-cockroach.server.serverpb.TraceEvent"></a>
#### TraceEvent



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| time | [google.protobuf.Timestamp](#cockroach.server.serverpb.AllocatorRangeResponse-google.protobuf.Timestamp) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| message | [string](#cockroach.server.serverpb.AllocatorRangeResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## ListSessions

`GET /_status/sessions`

ListSessions retrieves the SQL sessions across the entire cluster.

 Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/sessions   
  ->
 {
  "sessions": [
    {
      "nodeId": 1,
      "username": "root",
      "clientAddress": "127.0.0.1:51883",
      "applicationName": "$ cockroach demo",
      "activeQueries": [],
      "start": "2020-12-02T12:49:22.173886Z",
      "lastActiveQuery": "SHOW database",
      "id": "FkznMGk07HgAAAAAAAAAAQ==",
      "allocBytes": "0",
      "maxAllocBytes": "30720",
      "activeTxn": null,
      "lastActiveQueryAnon": "SHOW database"
    }
  ],
  "errors": []
}   
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




Request object for ListSessions and ListLocalSessions.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| username | [string](#cockroach.server.serverpb.ListSessionsRequest-string) |  | Username of the user making this request. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




Response object for ListSessions and ListLocalSessions.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| sessions | [Session](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.Session) | repeated | A list of sessions on this node or cluster. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| errors | [ListSessionsError](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ListSessionsError) | repeated | Any errors that occurred during fan-out calls to other nodes. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.Session"></a>
#### Session

Session represents one SQL session.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | ID of node where this session exists. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| username | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Username of the user for this session. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| client_address | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Connected client's IP address and port. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| application_name | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Application name specified by the client. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| active_queries | [ActiveQuery](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery) | repeated | Queries in progress on this session. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | Timestamp of session's start. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| last_active_query | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | SQL string of the last query executed on this session. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  | ID of the session (uint128 represented as raw bytes). | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | Number of currently allocated bytes in the session memory monitor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| max_alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | High water mark of allocated bytes in the session memory monitor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| active_txn | [TxnInfo](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.TxnInfo) |  | Information about the txn in progress on this session. Nil if the session doesn't currently have a transaction. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| last_active_query_anon | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | The SQL statement fingerprint of the last query executed on this session, compatible with StatementStatisticsKey. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery"></a>
#### ActiveQuery

ActiveQuery represents a query in flight on some Session.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | ID of the query (uint128 presented as a hexadecimal string). | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| txn_id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  | The UUID of the transaction this query is running in. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| sql | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | SQL query string specified by the user. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | Start timestamp of this query. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| is_distributed | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  | True if this query is distributed. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| phase | [ActiveQuery.Phase](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery.Phase) |  | phase stores the current phase of execution for this query. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| progress | [float](#cockroach.server.serverpb.ListSessionsResponse-float) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| sql_anon | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | The SQL statement fingerprint, compatible with StatementStatisticsKey. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.TxnInfo"></a>
#### TxnInfo

TxnInfo represents an in flight user transaction on some Session.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | The start timestamp of the transaction. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| txn_description | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | txn_description is a text description of the underlying kv.Txn, intended for troubleshooting purposes. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| num_statements_executed | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_statements_executed is the number of statements that were executed so far on this transaction. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| num_retries | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_retries is the number of times that this transaction was retried. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| num_auto_retries | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_retries is the number of times that this transaction was automatically retried by the SQL executor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| deadline | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | The deadline by which the transaction must be committed. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| implicit | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  | implicit is true if this transaction was an implicit SQL transaction. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | Number of currently allocated bytes in the txn memory monitor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| max_alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | High water mark of allocated bytes in the txn memory monitor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| read_only | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| is_historical | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| priority | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ListSessionsError"></a>
#### ListSessionsError

An error wrapper object for ListSessionsResponse.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | ID of node that was being contacted when this error occurred | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| message | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Error message. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## ListLocalSessions

`GET /_status/local_sessions`

ListLocalSessions retrieves the SQL sessions on this node.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




Request object for ListSessions and ListLocalSessions.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| username | [string](#cockroach.server.serverpb.ListSessionsRequest-string) |  | Username of the user making this request. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




Response object for ListSessions and ListLocalSessions.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| sessions | [Session](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.Session) | repeated | A list of sessions on this node or cluster. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| errors | [ListSessionsError](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ListSessionsError) | repeated | Any errors that occurred during fan-out calls to other nodes. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.Session"></a>
#### Session

Session represents one SQL session.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | ID of node where this session exists. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| username | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Username of the user for this session. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| client_address | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Connected client's IP address and port. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| application_name | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Application name specified by the client. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| active_queries | [ActiveQuery](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery) | repeated | Queries in progress on this session. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | Timestamp of session's start. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| last_active_query | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | SQL string of the last query executed on this session. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  | ID of the session (uint128 represented as raw bytes). | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | Number of currently allocated bytes in the session memory monitor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| max_alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | High water mark of allocated bytes in the session memory monitor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| active_txn | [TxnInfo](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.TxnInfo) |  | Information about the txn in progress on this session. Nil if the session doesn't currently have a transaction. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| last_active_query_anon | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | The SQL statement fingerprint of the last query executed on this session, compatible with StatementStatisticsKey. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery"></a>
#### ActiveQuery

ActiveQuery represents a query in flight on some Session.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | ID of the query (uint128 presented as a hexadecimal string). | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| txn_id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  | The UUID of the transaction this query is running in. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| sql | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | SQL query string specified by the user. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | Start timestamp of this query. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| is_distributed | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  | True if this query is distributed. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| phase | [ActiveQuery.Phase](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery.Phase) |  | phase stores the current phase of execution for this query. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| progress | [float](#cockroach.server.serverpb.ListSessionsResponse-float) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| sql_anon | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | The SQL statement fingerprint, compatible with StatementStatisticsKey. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.TxnInfo"></a>
#### TxnInfo

TxnInfo represents an in flight user transaction on some Session.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | The start timestamp of the transaction. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| txn_description | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | txn_description is a text description of the underlying kv.Txn, intended for troubleshooting purposes. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| num_statements_executed | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_statements_executed is the number of statements that were executed so far on this transaction. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| num_retries | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_retries is the number of times that this transaction was retried. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| num_auto_retries | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_retries is the number of times that this transaction was automatically retried by the SQL executor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| deadline | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | The deadline by which the transaction must be committed. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| implicit | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  | implicit is true if this transaction was an implicit SQL transaction. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | Number of currently allocated bytes in the txn memory monitor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| max_alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | High water mark of allocated bytes in the txn memory monitor. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| read_only | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| is_historical | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| priority | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ListSessionsError"></a>
#### ListSessionsError

An error wrapper object for ListSessionsResponse.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | ID of node that was being contacted when this error occurred | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| message | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Error message. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## CancelQuery

`POST /_status/cancel_query/{node_id}`

CancelQuery cancels a SQL query given its ID.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




Request object for issing a query cancel request.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.CancelQueryRequest-string) |  | ID of gateway node for the query to be canceled.<br><br>TODO(itsbilal): use [(gogoproto.customname) = "NodeID"] below. Need to figure out how to teach grpc-gateway about custom names.<br><br>node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| query_id | [string](#cockroach.server.serverpb.CancelQueryRequest-string) |  | ID of query to be canceled (converted to string). | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| username | [string](#cockroach.server.serverpb.CancelQueryRequest-string) |  | Username of the user making this cancellation request. This may be omitted if the user is the same as the one issuing the CancelQueryRequest. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




Response returned by target query's gateway node.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| canceled | [bool](#cockroach.server.serverpb.CancelQueryResponse-bool) |  | Whether the cancellation request succeeded and the query was canceled. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| error | [string](#cockroach.server.serverpb.CancelQueryResponse-string) |  | Error message (accompanied with canceled = false). | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## CancelSession

`POST /_status/cancel_session/{node_id}`

CancelSessions forcefully terminates a SQL session given its ID.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.CancelSessionRequest-string) |  | TODO(abhimadan): use [(gogoproto.customname) = "NodeID"] below. Need to figure out how to teach grpc-gateway about custom names.<br><br>node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| session_id | [bytes](#cockroach.server.serverpb.CancelSessionRequest-bytes) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| username | [string](#cockroach.server.serverpb.CancelSessionRequest-string) |  | Username of the user making this cancellation request. This may be omitted if the user is the same as the one issuing the CancelSessionRequest. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| canceled | [bool](#cockroach.server.serverpb.CancelSessionResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| error | [string](#cockroach.server.serverpb.CancelSessionResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## SpanStats

`POST /_status/span`

SpanStats accepts a key span and node ID, and returns a set of stats
summed from all ranges on the stores on that node which contain keys
in that span. This is designed to compute stats specific to a SQL table:
it will be called with the highest/lowest key for a SQL table, and return
information about the resources on a node used by that table.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.SpanStatsRequest-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| start_key | [bytes](#cockroach.server.serverpb.SpanStatsRequest-bytes) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| end_key | [bytes](#cockroach.server.serverpb.SpanStatsRequest-bytes) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_count | [int32](#cockroach.server.serverpb.SpanStatsResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| approximate_disk_bytes | [uint64](#cockroach.server.serverpb.SpanStatsResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| total_stats | [cockroach.storage.enginepb.MVCCStats](#cockroach.server.serverpb.SpanStatsResponse-cockroach.storage.enginepb.MVCCStats) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## Stacks

`GET /_status/stacks/{node_id}`

Stacks retrieves the stack traces of all goroutines on a given node.

 Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/stacks/1   
  ->
 {
  "data": "<payload>"
}   
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.StacksRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| type | [StacksType](#cockroach.server.serverpb.StacksRequest-cockroach.server.serverpb.StacksType) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| data | [bytes](#cockroach.server.serverpb.JSONResponse-bytes) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## Profile

`GET /_status/profile/{node_id}`

Profile retrieves a CPU profile on a given node.

 Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/profile/1   
  ->
 {
  "data": "<payload>"
}   
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.ProfileRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| type | [ProfileRequest.Type](#cockroach.server.serverpb.ProfileRequest-cockroach.server.serverpb.ProfileRequest.Type) |  | The type of profile to retrieve. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| seconds | [int32](#cockroach.server.serverpb.ProfileRequest-int32) |  | applies only to Type=CPU, defaults to 30 | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| data | [bytes](#cockroach.server.serverpb.JSONResponse-bytes) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## Metrics

`GET /_status/metrics/{node_id}`

Metrics retrieves the node metrics for a given node.

Note: this is a “reserved” API and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

 Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/metrics/1   
  ->
 {
  "data": "<payload>"
}   
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.MetricsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| data | [bytes](#cockroach.server.serverpb.JSONResponse-bytes) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## GetFiles

`GET /_status/files/{node_id}`

GetFiles retrieves heap or goroutine dump files from a given node.

 Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/files/1   
  ->
 {
 "files": [{
  "name": "...",
  "file_size": ...,
  "contents": "..."    
}]
}   
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.GetFilesRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| list_only | [bool](#cockroach.server.serverpb.GetFilesRequest-bool) |  | If list_only is true then the contents of the files will not be populated in the response. Only filenames and sizes will be returned. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| type | [FileType](#cockroach.server.serverpb.GetFilesRequest-cockroach.server.serverpb.FileType) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| patterns | [string](#cockroach.server.serverpb.GetFilesRequest-string) | repeated | Each pattern given is matched with Files of the above type in the node using filepath.Glob(). The patterns only match to filenames and so path separators cannot be used. Example: * will match all files of requested type. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| files | [File](#cockroach.server.serverpb.GetFilesResponse-cockroach.server.serverpb.File) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.GetFilesResponse-cockroach.server.serverpb.File"></a>
#### File



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| name | [string](#cockroach.server.serverpb.GetFilesResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| file_size | [int64](#cockroach.server.serverpb.GetFilesResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| contents | [bytes](#cockroach.server.serverpb.GetFilesResponse-bytes) |  | Contents may not be populated if only a list of Files are requested. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## LogFilesList

`GET /_status/logfiles/{node_id}`

LogFilesList retrieves a list of log files on a given node.

 Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/logfiles/1   
  ->
 {
 "files": [{
  "name": "...",
  "file_size": ...,
  "contents": "..."    
}]
}   
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.LogFilesListRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| files | [cockroach.util.log.FileInfo](#cockroach.server.serverpb.LogFilesListResponse-cockroach.util.log.FileInfo) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## LogFile

`GET /_status/logfiles/{node_id}/{file}`

LogFile retrieves a given log file.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.LogFileRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| file | [string](#cockroach.server.serverpb.LogFileRequest-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| redact | [bool](#cockroach.server.serverpb.LogFileRequest-bool) |  | redact, if true, requests redaction of sensitive data away from the retrieved log entries. Only admin users can send a request with redact = false. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| keep_redactable | [bool](#cockroach.server.serverpb.LogFileRequest-bool) |  | keep_redactable, if true, requests that retrieved entries preserve the redaction markers if any were present in the log files. If false, redaction markers are stripped away. Note that redact = false && redactable = false implies "flat" entries with all sensitive information enclosed and no markers; this is suitable for backward-compatibility with RPC clients from prior the introduction of redactable logs. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| entries | [cockroach.util.log.Entry](#cockroach.server.serverpb.LogEntriesResponse-cockroach.util.log.Entry) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## Logs

`GET /_status/logs/{node_id}`

Logs retrieves individual log entries.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.LogsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| level | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| start_time | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| end_time | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| max | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| pattern | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| redact | [bool](#cockroach.server.serverpb.LogsRequest-bool) |  | redact, if true, requests redaction of sensitive data away from the retrieved log entries. Only admin users can send a request with redact = false. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| keep_redactable | [bool](#cockroach.server.serverpb.LogsRequest-bool) |  | keep_redactable, if true, requests that retrieved entries preserve the redaction markers if any were present in the log files. If false, redaction markers are stripped away. Note that redact = false && redactable = false implies "flat" entries with all sensitive information enclosed and no markers; this is suitable for backward-compatibility with RPC clients from prior the introduction of redactable logs. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| entries | [cockroach.util.log.Entry](#cockroach.server.serverpb.LogEntriesResponse-cockroach.util.log.Entry) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## ProblemRanges

`GET /_status/problemranges`

ProblemRanges retrieves the list of “problem ranges”.

 Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/problemranges   
  ->
 {
  "nodeId": 1,
  "problemsByNodeId": {
    "1": {
      "errorMessage": "",
      "unavailableRangeIds": [],
      "raftLeaderNotLeaseHolderRangeIds": [],
      "noRaftLeaderRangeIds": [],
      "noLeaseRangeIds": [],
      "underreplicatedRangeIds": [],
      "overreplicatedRangeIds": [],
      "quiescentEqualsTickingRangeIds": [],
      "raftLogTooLargeRangeIds": []
    }
  }
}   
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.ProblemRangesRequest-string) |  | If left empty, problem ranges for all nodes/stores will be returned. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.ProblemRangesResponse-int32) |  | NodeID is the node that submitted all the requests. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| problems_by_node_id | [ProblemRangesResponse.ProblemsByNodeIdEntry](#cockroach.server.serverpb.ProblemRangesResponse-cockroach.server.serverpb.ProblemRangesResponse.ProblemsByNodeIdEntry) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.ProblemRangesResponse-cockroach.server.serverpb.ProblemRangesResponse.ProblemsByNodeIdEntry"></a>
#### ProblemRangesResponse.ProblemsByNodeIdEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.ProblemRangesResponse-int32) |  |  |  |
| value | [ProblemRangesResponse.NodeProblems](#cockroach.server.serverpb.ProblemRangesResponse-cockroach.server.serverpb.ProblemRangesResponse.NodeProblems) |  |  |  |





<a name="cockroach.server.serverpb.ProblemRangesResponse-cockroach.server.serverpb.ProblemRangesResponse.NodeProblems"></a>
#### ProblemRangesResponse.NodeProblems



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| error_message | [string](#cockroach.server.serverpb.ProblemRangesResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| unavailable_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| raft_leader_not_lease_holder_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| no_raft_leader_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| no_lease_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| underreplicated_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| overreplicated_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| quiescent_equals_ticking_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| raft_log_too_large_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## HotRanges

`GET /_status/hotranges`

HotRanges retrieves a list of ranges ordered by the amount of traffic being 
received.

 Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/hotranges   
  ->
 {
  "nodeId": 1,
  "hotRangesByNodeId": {
    "1": {
      "errorMessage": "",
      "stores": [
        {
          "storeId": 1,
          "hotRanges": [
            {
              "desc": {
                "rangeId": "6",
                "startKey": "iA==",
                "endKey": "kw==",
                "internalReplicas": [
                  {
                    "nodeId": 1,
                    "storeId": 1,
                    "replicaId": 1,
                    "type": null
                  }
                ],
                "nextReplicaId": 2,
                "generation": "0",
                "deprecatedGenerationComparable": null,
                "stickyBit": null
              },
              "queriesPerSecond": 5.36106511320822
            },
            ...
          ]
        }
      ]
    }
  }
}   
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




HotRangesRequest queries one or more cluster nodes for a list
of ranges currently considered “hot” by the node(s).

Note: this is an “alpha” API payload. It is subject to change without
advance notice in a subsequent release.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.HotRangesRequest-string) |  | NodeID indicates which node to query for a hot range report. It is posssible to populate any node ID; if the node receiving the request is not the target node, it will forward the request to the target node.<br><br>If left empty, the request is forwarded to every node in the cluster. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |







#### Response Parameters




HotRangesResponse is the payload produced in response
to a HotRangesRequest.

Note: this is an “alpha” API payload. It is subject to change without
advance notice in a subsequent release.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  | NodeID is the node that received the HotRangesRequest and forwarded requests to the selected target node(s). | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| hot_ranges_by_node_id | [HotRangesResponse.HotRangesByNodeIdEntry](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRangesByNodeIdEntry) | repeated | HotRangesByNodeID contains a hot range report for each selected target node ID in the HotRangesRequest. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |






<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRangesByNodeIdEntry"></a>
#### HotRangesResponse.HotRangesByNodeIdEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  |  |  |
| value | [HotRangesResponse.NodeResponse](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.NodeResponse) |  |  |  |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.NodeResponse"></a>
#### HotRangesResponse.NodeResponse

NodeResponse is a hot range report for a single target node.

Note: this is an “alpha” API payload. It is subject to change without
advance notice in a subsequent release.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| error_message | [string](#cockroach.server.serverpb.HotRangesResponse-string) |  | ErrorMessage is set to a non-empty string if this target node was unable to produce a hot range report.<br><br>The contents of this string indicates the cause of the failure. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| stores | [HotRangesResponse.StoreResponse](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.StoreResponse) | repeated | Stores contains the hot ranges report if no error was encountered. There is one part to the report for each store in the target node. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.StoreResponse"></a>
#### HotRangesResponse.StoreResponse

StoreResponse contains the part of a hot ranges report that
pertains to a single store on a target node.

Note: this is an “alpha” API payload. It is subject to change without
advance notice in a subsequent release.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| store_id | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  | StoreID identifies the store for which the report was produced. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| hot_ranges | [HotRangesResponse.HotRange](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRange) | repeated | HotRanges is the hot ranges report for this store on the target node. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRange"></a>
#### HotRangesResponse.HotRange

HotRange is a hot range report for a single store on one of the
target node(s) selected in a HotRangesRequest.

Note: this is an “alpha” API payload. It is subject to change without
advance notice in a subsequent release.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.RangeDescriptor](#cockroach.server.serverpb.HotRangesResponse-cockroach.roachpb.RangeDescriptor) |  | Desc is the descriptor of the range for which the report was produced.<br><br>TODO(knz): This field should be removed. See: https://github.com/cockroachdb/cockroach/issues/53212 | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| queries_per_second | [double](#cockroach.server.serverpb.HotRangesResponse-double) |  | QueriesPerSecond is the recent number of queries per second on this range. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |






## Range

`GET /_status/range/{range_id}`

TODO DURING REVIEW: Not sure if this needs a reference example

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int64](#cockroach.server.serverpb.RangeRequest-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.RangeResponse-int32) |  | NodeID is the node that submitted all the requests. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| range_id | [int64](#cockroach.server.serverpb.RangeResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| responses_by_node_id | [RangeResponse.ResponsesByNodeIdEntry](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeResponse.ResponsesByNodeIdEntry) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeResponse.ResponsesByNodeIdEntry"></a>
#### RangeResponse.ResponsesByNodeIdEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.RangeResponse-int32) |  |  |  |
| value | [RangeResponse.NodeResponse](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeResponse.NodeResponse) |  |  |  |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeResponse.NodeResponse"></a>
#### RangeResponse.NodeResponse



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| response | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| error_message | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| infos | [RangeInfo](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeInfo) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeInfo"></a>
#### RangeInfo



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| span | [PrettySpan](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.PrettySpan) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| raft_state | [RaftState](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| state | [cockroach.kv.kvserver.storagepb.RangeInfo](#cockroach.server.serverpb.RangeResponse-cockroach.kv.kvserver.storagepb.RangeInfo) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| source_node_id | [int32](#cockroach.server.serverpb.RangeResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| source_store_id | [int32](#cockroach.server.serverpb.RangeResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| error_message | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| lease_history | [cockroach.roachpb.Lease](#cockroach.server.serverpb.RangeResponse-cockroach.roachpb.Lease) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| problems | [RangeProblems](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeProblems) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| stats | [RangeStatistics](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeStatistics) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| latches_local | [cockroach.kv.kvserver.storagepb.LatchManagerInfo](#cockroach.server.serverpb.RangeResponse-cockroach.kv.kvserver.storagepb.LatchManagerInfo) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| latches_global | [cockroach.kv.kvserver.storagepb.LatchManagerInfo](#cockroach.server.serverpb.RangeResponse-cockroach.kv.kvserver.storagepb.LatchManagerInfo) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| lease_status | [cockroach.kv.kvserver.storagepb.LeaseStatus](#cockroach.server.serverpb.RangeResponse-cockroach.kv.kvserver.storagepb.LeaseStatus) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| quiescent | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| ticking | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.PrettySpan"></a>
#### PrettySpan



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| start_key | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| end_key | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState"></a>
#### RaftState

RaftState gives internal details about a Raft group's state.
Closely mirrors the upstream definitions in github.com/etcd-io/etcd/raft.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| replica_id | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| hard_state | [raftpb.HardState](#cockroach.server.serverpb.RangeResponse-raftpb.HardState) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| lead | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  | Lead is part of Raft's SoftState. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| state | [string](#cockroach.server.serverpb.RangeResponse-string) |  | State is part of Raft's SoftState. It's not an enum because this is primarily for ui consumption and there are issues associated with them. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| applied | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| progress | [RaftState.ProgressEntry](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState.ProgressEntry) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| lead_transferee | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState.ProgressEntry"></a>
#### RaftState.ProgressEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  |  |
| value | [RaftState.Progress](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState.Progress) |  |  |  |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState.Progress"></a>
#### RaftState.Progress



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| match | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| next | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| state | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| paused | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| pending_snapshot | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeProblems"></a>
#### RangeProblems



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| unavailable | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| leader_not_lease_holder | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| no_raft_leader | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| underreplicated | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| overreplicated | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| no_lease | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| quiescent_equals_ticking | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  | Quiescent ranges do not tick by definition, but we track this in two different ways and suspect that they're getting out of sync. If the replica's quiescent flag doesn't agree with the store's list of replicas that are ticking, warn about it. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| raft_log_too_large | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  | When the raft log is too large, it can be a symptom of other issues. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeStatistics"></a>
#### RangeStatistics



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| queries_per_second | [double](#cockroach.server.serverpb.RangeResponse-double) |  | Note that queries per second will only be known by the leaseholder. All other replicas will report it as 0. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| writes_per_second | [double](#cockroach.server.serverpb.RangeResponse-double) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## Diagnostics

`GET /_status/diagnostics/{node_id}`

TODO DURING REVIEW: Do we want to redact this response even further? 
 Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/diagnostics/1   
  ->
 {
  "node": {
    "nodeId": 1,
    "bytes": "21565824",
    "keyCount": "5668",
    "rangeCount": "65",
    "locality": {
      "tiers": [
        {
          "key": "5d64eba5",
          "value": "67b07925"
        },
        {
          "key": "d4f35d56",
          "value": "d5a474fc"
        }
      ]
    },
    "hardware": {
      "virtualization": "",
      "cpu": {...},
      "mem": {
        "total": "34359738368",
        "available": "15584899072"
      },
      "loadavg15": 4.7993164,
      "provider": "",
      "instanceClass": ""
    },
    "os": {
      "family": "Standalone Workstation",
      "platform": "darwin",
      "version": "10.15.7"
    },
    "build": {...},
    "uptime": "7443",
    "licenseType": "Evaluation",
    "topology": {
      "provider": "",
      "region": ""
    }
  },
  "stores": [
    {
      "nodeId": 1,
      "storeId": 1,
      "bytes": "21565824",
      "keyCount": "5668",
      "rangeCount": "65",
      "capacity": "536870912",
      "available": "536870912",
      "used": "0",
      "encryptionAlgorithm": "0"
    }
  ],
  "schema": [
    {
      "name": "_",
      "id": 53,
      "version": 7,
      "modificationTime": {
        "wallTime": "1606913362141165000",
        "logical": 0
      },
      "drainingNames": [],
      "parentId": 52,
      "unexposedParentSchemaId": 29,
      "columns": [
        {
          "name": "_",
          "id": 1,
          "type": {},
          "nullable": false,
          "defaultExpr": null,
          "hidden": false,
          "usesSequenceIds": [],
          "ownsSequenceIds": [],
          "computeExpr": null,
          "pgAttributeNum": 0,
          "alterColumnTypeInProgress": false,
          "systemColumnKind": 0
        },
        ...
      ],
      "nextColumnId": 6,
      "families": [
        {
          "name": "_",
          "id": 0,
          "columnNames": [
            "_",
            "_",
            "_",
            "_",
            "_"
          ],
          "columnIds": [
            1,
            2,
            3,
            4,
            5
          ],
          "defaultColumnId": 0
        }
      ],
      "nextFamilyId": 1,
      "primaryIndex": {
        "name": "_",
        "id": 1,
        "unique": true,
        "version": 1,
        "columnNames": [
          "_",
          "_"
        ],
        "columnDirections": [
          0,
          0
        ],
        "storeColumnNames": [],
        "columnIds": [
          2,
          1
        ],
        "extraColumnIds": [],
        "storeColumnIds": [],
        "compositeColumnIds": [],
        "foreignKey": {
          "table": 0,
          "index": 0,
          "name": "",
          "validity": 0,
          "sharedPrefixLen": 0,
          "onDelete": "NO_ACTION",
          "onUpdate": "NO_ACTION",
          "match": "SIMPLE"
        },
        "referencedBy": [],
        "interleave": {
          "ancestors": []
        },
        "interleavedBy": [],
        "partitioning": {
          "numColumns": 0,
          "list": [],
          "range": []
        },
        "type": 0,
        "createdExplicitly": false,
        "encodingType": 0,
        "sharded": {
          "isSharded": false,
          "name": "",
          "shardBuckets": 0,
          "columnNames": []
        },
        "disabled": false,
        "geoConfig": {
          "s2Geography": null,
          "s2Geometry": null
        },
        "predicate": ""
      },
      "indexes": [],
      "nextIndexId": 2,
      "privileges": {
        "users": [
          {
            "user": "_",
            "privileges": 2
          },
          {
            "user": "_",
            "privileges": 2
          }
        ],
        "owner": "_",
        "version": 1
      },
      "mutations": [],
      "lease": null,
      "nextMutationId": 1,
      "formatVersion": 3,
      "state": 0,
      "offlineReason": "",
      "checks": [],
      "viewQuery": "",
      "isMaterializedView": false,
      "dependsOn": [],
      "dependedOnBy": [],
      "mutationJobs": [],
      "sequenceOpts": null,
      "dropTime": "0",
      "replacementOf": {
        "id": 0,
        "time": {
          "wallTime": "0",
          "logical": 0
        }
      },
      "auditMode": 0,
      "dropJobId": "0",
      "gcMutations": [],
      "createQuery": "",
      "createAsOfTime": {
        "wallTime": "1606913361488922000",
        "logical": 0
      },
      "outboundFks": [],
      "inboundFks": [
        {
          "originTableId": 54,
          "originColumnIds": [
            2,
            4
          ],
          "referencedColumnIds": [
            2,
            1
          ],
          "referencedTableId": 53,
          "name": "_",
          "validity": 2,
          "onDelete": "NO_ACTION",
          "onUpdate": "NO_ACTION",
          "match": "SIMPLE"
        },
        {
          "originTableId": 55,
          "originColumnIds": [
            2,
            4
          ],
          "referencedColumnIds": [
            2,
            1
          ],
          "referencedTableId": 53,
          "name": "_",
          "validity": 2,
          "onDelete": "NO_ACTION",
          "onUpdate": "NO_ACTION",
          "match": "SIMPLE"
        },
        {
          "originTableId": 58,
          "originColumnIds": [
            1,
            2
          ],
          "referencedColumnIds": [
            2,
            1
          ],
          "referencedTableId": 53,
          "name": "_",
          "validity": 2,
          "onDelete": "NO_ACTION",
          "onUpdate": "NO_ACTION",
          "match": "SIMPLE"
        }
      ],
      "temporary": false
    },
    ...
  ],
  "sqlStats": [],
  "alteredSettings": {
    "cluster.organization": "<redacted>",
    "cluster.secret": "<redacted>",
    "diagnostics.reporting.enabled": "true",
    "enterprise.license": "<redacted>",
    "version": "20.2-1"
  },
  "zoneConfigs": {
    "0": {
      "rangeMinBytes": "134217728",
      "rangeMaxBytes": "536870912",
      "gc": {
        "ttlSeconds": 90000
      },
      "numReplicas": 1,
      "constraints": [],
      "inheritedConstraints": false,
      "leasePreferences": [],
      "inheritedLeasePreferences": false,
      "subzones": [],
      "subzoneSpans": []
    },
    ...
  },
  "featureUsage": {...},
  "legacyUnimplementedErrors": {},
  "legacyErrorCounts": {}
}   
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




DiagnosticsRequest requests a diagnostics report.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.DiagnosticsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters



## Stores

`GET /_status/stores/{node_id}`

Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/stores/1   
  ->
 {
  "stores": [
    {
      "storeId": 1,
      "encryptionStatus": null,
      "totalFiles": "0",
      "totalBytes": "0",
      "activeKeyFiles": "0",
      "activeKeyBytes": "0"
    }
  ]
}   
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.StoresRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| stores | [StoreDetails](#cockroach.server.serverpb.StoresResponse-cockroach.server.serverpb.StoreDetails) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.StoresResponse-cockroach.server.serverpb.StoreDetails"></a>
#### StoreDetails



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| store_id | [int32](#cockroach.server.serverpb.StoresResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| encryption_status | [bytes](#cockroach.server.serverpb.StoresResponse-bytes) |  | encryption_status is a serialized ccl/storageccl/engineccl/enginepbccl/stats.go::EncryptionStatus protobuf. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| total_files | [uint64](#cockroach.server.serverpb.StoresResponse-uint64) |  | Basic file stats when encryption is enabled. Total files/bytes. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| total_bytes | [uint64](#cockroach.server.serverpb.StoresResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| active_key_files | [uint64](#cockroach.server.serverpb.StoresResponse-uint64) |  | Files/bytes using the active data key. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| active_key_bytes | [uint64](#cockroach.server.serverpb.StoresResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## Statements

`GET /_status/statements`



Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.StatementsRequest-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| statements | [StatementsResponse.CollectedStatementStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.CollectedStatementStatistics) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| last_reset | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementsResponse-google.protobuf.Timestamp) |  | Timestamp of the last stats reset. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| internal_app_name_prefix | [string](#cockroach.server.serverpb.StatementsResponse-string) |  | If set and non-empty, indicates the prefix to application_name used for statements/queries issued internally by CockroachDB. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| transactions | [StatementsResponse.ExtendedCollectedTransactionStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedCollectedTransactionStatistics) | repeated | Transactions is transaction-level statistics for the collection of statements in this response. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.CollectedStatementStatistics"></a>
#### StatementsResponse.CollectedStatementStatistics



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [StatementsResponse.ExtendedStatementStatisticsKey](#cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedStatementStatisticsKey) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| id | [uint64](#cockroach.server.serverpb.StatementsResponse-uint64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| stats | [cockroach.sql.StatementStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.sql.StatementStatistics) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedStatementStatisticsKey"></a>
#### StatementsResponse.ExtendedStatementStatisticsKey



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key_data | [cockroach.sql.StatementStatisticsKey](#cockroach.server.serverpb.StatementsResponse-cockroach.sql.StatementStatisticsKey) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| node_id | [int32](#cockroach.server.serverpb.StatementsResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedCollectedTransactionStatistics"></a>
#### StatementsResponse.ExtendedCollectedTransactionStatistics



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| stats_data | [cockroach.sql.CollectedTransactionStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.sql.CollectedTransactionStatistics) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| node_id | [int32](#cockroach.server.serverpb.StatementsResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## CreateStatementDiagnosticsReport

`POST /_status/stmtdiagreports`



Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| statement_fingerprint | [string](#cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| report | [StatementDiagnosticsReport](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-cockroach.server.serverpb.StatementDiagnosticsReport) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-cockroach.server.serverpb.StatementDiagnosticsReport"></a>
#### StatementDiagnosticsReport



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [int64](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| completed | [bool](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| statement_fingerprint | [string](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| statement_diagnostics_id | [int64](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| requested_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-google.protobuf.Timestamp) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## StatementDiagnosticsRequests

`GET /_status/stmtdiagreports`



Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.








#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| reports | [StatementDiagnosticsReport](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-cockroach.server.serverpb.StatementDiagnosticsReport) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.StatementDiagnosticsReportsResponse-cockroach.server.serverpb.StatementDiagnosticsReport"></a>
#### StatementDiagnosticsReport



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [int64](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| completed | [bool](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| statement_fingerprint | [string](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| statement_diagnostics_id | [int64](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| requested_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-google.protobuf.Timestamp) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## StatementDiagnostics

`GET /_status/stmtdiag/{statement_diagnostics_id}`



Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| statement_diagnostics_id | [int64](#cockroach.server.serverpb.StatementDiagnosticsRequest-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| diagnostics | [StatementDiagnostics](#cockroach.server.serverpb.StatementDiagnosticsResponse-cockroach.server.serverpb.StatementDiagnostics) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.StatementDiagnosticsResponse-cockroach.server.serverpb.StatementDiagnostics"></a>
#### StatementDiagnostics



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [int64](#cockroach.server.serverpb.StatementDiagnosticsResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| statement_fingerprint | [string](#cockroach.server.serverpb.StatementDiagnosticsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| collected_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementDiagnosticsResponse-google.protobuf.Timestamp) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| trace | [string](#cockroach.server.serverpb.StatementDiagnosticsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## JobRegistryStatus

`GET /_status/job_registry/{node_id}`

Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/job_registry/1   
  ->
 {
  "nodeId": 1,
  "runningJobs": []
}   
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.JobRegistryStatusRequest-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.JobRegistryStatusResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| running_jobs | [JobRegistryStatusResponse.Job](#cockroach.server.serverpb.JobRegistryStatusResponse-cockroach.server.serverpb.JobRegistryStatusResponse.Job) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.JobRegistryStatusResponse-cockroach.server.serverpb.JobRegistryStatusResponse.Job"></a>
#### JobRegistryStatusResponse.Job



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [int64](#cockroach.server.serverpb.JobRegistryStatusResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## JobStatus

`GET /_status/job/{job_id}`

Reference example:
 ``` 
 curl http://127.0.0.1:51875/_status/job/612247017297477633   
  ->
 {
  "job": {
    "id": "612247017297477633",
    "progress": {
      "fractionCompleted": 1,
      "modifiedMicros": "1606913361839096",
      "runningStatus": "",
      "schemaChange": {}
    },
    "payload": {
      "description": "ALTER TABLE ...",
      "statement": "",
      "username": "root",
      "startedMicros": "1606913361836868",
      "finishedMicros": "1606913361839914",
      "descriptorIds": [
        53
      ],
      "error": "",
      "resumeErrors": [],
      "cleanupErrors": [],
      "finalResumeError": null,
      "lease": null,
      "noncancelable": false,
      "schemaChange": {
        "resumeSpanList": [],
        "droppedTables": [],
        "droppedTypes": [],
        "droppedSchemas": [],
        "droppedDatabaseId": 0,
        "descId": 53,
        "tableMutationId": 0,
        "formatVersion": 2
      }
    }
  }
}   
 ```

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| job_id | [int64](#cockroach.server.serverpb.JobStatusRequest-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| job | [cockroach.sql.jobs.jobspb.Job](#cockroach.server.serverpb.JobStatusResponse-cockroach.sql.jobs.jobspb.Job) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## Users

`GET /_admin/v1/users`

URL: /_admin/v1/users

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




UsersRequest requests a list of users.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.








#### Response Parameters




UsersResponse returns a list of users.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| users | [UsersResponse.User](#cockroach.server.serverpb.UsersResponse-cockroach.server.serverpb.UsersResponse.User) | repeated | usernames is a list of users for the CockroachDB cluster. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.UsersResponse-cockroach.server.serverpb.UsersResponse.User"></a>
#### UsersResponse.User

User is a CockroachDB user.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| username | [string](#cockroach.server.serverpb.UsersResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## Databases

`GET /_admin/v1/databases`

URL: /_admin/v1/databases

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




DatabasesRequest requests a list of databases.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.








#### Response Parameters




DatabasesResponse contains a list of databases.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| databases | [string](#cockroach.server.serverpb.DatabasesResponse-string) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## DatabaseDetails

`GET /_admin/v1/databases/{database}`

Example URL: /_admin/v1/databases/system

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




DatabaseDetailsRequest requests detailed information about the specified
database

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| database | [string](#cockroach.server.serverpb.DatabaseDetailsRequest-string) |  | database is the name of the database we are querying. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




DatabaseDetailsResponse contains grant information and table names for a
database.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| grants | [DatabaseDetailsResponse.Grant](#cockroach.server.serverpb.DatabaseDetailsResponse-cockroach.server.serverpb.DatabaseDetailsResponse.Grant) | repeated | grants are the results of SHOW GRANTS for this database. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| table_names | [string](#cockroach.server.serverpb.DatabaseDetailsResponse-string) | repeated | table_names contains the names of all tables in this database. Note that all responses will be schema-qualified (schema.table) and that every schema or table that contains a "sql unsafe character" such as uppercase letters or dots will be surrounded with double quotes, such as "naughty schema".table. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| descriptor_id | [int64](#cockroach.server.serverpb.DatabaseDetailsResponse-int64) |  | descriptor_id is an identifier used to uniquely identify this database. It can be used to find events pertaining to this database by filtering on the 'target_id' field of events. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| zone_config | [cockroach.config.zonepb.ZoneConfig](#cockroach.server.serverpb.DatabaseDetailsResponse-cockroach.config.zonepb.ZoneConfig) |  | The zone configuration in effect for this database. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| zone_config_level | [ZoneConfigurationLevel](#cockroach.server.serverpb.DatabaseDetailsResponse-cockroach.server.serverpb.ZoneConfigurationLevel) |  | The level at which this object's zone configuration is set. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.DatabaseDetailsResponse-cockroach.server.serverpb.DatabaseDetailsResponse.Grant"></a>
#### DatabaseDetailsResponse.Grant



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| user | [string](#cockroach.server.serverpb.DatabaseDetailsResponse-string) |  | user is the user that this grant applies to. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| privileges | [string](#cockroach.server.serverpb.DatabaseDetailsResponse-string) | repeated | privileges are the abilities this grant gives to the user. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## TableDetails

`GET /_admin/v1/databases/{database}/tables/{table}`

Example URL: /_admin/v1/databases/system/tables/ui

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




TableDetailsRequest is a request for detailed information about a table.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| database | [string](#cockroach.server.serverpb.TableDetailsRequest-string) |  | database is the database that contains the table we're interested in. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| table | [string](#cockroach.server.serverpb.TableDetailsRequest-string) |  | table is the name of the table that we're querying. Table may be schema-qualified (schema.table) and each name component that contains sql unsafe characters such as . or uppercase letters must be surrounded in double quotes like "naughty schema".table. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




TableDetailsResponse contains grants, column names, and indexes for
a table.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| grants | [TableDetailsResponse.Grant](#cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.TableDetailsResponse.Grant) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| columns | [TableDetailsResponse.Column](#cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.TableDetailsResponse.Column) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| indexes | [TableDetailsResponse.Index](#cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.TableDetailsResponse.Index) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| range_count | [int64](#cockroach.server.serverpb.TableDetailsResponse-int64) |  | range_count is the size of the table in ranges. This provides a rough estimate of the storage requirements for the table. TODO(mrtracy): The TableStats method also returns a range_count field which is more accurate than this one; TableDetails calculates this number using a potentially faster method that is subject to cache staleness. We should consider removing or renaming this field to reflect that difference. See Github issue #5435 for more information. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| create_table_statement | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | create_table_statement is the output of "SHOW CREATE" for this table; it is a SQL statement that would re-create the table's current schema if executed. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| zone_config | [cockroach.config.zonepb.ZoneConfig](#cockroach.server.serverpb.TableDetailsResponse-cockroach.config.zonepb.ZoneConfig) |  | The zone configuration in effect for this table. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| zone_config_level | [ZoneConfigurationLevel](#cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.ZoneConfigurationLevel) |  | The level at which this object's zone configuration is set. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| descriptor_id | [int64](#cockroach.server.serverpb.TableDetailsResponse-int64) |  | descriptor_id is an identifier used to uniquely identify this table. It can be used to find events pertaining to this table by filtering on the 'target_id' field of events. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.TableDetailsResponse.Grant"></a>
#### TableDetailsResponse.Grant

Grant is an entry from SHOW GRANTS.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| user | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | user is the user that this grant applies to. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| privileges | [string](#cockroach.server.serverpb.TableDetailsResponse-string) | repeated | privileges are the abilities this grant gives to the user. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.TableDetailsResponse.Column"></a>
#### TableDetailsResponse.Column



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| name | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | name is the name of the column. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| type | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | type is the SQL type (INT, STRING, etc.) of this column. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| nullable | [bool](#cockroach.server.serverpb.TableDetailsResponse-bool) |  | nullable is whether this column can contain NULL. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| default_value | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | default_value is the default value of this column. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| generation_expression | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | generation_expression is the generator expression if the column is computed. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| hidden | [bool](#cockroach.server.serverpb.TableDetailsResponse-bool) |  | hidden is whether this column is hidden. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.TableDetailsResponse-cockroach.server.serverpb.TableDetailsResponse.Index"></a>
#### TableDetailsResponse.Index



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| name | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | name is the name of this index. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| unique | [bool](#cockroach.server.serverpb.TableDetailsResponse-bool) |  | unique is whether this a unique index (i.e. CREATE UNIQUE INDEX). | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| seq | [int64](#cockroach.server.serverpb.TableDetailsResponse-int64) |  | seq is an internal variable that's passed along. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| column | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | column is the column that this index indexes. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| direction | [string](#cockroach.server.serverpb.TableDetailsResponse-string) |  | direction is either "ASC" (ascending) or "DESC" (descending). | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| storing | [bool](#cockroach.server.serverpb.TableDetailsResponse-bool) |  | storing is an internal variable that's passed along. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| implicit | [bool](#cockroach.server.serverpb.TableDetailsResponse-bool) |  | implicit is an internal variable that's passed along. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## TableStats

`GET /_admin/v1/databases/{database}/tables/{table}/stats`

Example URL: /_admin/v1/databases/system/tables/ui/stats

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




TableStatsRequest is a request for detailed, computationally expensive
information about a table.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| database | [string](#cockroach.server.serverpb.TableStatsRequest-string) |  | database is the database that contains the table we're interested in. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| table | [string](#cockroach.server.serverpb.TableStatsRequest-string) |  | table is the name of the table that we're querying. Table may be schema-qualified (schema.table) and each name component that contains sql unsafe characters such as . or uppercase letters must be surrounded in double quotes like "naughty schema".table. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




TableStatsResponse contains detailed, computationally expensive information
about a table.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_count | [int64](#cockroach.server.serverpb.TableStatsResponse-int64) |  | range_count is the number of ranges, as determined from a query of range meta keys. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| replica_count | [int64](#cockroach.server.serverpb.TableStatsResponse-int64) |  | replica_count is the number of replicas of any range of this table, as found by querying nodes which are known to have replicas. When compared with range_count, this can be used to estimate the current replication factor of the table. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| node_count | [int64](#cockroach.server.serverpb.TableStatsResponse-int64) |  | node_count is the number of nodes which contain data for this table, according to a query of range meta keys. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| stats | [cockroach.storage.enginepb.MVCCStats](#cockroach.server.serverpb.TableStatsResponse-cockroach.storage.enginepb.MVCCStats) |  | stats is the summation of MVCCStats for all replicas of this table across the cluster. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| approximate_disk_bytes | [uint64](#cockroach.server.serverpb.TableStatsResponse-uint64) |  | approximate_disk_bytes is an approximation of the disk space (in bytes) used for all replicas of this table across the cluster. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| missing_nodes | [TableStatsResponse.MissingNode](#cockroach.server.serverpb.TableStatsResponse-cockroach.server.serverpb.TableStatsResponse.MissingNode) | repeated | A list of nodes which should contain data for this table (according to cluster metadata), but could not be contacted during this request. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.TableStatsResponse-cockroach.server.serverpb.TableStatsResponse.MissingNode"></a>
#### TableStatsResponse.MissingNode

MissingNode represents information on a node which should contain data
for this table, but could not be contacted during this request.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.TableStatsResponse-string) |  | The ID of the missing node. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| error_message | [string](#cockroach.server.serverpb.TableStatsResponse-string) |  | The error message that resulted when the query sent to this node failed. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## NonTableStats

`GET /_admin/v1/nontablestats`

Example URL: /_admin/v1/nontablestats

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




NonTableStatsRequest requests statistics on cluster data ranges that do not
belong to SQL tables.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.








#### Response Parameters




NonTableStatsResponse returns statistics on various cluster data ranges
that do not belong to SQL tables. The statistics for each range are returned
as a TableStatsResponse.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| time_series_stats | [TableStatsResponse](#cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse) |  | Information on time series ranges. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| internal_use_stats | [TableStatsResponse](#cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse) |  | Information for remaining (non-table, non-time-series) ranges. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse"></a>
#### TableStatsResponse

TableStatsResponse contains detailed, computationally expensive information
about a table.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_count | [int64](#cockroach.server.serverpb.NonTableStatsResponse-int64) |  | range_count is the number of ranges, as determined from a query of range meta keys. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| replica_count | [int64](#cockroach.server.serverpb.NonTableStatsResponse-int64) |  | replica_count is the number of replicas of any range of this table, as found by querying nodes which are known to have replicas. When compared with range_count, this can be used to estimate the current replication factor of the table. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| node_count | [int64](#cockroach.server.serverpb.NonTableStatsResponse-int64) |  | node_count is the number of nodes which contain data for this table, according to a query of range meta keys. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| stats | [cockroach.storage.enginepb.MVCCStats](#cockroach.server.serverpb.NonTableStatsResponse-cockroach.storage.enginepb.MVCCStats) |  | stats is the summation of MVCCStats for all replicas of this table across the cluster. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| approximate_disk_bytes | [uint64](#cockroach.server.serverpb.NonTableStatsResponse-uint64) |  | approximate_disk_bytes is an approximation of the disk space (in bytes) used for all replicas of this table across the cluster. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| missing_nodes | [TableStatsResponse.MissingNode](#cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse.MissingNode) | repeated | A list of nodes which should contain data for this table (according to cluster metadata), but could not be contacted during this request. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse.MissingNode"></a>
#### TableStatsResponse.MissingNode

MissingNode represents information on a node which should contain data
for this table, but could not be contacted during this request.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#cockroach.server.serverpb.NonTableStatsResponse-string) |  | The ID of the missing node. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| error_message | [string](#cockroach.server.serverpb.NonTableStatsResponse-string) |  | The error message that resulted when the query sent to this node failed. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse"></a>
#### TableStatsResponse

TableStatsResponse contains detailed, computationally expensive information
about a table.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_count | [int64](#cockroach.server.serverpb.NonTableStatsResponse-int64) |  | range_count is the number of ranges, as determined from a query of range meta keys. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| replica_count | [int64](#cockroach.server.serverpb.NonTableStatsResponse-int64) |  | replica_count is the number of replicas of any range of this table, as found by querying nodes which are known to have replicas. When compared with range_count, this can be used to estimate the current replication factor of the table. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| node_count | [int64](#cockroach.server.serverpb.NonTableStatsResponse-int64) |  | node_count is the number of nodes which contain data for this table, according to a query of range meta keys. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| stats | [cockroach.storage.enginepb.MVCCStats](#cockroach.server.serverpb.NonTableStatsResponse-cockroach.storage.enginepb.MVCCStats) |  | stats is the summation of MVCCStats for all replicas of this table across the cluster. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| approximate_disk_bytes | [uint64](#cockroach.server.serverpb.NonTableStatsResponse-uint64) |  | approximate_disk_bytes is an approximation of the disk space (in bytes) used for all replicas of this table across the cluster. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| missing_nodes | [TableStatsResponse.MissingNode](#cockroach.server.serverpb.NonTableStatsResponse-cockroach.server.serverpb.TableStatsResponse.MissingNode) | repeated | A list of nodes which should contain data for this table (according to cluster metadata), but could not be contacted during this request. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






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

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




EventsRequest is a request for event log entries, optionally filtered
by the specified event type and/or target_id.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| type | [string](#cockroach.server.serverpb.EventsRequest-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| target_id | [int64](#cockroach.server.serverpb.EventsRequest-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| limit | [int32](#cockroach.server.serverpb.EventsRequest-int32) |  | limit is the total number of results that are retrieved by the query. If this is omitted or set to 0, the default maximum number of results are returned. When set to > 0, at most only that number of results are returned. When set to < 0, an unlimited number of results are returned. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| unredacted_events | [bool](#cockroach.server.serverpb.EventsRequest-bool) |  | unredacted_events indicates that the values in the events should not be redacted. The default is to redact, so that older versions of `cockroach zip` do not see un-redacted values by default. For good security, this field is only obeyed by the server after checking that the client of the RPC is an admin user. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




EventsResponse contains a set of event log entries. This is always limited
to the latest N entries (N is enforced in the associated endpoint).

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| events | [EventsResponse.Event](#cockroach.server.serverpb.EventsResponse-cockroach.server.serverpb.EventsResponse.Event) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.EventsResponse-cockroach.server.serverpb.EventsResponse.Event"></a>
#### EventsResponse.Event



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| timestamp | [google.protobuf.Timestamp](#cockroach.server.serverpb.EventsResponse-google.protobuf.Timestamp) |  | timestamp is the time at which the event occurred. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| event_type | [string](#cockroach.server.serverpb.EventsResponse-string) |  | event_type is the type of the event (e.g. "create_table", "drop_table". | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| target_id | [int64](#cockroach.server.serverpb.EventsResponse-int64) |  | target_id is the target for this event. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| reporting_id | [int64](#cockroach.server.serverpb.EventsResponse-int64) |  | reporting_id is the reporting ID for this event. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| info | [string](#cockroach.server.serverpb.EventsResponse-string) |  | info has more detailed information for the event. The contents vary depending on the event. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| unique_id | [bytes](#cockroach.server.serverpb.EventsResponse-bytes) |  | unique_id is a unique identifier for this event. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






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

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




SetUIDataRequest stores the given key/value pairs in the system.ui table.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key_values | [SetUIDataRequest.KeyValuesEntry](#cockroach.server.serverpb.SetUIDataRequest-cockroach.server.serverpb.SetUIDataRequest.KeyValuesEntry) | repeated | key_values is a map of keys to bytes values. Each key will be stored with its corresponding value as a separate row in system.ui. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.SetUIDataRequest-cockroach.server.serverpb.SetUIDataRequest.KeyValuesEntry"></a>
#### SetUIDataRequest.KeyValuesEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.SetUIDataRequest-string) |  |  |  |
| value | [bytes](#cockroach.server.serverpb.SetUIDataRequest-bytes) |  |  |  |






#### Response Parameters




SetUIDataResponse is currently an empty response.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.








## GetUIData

`GET /_admin/v1/uidata`

Example URLs:
- /_admin/v1/uidata?keys=MYKEY
- /_admin/v1/uidata?keys=MYKEY1&keys=MYKEY2

Yes, it's a little odd that the query parameter is named "keys" instead of
"key". I would've preferred that the URL parameter be named "key". However,
it's clearer for the protobuf field to be named "keys," which makes the URL
parameter "keys" as well.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




GETUIDataRequest requests the values for the given keys from the system.ui
table.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| keys | [string](#cockroach.server.serverpb.GetUIDataRequest-string) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




GetUIDataResponse contains the requested values and the times at which
the values were last updated.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key_values | [GetUIDataResponse.KeyValuesEntry](#cockroach.server.serverpb.GetUIDataResponse-cockroach.server.serverpb.GetUIDataResponse.KeyValuesEntry) | repeated | key_values maps keys to their retrieved values. If this doesn't contain a a requested key, that key was not found. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.GetUIDataResponse-cockroach.server.serverpb.GetUIDataResponse.KeyValuesEntry"></a>
#### GetUIDataResponse.KeyValuesEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.GetUIDataResponse-string) |  |  |  |
| value | [GetUIDataResponse.Value](#cockroach.server.serverpb.GetUIDataResponse-cockroach.server.serverpb.GetUIDataResponse.Value) |  |  |  |





<a name="cockroach.server.serverpb.GetUIDataResponse-cockroach.server.serverpb.GetUIDataResponse.Value"></a>
#### GetUIDataResponse.Value



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| value | [bytes](#cockroach.server.serverpb.GetUIDataResponse-bytes) |  | value is the value of the requested key. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| last_updated | [google.protobuf.Timestamp](#cockroach.server.serverpb.GetUIDataResponse-google.protobuf.Timestamp) |  | last_updated is the time at which the value was last updated. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## Cluster

`GET /_admin/v1/cluster`

Cluster returns metadata for the cluster.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




ClusterRequest requests metadata for the cluster.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.








#### Response Parameters




ClusterResponse contains metadata for the cluster.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| cluster_id | [string](#cockroach.server.serverpb.ClusterResponse-string) |  | The unique ID used to identify this cluster. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| reporting_enabled | [bool](#cockroach.server.serverpb.ClusterResponse-bool) |  | True if diagnostics reporting is enabled for the cluster. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| enterprise_enabled | [bool](#cockroach.server.serverpb.ClusterResponse-bool) |  | True if enterprise features are enabled for the cluster. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## Settings

`GET /_admin/v1/settings`

Settings returns the cluster-wide settings for the cluster.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




SettingsRequest inquires what are the current settings in the cluster.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| keys | [string](#cockroach.server.serverpb.SettingsRequest-string) | repeated | The array of setting names to retrieve. An empty keys array means "all". | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| unredacted_values | [bool](#cockroach.server.serverpb.SettingsRequest-bool) |  | Indicate whether to see unredacted setting values. This is opt-in so that a previous version `cockroach zip` does not start reporting values when this becomes active. For good security, the server only obeys this after it checks that the logger-in user has admin privilege. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




SettingsResponse is the response to SettingsRequest.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key_values | [SettingsResponse.KeyValuesEntry](#cockroach.server.serverpb.SettingsResponse-cockroach.server.serverpb.SettingsResponse.KeyValuesEntry) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.SettingsResponse-cockroach.server.serverpb.SettingsResponse.KeyValuesEntry"></a>
#### SettingsResponse.KeyValuesEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.SettingsResponse-string) |  |  |  |
| value | [SettingsResponse.Value](#cockroach.server.serverpb.SettingsResponse-cockroach.server.serverpb.SettingsResponse.Value) |  |  |  |





<a name="cockroach.server.serverpb.SettingsResponse-cockroach.server.serverpb.SettingsResponse.Value"></a>
#### SettingsResponse.Value



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| value | [string](#cockroach.server.serverpb.SettingsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| type | [string](#cockroach.server.serverpb.SettingsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| description | [string](#cockroach.server.serverpb.SettingsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| public | [bool](#cockroach.server.serverpb.SettingsResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## Health

`GET /health`

Health returns liveness for the node target of the request.

This is a public API endpoint.

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

This is a public API payload.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| ready | [bool](#cockroach.server.serverpb.HealthRequest-bool) |  | ready specifies whether the client wants to know whether the target node is ready to receive traffic. If a node is unready, an error will be returned. | This is a public API field. |







#### Response Parameters




HealthResponse is the response to HealthRequest. It currently does not
contain any information.

This is a public API payload.








## Liveness

`GET /_admin/v1/liveness`

Liveness returns the liveness state of all nodes on the cluster.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




LivenessRequest requests liveness data for all nodes on the cluster.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.








#### Response Parameters




LivenessResponse contains the liveness status of each node on the cluster.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| livenesses | [cockroach.kv.kvserver.storagepb.Liveness](#cockroach.server.serverpb.LivenessResponse-cockroach.kv.kvserver.storagepb.Liveness) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| statuses | [LivenessResponse.StatusesEntry](#cockroach.server.serverpb.LivenessResponse-cockroach.server.serverpb.LivenessResponse.StatusesEntry) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.LivenessResponse-cockroach.server.serverpb.LivenessResponse.StatusesEntry"></a>
#### LivenessResponse.StatusesEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#cockroach.server.serverpb.LivenessResponse-int32) |  |  |  |
| value | [cockroach.kv.kvserver.storagepb.NodeLivenessStatus](#cockroach.server.serverpb.LivenessResponse-cockroach.kv.kvserver.storagepb.NodeLivenessStatus) |  |  |  |






## Jobs

`GET /_admin/v1/jobs`

Jobs returns the job records for all jobs of the given status and type.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




JobsRequest requests system job information of the given status and type.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| limit | [int32](#cockroach.server.serverpb.JobsRequest-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| status | [string](#cockroach.server.serverpb.JobsRequest-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| type | [cockroach.sql.jobs.jobspb.Type](#cockroach.server.serverpb.JobsRequest-cockroach.sql.jobs.jobspb.Type) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




JobsResponse contains the job record for each matching job.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| jobs | [JobsResponse.Job](#cockroach.server.serverpb.JobsResponse-cockroach.server.serverpb.JobsResponse.Job) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.JobsResponse-cockroach.server.serverpb.JobsResponse.Job"></a>
#### JobsResponse.Job



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| id | [int64](#cockroach.server.serverpb.JobsResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| type | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| description | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| statement | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| username | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| descriptor_ids | [uint32](#cockroach.server.serverpb.JobsResponse-uint32) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| status | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| created | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| started | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| finished | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| modified | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| fraction_completed | [float](#cockroach.server.serverpb.JobsResponse-float) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| error | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| highwater_timestamp | [google.protobuf.Timestamp](#cockroach.server.serverpb.JobsResponse-google.protobuf.Timestamp) |  | highwater_timestamp is the highwater timestamp returned as normal timestamp. This is appropriate for display to humans. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| highwater_decimal | [string](#cockroach.server.serverpb.JobsResponse-string) |  | highwater_decimal is the highwater timestamp in the proprietary decimal form used by logical timestamps internally. This is appropriate to pass to a "AS OF SYSTEM TIME" SQL statement. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| running_status | [string](#cockroach.server.serverpb.JobsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## Locations

`GET /_admin/v1/locations`

Locations returns the locality location records.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




LocationsRequest requests system locality location information.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.








#### Response Parameters




JobsResponse contains the job record for each matching job.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| locations | [LocationsResponse.Location](#cockroach.server.serverpb.LocationsResponse-cockroach.server.serverpb.LocationsResponse.Location) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.LocationsResponse-cockroach.server.serverpb.LocationsResponse.Location"></a>
#### LocationsResponse.Location



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| locality_key | [string](#cockroach.server.serverpb.LocationsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| locality_value | [string](#cockroach.server.serverpb.LocationsResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| latitude | [double](#cockroach.server.serverpb.LocationsResponse-double) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| longitude | [double](#cockroach.server.serverpb.LocationsResponse-double) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## QueryPlan

`GET /_admin/v1/queryplan`

QueryPlan returns the query plans for a SQL string.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




QueryPlanRequest requests the query plans for a SQL string.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| query | [string](#cockroach.server.serverpb.QueryPlanRequest-string) |  | query is the SQL query string. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




QueryPlanResponse contains the query plans for a SQL string (currently only
the distsql physical query plan).

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| distsql_physical_query_plan | [string](#cockroach.server.serverpb.QueryPlanResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## Drain



Drain puts the node into the specified drain mode(s) and optionally
instructs the process to terminate.
We do not expose this via HTTP unless we have a way to authenticate
+ authorize streaming RPC connections. See #42567.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




DrainRequest instructs the receiving node to drain.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| pre201_marker | [int32](#cockroach.server.serverpb.DrainRequest-int32) | repeated | pre_201_marker represents a field that clients stopped using in 20.1. It's maintained to reject requests from such clients, since they're not setting other required fields. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| shutdown | [bool](#cockroach.server.serverpb.DrainRequest-bool) |  | When true, terminates the process after the server has started draining. Setting both shutdown and do_drain to false causes the request to only operate as a probe. Setting do_drain to false and shutdown to true causes the server to shut down immediately without first draining. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| do_drain | [bool](#cockroach.server.serverpb.DrainRequest-bool) |  | When true, perform the drain phase. See the comment above on shutdown for an explanation of the interaction between the two. do_drain is also implied by a non-nil deprecated_probe_indicator. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




DrainResponse is the response to a successful DrainRequest.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| is_draining | [bool](#cockroach.server.serverpb.DrainResponse-bool) |  | is_draining is set to true iff the server is currently draining. This is set to true in response to a request where skip_drain is false; but it can also be set to true in response to a probe request (!shutdown && skip_drain) if another drain request has been issued prior or asynchronously. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| drain_remaining_indicator | [uint64](#cockroach.server.serverpb.DrainResponse-uint64) |  | drain_remaining_indicator measures, at the time of starting to process the corresponding drain request, how many actions to fully drain the node were deemed to be necessary. Some, but not all, of these actions may already have been carried out by the time this indicator is received by the client. The client should issue requests until this indicator first reaches zero, which indicates that the node is fully drained.<br><br>The API contract is the following:<br><br>- upon a first Drain call with do_drain set, the remaining   indicator will have some value >=0. If >0, it indicates that   drain is pushing state away from the node. (What this state   precisely means is left unspecified for this field. See below   for details.)<br><br>- upon a subsequent Drain call with do_drain set, the remaining   indicator should have reduced in value. The drain process does best   effort at shedding state away from the node; hopefully, all the   state is shed away upon the first call and the progress   indicator can be zero as early as the second call. However,   if there was a lot of state to shed, it is possible for   timeout to be encountered upon the first call. In that case, the   second call will do some more work and return a non-zero value   as well.<br><br>- eventually, in an iterated sequence of DrainRequests with   do_drain set, the remaining indicator should reduce to zero. At   that point the client can conclude that no state is left to   shed, and it should be safe to shut down the node with a   DrainRequest with shutdown = true.<br><br>Note that this field is left unpopulated (and thus remains at zero) for pre-20.1 nodes. A client can recognize this by observing is_draining to be false after a request with do_drain = true: the is_draining field is also left unpopulated by pre-20.1 nodes. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| drain_remaining_description | [string](#cockroach.server.serverpb.DrainResponse-string) |  | drain_remaining_description is an informal (= not machine-parsable) string that explains the progress of the drain process to human eyes. This is intended for use mainly for troubleshooting.<br><br>The field is only populated if do_drain is true in the request. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







## Decommission



Decommission puts the node(s) into the specified decommissioning state.
If this ever becomes exposed via HTTP, ensure that it performs
authorization. See #42567.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




DecommissionRequest requests the server to set the membership status on
all nodes specified by NodeIDs to the value of TargetMembership.

If no NodeIDs are given, it targets the recipient node.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_ids | [int32](#cockroach.server.serverpb.DecommissionRequest-int32) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| target_membership | [cockroach.kv.kvserver.storagepb.MembershipStatus](#cockroach.server.serverpb.DecommissionRequest-cockroach.kv.kvserver.storagepb.MembershipStatus) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




DecommissionStatusResponse lists decommissioning statuses for a number of NodeIDs.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| status | [DecommissionStatusResponse.Status](#cockroach.server.serverpb.DecommissionStatusResponse-cockroach.server.serverpb.DecommissionStatusResponse.Status) | repeated | Status of all affected nodes. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.DecommissionStatusResponse-cockroach.server.serverpb.DecommissionStatusResponse.Status"></a>
#### DecommissionStatusResponse.Status



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.DecommissionStatusResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| is_live | [bool](#cockroach.server.serverpb.DecommissionStatusResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| replica_count | [int64](#cockroach.server.serverpb.DecommissionStatusResponse-int64) |  | The number of replicas on the node, computed by scanning meta2 ranges. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| membership | [cockroach.kv.kvserver.storagepb.MembershipStatus](#cockroach.server.serverpb.DecommissionStatusResponse-cockroach.kv.kvserver.storagepb.MembershipStatus) |  | The membership status of the given node. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| draining | [bool](#cockroach.server.serverpb.DecommissionStatusResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## DecommissionStatus



DecommissionStatus retrieves the decommissioning status of the specified nodes.
If this ever becomes exposed via HTTP, ensure that it performs
authorization. See #42567.

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




DecommissionStatusRequest requests the decommissioning status for the
specified or, if none are specified, all nodes.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_ids | [int32](#cockroach.server.serverpb.DecommissionStatusRequest-int32) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




DecommissionStatusResponse lists decommissioning statuses for a number of NodeIDs.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| status | [DecommissionStatusResponse.Status](#cockroach.server.serverpb.DecommissionStatusResponse-cockroach.server.serverpb.DecommissionStatusResponse.Status) | repeated | Status of all affected nodes. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.DecommissionStatusResponse-cockroach.server.serverpb.DecommissionStatusResponse.Status"></a>
#### DecommissionStatusResponse.Status



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.DecommissionStatusResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| is_live | [bool](#cockroach.server.serverpb.DecommissionStatusResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| replica_count | [int64](#cockroach.server.serverpb.DecommissionStatusResponse-int64) |  | The number of replicas on the node, computed by scanning meta2 ranges. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| membership | [cockroach.kv.kvserver.storagepb.MembershipStatus](#cockroach.server.serverpb.DecommissionStatusResponse-cockroach.kv.kvserver.storagepb.MembershipStatus) |  | The membership status of the given node. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| draining | [bool](#cockroach.server.serverpb.DecommissionStatusResponse-bool) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## RangeLog

`GET /_admin/v1/rangelog/{range_id}`

URL: /_admin/v1/rangelog
URL: /_admin/v1/rangelog?limit=100
URL: /_admin/v1/rangelog/1
URL: /_admin/v1/rangelog/1?limit=100

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




RangeLogRequest request the history of a range from the range log.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| range_id | [int64](#cockroach.server.serverpb.RangeLogRequest-int64) |  | TODO(tamird): use [(gogoproto.customname) = "RangeID"] below. Need to figure out how to teach grpc-gateway about custom names. If RangeID is 0, returns range log history without filtering by range. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| limit | [int32](#cockroach.server.serverpb.RangeLogRequest-int32) |  | limit is the total number of results that are retrieved by the query. If this is omitted or set to 0, the default maximum number of results are returned. When set to > 0, at most only that number of results are returned. When set to < 0, an unlimited number of results are returned. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters




RangeLogResponse contains a list of entries from the range log table.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| events | [RangeLogResponse.Event](#cockroach.server.serverpb.RangeLogResponse-cockroach.server.serverpb.RangeLogResponse.Event) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.RangeLogResponse-cockroach.server.serverpb.RangeLogResponse.Event"></a>
#### RangeLogResponse.Event



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| event | [cockroach.kv.kvserver.storagepb.RangeLogEvent](#cockroach.server.serverpb.RangeLogResponse-cockroach.kv.kvserver.storagepb.RangeLogEvent) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| pretty_info | [RangeLogResponse.PrettyInfo](#cockroach.server.serverpb.RangeLogResponse-cockroach.server.serverpb.RangeLogResponse.PrettyInfo) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.RangeLogResponse-cockroach.server.serverpb.RangeLogResponse.PrettyInfo"></a>
#### RangeLogResponse.PrettyInfo

To avoid porting the pretty printing of keys and descriptors to
javascript, they will be precomputed on the serverside.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| updated_desc | [string](#cockroach.server.serverpb.RangeLogResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| new_desc | [string](#cockroach.server.serverpb.RangeLogResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| added_replica | [string](#cockroach.server.serverpb.RangeLogResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| removed_replica | [string](#cockroach.server.serverpb.RangeLogResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| reason | [string](#cockroach.server.serverpb.RangeLogResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| details | [string](#cockroach.server.serverpb.RangeLogResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## DataDistribution

`GET /_admin/v1/data_distribution`



Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.








#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| database_info | [DataDistributionResponse.DatabaseInfoEntry](#cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.DatabaseInfoEntry) | repeated | By database name. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| zone_configs | [DataDistributionResponse.ZoneConfigsEntry](#cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.ZoneConfigsEntry) | repeated | By zone name. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.DatabaseInfoEntry"></a>
#### DataDistributionResponse.DatabaseInfoEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.DataDistributionResponse-string) |  |  |  |
| value | [DataDistributionResponse.DatabaseInfo](#cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.DatabaseInfo) |  |  |  |





<a name="cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.DatabaseInfo"></a>
#### DataDistributionResponse.DatabaseInfo



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| table_info | [DataDistributionResponse.DatabaseInfo.TableInfoEntry](#cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.DatabaseInfo.TableInfoEntry) | repeated | By table name. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.DatabaseInfo.TableInfoEntry"></a>
#### DataDistributionResponse.DatabaseInfo.TableInfoEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.DataDistributionResponse-string) |  |  |  |
| value | [DataDistributionResponse.TableInfo](#cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.TableInfo) |  |  |  |





<a name="cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.TableInfo"></a>
#### DataDistributionResponse.TableInfo



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| replica_count_by_node_id | [DataDistributionResponse.TableInfo.ReplicaCountByNodeIdEntry](#cockroach.server.serverpb.DataDistributionResponse-cockroach.server.serverpb.DataDistributionResponse.TableInfo.ReplicaCountByNodeIdEntry) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| zone_config_id | [int64](#cockroach.server.serverpb.DataDistributionResponse-int64) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| dropped_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.DataDistributionResponse-google.protobuf.Timestamp) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





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



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| target | [string](#cockroach.server.serverpb.DataDistributionResponse-string) |  | target is the object the zone config applies to, e.g. "DATABASE db" or "PARTITION north_america OF TABLE users". | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| config | [cockroach.config.zonepb.ZoneConfig](#cockroach.server.serverpb.DataDistributionResponse-cockroach.config.zonepb.ZoneConfig) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| config_sql | [string](#cockroach.server.serverpb.DataDistributionResponse-string) |  | config_sql is the SQL representation of config. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






## AllMetricMetadata

`GET /_admin/v1/metricmetadata`

URL: /_admin/v1/metricmetadata

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




MetricMetadataRequest requests metadata for all metrics.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.








#### Response Parameters




MetricMetadataResponse contains the metadata for all metics.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| metadata | [MetricMetadataResponse.MetadataEntry](#cockroach.server.serverpb.MetricMetadataResponse-cockroach.server.serverpb.MetricMetadataResponse.MetadataEntry) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.MetricMetadataResponse-cockroach.server.serverpb.MetricMetadataResponse.MetadataEntry"></a>
#### MetricMetadataResponse.MetadataEntry





| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [string](#cockroach.server.serverpb.MetricMetadataResponse-string) |  |  |  |
| value | [cockroach.util.metric.Metadata](#cockroach.server.serverpb.MetricMetadataResponse-cockroach.util.metric.Metadata) |  |  |  |






## ChartCatalog

`GET /_admin/v1/chartcatalog`

URL: /_admin/v1/chartcatalog

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters




ChartCatalogRequest requests returns a catalog of Admin UI charts.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.








#### Response Parameters




ChartCatalogResponse returns a catalog of Admin UI charts useful for debugging.

Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| catalog | [cockroach.ts.catalog.ChartSection](#cockroach.server.serverpb.ChartCatalogResponse-cockroach.ts.catalog.ChartSection) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







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

Note: this is a “reserved” API endpoint and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

#### Request Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.EnqueueRangeRequest-int32) |  | The node on which the queue should process the range. If node_id is 0, the request will be forwarded to all other nodes. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| queue | [string](#cockroach.server.serverpb.EnqueueRangeRequest-string) |  | The name of the replica queue to run the range through. Matched against each queue's name field. See the implementation of baseQueue for details. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| range_id | [int32](#cockroach.server.serverpb.EnqueueRangeRequest-int32) |  | The ID of the range to run through the queue. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| skip_should_queue | [bool](#cockroach.server.serverpb.EnqueueRangeRequest-bool) |  | If set, run the queue's process method without first checking whether the replica should be processed by calling shouldQueue. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |







#### Response Parameters






Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| details | [EnqueueRangeResponse.Details](#cockroach.server.serverpb.EnqueueRangeResponse-cockroach.server.serverpb.EnqueueRangeResponse.Details) | repeated |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






<a name="cockroach.server.serverpb.EnqueueRangeResponse-cockroach.server.serverpb.EnqueueRangeResponse.Details"></a>
#### EnqueueRangeResponse.Details



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#cockroach.server.serverpb.EnqueueRangeResponse-int32) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| events | [TraceEvent](#cockroach.server.serverpb.EnqueueRangeResponse-cockroach.server.serverpb.TraceEvent) | repeated | All trace events collected while processing the range in the queue. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| error | [string](#cockroach.server.serverpb.EnqueueRangeResponse-string) |  | The error message from the queue's processing, if any. | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |





<a name="cockroach.server.serverpb.EnqueueRangeResponse-cockroach.server.serverpb.TraceEvent"></a>
#### TraceEvent



Note: this is a “reserved” API payload and should not be relied upon to
build external tools. No guarantee is made about its
availability and stability in external uses.

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| time | [google.protobuf.Timestamp](#cockroach.server.serverpb.EnqueueRangeResponse-google.protobuf.Timestamp) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |
| message | [string](#cockroach.server.serverpb.EnqueueRangeResponse-string) |  |  | Note: this is a “reserved” API field and should not be relied upon to build external tools. No guarantee is made about its availability and stability in external uses. |






