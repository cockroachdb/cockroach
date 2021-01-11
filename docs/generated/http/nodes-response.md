

<a name="cockroach.server.serverpb.NodesResponse"></a>
#### NodesResponse

NodesResponse describe the nodes in the cluster.

Support status: [alpha](#support-status)


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| nodes | [cockroach.server.status.statuspb.NodeStatus](#cockroach.server.status.statuspb.NodeStatus) | repeated | nodes carries the status payloads for all nodes in the cluster. | [alpha](#support-status) |
| liveness_by_node_id | [NodesResponse.LivenessByNodeIdEntry](#cockroach.server.serverpb.NodesResponse.LivenessByNodeIdEntry) | repeated | liveness_by_node_id maps each node ID to a liveness status. | [reserved](#support-status) |


