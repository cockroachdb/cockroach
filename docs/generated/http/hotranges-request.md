

<a name="cockroach.server.serverpb.HotRangesRequest"></a>
#### HotRangesRequest

HotRangesRequest queries one or more cluster nodes for a list
of ranges currently considered “hot” by the node(s).

Note: this is an “alpha” API payload. It is subject to change without
advance notice in a subsequent release.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [string](#string) |  | NodeID indicates which node to query for a hot range report. It is posssible to populate any node ID; if the node receiving the request is not the target node, it will forward the request to the target node.<br><br>If left empty, the request is forwarded to every node in the cluster. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |


