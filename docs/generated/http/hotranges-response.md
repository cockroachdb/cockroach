

<a name="cockroach.server.serverpb.HotRangesResponse"></a>
#### HotRangesResponse

HotRangesResponse is the payload produced in response
to a HotRangesRequest.

Note: this is an “alpha” API payload. It is subject to change without
advance notice in a subsequent release.


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| node_id | [int32](#int32) |  | NodeID is the node that received the HotRangesRequest and forwarded requests to the selected target node(s). | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |
| hot_ranges_by_node_id | [HotRangesResponse.HotRangesByNodeIdEntry](#cockroach.server.serverpb.HotRangesResponse.HotRangesByNodeIdEntry) | repeated | HotRangesByNodeID contains a hot range report for each selected target node ID in the HotRangesRequest. | Note: this is an “alpha” API field. It is subject to change without advance notice in a subsequent release. |


