

<a name="cockroach.server.serverpb.HotRangesResponse.HotRangesByNodeIdEntry"></a>
#### HotRangesResponse.HotRangesByNodeIdEntry






| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| key | [int32](#int32) |  |  |  |
| value | [HotRangesResponse.NodeResponse](#cockroach.server.serverpb.HotRangesResponse.NodeResponse) |  |  |  |




<a name="cockroach.server.serverpb.HotRangesResponse.NodeResponse"></a>
#### HotRangesResponse.NodeResponse

NodeResponse is a hot range report for a single target node.

Support status: [alpha](#support-status)


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| error_message | [string](#string) |  | ErrorMessage is set to a non-empty string if this target node was unable to produce a hot range report.<br><br>The contents of this string indicates the cause of the failure. | [alpha](#support-status) |
| stores | [HotRangesResponse.StoreResponse](#cockroach.server.serverpb.HotRangesResponse.StoreResponse) | repeated | Stores contains the hot ranges report if no error was encountered. There is one part to the report for each store in the target node. | [alpha](#support-status) |




<a name="cockroach.server.serverpb.HotRangesResponse.StoreResponse"></a>
#### HotRangesResponse.StoreResponse

StoreResponse contains the part of a hot ranges report that
pertains to a single store on a target node.

Support status: [alpha](#support-status)


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| store_id | [int32](#int32) |  | StoreID identifies the store for which the report was produced. | [alpha](#support-status) |
| hot_ranges | [HotRangesResponse.HotRange](#cockroach.server.serverpb.HotRangesResponse.HotRange) | repeated | HotRanges is the hot ranges report for this store on the target node. | [alpha](#support-status) |




<a name="cockroach.server.serverpb.HotRangesResponse.HotRange"></a>
#### HotRangesResponse.HotRange

HotRange is a hot range report for a single store on one of the
target node(s) selected in a HotRangesRequest.

Support status: [alpha](#support-status)


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| desc | [cockroach.roachpb.RangeDescriptor](#cockroach.roachpb.RangeDescriptor) |  | Desc is the descriptor of the range for which the report was produced.<br><br>TODO(knz): This field should be removed. See: https://github.com/cockroachdb/cockroach/issues/53212 | [reserved](#support-status) |
| queries_per_second | [double](#double) |  | QueriesPerSecond is the recent number of queries per second on this range. | [alpha](#support-status) |
| leaseholder_node_id | [int32](#int32) |  | LeaseholderNodeID indicates the Node ID that is the current leaseholder for the given range. | [reserved](#support-status) |


