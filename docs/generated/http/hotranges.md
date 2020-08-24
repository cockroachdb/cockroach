

## Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.HotRangesRequest-string) |  | NodeID indicates which node to query for a hot range report. It is posssible to populate any node ID; if the node receiving the request is not the target node, it will forward the request to the target node.<br><br>If left empty, the request is forwarded to every node in the cluster. |







## Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  | NodeID is the node that received the HotRangesRequest and forwarded requests to the selected target node(s). |
| hot_ranges_by_node_id | [HotRangesResponse.HotRangesByNodeIdEntry](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRangesByNodeIdEntry) | repeated | HotRangesByNodeID contains a hot range report for each selected target node ID in the HotRangesRequest. |






<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRangesByNodeIdEntry"></a>
### HotRangesResponse.HotRangesByNodeIdEntry

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  |  |
| value | [HotRangesResponse.NodeResponse](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.NodeResponse) |  |  |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.NodeResponse"></a>
### HotRangesResponse.NodeResponse

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error_message | [string](#cockroach.server.serverpb.HotRangesResponse-string) |  | ErrorMessage is set to a non-empty string if this target node was unable to produce a hot range report.<br><br>The contents of this string indicates the cause of the failure. |
| stores | [HotRangesResponse.StoreResponse](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.StoreResponse) | repeated | Stores contains the hot ranges report if no error was encountered. There is one part to the report for each store in the target node. |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.StoreResponse"></a>
### HotRangesResponse.StoreResponse

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_id | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  | StoreID identifies the store for which the report was produced. |
| hot_ranges | [HotRangesResponse.HotRange](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRange) | repeated | HotRanges is the hot ranges report for this store on the target node. |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRange"></a>
### HotRangesResponse.HotRange

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| desc | [cockroach.roachpb.RangeDescriptor](#cockroach.server.serverpb.HotRangesResponse-cockroach.roachpb.RangeDescriptor) |  | Desc is the descriptor of the range for which the report was produced.<br><br>Note: this field is generally RESERVED and will likely be removed or replaced in a later version. See: https://github.com/cockroachdb/cockroach/issues/53212 |
| queries_per_second | [double](#cockroach.server.serverpb.HotRangesResponse-double) |  | QueriesPerSecond is the recent number of queries per second on this range. |





