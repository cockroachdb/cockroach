

### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.HotRangesRequest-string) |  | If left empty, hot ranges for all nodes/stores will be returned. |







### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  | NodeID is the node that submitted all the requests. |
| hot_ranges_by_node_id | [HotRangesResponse.HotRangesByNodeIdEntry](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRangesByNodeIdEntry) | repeated |  |






<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRangesByNodeIdEntry"></a>
#### HotRangesResponse.HotRangesByNodeIdEntry

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  |  |
| value | [HotRangesResponse.NodeResponse](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.NodeResponse) |  |  |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.NodeResponse"></a>
#### HotRangesResponse.NodeResponse

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error_message | [string](#cockroach.server.serverpb.HotRangesResponse-string) |  |  |
| stores | [HotRangesResponse.StoreResponse](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.StoreResponse) | repeated |  |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.StoreResponse"></a>
#### HotRangesResponse.StoreResponse

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_id | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  |  |
| hot_ranges | [HotRangesResponse.HotRange](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRange) | repeated |  |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRange"></a>
#### HotRangesResponse.HotRange

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| desc | [cockroach.roachpb.RangeDescriptor](#cockroach.server.serverpb.HotRangesResponse-cockroach.roachpb.RangeDescriptor) |  |  |
| queries_per_second | [double](#cockroach.server.serverpb.HotRangesResponse-double) |  |  |





