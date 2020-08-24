

<a name="cockroach.server.serverpb.HotRangesResponse.HotRangesByNodeIdEntry"></a>
#### HotRangesResponse.HotRangesByNodeIdEntry

| Field | Type | Label |
| ----- | ---- | ----- |
| key | [int32](#int32) |  |
| value | [HotRangesResponse.NodeResponse](#cockroach.server.serverpb.HotRangesResponse.NodeResponse) |  |




<a name="cockroach.server.serverpb.HotRangesResponse.NodeResponse"></a>
#### HotRangesResponse.NodeResponse

| Field | Type | Label |
| ----- | ---- | ----- |
| error_message | [string](#string) |  |
| stores | [HotRangesResponse.StoreResponse](#cockroach.server.serverpb.HotRangesResponse.StoreResponse) | repeated |




<a name="cockroach.server.serverpb.HotRangesResponse.StoreResponse"></a>
#### HotRangesResponse.StoreResponse

| Field | Type | Label |
| ----- | ---- | ----- |
| store_id | [int32](#int32) |  |
| hot_ranges | [HotRangesResponse.HotRange](#cockroach.server.serverpb.HotRangesResponse.HotRange) | repeated |




<a name="cockroach.server.serverpb.HotRangesResponse.HotRange"></a>
#### HotRangesResponse.HotRange

| Field | Type | Label |
| ----- | ---- | ----- |
| desc | [cockroach.roachpb.RangeDescriptor](#cockroach.roachpb.RangeDescriptor) |  |
| queries_per_second | [double](#double) |  |


