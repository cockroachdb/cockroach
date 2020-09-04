## Certificates

`GET /_status/certificates/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.CertificatesRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| certificates | [CertificateDetails](#cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails) | repeated |  |






<a name="cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails"></a>
#### CertificateDetails

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [CertificateDetails.CertificateType](#cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails.CertificateType) |  |  |
| error_message | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  | "error_message" and "data" are mutually exclusive. |
| data | [bytes](#cockroach.server.serverpb.CertificatesResponse-bytes) |  | data is the raw file contents of the certificate. This means PEM-encoded DER data. |
| fields | [CertificateDetails.Fields](#cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails.Fields) | repeated |  |





<a name="cockroach.server.serverpb.CertificatesResponse-cockroach.server.serverpb.CertificateDetails.Fields"></a>
#### CertificateDetails.Fields

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| issuer | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  |  |
| subject | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  |  |
| valid_from | [int64](#cockroach.server.serverpb.CertificatesResponse-int64) |  |  |
| valid_until | [int64](#cockroach.server.serverpb.CertificatesResponse-int64) |  |  |
| addresses | [string](#cockroach.server.serverpb.CertificatesResponse-string) | repeated |  |
| signature_algorithm | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  |  |
| public_key | [string](#cockroach.server.serverpb.CertificatesResponse-string) |  |  |
| key_usage | [string](#cockroach.server.serverpb.CertificatesResponse-string) | repeated |  |
| extended_key_usage | [string](#cockroach.server.serverpb.CertificatesResponse-string) | repeated |  |






## Details

`GET /_status/details/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.DetailsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [int32](#cockroach.server.serverpb.DetailsResponse-int32) |  |  |
| address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.DetailsResponse-cockroach.util.UnresolvedAddr) |  |  |
| build_info | [cockroach.build.Info](#cockroach.server.serverpb.DetailsResponse-cockroach.build.Info) |  |  |
| system_info | [SystemInfo](#cockroach.server.serverpb.DetailsResponse-cockroach.server.serverpb.SystemInfo) |  |  |
| sql_address | [cockroach.util.UnresolvedAddr](#cockroach.server.serverpb.DetailsResponse-cockroach.util.UnresolvedAddr) |  |  |






<a name="cockroach.server.serverpb.DetailsResponse-cockroach.server.serverpb.SystemInfo"></a>
#### SystemInfo

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| system_info | [string](#cockroach.server.serverpb.DetailsResponse-string) |  | system_info is the output from `uname -a` |
| kernel_info | [string](#cockroach.server.serverpb.DetailsResponse-string) |  | kernel_info is the output from `uname -r`. |






## Nodes

`GET /_status/nodes`

Don't introduce additional usages of this RPC. See #50707 for more details.
The underlying response type is something we're looking to get rid of.

#### Request Parameters










#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| nodes | [cockroach.server.status.statuspb.NodeStatus](#cockroach.server.serverpb.NodesResponse-cockroach.server.status.statuspb.NodeStatus) | repeated |  |
| liveness_by_node_id | [NodesResponse.LivenessByNodeIdEntry](#cockroach.server.serverpb.NodesResponse-cockroach.server.serverpb.NodesResponse.LivenessByNodeIdEntry) | repeated |  |






<a name="cockroach.server.serverpb.NodesResponse-cockroach.server.serverpb.NodesResponse.LivenessByNodeIdEntry"></a>
#### NodesResponse.LivenessByNodeIdEntry

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [int32](#cockroach.server.serverpb.NodesResponse-int32) |  |  |
| value | [cockroach.kv.kvserver.storagepb.NodeLivenessStatus](#cockroach.server.serverpb.NodesResponse-cockroach.kv.kvserver.storagepb.NodeLivenessStatus) |  |  |






## Node

`GET /_status/nodes/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.NodeRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |







#### Response Parameters



## RaftDebug

`GET /_status/raft`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| range_ids | [int64](#cockroach.server.serverpb.RaftDebugRequest-int64) | repeated |  |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ranges | [RaftDebugResponse.RangesEntry](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftDebugResponse.RangesEntry) | repeated |  |
| errors | [RaftRangeError](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeError) | repeated |  |






<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftDebugResponse.RangesEntry"></a>
#### RaftDebugResponse.RangesEntry

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  |
| value | [RaftRangeStatus](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeStatus) |  |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeStatus"></a>
#### RaftRangeStatus

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| range_id | [int64](#cockroach.server.serverpb.RaftDebugResponse-int64) |  |  |
| errors | [RaftRangeError](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeError) | repeated |  |
| nodes | [RaftRangeNode](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeNode) | repeated |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeError"></a>
#### RaftRangeError

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeNode"></a>
#### RaftRangeNode

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [int32](#cockroach.server.serverpb.RaftDebugResponse-int32) |  |  |
| range | [RangeInfo](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeInfo) |  |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeInfo"></a>
#### RangeInfo

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| span | [PrettySpan](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.PrettySpan) |  |  |
| raft_state | [RaftState](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState) |  |  |
| state | [cockroach.kv.kvserver.storagepb.RangeInfo](#cockroach.server.serverpb.RaftDebugResponse-cockroach.kv.kvserver.storagepb.RangeInfo) |  |  |
| source_node_id | [int32](#cockroach.server.serverpb.RaftDebugResponse-int32) |  |  |
| source_store_id | [int32](#cockroach.server.serverpb.RaftDebugResponse-int32) |  |  |
| error_message | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  |
| lease_history | [cockroach.roachpb.Lease](#cockroach.server.serverpb.RaftDebugResponse-cockroach.roachpb.Lease) | repeated |  |
| problems | [RangeProblems](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeProblems) |  |  |
| stats | [RangeStatistics](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeStatistics) |  |  |
| latches_local | [cockroach.kv.kvserver.storagepb.LatchManagerInfo](#cockroach.server.serverpb.RaftDebugResponse-cockroach.kv.kvserver.storagepb.LatchManagerInfo) |  |  |
| latches_global | [cockroach.kv.kvserver.storagepb.LatchManagerInfo](#cockroach.server.serverpb.RaftDebugResponse-cockroach.kv.kvserver.storagepb.LatchManagerInfo) |  |  |
| lease_status | [cockroach.kv.kvserver.storagepb.LeaseStatus](#cockroach.server.serverpb.RaftDebugResponse-cockroach.kv.kvserver.storagepb.LeaseStatus) |  |  |
| quiescent | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  |
| ticking | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.PrettySpan"></a>
#### PrettySpan

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| start_key | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  |
| end_key | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState"></a>
#### RaftState

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| replica_id | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  |
| hard_state | [raftpb.HardState](#cockroach.server.serverpb.RaftDebugResponse-raftpb.HardState) |  |  |
| lead | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  | Lead is part of Raft's SoftState. |
| state | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  | State is part of Raft's SoftState. It's not an enum because this is primarily for ui consumption and there are issues associated with them. |
| applied | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  |
| progress | [RaftState.ProgressEntry](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState.ProgressEntry) | repeated |  |
| lead_transferee | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState.ProgressEntry"></a>
#### RaftState.ProgressEntry

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  |
| value | [RaftState.Progress](#cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState.Progress) |  |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftState.Progress"></a>
#### RaftState.Progress

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| match | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  |
| next | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  |
| state | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  |
| paused | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  |
| pending_snapshot | [uint64](#cockroach.server.serverpb.RaftDebugResponse-uint64) |  |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeProblems"></a>
#### RangeProblems

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| unavailable | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  |
| leader_not_lease_holder | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  |
| no_raft_leader | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  |
| underreplicated | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  |
| overreplicated | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  |
| no_lease | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  |  |
| quiescent_equals_ticking | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  | Quiescent ranges do not tick by definition, but we track this in two different ways and suspect that they're getting out of sync. If the replica's quiescent flag doesn't agree with the store's list of replicas that are ticking, warn about it. |
| raft_log_too_large | [bool](#cockroach.server.serverpb.RaftDebugResponse-bool) |  | When the raft log is too large, it can be a symptom of other issues. |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RangeStatistics"></a>
#### RangeStatistics

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| queries_per_second | [double](#cockroach.server.serverpb.RaftDebugResponse-double) |  | Note that queries per second will only be known by the leaseholder. All other replicas will report it as 0. |
| writes_per_second | [double](#cockroach.server.serverpb.RaftDebugResponse-double) |  |  |





<a name="cockroach.server.serverpb.RaftDebugResponse-cockroach.server.serverpb.RaftRangeError"></a>
#### RaftRangeError

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#cockroach.server.serverpb.RaftDebugResponse-string) |  |  |






## Ranges

`GET /_status/ranges/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.RangesRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |
| range_ids | [int64](#cockroach.server.serverpb.RangesRequest-int64) | repeated |  |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ranges | [RangeInfo](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeInfo) | repeated |  |






<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeInfo"></a>
#### RangeInfo

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| span | [PrettySpan](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.PrettySpan) |  |  |
| raft_state | [RaftState](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState) |  |  |
| state | [cockroach.kv.kvserver.storagepb.RangeInfo](#cockroach.server.serverpb.RangesResponse-cockroach.kv.kvserver.storagepb.RangeInfo) |  |  |
| source_node_id | [int32](#cockroach.server.serverpb.RangesResponse-int32) |  |  |
| source_store_id | [int32](#cockroach.server.serverpb.RangesResponse-int32) |  |  |
| error_message | [string](#cockroach.server.serverpb.RangesResponse-string) |  |  |
| lease_history | [cockroach.roachpb.Lease](#cockroach.server.serverpb.RangesResponse-cockroach.roachpb.Lease) | repeated |  |
| problems | [RangeProblems](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeProblems) |  |  |
| stats | [RangeStatistics](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeStatistics) |  |  |
| latches_local | [cockroach.kv.kvserver.storagepb.LatchManagerInfo](#cockroach.server.serverpb.RangesResponse-cockroach.kv.kvserver.storagepb.LatchManagerInfo) |  |  |
| latches_global | [cockroach.kv.kvserver.storagepb.LatchManagerInfo](#cockroach.server.serverpb.RangesResponse-cockroach.kv.kvserver.storagepb.LatchManagerInfo) |  |  |
| lease_status | [cockroach.kv.kvserver.storagepb.LeaseStatus](#cockroach.server.serverpb.RangesResponse-cockroach.kv.kvserver.storagepb.LeaseStatus) |  |  |
| quiescent | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  |
| ticking | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.PrettySpan"></a>
#### PrettySpan

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| start_key | [string](#cockroach.server.serverpb.RangesResponse-string) |  |  |
| end_key | [string](#cockroach.server.serverpb.RangesResponse-string) |  |  |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState"></a>
#### RaftState

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| replica_id | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  |
| hard_state | [raftpb.HardState](#cockroach.server.serverpb.RangesResponse-raftpb.HardState) |  |  |
| lead | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  | Lead is part of Raft's SoftState. |
| state | [string](#cockroach.server.serverpb.RangesResponse-string) |  | State is part of Raft's SoftState. It's not an enum because this is primarily for ui consumption and there are issues associated with them. |
| applied | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  |
| progress | [RaftState.ProgressEntry](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState.ProgressEntry) | repeated |  |
| lead_transferee | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState.ProgressEntry"></a>
#### RaftState.ProgressEntry

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  |
| value | [RaftState.Progress](#cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState.Progress) |  |  |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RaftState.Progress"></a>
#### RaftState.Progress

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| match | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  |
| next | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  |
| state | [string](#cockroach.server.serverpb.RangesResponse-string) |  |  |
| paused | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  |
| pending_snapshot | [uint64](#cockroach.server.serverpb.RangesResponse-uint64) |  |  |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeProblems"></a>
#### RangeProblems

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| unavailable | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  |
| leader_not_lease_holder | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  |
| no_raft_leader | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  |
| underreplicated | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  |
| overreplicated | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  |
| no_lease | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  |  |
| quiescent_equals_ticking | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  | Quiescent ranges do not tick by definition, but we track this in two different ways and suspect that they're getting out of sync. If the replica's quiescent flag doesn't agree with the store's list of replicas that are ticking, warn about it. |
| raft_log_too_large | [bool](#cockroach.server.serverpb.RangesResponse-bool) |  | When the raft log is too large, it can be a symptom of other issues. |





<a name="cockroach.server.serverpb.RangesResponse-cockroach.server.serverpb.RangeStatistics"></a>
#### RangeStatistics

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| queries_per_second | [double](#cockroach.server.serverpb.RangesResponse-double) |  | Note that queries per second will only be known by the leaseholder. All other replicas will report it as 0. |
| writes_per_second | [double](#cockroach.server.serverpb.RangesResponse-double) |  |  |






## Gossip

`GET /_status/gossip/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.GossipRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |







#### Response Parameters



## EngineStats

`GET /_status/enginestats/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.EngineStatsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stats | [EngineStatsInfo](#cockroach.server.serverpb.EngineStatsResponse-cockroach.server.serverpb.EngineStatsInfo) | repeated |  |






<a name="cockroach.server.serverpb.EngineStatsResponse-cockroach.server.serverpb.EngineStatsInfo"></a>
#### EngineStatsInfo

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_id | [int32](#cockroach.server.serverpb.EngineStatsResponse-int32) |  |  |
| tickers_and_histograms | [cockroach.storage.enginepb.TickersAndHistograms](#cockroach.server.serverpb.EngineStatsResponse-cockroach.storage.enginepb.TickersAndHistograms) |  |  |
| engine_type | [cockroach.storage.enginepb.EngineType](#cockroach.server.serverpb.EngineStatsResponse-cockroach.storage.enginepb.EngineType) |  |  |






## Allocator

`GET /_status/allocator/node/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.AllocatorRequest-string) |  |  |
| range_ids | [int64](#cockroach.server.serverpb.AllocatorRequest-int64) | repeated |  |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dry_runs | [AllocatorDryRun](#cockroach.server.serverpb.AllocatorResponse-cockroach.server.serverpb.AllocatorDryRun) | repeated |  |






<a name="cockroach.server.serverpb.AllocatorResponse-cockroach.server.serverpb.AllocatorDryRun"></a>
#### AllocatorDryRun

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| range_id | [int64](#cockroach.server.serverpb.AllocatorResponse-int64) |  |  |
| events | [TraceEvent](#cockroach.server.serverpb.AllocatorResponse-cockroach.server.serverpb.TraceEvent) | repeated |  |





<a name="cockroach.server.serverpb.AllocatorResponse-cockroach.server.serverpb.TraceEvent"></a>
#### TraceEvent

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| time | [google.protobuf.Timestamp](#cockroach.server.serverpb.AllocatorResponse-google.protobuf.Timestamp) |  |  |
| message | [string](#cockroach.server.serverpb.AllocatorResponse-string) |  |  |






## AllocatorRange

`GET /_status/allocator/range/{range_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| range_id | [int64](#cockroach.server.serverpb.AllocatorRangeRequest-int64) |  |  |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [int64](#cockroach.server.serverpb.AllocatorRangeResponse-int64) |  | The NodeID of the store whose dry run is returned. Only the leaseholder for a given range will do an allocator dry run for it. |
| dry_run | [AllocatorDryRun](#cockroach.server.serverpb.AllocatorRangeResponse-cockroach.server.serverpb.AllocatorDryRun) |  |  |






<a name="cockroach.server.serverpb.AllocatorRangeResponse-cockroach.server.serverpb.AllocatorDryRun"></a>
#### AllocatorDryRun

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| range_id | [int64](#cockroach.server.serverpb.AllocatorRangeResponse-int64) |  |  |
| events | [TraceEvent](#cockroach.server.serverpb.AllocatorRangeResponse-cockroach.server.serverpb.TraceEvent) | repeated |  |





<a name="cockroach.server.serverpb.AllocatorRangeResponse-cockroach.server.serverpb.TraceEvent"></a>
#### TraceEvent

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| time | [google.protobuf.Timestamp](#cockroach.server.serverpb.AllocatorRangeResponse-google.protobuf.Timestamp) |  |  |
| message | [string](#cockroach.server.serverpb.AllocatorRangeResponse-string) |  |  |






## ListSessions

`GET /_status/sessions`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| username | [string](#cockroach.server.serverpb.ListSessionsRequest-string) |  | Username of the user making this request. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sessions | [Session](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.Session) | repeated | A list of sessions on this node or cluster. |
| errors | [ListSessionsError](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ListSessionsError) | repeated | Any errors that occurred during fan-out calls to other nodes. |






<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.Session"></a>
#### Session

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | ID of node where this session exists. |
| username | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Username of the user for this session. |
| client_address | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Connected client's IP address and port. |
| application_name | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Application name specified by the client. |
| active_queries | [ActiveQuery](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery) | repeated | Queries in progress on this session. |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | Timestamp of session's start. |
| last_active_query | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | SQL string of the last query executed on this session. |
| id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  | ID of the session (uint128 represented as raw bytes). |
| alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | Number of currently allocated bytes in the session memory monitor. |
| max_alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | High water mark of allocated bytes in the session memory monitor. |
| active_txn | [TxnInfo](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.TxnInfo) |  | Information about the txn in progress on this session. Nil if the session doesn't currently have a transaction. |
| last_active_query_anon | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | The SQL statement fingerprint of the last query executed on this session, compatible with StatementStatisticsKey. |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery"></a>
#### ActiveQuery

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | ID of the query (uint128 presented as a hexadecimal string). |
| txn_id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  | The UUID of the transaction this query is running in. |
| sql | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | SQL query string specified by the user. |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | Start timestamp of this query. |
| is_distributed | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  | True if this query is distributed. |
| phase | [ActiveQuery.Phase](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery.Phase) |  | phase stores the current phase of execution for this query. |
| progress | [float](#cockroach.server.serverpb.ListSessionsResponse-float) |  |  |
| sql_anon | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | The SQL statement fingerprint, compatible with StatementStatisticsKey. |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.TxnInfo"></a>
#### TxnInfo

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  |  |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | The start timestamp of the transaction. |
| txn_description | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | txn_description is a text description of the underlying kv.Txn, intended for troubleshooting purposes. |
| num_statements_executed | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_statements_executed is the number of statements that were executed so far on this transaction. |
| num_retries | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_retries is the number of times that this transaction was retried. |
| num_auto_retries | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_retries is the number of times that this transaction was automatically retried by the SQL executor. |
| deadline | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | The deadline by which the transaction must be committed. |
| implicit | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  | implicit is true if this transaction was an implicit SQL transaction. |
| alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | Number of currently allocated bytes in the txn memory monitor. |
| max_alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | High water mark of allocated bytes in the txn memory monitor. |
| read_only | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  |  |
| is_historical | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  |  |
| priority | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  |  |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ListSessionsError"></a>
#### ListSessionsError

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | ID of node that was being contacted when this error occurred |
| message | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Error message. |






## ListLocalSessions

`GET /_status/local_sessions`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| username | [string](#cockroach.server.serverpb.ListSessionsRequest-string) |  | Username of the user making this request. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sessions | [Session](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.Session) | repeated | A list of sessions on this node or cluster. |
| errors | [ListSessionsError](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ListSessionsError) | repeated | Any errors that occurred during fan-out calls to other nodes. |






<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.Session"></a>
#### Session

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | ID of node where this session exists. |
| username | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Username of the user for this session. |
| client_address | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Connected client's IP address and port. |
| application_name | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Application name specified by the client. |
| active_queries | [ActiveQuery](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery) | repeated | Queries in progress on this session. |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | Timestamp of session's start. |
| last_active_query | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | SQL string of the last query executed on this session. |
| id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  | ID of the session (uint128 represented as raw bytes). |
| alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | Number of currently allocated bytes in the session memory monitor. |
| max_alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | High water mark of allocated bytes in the session memory monitor. |
| active_txn | [TxnInfo](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.TxnInfo) |  | Information about the txn in progress on this session. Nil if the session doesn't currently have a transaction. |
| last_active_query_anon | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | The SQL statement fingerprint of the last query executed on this session, compatible with StatementStatisticsKey. |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery"></a>
#### ActiveQuery

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | ID of the query (uint128 presented as a hexadecimal string). |
| txn_id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  | The UUID of the transaction this query is running in. |
| sql | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | SQL query string specified by the user. |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | Start timestamp of this query. |
| is_distributed | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  | True if this query is distributed. |
| phase | [ActiveQuery.Phase](#cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ActiveQuery.Phase) |  | phase stores the current phase of execution for this query. |
| progress | [float](#cockroach.server.serverpb.ListSessionsResponse-float) |  |  |
| sql_anon | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | The SQL statement fingerprint, compatible with StatementStatisticsKey. |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.TxnInfo"></a>
#### TxnInfo

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [bytes](#cockroach.server.serverpb.ListSessionsResponse-bytes) |  |  |
| start | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | The start timestamp of the transaction. |
| txn_description | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | txn_description is a text description of the underlying kv.Txn, intended for troubleshooting purposes. |
| num_statements_executed | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_statements_executed is the number of statements that were executed so far on this transaction. |
| num_retries | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_retries is the number of times that this transaction was retried. |
| num_auto_retries | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | num_retries is the number of times that this transaction was automatically retried by the SQL executor. |
| deadline | [google.protobuf.Timestamp](#cockroach.server.serverpb.ListSessionsResponse-google.protobuf.Timestamp) |  | The deadline by which the transaction must be committed. |
| implicit | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  | implicit is true if this transaction was an implicit SQL transaction. |
| alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | Number of currently allocated bytes in the txn memory monitor. |
| max_alloc_bytes | [int64](#cockroach.server.serverpb.ListSessionsResponse-int64) |  | High water mark of allocated bytes in the txn memory monitor. |
| read_only | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  |  |
| is_historical | [bool](#cockroach.server.serverpb.ListSessionsResponse-bool) |  |  |
| priority | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  |  |





<a name="cockroach.server.serverpb.ListSessionsResponse-cockroach.server.serverpb.ListSessionsError"></a>
#### ListSessionsError

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [int32](#cockroach.server.serverpb.ListSessionsResponse-int32) |  | ID of node that was being contacted when this error occurred |
| message | [string](#cockroach.server.serverpb.ListSessionsResponse-string) |  | Error message. |






## CancelQuery

`POST /_status/cancel_query/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.CancelQueryRequest-string) |  | ID of gateway node for the query to be canceled.<br><br>TODO(itsbilal): use [(gogoproto.customname) = "NodeID"] below. Need to figure out how to teach grpc-gateway about custom names.<br><br>node_id is a string so that "local" can be used to specify that no forwarding is necessary. |
| query_id | [string](#cockroach.server.serverpb.CancelQueryRequest-string) |  | ID of query to be canceled (converted to string). |
| username | [string](#cockroach.server.serverpb.CancelQueryRequest-string) |  | Username of the user making this cancellation request. This may be omitted if the user is the same as the one issuing the CancelQueryRequest. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| canceled | [bool](#cockroach.server.serverpb.CancelQueryResponse-bool) |  | Whether the cancellation request succeeded and the query was canceled. |
| error | [string](#cockroach.server.serverpb.CancelQueryResponse-string) |  | Error message (accompanied with canceled = false). |







## CancelSession

`POST /_status/cancel_session/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.CancelSessionRequest-string) |  | TODO(abhimadan): use [(gogoproto.customname) = "NodeID"] below. Need to figure out how to teach grpc-gateway about custom names.<br><br>node_id is a string so that "local" can be used to specify that no forwarding is necessary. |
| session_id | [bytes](#cockroach.server.serverpb.CancelSessionRequest-bytes) |  |  |
| username | [string](#cockroach.server.serverpb.CancelSessionRequest-string) |  | Username of the user making this cancellation request. This may be omitted if the user is the same as the one issuing the CancelSessionRequest. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| canceled | [bool](#cockroach.server.serverpb.CancelSessionResponse-bool) |  |  |
| error | [string](#cockroach.server.serverpb.CancelSessionResponse-string) |  |  |







## SpanStats

`POST /_status/span`

SpanStats accepts a key span and node ID, and returns a set of stats
summed from all ranges on the stores on that node which contain keys
in that span. This is designed to compute stats specific to a SQL table:
it will be called with the highest/lowest key for a SQL table, and return
information about the resources on a node used by that table.

#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.SpanStatsRequest-string) |  |  |
| start_key | [bytes](#cockroach.server.serverpb.SpanStatsRequest-bytes) |  |  |
| end_key | [bytes](#cockroach.server.serverpb.SpanStatsRequest-bytes) |  |  |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| range_count | [int32](#cockroach.server.serverpb.SpanStatsResponse-int32) |  |  |
| approximate_disk_bytes | [uint64](#cockroach.server.serverpb.SpanStatsResponse-uint64) |  |  |
| total_stats | [cockroach.storage.enginepb.MVCCStats](#cockroach.server.serverpb.SpanStatsResponse-cockroach.storage.enginepb.MVCCStats) |  |  |







## Stacks

`GET /_status/stacks/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.StacksRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |
| type | [StacksType](#cockroach.server.serverpb.StacksRequest-cockroach.server.serverpb.StacksType) |  |  |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#cockroach.server.serverpb.JSONResponse-bytes) |  |  |







## Profile

`GET /_status/profile/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.ProfileRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |
| type | [ProfileRequest.Type](#cockroach.server.serverpb.ProfileRequest-cockroach.server.serverpb.ProfileRequest.Type) |  | The type of profile to retrieve. |
| seconds | [int32](#cockroach.server.serverpb.ProfileRequest-int32) |  | applies only to Type=CPU, defaults to 30 |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#cockroach.server.serverpb.JSONResponse-bytes) |  |  |







## Metrics

`GET /_status/metrics/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.MetricsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#cockroach.server.serverpb.JSONResponse-bytes) |  |  |







## GetFiles

`GET /_status/files/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.GetFilesRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |
| list_only | [bool](#cockroach.server.serverpb.GetFilesRequest-bool) |  | If list_only is true then the contents of the files will not be populated in the response. Only filenames and sizes will be returned. |
| type | [FileType](#cockroach.server.serverpb.GetFilesRequest-cockroach.server.serverpb.FileType) |  |  |
| patterns | [string](#cockroach.server.serverpb.GetFilesRequest-string) | repeated | Each pattern given is matched with Files of the above type in the node using filepath.Glob(). The patterns only match to filenames and so path separators cannot be used. Example: * will match all files of requested type. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| files | [File](#cockroach.server.serverpb.GetFilesResponse-cockroach.server.serverpb.File) | repeated |  |






<a name="cockroach.server.serverpb.GetFilesResponse-cockroach.server.serverpb.File"></a>
#### File

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#cockroach.server.serverpb.GetFilesResponse-string) |  |  |
| file_size | [int64](#cockroach.server.serverpb.GetFilesResponse-int64) |  |  |
| contents | [bytes](#cockroach.server.serverpb.GetFilesResponse-bytes) |  | Contents may not be populated if only a list of Files are requested. |






## LogFilesList

`GET /_status/logfiles/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.LogFilesListRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| files | [cockroach.util.log.FileInfo](#cockroach.server.serverpb.LogFilesListResponse-cockroach.util.log.FileInfo) | repeated |  |







## LogFile

`GET /_status/logfiles/{node_id}/{file}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.LogFileRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |
| file | [string](#cockroach.server.serverpb.LogFileRequest-string) |  |  |
| redact | [bool](#cockroach.server.serverpb.LogFileRequest-bool) |  | redact, if true, requests redaction of sensitive data away from the retrieved log entries. Only admin users can send a request with redact = false. |
| keep_redactable | [bool](#cockroach.server.serverpb.LogFileRequest-bool) |  | keep_redactable, if true, requests that retrieved entries preserve the redaction markers if any were present in the log files. If false, redaction markers are stripped away. Note that redact = false && redactable = false implies "flat" entries with all sensitive information enclosed and no markers; this is suitable for backward-compatibility with RPC clients from prior the introduction of redactable logs. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entries | [cockroach.util.log.Entry](#cockroach.server.serverpb.LogEntriesResponse-cockroach.util.log.Entry) | repeated |  |







## Logs

`GET /_status/logs/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.LogsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |
| level | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  |
| start_time | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  |
| end_time | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  |
| max | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  |
| pattern | [string](#cockroach.server.serverpb.LogsRequest-string) |  |  |
| redact | [bool](#cockroach.server.serverpb.LogsRequest-bool) |  | redact, if true, requests redaction of sensitive data away from the retrieved log entries. Only admin users can send a request with redact = false. |
| keep_redactable | [bool](#cockroach.server.serverpb.LogsRequest-bool) |  | keep_redactable, if true, requests that retrieved entries preserve the redaction markers if any were present in the log files. If false, redaction markers are stripped away. Note that redact = false && redactable = false implies "flat" entries with all sensitive information enclosed and no markers; this is suitable for backward-compatibility with RPC clients from prior the introduction of redactable logs. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entries | [cockroach.util.log.Entry](#cockroach.server.serverpb.LogEntriesResponse-cockroach.util.log.Entry) | repeated |  |







## ProblemRanges

`GET /_status/problemranges`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.ProblemRangesRequest-string) |  | If left empty, problem ranges for all nodes/stores will be returned. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [int32](#cockroach.server.serverpb.ProblemRangesResponse-int32) |  | NodeID is the node that submitted all the requests. |
| problems_by_node_id | [ProblemRangesResponse.ProblemsByNodeIdEntry](#cockroach.server.serverpb.ProblemRangesResponse-cockroach.server.serverpb.ProblemRangesResponse.ProblemsByNodeIdEntry) | repeated |  |






<a name="cockroach.server.serverpb.ProblemRangesResponse-cockroach.server.serverpb.ProblemRangesResponse.ProblemsByNodeIdEntry"></a>
#### ProblemRangesResponse.ProblemsByNodeIdEntry

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [int32](#cockroach.server.serverpb.ProblemRangesResponse-int32) |  |  |
| value | [ProblemRangesResponse.NodeProblems](#cockroach.server.serverpb.ProblemRangesResponse-cockroach.server.serverpb.ProblemRangesResponse.NodeProblems) |  |  |





<a name="cockroach.server.serverpb.ProblemRangesResponse-cockroach.server.serverpb.ProblemRangesResponse.NodeProblems"></a>
#### ProblemRangesResponse.NodeProblems

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error_message | [string](#cockroach.server.serverpb.ProblemRangesResponse-string) |  |  |
| unavailable_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  |
| raft_leader_not_lease_holder_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  |
| no_raft_leader_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  |
| no_lease_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  |
| underreplicated_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  |
| overreplicated_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  |
| quiescent_equals_ticking_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  |
| raft_log_too_large_range_ids | [int64](#cockroach.server.serverpb.ProblemRangesResponse-int64) | repeated |  |






## HotRanges

`GET /_status/hotranges`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.HotRangesRequest-string) |  | NodeID indicates which node to query for a hot range report. It is posssible to populate any node ID; if the node receiving the request is not the target node, it will forward the request to the target node.<br><br>If left empty, the request is forwarded to every node in the cluster. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  | NodeID is the node that received the HotRangesRequest and forwarded requests to the selected target node(s). |
| hot_ranges_by_node_id | [HotRangesResponse.HotRangesByNodeIdEntry](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRangesByNodeIdEntry) | repeated | HotRangesByNodeID contains a hot range report for each selected target node ID in the HotRangesRequest. |






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
| error_message | [string](#cockroach.server.serverpb.HotRangesResponse-string) |  | ErrorMessage is set to a non-empty string if this target node was unable to produce a hot range report.<br><br>The contents of this string indicates the cause of the failure. |
| stores | [HotRangesResponse.StoreResponse](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.StoreResponse) | repeated | Stores contains the hot ranges report if no error was encountered. There is one part to the report for each store in the target node. |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.StoreResponse"></a>
#### HotRangesResponse.StoreResponse

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_id | [int32](#cockroach.server.serverpb.HotRangesResponse-int32) |  | StoreID identifies the store for which the report was produced. |
| hot_ranges | [HotRangesResponse.HotRange](#cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRange) | repeated | HotRanges is the hot ranges report for this store on the target node. |





<a name="cockroach.server.serverpb.HotRangesResponse-cockroach.server.serverpb.HotRangesResponse.HotRange"></a>
#### HotRangesResponse.HotRange

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| desc | [cockroach.roachpb.RangeDescriptor](#cockroach.server.serverpb.HotRangesResponse-cockroach.roachpb.RangeDescriptor) |  | Desc is the descriptor of the range for which the report was produced.<br><br>Note: this field is generally RESERVED and will likely be removed or replaced in a later version. See: https://github.com/cockroachdb/cockroach/issues/53212 |
| queries_per_second | [double](#cockroach.server.serverpb.HotRangesResponse-double) |  | QueriesPerSecond is the recent number of queries per second on this range. |






## Range

`GET /_status/range/{range_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| range_id | [int64](#cockroach.server.serverpb.RangeRequest-int64) |  |  |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [int32](#cockroach.server.serverpb.RangeResponse-int32) |  | NodeID is the node that submitted all the requests. |
| range_id | [int64](#cockroach.server.serverpb.RangeResponse-int64) |  |  |
| responses_by_node_id | [RangeResponse.ResponsesByNodeIdEntry](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeResponse.ResponsesByNodeIdEntry) | repeated |  |






<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeResponse.ResponsesByNodeIdEntry"></a>
#### RangeResponse.ResponsesByNodeIdEntry

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [int32](#cockroach.server.serverpb.RangeResponse-int32) |  |  |
| value | [RangeResponse.NodeResponse](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeResponse.NodeResponse) |  |  |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeResponse.NodeResponse"></a>
#### RangeResponse.NodeResponse

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| response | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  |
| error_message | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  |
| infos | [RangeInfo](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeInfo) | repeated |  |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeInfo"></a>
#### RangeInfo

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| span | [PrettySpan](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.PrettySpan) |  |  |
| raft_state | [RaftState](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState) |  |  |
| state | [cockroach.kv.kvserver.storagepb.RangeInfo](#cockroach.server.serverpb.RangeResponse-cockroach.kv.kvserver.storagepb.RangeInfo) |  |  |
| source_node_id | [int32](#cockroach.server.serverpb.RangeResponse-int32) |  |  |
| source_store_id | [int32](#cockroach.server.serverpb.RangeResponse-int32) |  |  |
| error_message | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  |
| lease_history | [cockroach.roachpb.Lease](#cockroach.server.serverpb.RangeResponse-cockroach.roachpb.Lease) | repeated |  |
| problems | [RangeProblems](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeProblems) |  |  |
| stats | [RangeStatistics](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeStatistics) |  |  |
| latches_local | [cockroach.kv.kvserver.storagepb.LatchManagerInfo](#cockroach.server.serverpb.RangeResponse-cockroach.kv.kvserver.storagepb.LatchManagerInfo) |  |  |
| latches_global | [cockroach.kv.kvserver.storagepb.LatchManagerInfo](#cockroach.server.serverpb.RangeResponse-cockroach.kv.kvserver.storagepb.LatchManagerInfo) |  |  |
| lease_status | [cockroach.kv.kvserver.storagepb.LeaseStatus](#cockroach.server.serverpb.RangeResponse-cockroach.kv.kvserver.storagepb.LeaseStatus) |  |  |
| quiescent | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  |
| ticking | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.PrettySpan"></a>
#### PrettySpan

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| start_key | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  |
| end_key | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState"></a>
#### RaftState

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| replica_id | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  |
| hard_state | [raftpb.HardState](#cockroach.server.serverpb.RangeResponse-raftpb.HardState) |  |  |
| lead | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  | Lead is part of Raft's SoftState. |
| state | [string](#cockroach.server.serverpb.RangeResponse-string) |  | State is part of Raft's SoftState. It's not an enum because this is primarily for ui consumption and there are issues associated with them. |
| applied | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  |
| progress | [RaftState.ProgressEntry](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState.ProgressEntry) | repeated |  |
| lead_transferee | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState.ProgressEntry"></a>
#### RaftState.ProgressEntry

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  |
| value | [RaftState.Progress](#cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState.Progress) |  |  |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RaftState.Progress"></a>
#### RaftState.Progress

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| match | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  |
| next | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  |
| state | [string](#cockroach.server.serverpb.RangeResponse-string) |  |  |
| paused | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  |
| pending_snapshot | [uint64](#cockroach.server.serverpb.RangeResponse-uint64) |  |  |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeProblems"></a>
#### RangeProblems

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| unavailable | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  |
| leader_not_lease_holder | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  |
| no_raft_leader | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  |
| underreplicated | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  |
| overreplicated | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  |
| no_lease | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  |  |
| quiescent_equals_ticking | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  | Quiescent ranges do not tick by definition, but we track this in two different ways and suspect that they're getting out of sync. If the replica's quiescent flag doesn't agree with the store's list of replicas that are ticking, warn about it. |
| raft_log_too_large | [bool](#cockroach.server.serverpb.RangeResponse-bool) |  | When the raft log is too large, it can be a symptom of other issues. |





<a name="cockroach.server.serverpb.RangeResponse-cockroach.server.serverpb.RangeStatistics"></a>
#### RangeStatistics

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| queries_per_second | [double](#cockroach.server.serverpb.RangeResponse-double) |  | Note that queries per second will only be known by the leaseholder. All other replicas will report it as 0. |
| writes_per_second | [double](#cockroach.server.serverpb.RangeResponse-double) |  |  |






## Diagnostics

`GET /_status/diagnostics/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.DiagnosticsRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |







#### Response Parameters



## Stores

`GET /_status/stores/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.StoresRequest-string) |  | node_id is a string so that "local" can be used to specify that no forwarding is necessary. |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stores | [StoreDetails](#cockroach.server.serverpb.StoresResponse-cockroach.server.serverpb.StoreDetails) | repeated |  |






<a name="cockroach.server.serverpb.StoresResponse-cockroach.server.serverpb.StoreDetails"></a>
#### StoreDetails

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store_id | [int32](#cockroach.server.serverpb.StoresResponse-int32) |  |  |
| encryption_status | [bytes](#cockroach.server.serverpb.StoresResponse-bytes) |  | encryption_status is a serialized ccl/storageccl/engineccl/enginepbccl/stats.go::EncryptionStatus protobuf. |
| total_files | [uint64](#cockroach.server.serverpb.StoresResponse-uint64) |  | Basic file stats when encryption is enabled. Total files/bytes. |
| total_bytes | [uint64](#cockroach.server.serverpb.StoresResponse-uint64) |  |  |
| active_key_files | [uint64](#cockroach.server.serverpb.StoresResponse-uint64) |  | Files/bytes using the active data key. |
| active_key_bytes | [uint64](#cockroach.server.serverpb.StoresResponse-uint64) |  |  |






## Statements

`GET /_status/statements`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.StatementsRequest-string) |  |  |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| statements | [StatementsResponse.CollectedStatementStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.CollectedStatementStatistics) | repeated |  |
| last_reset | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementsResponse-google.protobuf.Timestamp) |  | Timestamp of the last stats reset. |
| internal_app_name_prefix | [string](#cockroach.server.serverpb.StatementsResponse-string) |  | If set and non-empty, indicates the prefix to application_name used for statements/queries issued internally by CockroachDB. |
| transactions | [StatementsResponse.ExtendedCollectedTransactionStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedCollectedTransactionStatistics) | repeated | Transactions is transaction-level statistics for the collection of statements in this response. |






<a name="cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.CollectedStatementStatistics"></a>
#### StatementsResponse.CollectedStatementStatistics

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [StatementsResponse.ExtendedStatementStatisticsKey](#cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedStatementStatisticsKey) |  |  |
| id | [string](#cockroach.server.serverpb.StatementsResponse-string) |  |  |
| stats | [cockroach.sql.StatementStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.sql.StatementStatistics) |  |  |





<a name="cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedStatementStatisticsKey"></a>
#### StatementsResponse.ExtendedStatementStatisticsKey

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key_data | [cockroach.sql.StatementStatisticsKey](#cockroach.server.serverpb.StatementsResponse-cockroach.sql.StatementStatisticsKey) |  |  |
| node_id | [int32](#cockroach.server.serverpb.StatementsResponse-int32) |  |  |





<a name="cockroach.server.serverpb.StatementsResponse-cockroach.server.serverpb.StatementsResponse.ExtendedCollectedTransactionStatistics"></a>
#### StatementsResponse.ExtendedCollectedTransactionStatistics

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stats_data | [cockroach.sql.CollectedTransactionStatistics](#cockroach.server.serverpb.StatementsResponse-cockroach.sql.CollectedTransactionStatistics) |  |  |
| node_id | [int32](#cockroach.server.serverpb.StatementsResponse-int32) |  |  |






## CreateStatementDiagnosticsReport

`POST /_status/stmtdiagreports`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| statement_fingerprint | [string](#cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest-string) |  |  |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| report | [StatementDiagnosticsReport](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-cockroach.server.serverpb.StatementDiagnosticsReport) |  |  |






<a name="cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-cockroach.server.serverpb.StatementDiagnosticsReport"></a>
#### StatementDiagnosticsReport

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int64](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-int64) |  |  |
| completed | [bool](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-bool) |  |  |
| statement_fingerprint | [string](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-string) |  |  |
| statement_diagnostics_id | [int64](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-int64) |  |  |
| requested_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse-google.protobuf.Timestamp) |  |  |






## StatementDiagnosticsRequests

`GET /_status/stmtdiagreports`



#### Request Parameters










#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reports | [StatementDiagnosticsReport](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-cockroach.server.serverpb.StatementDiagnosticsReport) | repeated |  |






<a name="cockroach.server.serverpb.StatementDiagnosticsReportsResponse-cockroach.server.serverpb.StatementDiagnosticsReport"></a>
#### StatementDiagnosticsReport

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int64](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-int64) |  |  |
| completed | [bool](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-bool) |  |  |
| statement_fingerprint | [string](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-string) |  |  |
| statement_diagnostics_id | [int64](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-int64) |  |  |
| requested_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementDiagnosticsReportsResponse-google.protobuf.Timestamp) |  |  |






## StatementDiagnostics

`GET /_status/stmtdiag/{statement_diagnostics_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| statement_diagnostics_id | [int64](#cockroach.server.serverpb.StatementDiagnosticsRequest-int64) |  |  |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| diagnostics | [StatementDiagnostics](#cockroach.server.serverpb.StatementDiagnosticsResponse-cockroach.server.serverpb.StatementDiagnostics) |  |  |






<a name="cockroach.server.serverpb.StatementDiagnosticsResponse-cockroach.server.serverpb.StatementDiagnostics"></a>
#### StatementDiagnostics

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int64](#cockroach.server.serverpb.StatementDiagnosticsResponse-int64) |  |  |
| statement_fingerprint | [string](#cockroach.server.serverpb.StatementDiagnosticsResponse-string) |  |  |
| collected_at | [google.protobuf.Timestamp](#cockroach.server.serverpb.StatementDiagnosticsResponse-google.protobuf.Timestamp) |  |  |
| trace | [string](#cockroach.server.serverpb.StatementDiagnosticsResponse-string) |  |  |






## JobRegistryStatus

`GET /_status/job_registry/{node_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#cockroach.server.serverpb.JobRegistryStatusRequest-string) |  |  |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [int32](#cockroach.server.serverpb.JobRegistryStatusResponse-int32) |  |  |
| running_jobs | [JobRegistryStatusResponse.Job](#cockroach.server.serverpb.JobRegistryStatusResponse-cockroach.server.serverpb.JobRegistryStatusResponse.Job) | repeated |  |






<a name="cockroach.server.serverpb.JobRegistryStatusResponse-cockroach.server.serverpb.JobRegistryStatusResponse.Job"></a>
#### JobRegistryStatusResponse.Job

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int64](#cockroach.server.serverpb.JobRegistryStatusResponse-int64) |  |  |






## JobStatus

`GET /_status/job/{job_id}`



#### Request Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [int64](#cockroach.server.serverpb.JobStatusRequest-int64) |  |  |







#### Response Parameters




| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job | [cockroach.sql.jobs.jobspb.Job](#cockroach.server.serverpb.JobStatusResponse-cockroach.sql.jobs.jobspb.Job) |  |  |







