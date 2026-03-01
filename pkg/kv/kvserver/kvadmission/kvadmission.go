// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package kvadmission is the integration layer between KV and admission
// control.
package kvadmission

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/replica_rac2"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// elasticCPUDurationPerExportRequest controls how many CPU tokens are allotted
// for each export request.
var elasticCPUDurationPerExportRequest = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kvadmission.elastic_cpu.duration_per_export_request",
	"controls how many CPU tokens are allotted for each export request",
	admission.MaxElasticCPUDuration,
	settings.DurationInRange(admission.MinElasticCPUDuration, admission.MaxElasticCPUDuration),
)

// elasticCPUDurationPerInternalLowPriRead controls how many CPU tokens are
// allotted for each internally submitted low priority read request.
var elasticCPUDurationPerInternalLowPriRead = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kvadmission.elastic_cpu.duration_per_low_pri_read",
	"controls how many CPU tokens are allotted for each internally submitted low priority read request",
	10*time.Millisecond,
	settings.DurationInRange(admission.MinElasticCPUDuration, admission.MaxElasticCPUDuration),
)

// ElasticAdmission determines whether internally
// submitted bulk pri requests integrate with elastic CPU control.
var ElasticAdmission = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kvadmission.elastic_control_bulk_low_priority.enabled",
	"determines whether the all low bulk priority requests integrate with elastic CPU control",
	true,
)

// elasticCPUDurationPerRangefeedScanUnit controls how many CPU tokens are
// allotted for each unit of work during rangefeed catchup scans. Only takes
// effect if kvadmission.rangefeed_catchup_scan_elastic_control.enabled is set.
var elasticCPUDurationPerRangefeedScanUnit = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kvadmission.elastic_cpu.duration_per_rangefeed_scan_unit",
	"controls how many CPU tokens are allotted for each unit of work during rangefeed catchup scans",
	admission.MaxElasticCPUDuration,
	settings.DurationInRange(admission.MinElasticCPUDuration, admission.MaxElasticCPUDuration),
)

// rangefeedCatchupScanElasticControlEnabled determines whether rangefeed catch
// up scans integrate with elastic CPU control.
var rangefeedCatchupScanElasticControlEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kvadmission.rangefeed_catchup_scan_elastic_control.enabled",
	"determines whether rangefeed catchup scans integrate with the elastic CPU control",
	true,
)

// ProvisionedBandwidth set a value of the provisioned
// bandwidth for each store in the cluster.
var ProvisionedBandwidth = settings.RegisterByteSizeSetting(
	settings.SystemOnly, "kvadmission.store.provisioned_bandwidth",
	"if set to a non-zero value, this is used as the provisioned bandwidth (in bytes/s), for "+
		"each store. It can be overridden on a per-store basis using the --store flag. Note that "+
		"setting the provisioned bandwidth to a positive value may enable disk bandwidth based "+
		"admission control, since admission.disk_bandwidth_tokens.elastic.enabled defaults to true",
	0,
	settings.WithPublic)

// FlowTokenDropInterval determines the frequency at which we check for pending
// flow token dispatches to nodes we're no longer connected to, in order to drop
// them.
var FlowTokenDropInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kvadmission.flow_token.drop_interval",
	"the interval at which the raft transport checks for pending flow token dispatches "+
		"to nodes we're no longer connected to, in order to drop them; set to 0 to disable the mechanism",
	30*time.Second,
)

// FlowTokenDispatchInterval determines the frequency at which we check for
// pending flow token dispatches from idle connections, in order to deliver
// them.
var FlowTokenDispatchInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kvadmission.flow_token.dispatch_interval",
	"the interval at which the raft transport checks for pending flow token dispatches "+
		"from idle connections and delivers them",
	time.Second,
	settings.PositiveDuration, settings.NonNegativeDurationWithMaximum(time.Minute),
)

// FlowTokenDispatchMaxBytes determines the maximum number of bytes of dispatch
// messages that are annotated onto a single RaftTransport message.
var FlowTokenDispatchMaxBytes = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kvadmission.flow_control.dispatch.max_bytes",
	"limits the size of flow control dispatch messages being attached to a single raft message",
	64<<20,                         // 64 MB
	settings.IntWithMinimum(1<<20), // 1 MB
)

// ConnectedStoreExpiration controls how long the RaftTransport layers considers
// a stream connected without it having observed any messages from it.
var ConnectedStoreExpiration = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kvadmission.raft_transport.connected_store_expiration",
	"the interval at which the raft transport prunes its set of connected stores; set to 0 to disable the mechanism",
	5*time.Minute,
)

// useRangeTenantIDForNonAdminEnabled determines whether the range's tenant ID
// is used by admission control when called by the system tenant. When false,
// the requester's tenant ID (i.e., the system tenant ID) is used.
var useRangeTenantIDForNonAdminEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kvadmission.use_range_tenant_id_for_non_admin.enabled",
	"when true, and the caller is the system tenant, the tenantID used by admission control "+
		"for non-admin requests is overridden to the range's tenantID",
	true,
)

// Controller provides admission control for the KV layer.
type Controller interface {
	// AdmitKVWork must be called before performing KV work.
	// BatchRequest.AdmissionHeader and BatchRequest.Replica.StoreID must be
	// populated for admission to work correctly. The requestTenantID represents
	// the authenticated caller and must be populated. The rangeTenantID
	// represents the tenant of the range on which the work is being performed
	// -- in rare cases it may be unpopulated.
	//
	// If err is non-nil, the returned handle can be ignored. If err is nil,
	// AdmittedKVWorkDone must be called after the KV work is done executing.
	AdmitKVWork(
		_ context.Context, requestTenantID roachpb.TenantID, rangeTenantID roachpb.TenantID,
		_ *kvpb.BatchRequest,
	) (Handle, error)
	// AdmittedKVWorkDone is called after the admitted KV work is done
	// executing.
	AdmittedKVWorkDone(Handle, *StoreWriteBytes)
	// AdmitRangefeedRequest must be called before serving rangefeed requests.
	// If enabled, it returns a non-nil Pacer that's to be used within rangefeed
	// catchup scans (typically CPU-intensive and affecting scheduling
	// latencies).
	AdmitRangefeedRequest(roachpb.TenantID, *kvpb.RangeFeedRequest) *admission.Pacer
	// SetTenantWeightProvider is used to set the provider that will be
	// periodically polled for weights. The stopper should be used to terminate
	// the periodic polling.
	SetTenantWeightProvider(TenantWeightProvider, *stop.Stopper)
	// SnapshotIngestedOrWritten informs admission control about a range
	// snapshot ingestion or a range snapshot written as a normal write.
	// writeBytes should roughly correspond to the size of the write when
	// flushed to a sstable.
	SnapshotIngestedOrWritten(_ roachpb.StoreID, _ pebble.IngestOperationStats, writeBytes uint64)
	// FollowerStoreWriteBytes informs admission control about writes
	// replicated to a raft follower, that have not been subject to admission
	// control.
	FollowerStoreWriteBytes(roachpb.StoreID, FollowerStoreWriteBytes)
	replica_rac2.ACWorkQueue
	// GetSnapshotQueue returns the SnapshotQueue which is used for ingesting raft
	// snapshots.
	GetSnapshotQueue(roachpb.StoreID) *admission.SnapshotQueue
	// GetProvisionedBandwidth returns the provisioned disk bandwidth for the
	// given store in bytes/second, or 0 if the store is not found or bandwidth
	// is not configured.
	GetProvisionedBandwidth(roachpb.StoreID) int64
}

// TenantWeightProvider can be periodically asked to provide the tenant
// weights.
type TenantWeightProvider interface {
	GetTenantWeights() TenantWeights
}

// TenantWeights contains the various tenant weights.
type TenantWeights struct {
	// Node is the node level tenant ID => weight.
	Node map[uint64]uint32
	// Stores contains the per-store tenant weights.
	Stores []TenantWeightsForStore
}

// TenantWeightsForStore contains the tenant weights for a store.
type TenantWeightsForStore struct {
	roachpb.StoreID
	// Weights is tenant ID => weight.
	Weights map[uint64]uint32
}

// controllerImpl implements Controller interface.
type controllerImpl struct {
	nodeID *base.NodeIDContainer

	// Admission control queues and coordinators. All three should be nil or
	// non-nil.
	cpuGrantCoords             *admission.CPUGrantCoordinators
	storeGrantCoords           *admission.StoreGrantCoordinators
	elasticCPUGrantCoordinator *admission.ElasticCPUGrantCoordinator
	kvflowHandles              kvflowcontrol.ReplicationAdmissionHandles

	settings *cluster.Settings
	every    log.EveryN
}

var _ Controller = &controllerImpl{}

// Handle groups data around some piece admitted work. Depending on the
// type of work, it holds (a) references to specific work queues, (b) state
// needed to inform said work queues of what work was done after the fact, and
// (c) information around how much work a request is allowed to do (used for
// cooperative scheduling with elastic CPU granters).
type Handle struct {
	tenantID             roachpb.TenantID
	storeAdmissionQ      *admission.StoreWorkQueue
	storeWorkHandle      admission.StoreWorkHandle
	elasticCPUWorkHandle *admission.ElasticCPUWorkHandle
	raftAdmissionMeta    *kvflowcontrolpb.RaftAdmissionMeta
	ghToUnpause          *admission.GoroutineCPUHandle

	cpuStart            time.Duration
	cpuAdmissionQueue   *admission.WorkQueue
	cpuKVAdmissionQResp admission.AdmitResponse
}

// AnnotateCtx annotates the given context with the ElasticCPUWorkHandle,
// which is used deep in the storage layer (e.g., mvccExportToWriter) to pace
// CPU intensive operations.
func (h *Handle) AnnotateCtx(ctx context.Context) context.Context {
	if h.elasticCPUWorkHandle != nil {
		ctx = admission.ContextWithElasticCPUWorkHandle(ctx, h.elasticCPUWorkHandle)
	}
	return ctx
}

// AdmissionInfo returns admission control state that should be plumbed
// through the KV layer via explicit function parameters.
func (h *Handle) AdmissionInfo() AdmissionInfo {
	return AdmissionInfo{
		RaftAdmissionMeta: h.raftAdmissionMeta,
	}
}

// AdmissionInfo contains admission control state that is plumbed through the
// KV layer via explicit function parameters rather than context.
type AdmissionInfo struct {
	// RaftAdmissionMeta is the metadata used for replication flow control.
	// It is populated for requests that will be proposed to Raft.
	RaftAdmissionMeta *kvflowcontrolpb.RaftAdmissionMeta
}

// MakeController returns a Controller. All three parameters must together be
// nil or non-nil.
func MakeController(
	nodeID *base.NodeIDContainer,
	cpuGrantCoords *admission.CPUGrantCoordinators,
	elasticCPUGrantCoordinator *admission.ElasticCPUGrantCoordinator,
	storeGrantCoords *admission.StoreGrantCoordinators,
	kvflowHandles kvflowcontrol.ReplicationAdmissionHandles,
	settings *cluster.Settings,
) Controller {
	return &controllerImpl{
		nodeID:                     nodeID,
		cpuGrantCoords:             cpuGrantCoords,
		storeGrantCoords:           storeGrantCoords,
		elasticCPUGrantCoordinator: elasticCPUGrantCoordinator,
		kvflowHandles:              kvflowHandles,
		settings:                   settings,
		every:                      log.Every(10 * time.Second),
	}
}

// AdmitKVWork implements the Controller interface.
//
// TODO(irfansharif): There's a fair bit happening here and there's no test
// coverage. Fix that.
func (n *controllerImpl) AdmitKVWork(
	ctx context.Context,
	requestTenantID roachpb.TenantID,
	rangeTenantID roachpb.TenantID,
	ba *kvpb.BatchRequest,
) (_ Handle, retErr error) {
	if n.cpuGrantCoords == nil {
		return Handle{}, nil
	}
	admissionInfo := workInfoForBatch(n.settings, requestTenantID, rangeTenantID, ba)
	ah := Handle{tenantID: admissionInfo.TenantID}
	admissionEnabled := true
	// Don't subject HeartbeatTxnRequest to the storeAdmissionQ. Even though
	// it would bypass admission, it would consume a slot. When writes are
	// throttled, we start generating more txn heartbeats, which then consume
	// all the slots, causing no useful work to happen. We do want useful work
	// to continue even when throttling since there are often significant
	// number of tokens available.
	if ba.IsWrite() && !ba.IsSingleHeartbeatTxnRequest() {
		var admitted bool
		attemptFlowControl := kvflowcontrol.Enabled.Get(&n.settings.SV)
		if attemptFlowControl && !admissionInfo.BypassAdmission {
			kvflowHandle, found := n.kvflowHandles.LookupReplicationAdmissionHandle(ba.RangeID)
			if !found {
				return Handle{}, nil
			}
			var err error
			admitted, err = kvflowHandle.Admit(
				ctx, admissionInfo.Priority, timeutil.FromUnixNanos(admissionInfo.CreateTime))
			if err != nil {
				return Handle{}, err
			} else if admitted {
				// NB: It's possible for us to be waiting for available flow tokens
				// for a different set of streams that the ones we'll eventually
				// deduct tokens from, if the range experiences a split between now
				// and the point of deduction. That's ok, there's no strong
				// synchronization needed between these two points.
				ah.raftAdmissionMeta = &kvflowcontrolpb.RaftAdmissionMeta{
					// NOTE: The priority is identical for v1 and v2, a
					// admissionpb.WorkPriority,  until we encode the command in
					// replica_raft, where if the range is using racv2 encoding we will
					// convert the priority to a raftpb.Priority.
					AdmissionPriority:   int32(admissionInfo.Priority),
					AdmissionCreateTime: admissionInfo.CreateTime,
					AdmissionOriginNode: n.nodeID.Get(),
				}
			}
		}
		// If flow control is disabled or if work bypasses flow control, we still
		// subject it above-raft, leaseholder-only IO admission control.
		if !attemptFlowControl || !admitted {
			storeAdmissionQ := n.storeGrantCoords.TryGetQueueForStore(ba.Replica.StoreID)
			if storeAdmissionQ != nil {
				//  NB: Even though we would know here we're bypassing admission (via
				//  `bypassAdmission`), we still have to explicitly invoke `.Admit()`.
				//  We do it for correct token accounting (i.e. we deduct tokens without
				//  blocking).
				storeWorkHandle, err := storeAdmissionQ.Admit(
					ctx, admission.StoreWriteWorkInfo{WorkInfo: admissionInfo})
				if err != nil {
					return Handle{}, err
				}
				admissionEnabled = storeWorkHandle.UseAdmittedWorkDone()
				if admissionEnabled {
					defer func() {
						if retErr != nil {
							// No bytes were written.
							_ = storeAdmissionQ.AdmittedWorkDone(ah.storeWorkHandle, admission.StoreWorkDoneInfo{})
						}
					}()
					ah.storeAdmissionQ, ah.storeWorkHandle = storeAdmissionQ, storeWorkHandle
				}
			}
		}
	}
	cpuAdmissionQ := n.cpuGrantCoords.GetKVWorkQueue(admissionInfo.TenantID.IsSystem())
	if admissionEnabled {
		// Bulk jobs such as backups or row-level TTL issue KV requests with a
		// priority of admissionpb.BulkNormalPri or lower; these are eligible for
		// "elastic" CPU control, to adaptively utilize more or less CPU capacity
		// depending on observed scheduling latencies. Requests with priority above
		// admissionpb.BulkNormalPri uses the normal slots mechanism.
		//
		// NB: Background QoS in SQL uses UserLowPri. UserLowPri is currently below
		// BulkNormalPri, meaning requests sent by background SQL are eligible
		// for elastic AC. This is intentional: elastic AC is designed
		// specifically to run "background" work elastically at a rate that doesn't
		// impact the cluster.
		if admissionInfo.Priority <= admissionpb.BulkNormalPri && ElasticAdmission.Get(&n.settings.SV) {
			var admitDuration time.Duration
			if ba.IsSingleExportRequest() {
				admitDuration = elasticCPUDurationPerExportRequest.Get(&n.settings.SV)
			} else { // isInternalLowPriRead
				admitDuration = elasticCPUDurationPerInternalLowPriRead.Get(&n.settings.SV)
			}

			// TODO(irfansharif): For export requests it's possible to preempt,
			// i.e. once the CPU slice is used up we terminate the work. We
			// don't do this for the general case of low priority internal
			// reads, so in some sense, the integration is incomplete. This is
			// probably harmless.
			elasticWorkHandle, err := n.elasticCPUGrantCoordinator.ElasticCPUWorkQueue.Admit(
				// NB: yieldInHandle is always false at this point, since requests may
				// subsequently acquire latches, and requests holding latches should
				// never yield. Later code, that knows about the state of latching,
				// can revise this decision.
				ctx, admitDuration, admissionInfo, false,
			)
			if err != nil {
				return Handle{}, err
			}
			ah.elasticCPUWorkHandle = elasticWorkHandle
			defer func() {
				if retErr != nil {
					// No elastic work was done.
					n.elasticCPUGrantCoordinator.ElasticCPUWorkQueue.AdmittedWorkDone(ah.elasticCPUWorkHandle)
				}
			}()
		} else {
			// Use the slots-based mechanism for everything else.
			resp, err := cpuAdmissionQ.Admit(ctx, admissionInfo)
			if err != nil {
				return Handle{}, err
			}
			if resp.Enabled {
				// We include the time to do other activities like intent resolution,
				// since it is acceptable to charge them to the tenant.
				ah.cpuStart = grunning.Time()
			}
			ah.cpuAdmissionQueue = cpuAdmissionQ
			ah.cpuKVAdmissionQResp = resp
		}
	}
	// Pause CPU measurement for SQL work if it is happening locally on this
	// goroutine.
	//
	// NB: if we never reach this point in the function, either there is an
	// error, or some aspect of admission is disabled (which is never the case
	// in production). In the latter case, we will not pause measurement, which
	// we deem acceptable.
	//
	// TODO(sumeer): improve the structure of AdmitKVWork to make the behavior
	// easier to understand.
	sqlHandle := admission.SQLCPUHandleFromContext(ctx)
	if sqlHandle != nil {
		ah.ghToUnpause = sqlHandle.RegisterGoroutine()
		ah.ghToUnpause.PauseMeasuring()
	}
	return ah, nil
}

// AdmittedKVWorkDone implements the Controller interface.
func (n *controllerImpl) AdmittedKVWorkDone(ah Handle, writeBytes *StoreWriteBytes) {
	n.elasticCPUGrantCoordinator.ElasticCPUWorkQueue.AdmittedWorkDone(ah.elasticCPUWorkHandle)
	if ah.cpuKVAdmissionQResp.Enabled {
		cpuTime := grunning.Time() - ah.cpuStart
		if cpuTime < 0 {
			// We sometimes see cpuTime to be negative. We use 1ns here, arbitrarily.
			// This issue is tracked by
			// https://github.com/cockroachdb/cockroach/issues/126681.
			if buildutil.CrdbTestBuild {
				log.KvDistribution.Warningf(context.Background(), "grunning.Time() should be non-decreasing, cpuTime=%s", cpuTime)
			}
			cpuTime = 1
		}
		ah.cpuAdmissionQueue.AdmittedWorkDone(ah.cpuKVAdmissionQResp, cpuTime)
	}
	if ah.storeAdmissionQ != nil {
		var doneInfo admission.StoreWorkDoneInfo
		if writeBytes != nil {
			doneInfo = admission.StoreWorkDoneInfo(*writeBytes)
		}
		err := ah.storeAdmissionQ.AdmittedWorkDone(ah.storeWorkHandle, doneInfo)
		if err != nil {
			// This shouldn't be happening.
			if buildutil.CrdbTestBuild {
				log.KvDistribution.Fatalf(context.Background(), "%s", errors.WithAssertionFailure(err))
			}
			if n.every.ShouldLog() {
				log.KvDistribution.Errorf(context.Background(), "%s", err)
			}
		}
	}
	if ah.ghToUnpause != nil {
		ah.ghToUnpause.UnpauseMeasuring()
	}
}

// AdmitRangefeedRequest implements the Controller interface.
func (n *controllerImpl) AdmitRangefeedRequest(
	tenantID roachpb.TenantID, request *kvpb.RangeFeedRequest,
) *admission.Pacer {
	if !rangefeedCatchupScanElasticControlEnabled.Get(&n.settings.SV) {
		return nil
	}

	return n.elasticCPUGrantCoordinator.NewPacer(
		elasticCPUDurationPerRangefeedScanUnit.Get(&n.settings.SV),
		admission.WorkInfo{
			TenantID:        tenantID,
			Priority:        admissionpb.WorkPriority(request.AdmissionHeader.Priority),
			CreateTime:      request.AdmissionHeader.CreateTime,
			BypassAdmission: false,
		})
}

// SetTenantWeightProvider implements the Controller interface.
func (n *controllerImpl) SetTenantWeightProvider(
	provider TenantWeightProvider, stopper *stop.Stopper,
) {
	// TODO(irfansharif): Use a stopper here instead.
	go func() {
		const weightCalculationPeriod = 10 * time.Minute
		ticker := time.NewTicker(weightCalculationPeriod)
		// Used for short-circuiting the weights calculation if all weights are
		// disabled.
		allWeightsDisabled := false
		for {
			select {
			case <-ticker.C:
				kvDisabled := !admission.KVTenantWeightsEnabled.Get(&n.settings.SV)
				kvStoresDisabled := !admission.KVStoresTenantWeightsEnabled.Get(&n.settings.SV)
				if allWeightsDisabled && kvDisabled && kvStoresDisabled {
					// Have already transitioned to disabled, so noop.
					continue
				}
				weights := provider.GetTenantWeights()
				if kvDisabled {
					weights.Node = nil
				}
				n.cpuGrantCoords.SetTenantWeights(weights.Node)
				n.elasticCPUGrantCoordinator.ElasticCPUWorkQueue.SetTenantWeights(weights.Node)

				for _, storeWeights := range weights.Stores {
					q := n.storeGrantCoords.TryGetQueueForStore(storeWeights.StoreID)
					if q != nil {
						if kvStoresDisabled {
							storeWeights.Weights = nil
						}
						q.SetTenantWeights(storeWeights.Weights)
					}
				}
				allWeightsDisabled = kvDisabled && kvStoresDisabled
			case <-stopper.ShouldQuiesce():
				ticker.Stop()
				return
			}
		}
	}()
}

// SnapshotIngestedOrWritten implements the Controller interface.
func (n *controllerImpl) SnapshotIngestedOrWritten(
	storeID roachpb.StoreID, ingestStats pebble.IngestOperationStats, writeBytes uint64,
) {
	storeAdmissionQ := n.storeGrantCoords.TryGetQueueForStore(storeID)
	if storeAdmissionQ == nil {
		return
	}
	storeAdmissionQ.StatsToIgnore(ingestStats, writeBytes)
}

// FollowerStoreWriteBytes implements the Controller interface.
func (n *controllerImpl) FollowerStoreWriteBytes(
	storeID roachpb.StoreID, followerWriteBytes FollowerStoreWriteBytes,
) {
	if followerWriteBytes.WriteBytes == 0 && followerWriteBytes.IngestedBytes == 0 {
		return
	}
	storeAdmissionQ := n.storeGrantCoords.TryGetQueueForStore(storeID)
	if storeAdmissionQ == nil {
		return
	}
	storeAdmissionQ.BypassedWorkDone(
		followerWriteBytes.NumEntries, followerWriteBytes.StoreWorkDoneInfo)
}

var _ replica_rac2.ACWorkQueue = &controllerImpl{}

var logUnableToFindQueueForStore = log.Every(time.Second)

// Admit implements replica_rac2.ACWorkQueue. It is only used for the RACv2 protocol.
func (n *controllerImpl) Admit(ctx context.Context, entry replica_rac2.EntryForAdmission) bool {
	storeAdmissionQ := n.storeGrantCoords.TryGetQueueForStore(entry.StoreID)
	if storeAdmissionQ == nil {
		// This can happen during node startup before
		// StoreGrantCoordinators.SetPebbleMetricsProvider has run.
		if logUnableToFindQueueForStore.ShouldLog() {
			log.KvDistribution.VInfof(ctx, 1, "unable to find queue for store: %s", entry.StoreID)
		}
		return false // nothing to do
	}

	if entry.RequestedCount == 0 {
		log.KvDistribution.Fatal(ctx, "found (unexpected) empty raft command for below-raft admission")
	}
	wi := admission.WorkInfo{
		TenantID:        entry.TenantID,
		Priority:        entry.Priority,
		CreateTime:      entry.CreateTime,
		BypassAdmission: false,
		RequestedCount:  entry.RequestedCount,
	}
	wi.ReplicatedWorkInfo = admission.ReplicatedWorkInfo{
		Enabled:    true,
		RangeID:    entry.RangeID,
		ReplicaID:  entry.ReplicaID,
		LeaderTerm: entry.CallbackState.Mark.Term,
		LogPosition: admission.LogPosition{
			Term:  0, // Ignored by callback in RACv2.
			Index: entry.CallbackState.Mark.Index,
		},
		RaftPri:  entry.CallbackState.Priority,
		Ingested: entry.Ingested,
	}

	handle, err := storeAdmissionQ.Admit(ctx, admission.StoreWriteWorkInfo{
		WorkInfo: wi,
	})
	if err != nil {
		log.KvDistribution.Errorf(ctx, "error while admitting to store admission queue: %v", err)
		return false
	}
	if handle.UseAdmittedWorkDone() {
		log.KvDistribution.Fatalf(ctx, "unexpected handle.UseAdmittedWorkDone")
	}
	return true
}

func (n *controllerImpl) GetSnapshotQueue(storeID roachpb.StoreID) *admission.SnapshotQueue {
	sq := n.storeGrantCoords.TryGetSnapshotQueueForStore(storeID)
	if sq == nil {
		return nil
	}
	return sq.(*admission.SnapshotQueue)
}

func (n *controllerImpl) GetProvisionedBandwidth(storeID roachpb.StoreID) int64 {
	return n.storeGrantCoords.TryGetProvisionedBandwidthForStore(storeID)
}

// FollowerStoreWriteBytes captures stats about writes done to a store by a
// replica that is not the leaseholder. These are used for admission control.
type FollowerStoreWriteBytes struct {
	NumEntries int64
	admission.StoreWorkDoneInfo
}

// Merge follower store write statistics using the given data.
func (f *FollowerStoreWriteBytes) Merge(from FollowerStoreWriteBytes) {
	f.NumEntries += from.NumEntries
	f.WriteBytes += from.WriteBytes
	f.IngestedBytes += from.IngestedBytes
}

// StoreWriteBytes aliases admission.StoreWorkDoneInfo, since the notion of
// "work is done" is specific to admission control and doesn't need to leak
// everywhere.
type StoreWriteBytes admission.StoreWorkDoneInfo

var storeWriteBytesPool = sync.Pool{
	New: func() interface{} { return &StoreWriteBytes{} },
}

// NewStoreWriteBytes constructs a new StoreWriteBytes.
func NewStoreWriteBytes() *StoreWriteBytes {
	wb := storeWriteBytesPool.Get().(*StoreWriteBytes)
	*wb = StoreWriteBytes{}
	return wb
}

// Release returns the *StoreWriteBytes to the pool.
func (wb *StoreWriteBytes) Release() {
	if wb == nil {
		return
	}
	storeWriteBytesPool.Put(wb)
}

func workInfoForBatch(
	st *cluster.Settings,
	requestTenantID roachpb.TenantID,
	rangeTenantID roachpb.TenantID,
	ba *kvpb.BatchRequest,
) admission.WorkInfo {
	bypassAdmission := ba.IsAdmin()
	source := ba.AdmissionHeader.Source
	tenantID := requestTenantID
	if requestTenantID.IsSystem() {
		if useRangeTenantIDForNonAdminEnabled.Get(&st.SV) && !bypassAdmission &&
			rangeTenantID.IsSet() {
			tenantID = rangeTenantID
		}
		// Else, either it is an admin request (common), or rangeTenantID is not
		// set (rare), or the cluster setting is disabled, so continue using the
		// SystemTenantID.
	} else {
		// Request is from a SQL node.
		bypassAdmission = false
		source = kvpb.AdmissionHeader_FROM_SQL
	}
	if source == kvpb.AdmissionHeader_OTHER {
		bypassAdmission = true
	}
	// TODO(abaptist): Revisit and deprecate this setting in v23.1.
	if admission.KVBulkOnlyAdmissionControlEnabled.Get(&st.SV) {
		if admissionpb.WorkPriority(ba.AdmissionHeader.Priority) >= admissionpb.NormalPri {
			bypassAdmission = true
		}
	}
	// LeaseInfo requests are used as makeshift replica health probes by
	// DistSender circuit breakers, make sure they bypass AC.
	//
	// TODO(erikgrinaker): the various bypass conditions here should be moved to
	// one or more request flags.
	if ba.IsSingleLeaseInfoRequest() {
		bypassAdmission = true
	}
	createTime := ba.AdmissionHeader.CreateTime
	if !bypassAdmission && createTime == 0 {
		// TODO(sumeer): revisit this for multi-tenant. Specifically, the SQL use
		// of zero CreateTime needs to be revisited. It should use high priority.
		createTime = timeutil.Now().UnixNano()
	}
	admissionInfo := admission.WorkInfo{
		TenantID:        tenantID,
		Priority:        admissionpb.WorkPriority(ba.AdmissionHeader.Priority),
		CreateTime:      createTime,
		BypassAdmission: bypassAdmission,
	}
	return admissionInfo
}
