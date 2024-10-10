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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
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

// elasticAdmissionAllLowPri determines whether internally
// submitted low bulk pri requests integrate with elastic CPU control.
var elasticAdmissionAllLowPri = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kvadmission.elastic_control_bulk_low_priority.enabled",
	"determines whether the all low bulk priority requests integrate with elastic CPU control",
	true,
)

// exportRequestElasticControlEnabled determines whether export requests
// integrate with elastic CPU control.
var exportRequestElasticControlEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kvadmission.export_request_elastic_control.enabled",
	"determines whether the export requests integrate with elastic CPU control",
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
	settings.NonNegativeDuration,
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
	settings.NonNegativeDuration,
)

// Controller provides admission control for the KV layer.
type Controller interface {
	// AdmitKVWork must be called before performing KV work.
	// BatchRequest.AdmissionHeader and BatchRequest.Replica.StoreID must be
	// populated for admission to work correctly. If err is non-nil, the
	// returned handle can be ignored. If err is nil, AdmittedKVWorkDone must be
	// called after the KV work is done executing.
	AdmitKVWork(context.Context, roachpb.TenantID, *kvpb.BatchRequest) (Handle, error)
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
	// AdmitRaftEntry informs admission control of a raft log entry being
	// written to storage.
	AdmitRaftEntry(
		_ context.Context, _ roachpb.TenantID, _ roachpb.StoreID, _ roachpb.RangeID, _ roachpb.ReplicaID,
		leaderTerm uint64, _ raftpb.Entry)
	replica_rac2.ACWorkQueue
	// GetSnapshotQueue returns the SnapshotQueue which is used for ingesting raft
	// snapshots.
	GetSnapshotQueue(roachpb.StoreID) *admission.SnapshotQueue
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
	kvAdmissionQ               *admission.WorkQueue
	storeGrantCoords           *admission.StoreGrantCoordinators
	elasticCPUGrantCoordinator *admission.ElasticCPUGrantCoordinator
	kvflowController           kvflowcontrol.Controller
	kvflowHandles              kvflowcontrol.Handles

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

	callAdmittedWorkDoneOnKVAdmissionQ bool
	cpuStart                           time.Duration
}

// AnnotateCtx annotates the given context with request-scoped admission
// data, plumbed through the KV stack using context.Contexts.
func (h *Handle) AnnotateCtx(ctx context.Context) context.Context {
	if h.elasticCPUWorkHandle != nil {
		ctx = admission.ContextWithElasticCPUWorkHandle(ctx, h.elasticCPUWorkHandle)
	}
	if h.raftAdmissionMeta != nil {
		ctx = kvflowcontrol.ContextWithMeta(ctx, h.raftAdmissionMeta)
	}
	return ctx
}

// MakeController returns a Controller. All three parameters must together be
// nil or non-nil.
func MakeController(
	nodeID *base.NodeIDContainer,
	kvAdmissionQ *admission.WorkQueue,
	elasticCPUGrantCoordinator *admission.ElasticCPUGrantCoordinator,
	storeGrantCoords *admission.StoreGrantCoordinators,
	kvflowController kvflowcontrol.Controller,
	kvflowHandles kvflowcontrol.Handles,
	settings *cluster.Settings,
) Controller {
	return &controllerImpl{
		nodeID:                     nodeID,
		kvAdmissionQ:               kvAdmissionQ,
		storeGrantCoords:           storeGrantCoords,
		elasticCPUGrantCoordinator: elasticCPUGrantCoordinator,
		kvflowController:           kvflowController,
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
	ctx context.Context, tenantID roachpb.TenantID, ba *kvpb.BatchRequest,
) (handle Handle, retErr error) {
	ah := Handle{tenantID: tenantID}
	if n.kvAdmissionQ == nil {
		return ah, nil
	}

	bypassAdmission := ba.IsAdmin()
	source := ba.AdmissionHeader.Source
	if !roachpb.IsSystemTenantID(tenantID.ToUint64()) {
		// Request is from a SQL node.
		bypassAdmission = false
		source = kvpb.AdmissionHeader_FROM_SQL
	}
	if source == kvpb.AdmissionHeader_OTHER {
		bypassAdmission = true
	}
	// TODO(abaptist): Revisit and deprecate this setting in v23.1.
	if admission.KVBulkOnlyAdmissionControlEnabled.Get(&n.settings.SV) {
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
		if attemptFlowControl && !bypassAdmission {
			kvflowHandle, found := n.kvflowHandles.LookupReplicationAdmissionHandle(ba.RangeID)
			if !found {
				return Handle{}, nil
			}
			var err error
			admitted, err = kvflowHandle.Admit(ctx, admissionInfo.Priority, timeutil.FromUnixNanos(createTime))
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
	if admissionEnabled {
		// - Backups generate batches with single export requests, which we
		//   admit through the elastic CPU work queue. We grant this
		//   CPU-intensive work a set amount of CPU time and expect it to
		//   terminate (cooperatively) once it exceeds its grant. The amount
		//   disbursed is 100ms, which we've experimentally found to be long
		//   enough to do enough useful work per-request while not causing too
		//   much in the way of scheduling delays on individual cores. Within
		//   admission control we have machinery that observes scheduling
		//   latencies periodically and reduces the total amount of CPU time
		//   handed out through this mechanism, as a way to provide latency
		//   isolation to non-elastic ("latency sensitive") work running on the
		//   same machine.
		// - We do the same for internally submitted bulk low priority requests in
		//   general (notably, for KV work done on the behalf of row-level TTL
		//   reads or other jobs). Everything admissionpb.UserLowPri and above uses
		//   the slots mechanism.
		shouldUseElasticCPU :=
			(exportRequestElasticControlEnabled.Get(&n.settings.SV) && ba.IsSingleExportRequest()) ||
				(admissionInfo.Priority <= admissionpb.BulkLowPri && elasticAdmissionAllLowPri.Get(&n.settings.SV))

		if shouldUseElasticCPU {
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
				ctx, admitDuration, admissionInfo,
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
			callAdmittedWorkDoneOnKVAdmissionQ, err := n.kvAdmissionQ.Admit(ctx, admissionInfo)
			if err != nil {
				return Handle{}, err
			}
			if callAdmittedWorkDoneOnKVAdmissionQ {
				// We include the time to do other activities like intent resolution,
				// since it is acceptable to charge them to the tenant.
				ah.cpuStart = grunning.Time()
			}
			ah.callAdmittedWorkDoneOnKVAdmissionQ = callAdmittedWorkDoneOnKVAdmissionQ
		}
	}
	return ah, nil
}

// AdmittedKVWorkDone implements the Controller interface.
func (n *controllerImpl) AdmittedKVWorkDone(ah Handle, writeBytes *StoreWriteBytes) {
	n.elasticCPUGrantCoordinator.ElasticCPUWorkQueue.AdmittedWorkDone(ah.elasticCPUWorkHandle)
	if ah.callAdmittedWorkDoneOnKVAdmissionQ {
		cpuTime := grunning.Time() - ah.cpuStart
		if cpuTime < 0 {
			// We sometimes see cpuTime to be negative. We use 1ns here, arbitrarily.
			// This issue is tracked by
			// https://github.com/cockroachdb/cockroach/issues/126681.
			if buildutil.CrdbTestBuild {
				log.Warningf(context.Background(), "grunning.Time() should be non-decreasing, cpuTime=%s", cpuTime)
			}
			cpuTime = 1
		}
		n.kvAdmissionQ.AdmittedWorkDone(ah.tenantID, cpuTime)
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
				log.Fatalf(context.Background(), "%s", errors.WithAssertionFailure(err))
			}
			if n.every.ShouldLog() {
				log.Errorf(context.Background(), "%s", err)
			}
		}
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
				n.kvAdmissionQ.SetTenantWeights(weights.Node)
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

// AdmitRaftEntry implements the Controller interface. It is only used for the
// RACv1 protocol.
func (n *controllerImpl) AdmitRaftEntry(
	ctx context.Context,
	tenantID roachpb.TenantID,
	storeID roachpb.StoreID,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	leaderTerm uint64,
	entry raftpb.Entry,
) {
	typ, _, err := raftlog.EncodingOf(entry)
	if err != nil {
		log.Errorf(ctx, "unable to determine raft command encoding: %v", err)
		return
	}
	if !typ.UsesAdmissionControl() {
		return // nothing to do
	}
	meta, err := raftlog.DecodeRaftAdmissionMeta(entry.Data)
	if err != nil {
		log.Errorf(ctx, "unable to decode raft command admission data: %v", err)
		return
	}

	if log.V(1) {
		log.Infof(ctx, "decoded raft admission meta below-raft: pri=%s create-time=%d proposer=n%s receiver=[n%d,s%s] tenant=t%d tokens≈%d sideloaded=%t raft-entry=%d/%d",
			admissionpb.WorkPriority(meta.AdmissionPriority),
			meta.AdmissionCreateTime,
			meta.AdmissionOriginNode,
			n.nodeID.Get(),
			storeID,
			tenantID.ToUint64(),
			kvflowcontrol.Tokens(len(entry.Data)),
			typ.IsSideloaded(),
			entry.Term,
			entry.Index,
		)
	}

	storeAdmissionQ := n.storeGrantCoords.TryGetQueueForStore(storeID)
	if storeAdmissionQ == nil {
		log.Errorf(ctx, "unable to find queue for store: %s", storeID)
		return // nothing to do
	}

	if len(entry.Data) == 0 {
		log.Fatal(ctx, "found (unexpected) empty raft command for below-raft admission")
	}
	wi := admission.WorkInfo{
		TenantID:        tenantID,
		Priority:        admissionpb.WorkPriority(meta.AdmissionPriority),
		CreateTime:      meta.AdmissionCreateTime,
		BypassAdmission: false,
		RequestedCount:  int64(len(entry.Data)),
	}
	wi.ReplicatedWorkInfo = admission.ReplicatedWorkInfo{
		Enabled:    true,
		RangeID:    rangeID,
		ReplicaID:  replicaID,
		LeaderTerm: leaderTerm,
		LogPosition: admission.LogPosition{
			Term:  entry.Term,
			Index: entry.Index,
		},
		Origin:       meta.AdmissionOriginNode,
		IsV2Protocol: false,
		Ingested:     typ.IsSideloaded(),
	}

	handle, err := storeAdmissionQ.Admit(ctx, admission.StoreWriteWorkInfo{
		WorkInfo: wi,
	})
	if err != nil {
		log.Errorf(ctx, "error while admitting to store admission queue: %v", err)
		return
	}
	if handle.UseAdmittedWorkDone() {
		log.Fatalf(ctx, "unexpected handle.UseAdmittedWorkDone")
	}
}

var _ replica_rac2.ACWorkQueue = &controllerImpl{}

// Admit implements replica_rac2.ACWorkQueue. It is only used for the RACv2 protocol.
func (n *controllerImpl) Admit(ctx context.Context, entry replica_rac2.EntryForAdmission) bool {
	storeAdmissionQ := n.storeGrantCoords.TryGetQueueForStore(entry.StoreID)
	if storeAdmissionQ == nil {
		log.Errorf(ctx, "unable to find queue for store: %s", entry.StoreID)
		return false // nothing to do
	}

	if entry.RequestedCount == 0 {
		log.Fatal(ctx, "found (unexpected) empty raft command for below-raft admission")
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
		Origin:       0,
		RaftPri:      entry.CallbackState.Priority,
		IsV2Protocol: true,
		Ingested:     entry.Ingested,
	}

	handle, err := storeAdmissionQ.Admit(ctx, admission.StoreWriteWorkInfo{
		WorkInfo: wi,
	})
	if err != nil {
		log.Errorf(ctx, "error while admitting to store admission queue: %v", err)
		return false
	}
	if handle.UseAdmittedWorkDone() {
		log.Fatalf(ctx, "unexpected handle.UseAdmittedWorkDone")
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
