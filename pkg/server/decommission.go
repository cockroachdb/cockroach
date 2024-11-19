// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/decommissioning"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// decommissioningNodeMap tracks the set of nodes that we know are
// decommissioning. This map is used to inform whether we need to proactively
// enqueue some decommissioning node's ranges for rebalancing.
type decommissioningNodeMap struct {
	syncutil.RWMutex
	nodes map[roachpb.NodeID]interface{}
}

// makeOnNodeDecommissioningCallback returns a callback that enqueues the
// decommissioning node's ranges into the `stores`' replicateQueues for
// rebalancing.
func (t *decommissioningNodeMap) makeOnNodeDecommissioningCallback(
	stores *kvserver.Stores,
) func(id roachpb.NodeID) {
	return func(decommissioningNodeID roachpb.NodeID) {
		ctx := context.Background()
		t.Lock()
		defer t.Unlock()
		if _, ok := t.nodes[decommissioningNodeID]; ok {
			// We've already enqueued this node's replicas up for processing.
			// Nothing more to do.
			return
		}
		t.nodes[decommissioningNodeID] = struct{}{}

		logLimiter := log.Every(5 * time.Second) // avoid log spam
		if err := stores.VisitStores(func(store *kvserver.Store) error {
			// For each range that we have a lease for, check if it has a replica
			// on the decommissioning node. If so, proactively enqueue this replica
			// into our local replicateQueue.
			store.VisitReplicas(
				func(replica *kvserver.Replica) (wantMore bool) {
					shouldEnqueue := replica.Desc().Replicas().HasReplicaOnNode(decommissioningNodeID) &&
						// Only bother enqueuing if we own the lease for this replica.
						replica.OwnsValidLease(ctx, replica.Clock().NowAsClockTimestamp())
					if !shouldEnqueue {
						return true /* wantMore */
					}
					processErr, enqueueErr := store.Enqueue(
						// NB: We elide the shouldQueue check since we _know_ that the
						// range being enqueued has replicas on a decommissioning node.
						// Unfortunately, until
						// https://github.com/cockroachdb/cockroach/issues/79266 is fixed,
						// the shouldQueue() method can return false negatives (i.e. it
						// would return false when it really shouldn't).
						ctx, "replicate", replica, true /* skipShouldQueue */, true, /* async */
					)
					if processErr != nil && logLimiter.ShouldLog() {
						// NB: The only case where we would expect to see a processErr when
						// enqueuing a replica async is if it does not have the lease. We
						// are checking that above, but that check is inherently racy.
						log.Warningf(
							ctx, "unexpected processing error when enqueuing replica asynchronously: %v", processErr,
						)
					}
					if enqueueErr != nil && logLimiter.ShouldLog() {
						log.Warningf(ctx, "unable to enqueue replica: %s", enqueueErr)
					}
					return true /* wantMore */
				})
			return nil
		}); err != nil {
			// We're swallowing any errors above, so this shouldn't ever happen.
			log.Fatalf(
				ctx, "error while nudging replicas for decommissioning node n%d", decommissioningNodeID,
			)
		}
	}
}

func (t *decommissioningNodeMap) onNodeDecommissioned(nodeID roachpb.NodeID) {
	t.Lock()
	defer t.Unlock()
	// NB: We may have already deleted this node, but that's ok.
	delete(t.nodes, nodeID)
}

func getPingCheckDecommissionFn(
	engines Engines,
) (*nodeTombstoneStorage, func(context.Context, roachpb.NodeID, codes.Code) error) {
	nodeTombStorage := &nodeTombstoneStorage{engs: engines}
	return nodeTombStorage, func(ctx context.Context, nodeID roachpb.NodeID, errorCode codes.Code) error {
		ts, err := nodeTombStorage.IsDecommissioned(ctx, nodeID)
		if err != nil {
			return errors.Wrapf(err, "unable to read decommissioned status for n%d", nodeID)
		}
		if !ts.IsZero() {
			return kvpb.NewDecommissionedStatusErrorf(errorCode,
				"n%d was permanently removed from the cluster at %s; it is not allowed to rejoin the cluster",
				nodeID, ts)
		}
		// The common case - target node is not decommissioned.
		return nil
	}
}

// DecommissionPreCheck is used to evaluate if nodes are ready for decommission,
// prior to starting the Decommission(..) process. This is evaluated by checking
// that any replicas on the given nodes are able to be replaced or removed,
// following the current state of the cluster as well as the configuration.
// If strictReadiness is true, all replicas are expected to need only replace
// or remove actions. If maxErrors >0, range checks will stop once maxError is
// reached.
// The error returned is a gRPC error.
func (s *topLevelServer) DecommissionPreCheck(
	ctx context.Context,
	nodeIDs []roachpb.NodeID,
	strictReadiness bool,
	collectTraces bool,
	maxErrors int,
) (decommissioning.PreCheckResult, error) {
	// Ensure that if collectTraces is enabled, that a maxErrors >0 is set in
	// order to avoid unlimited memory usage.
	if collectTraces && maxErrors <= 0 {
		return decommissioning.PreCheckResult{},
			grpcstatus.Error(codes.InvalidArgument, "MaxErrors must be set to collect traces.")
	}

	var rangesChecked int
	decommissionCheckNodeIDs := make(map[roachpb.NodeID]livenesspb.NodeLivenessStatus)
	replicasByNode := make(map[roachpb.NodeID][]roachpb.ReplicaIdent)
	actionCounts := make(map[string]int)
	var rangeErrors []decommissioning.RangeCheckResult
	const pageSize = 10000

	for _, nodeID := range nodeIDs {
		decommissionCheckNodeIDs[nodeID] = livenesspb.NodeLivenessStatus_DECOMMISSIONING
	}

	// Counters need to be reset on any transaction retries during the scan
	// through range descriptors.
	initCounters := func() {
		rangesChecked = 0
		for action := range actionCounts {
			actionCounts[action] = 0
		}
		rangeErrors = rangeErrors[:0]
		for nid := range replicasByNode {
			replicasByNode[nid] = replicasByNode[nid][:0]
		}
	}

	// Only check using the first store on this node, as they should all give
	// identical results.
	var evalStore *kvserver.Store
	err := s.node.stores.VisitStores(func(s *kvserver.Store) error {
		if evalStore == nil {
			evalStore = s
		}
		return nil
	})
	if err == nil && evalStore == nil {
		err = errors.Errorf("n%d has no initialized store", s.NodeID())
	}
	if err != nil {
		return decommissioning.PreCheckResult{}, grpcstatus.Error(codes.NotFound, err.Error())
	}

	// Define our node liveness overrides to simulate that the nodeIDs for which
	// we are checking decommission readiness are in the DECOMMISSIONING state.
	// All other nodeIDs should use their actual liveness status.
	existingStorePool := evalStore.GetStoreConfig().StorePool
	overrideNodeLivenessFn := storepool.OverrideNodeLivenessFunc(
		decommissionCheckNodeIDs, existingStorePool.NodeLivenessFn,
	)
	overrideNodeCount := storepool.OverrideNodeCountFunc(
		decommissionCheckNodeIDs, evalStore.GetStoreConfig().NodeLiveness,
	)
	overrideStorePool := storepool.NewOverrideStorePool(
		existingStorePool, overrideNodeLivenessFn, overrideNodeCount,
	)

	// Define our replica filter to only look at the replicas on the checked nodes.
	predHasDecommissioningReplica := func(rDesc roachpb.ReplicaDescriptor) bool {
		_, ok := decommissionCheckNodeIDs[rDesc.NodeID]
		return ok
	}

	// Iterate through all range descriptors using the rangedesc.Scanner, which
	// will perform the requisite meta1/meta2 lookups, including retries.
	rangeDescScanner := rangedesc.NewScanner(s.db)
	err = rangeDescScanner.Scan(ctx, pageSize, initCounters, keys.EverythingSpan, func(descriptors ...roachpb.RangeDescriptor) error {
		for _, desc := range descriptors {
			// Track replicas by node for recording purposes.
			// Skip checks if this range doesn't exist on a potentially decommissioning node.
			replicasToMove := desc.Replicas().FilterToDescriptors(predHasDecommissioningReplica)
			if len(replicasToMove) == 0 {
				continue
			}
			for _, rDesc := range replicasToMove {
				rIdent := roachpb.ReplicaIdent{
					RangeID: desc.RangeID,
					Replica: rDesc,
				}
				replicasByNode[rDesc.NodeID] = append(replicasByNode[rDesc.NodeID], rIdent)
			}

			if maxErrors > 0 && len(rangeErrors) >= maxErrors {
				// TODO(sarkesian): Consider adding a per-range descriptor iterator to
				// rangedesc.Scanner, which will correctly stop iteration on the
				// function returning iterutil.StopIteration().
				continue
			}

			action, _, recording, rErr := evalStore.AllocatorCheckRange(ctx, &desc, collectTraces, overrideStorePool)
			rangesChecked += 1
			actionCounts[action.String()] += 1

			if passed, checkResult := evaluateRangeCheckResult(strictReadiness, collectTraces,
				&desc, action, recording, rErr,
			); !passed {
				rangeErrors = append(rangeErrors, checkResult)
			}
		}

		return nil
	})

	if err != nil {
		return decommissioning.PreCheckResult{}, grpcstatus.Error(codes.Internal, err.Error())
	}

	return decommissioning.PreCheckResult{
		RangesChecked:  rangesChecked,
		ReplicasByNode: replicasByNode,
		ActionCounts:   actionCounts,
		RangesNotReady: rangeErrors,
	}, nil
}

// evaluateRangeCheckResult returns true or false if the range has passed
// decommissioning checks (based on if we are testing strict readiness or not),
// as well as the encapsulated range check result with errors defined as needed.
func evaluateRangeCheckResult(
	strictReadiness bool,
	collectTraces bool,
	desc *roachpb.RangeDescriptor,
	action allocatorimpl.AllocatorAction,
	recording tracingpb.Recording,
	rErr error,
) (passed bool, _ decommissioning.RangeCheckResult) {
	checkResult := decommissioning.RangeCheckResult{
		Desc:   *desc,
		Action: action.String(),
		Err:    rErr,
	}

	if collectTraces {
		checkResult.TracingSpans = recording
	}

	if rErr != nil {
		return false, checkResult
	}

	if action == allocatorimpl.AllocatorRangeUnavailable ||
		action == allocatorimpl.AllocatorNoop ||
		action == allocatorimpl.AllocatorConsiderRebalance {
		checkResult.Err = errors.Errorf("range r%d requires unexpected allocation action: %s",
			desc.RangeID, action,
		)
		return false, checkResult
	}

	if strictReadiness && !(action.Replace() || action.Remove()) {
		checkResult.Err = errors.Errorf(
			"range r%d needs repair beyond replacing/removing the decommissioning replica: %s",
			desc.RangeID, action,
		)
		return false, checkResult
	}

	return true, checkResult
}

// Decommission idempotently sets the decommissioning flag for specified nodes.
// The error return is a gRPC error.
func (s *topLevelServer) Decommission(
	ctx context.Context, targetStatus livenesspb.MembershipStatus, nodeIDs []roachpb.NodeID,
) error {
	// If we're asked to decommission ourself we may lose access to cluster RPC,
	// so we decommission ourself last. We copy the slice to avoid mutating the
	// input slice.
	if targetStatus == livenesspb.MembershipStatus_DECOMMISSIONED {
		orderedNodeIDs := make([]roachpb.NodeID, len(nodeIDs))
		copy(orderedNodeIDs, nodeIDs)
		sort.SliceStable(orderedNodeIDs, func(i, j int) bool {
			return orderedNodeIDs[j] == s.NodeID()
		})
		nodeIDs = orderedNodeIDs
	}

	newEvent := func() (event logpb.EventPayload, nodeDetails *eventpb.CommonNodeDecommissionDetails) {
		if targetStatus.Decommissioning() {
			ev := &eventpb.NodeDecommissioning{}
			nodeDetails = &ev.CommonNodeDecommissionDetails
			event = ev
		} else if targetStatus.Decommissioned() {
			ev := &eventpb.NodeDecommissioned{}
			nodeDetails = &ev.CommonNodeDecommissionDetails
			event = ev
		} else if targetStatus.Active() {
			ev := &eventpb.NodeRecommissioned{}
			nodeDetails = &ev.CommonNodeDecommissionDetails
			event = ev
		} else {
			panic(errors.AssertionFailedf("unexpected target membership status: %v", targetStatus))
		}
		event.CommonDetails().Timestamp = timeutil.Now().UnixNano()
		nodeDetails.RequestingNodeID = int32(s.NodeID())
		return event, nodeDetails
	}

	for _, nodeID := range nodeIDs {
		statusChanged, err := s.nodeLiveness.SetMembershipStatus(ctx, nodeID, targetStatus)
		if err != nil {
			if errors.Is(err, liveness.ErrMissingRecord) {
				return grpcstatus.Error(codes.NotFound, liveness.ErrMissingRecord.Error())
			}
			log.Errorf(ctx, "%+s", err)
			return grpcstatus.Error(codes.Internal, err.Error())
		}
		if statusChanged {
			event, nodeDetails := newEvent()
			nodeDetails.TargetNodeID = int32(nodeID)
			// Ensure an entry is produced in the external log in all cases.
			log.StructuredEvent(ctx, severity.INFO, event)

			// If we die right now or if this transaction fails to commit, the
			// membership event will not be recorded to the event log. While we
			// could insert the event record in the same transaction as the liveness
			// update, this would force a 2PC and potentially leave write intents in
			// the node liveness range. Better to make the event logging best effort
			// than to slow down future node liveness transactions.
			sql.InsertEventRecords(
				ctx,
				s.sqlServer.execCfg,
				sql.LogToSystemTable|sql.LogToDevChannelIfVerbose, /* not LogExternally: we already call log.StructuredEvent above */
				event,
			)
		}

		// Similarly to the log event above, we may not be able to clean up the
		// status entry if we crash or fail -- the status entry is inline, and
		// thus cannot be transactional. However, since decommissioning is
		// idempotent, we can attempt to remove the key regardless of whether
		// the status changed, such that a stale key can be removed by
		// decommissioning the node again.
		if targetStatus.Decommissioned() {
			if err := s.db.PutInline(ctx, keys.NodeStatusKey(nodeID), nil); err != nil {
				log.Errorf(ctx, "unable to clean up node status data for node %d: %s", nodeID, err)
			}
		}
	}
	return nil
}

// DecommissioningNodeMap returns the set of node IDs that are decommissioning
// from the perspective of the server.
func (s *topLevelServer) DecommissioningNodeMap() map[roachpb.NodeID]interface{} {
	s.decomNodeMap.RLock()
	defer s.decomNodeMap.RUnlock()
	nodes := make(map[roachpb.NodeID]interface{})
	for key, val := range s.decomNodeMap.nodes {
		nodes[key] = val
	}
	return nodes
}
