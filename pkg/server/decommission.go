// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
) liveness.OnNodeDecommissionCallback {
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
					_, processErr, enqueueErr := store.Enqueue(
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
			// An error here means something very basic is not working. Better to terminate
			// than to limp along.
			log.Fatalf(ctx, "unable to read decommissioned status for n%d: %v", nodeID, err)
		}
		if !ts.IsZero() {
			// The node was decommissioned.
			return grpcstatus.Errorf(errorCode,
				"n%d was permanently removed from the cluster at %s; it is not allowed to rejoin the cluster",
				nodeID, ts,
			)
		}
		// The common case - target node is not decommissioned.
		return nil
	}
}

// Decommission idempotently sets the decommissioning flag for specified nodes.
// The error return is a gRPC error.
func (s *Server) Decommission(
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

	var event logpb.EventPayload
	var nodeDetails *eventpb.CommonNodeDecommissionDetails
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
		panic("unexpected target membership status")
	}
	event.CommonDetails().Timestamp = timeutil.Now().UnixNano()
	nodeDetails.RequestingNodeID = int32(s.NodeID())

	for _, nodeID := range nodeIDs {
		statusChanged, err := s.nodeLiveness.SetMembershipStatus(ctx, nodeID, targetStatus)
		if err != nil {
			if errors.Is(err, liveness.ErrMissingRecord) {
				return grpcstatus.Error(codes.NotFound, liveness.ErrMissingRecord.Error())
			}
			log.Errorf(ctx, "%+s", err)
			return grpcstatus.Errorf(codes.Internal, err.Error())
		}
		if statusChanged {
			nodeDetails.TargetNodeID = int32(nodeID)
			// Ensure an entry is produced in the external log in all cases.
			log.StructuredEvent(ctx, event)

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
func (s *Server) DecommissioningNodeMap() map[roachpb.NodeID]interface{} {
	s.decomNodeMap.RLock()
	defer s.decomNodeMap.RUnlock()
	nodes := make(map[roachpb.NodeID]interface{})
	for key, val := range s.decomNodeMap.nodes {
		nodes[key] = val
	}
	return nodes
}
