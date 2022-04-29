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
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// decommissionMonitor keeps track of the state associated with each
// `nodeDecommissionNudger` and is incharge of initializing and closing them.
type decommissionMonitor struct {
	log.AmbientContext
	stopper              *stop.Stopper
	localNodeIDContainer *base.NodeIDContainer
	localStores          *kvserver.Stores
	mu                   struct {
		syncutil.RWMutex
		nudgersByNodeID map[roachpb.NodeID]*nodeDecommissionNudger
	}
	nudgerSem *quotapool.IntPool
}

// makeOnNodeDecommissionCallback returns a function that needs to be called
// every time a node is detected to be decommissioning. It initializes and
// starts a `nodeDecommissionNudger` if one doesn't exist for the node. We only
// ever spin up one nodeDecommissionNudger for each decommissioning node, so
// this function is idempotent.
func (ms *decommissionMonitor) makeOnNodeDecommissioningCallback() liveness.OnNodeDecommissionCallback {
	ctx := context.Background()
	return func(decommissioningNodeID roachpb.NodeID) {
		ms.mu.Lock()
		defer ms.mu.Unlock()
		if _, ok := ms.mu.nudgersByNodeID[decommissioningNodeID]; ok {
			// We've already have an active nudger for this decommissioning node.
			// Nothing to do.
			return
		}

		// TODO(aayush): In a cluster with a ton of nodes, we may want to consider
		// limiting the maximum number of decommissionNudgers that can be spun up.
		nudger := &nodeDecommissionNudger{
			stopper:               ms.stopper,
			localStores:           ms.localStores,
			nodeDecommissionedCh:  make(chan struct{}),
			logLimiter:            log.Every(10 * time.Second),
			localNodeID:           ms.localNodeIDContainer.Get(),
			decommissioningNodeID: decommissioningNodeID,
			nudgeInterval:         1 * time.Minute,
			sem:                   ms.nudgerSem,
		}
		nudger.start(ctx)
		ms.mu.nudgersByNodeID[decommissioningNodeID] = nudger
	}
}

func (ms *decommissionMonitor) onNodeDecommissioned(nodeID roachpb.NodeID) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	nudger, ok := ms.mu.nudgersByNodeID[nodeID]
	if !ok {
		// We don't have an active nudger for this decommissioning node, which means
		// we already shut it down.
		return
	}
	nudger.stop()
	delete(ms.mu.nudgersByNodeID, nodeID)
}

// getActiveDecommissionNudgers returns the set of nodeIds for which we have a
// currently active nudger task.
func (ms *decommissionMonitor) getActiveDecommissionNudgers() []roachpb.NodeID {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	var nodeIDs []roachpb.NodeID
	for nodeID := range ms.mu.nudgersByNodeID {
		nodeIDs = append(nodeIDs, nodeID)
	}
	return nodeIDs
}

// nodeDecommissioningNudger is responsible for proactively enqueuing a given
// decommissioning node's replicas into the local node's replicateQueues. Then,
// it periodically nudges the decommissioning node's straggling replicas (i.e.
// replicas that still haven't been rebalanced after the given interval).
type nodeDecommissionNudger struct {
	log.AmbientContext
	stopper              *stop.Stopper
	localStores          *kvserver.Stores
	nodeDecommissionedCh chan struct{}
	logLimiter           log.EveryN // avoid log spam

	localNodeID, decommissioningNodeID roachpb.NodeID
	nudgeInterval                      time.Duration
	sem                                *quotapool.IntPool
}

// start starts the nodeDecommissionNudger.
func (n *nodeDecommissionNudger) start(ctx context.Context) {
	n.AddLogTag(fmt.Sprintf("n%d", n.localNodeID), nil)
	ctx = n.AnnotateCtx(ctx)

	taskName := fmt.Sprintf("decommission-nudger-for-n%d", n.decommissioningNodeID)
	if err := n.stopper.RunAsyncTask(ctx, taskName, func(ctx context.Context) {
		alloc, err := n.sem.Acquire(ctx, 1)
		if err != nil {
			log.Fatalf(ctx, "error while trying to acquire quota to start nodeDecommissionNudger: %v", err)
			return
		}
		defer n.sem.Release(alloc)
		log.Infof(
			ctx, "n%d is decommissioning; enqueueing its replicas for rebalancing", n.decommissioningNodeID,
		)
		n.enqueueReplicas(ctx)

		for {
			select {
			case <-time.After(n.nudgeInterval):
				n.enqueueReplicas(ctx)
			case <-n.stopper.ShouldQuiesce():
				return
			case <-n.nodeDecommissionedCh:
				log.Infof(ctx, "n%d fully decommissioned; stopping", n.decommissioningNodeID)
				return
			}
		}
	},
	); err != nil && n.logLimiter.ShouldLog() {
		log.Warningf(
			ctx, "could not start decommission nudger for node n%d: %s", n.decommissioningNodeID, err,
		)
	}
}

func (n *nodeDecommissionNudger) stop() {
	// TODO(aayush): Should we instead block here until the nudger actually stops?
	close(n.nodeDecommissionedCh)
}

func (n *nodeDecommissionNudger) enqueueReplicas(ctx context.Context) {
	if err := n.localStores.VisitStores(func(store *kvserver.Store) error {
		// For each range that we have a lease for, check if it has a replica
		// on the decommissioning node. If so, proactively enqueue this replica
		// into our local replicateQueue.
		store.VisitReplicas(
			func(replica *kvserver.Replica) (wantMore bool) {
				if !replica.Desc().Replicas().HasReplicaOnNode(n.decommissioningNodeID) &&
					// Only bother enqueuing if we own the lease for this replica.
					replica.OwnsValidLease(ctx, replica.Clock().NowAsClockTimestamp()) {
					return true
				}
				_, processErr, enqueueErr := store.Enqueue(
					// NB: We elide the shouldQueue check since we _know_ that the range
					// being enqueued has replicas on a decommissioning node.
					// Unfortunately, until https://github.com/cockroachdb/cockroach/issues/79266
					// is fixed, the shouldQueue() method can return false negatives (i.e.
					// it would return false when it really shouldn't).
					ctx, "replicate", replica, true /* skipShouldQueue */, true, /* async */
				)
				if processErr != nil && n.logLimiter.ShouldLog() {
					// When we enqueue a replica into the queue async, we don't
					// expect to see any processing errors (since we are not waiting
					// for the replica to be processed at all).
					log.Warningf(ctx, "unexpected error processing replica: %s", processErr)
				}
				if enqueueErr != nil && n.logLimiter.ShouldLog() {
					log.Warningf(ctx, "unable to enqueue replica: %s", enqueueErr)
				}
				return true /* wantMore */
			})
		return nil
	}); err != nil {
		// We're swallowing any errors above, so this shouldn't ever happen.
		log.Fatalf(
			ctx, "error while nudging replicas for decommissioning node n%d", n.decommissioningNodeID,
		)
	}
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

	var event eventpb.EventPayload
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
			if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				return sql.InsertEventRecord(
					ctx,
					s.sqlServer.execCfg.InternalExecutor,
					txn,
					int32(s.NodeID()), /* reporting ID: the node where the event is logged */
					sql.LogToSystemTable|sql.LogToDevChannelIfVerbose, /* we already call log.StructuredEvent above */
					int32(nodeID), /* target ID: the node that we wee a membership change for */
					event,
				)
			}); err != nil {
				log.Ops.Errorf(ctx, "unable to record event: %+v: %+v", event, err)
			}
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
