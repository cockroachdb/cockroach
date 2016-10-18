// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package storage

import (
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// ErrNoLivenessRecord is returned when asking for liveness information
// about a node for which nothing is known.
var ErrNoLivenessRecord = errors.New("node not in the liveness table")

// Node liveness metrics counter names.
var (
	metaHeartbeatSuccesses = metric.Metadata{Name: "liveness.heartbeatsuccesses"}
	metaHeartbeatFailures  = metric.Metadata{Name: "liveness.heartbeatfailures"}
	metaEpochIncrements    = metric.Metadata{Name: "liveness.epochincrements"}
)

func (l *Liveness) isLive(clock *hlc.Clock) bool {
	expiration := l.Expiration.Add(-int64(clock.MaxOffset()), 0)
	return clock.Now().Less(expiration)
}

// LivenessMetrics holds metrics for use with node liveness activity.
type LivenessMetrics struct {
	HeartbeatSuccesses *metric.Counter
	HeartbeatFailures  *metric.Counter
	EpochIncrements    *metric.Counter
}

// NodeLiveness encapsulates information on node liveness and provides
// an API for querying, updating, and invalidating node
// liveness. Nodes periodically "heartbeat" the range holding the node
// liveness system table to indicate that they're available. The
// resulting liveness information is used to ignore unresponsive nodes
// while making range quiescense decisions, as well as for efficient,
// node liveness epoch-based range leases.
type NodeLiveness struct {
	clock             *hlc.Clock
	db                *client.DB
	gossip            *gossip.Gossip
	stopHeartbeat     chan struct{}
	livenessThreshold time.Duration
	heartbeatInterval time.Duration
	metrics           LivenessMetrics

	mu struct {
		syncutil.RWMutex
		self  Liveness
		nodes map[roachpb.NodeID]Liveness
	}
}

// NewNodeLiveness returns a new instance of NodeLiveness configured
// with the specified gossip instance.
func NewNodeLiveness(
	clock *hlc.Clock,
	db *client.DB,
	g *gossip.Gossip,
	livenessThreshold time.Duration,
	heartbeatInterval time.Duration,
) *NodeLiveness {
	nl := &NodeLiveness{
		clock:             clock,
		db:                db,
		gossip:            g,
		livenessThreshold: livenessThreshold,
		heartbeatInterval: heartbeatInterval,
		stopHeartbeat:     make(chan struct{}),
		metrics: LivenessMetrics{
			HeartbeatSuccesses: metric.NewCounter(metaHeartbeatSuccesses),
			HeartbeatFailures:  metric.NewCounter(metaHeartbeatFailures),
			EpochIncrements:    metric.NewCounter(metaEpochIncrements),
		},
	}
	nl.mu.nodes = map[roachpb.NodeID]Liveness{}

	livenessRegex := gossip.MakePrefixPattern(gossip.KeyNodeLivenessPrefix)
	nl.gossip.RegisterCallback(livenessRegex, nl.livenessGossipUpdate)

	return nl
}

// IsLive returns whether or not the specified node is considered live
// based on the last receipt of a liveness update via gossip. It is an
// error if the specified node is not in the local liveness table.
func (nl *NodeLiveness) IsLive(nodeID roachpb.NodeID) (bool, error) {
	liveness, err := nl.GetLiveness(nodeID)
	if err != nil {
		return false, err
	}
	return liveness.isLive(nl.clock), nil
}

// StartHeartbeat starts a periodic heartbeat to refresh this node's
// last heartbeat in the node liveness table.
func (nl *NodeLiveness) StartHeartbeat(ctx context.Context, stopper *stop.Stopper) {
	ctx, span := tracing.ForkCtxSpan(ctx, "node liveness heartbeat")
	log.VEventf(ctx, 1, "starting liveness heartbeat")

	stopper.RunWorker(func() {
		defer tracing.FinishSpan(span)
		ticker := time.NewTicker(nl.heartbeatInterval)
		defer ticker.Stop()
		for {
			if err := nl.heartbeat(ctx); err != nil {
				log.Errorf(ctx, "failed liveness heartbeat: %s", err)
			}
			select {
			case <-ticker.C:
			case <-nl.stopHeartbeat:
				return
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// ManualHeartbeat triggers a heartbeat outside of the normal periodic
// heartbeat loop. Used for unittesting.
func (nl *NodeLiveness) ManualHeartbeat() error {
	return nl.heartbeat(context.Background())
}

// heartbeat is called to update a node's expiration timestamp. This
// method does a conditional put on the node liveness record, and if
// successful, stores the updated liveness record in the nodes map.
func (nl *NodeLiveness) heartbeat(ctx context.Context) error {
	nodeID := nl.gossip.GetNodeID()

	var newLiveness Liveness
	var oldLiveness *Liveness
	liveness, err := nl.GetLiveness(nodeID)
	if err == nil {
		oldLiveness = &liveness
		newLiveness = liveness
	} else {
		newLiveness = Liveness{
			NodeID: nodeID,
			Epoch:  1,
		}
	}

	// Retry heartbeat in the event the conditional put fails.
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		newLiveness.Expiration = nl.clock.Now().Add(nl.livenessThreshold.Nanoseconds(), 0)
		tryAgain := false
		if err := nl.updateLiveness(ctx, nodeID, &newLiveness, oldLiveness, func(actual Liveness) {
			oldLiveness = &actual
			newLiveness = actual
			tryAgain = true
		}); err != nil {
			nl.metrics.HeartbeatFailures.Inc(1)
			return err
		}
		if !tryAgain {
			break
		}
	}

	log.VEventf(ctx, 1, "heartbeat node %d liveness with expiration %s", nodeID, newLiveness.Expiration)
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.mu.self = newLiveness
	nl.metrics.HeartbeatSuccesses.Inc(1)
	return nil
}

// Self returns the liveness record for this node. ErrNoLivenessRecord
// is returned in the event that the node has neither heartbeat its
// liveness record successfully, nor received a gossip message containing
// a former liveness update on restart.
func (nl *NodeLiveness) Self() (Liveness, error) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	return nl.getLivenessLocked(nl.gossip.GetNodeID())
}

// GetLiveness returns the liveness record for the specified nodeID.
// ErrNoLivenessRecord is returned in the event that nothing is yet
// known about nodeID via liveness gossip.
func (nl *NodeLiveness) GetLiveness(nodeID roachpb.NodeID) (Liveness, error) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	return nl.getLivenessLocked(nodeID)
}

func (nl *NodeLiveness) getLivenessLocked(nodeID roachpb.NodeID) (Liveness, error) {
	if nodeID != 0 && nodeID == nl.mu.self.NodeID {
		return nl.mu.self, nil
	}
	l, ok := nl.mu.nodes[nodeID]
	if !ok {
		return Liveness{}, ErrNoLivenessRecord
	}
	return l, nil
}

// IncrementEpoch is called by nodes on other nodes in order to
// increment the current liveness epoch, thereby invalidating anything
// relying on the liveness of the previous epoch. This method does a
// conditional put on the node liveness record, and if successful,
// stores the updated liveness record in the nodes map.
//
// TODO(spencer): make sure calls to this method are captured in
// range lease metrics.
func (nl *NodeLiveness) IncrementEpoch(ctx context.Context, nodeID roachpb.NodeID) error {
	liveness, err := nl.GetLiveness(nodeID)
	if err != nil {
		return err
	}
	if liveness.isLive(nl.clock) {
		return errors.Errorf("cannot increment epoch on live node: %+v", liveness)
	}
	newLiveness := liveness
	newLiveness.Epoch++
	if err := nl.updateLiveness(ctx, nodeID, &newLiveness, &liveness, nil); err != nil {
		return err
	}

	log.VEventf(ctx, 1, "incremented node %d liveness epoch to %d", nodeID, newLiveness.Epoch)
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.mu.nodes[nodeID] = newLiveness
	nl.metrics.EpochIncrements.Inc(1)
	return nil
}

// Metrics returns a struct which contains metrics related to node
// liveness activity.
func (nl *NodeLiveness) Metrics() LivenessMetrics {
	return nl.metrics
}

// updateLiveness does a conditional put on the node liveness record
// for the node specified by nodeID. In the event that the conditional
// put fails, and the handleCondFailed callback is not nil, it's
// invoked with the actual node liveness record and nil is returned
// for an error. If handleCondFailed is nil, any conditional put
// failure is returned as an error to the caller. The conditional put
// is done as a 1PC transaction with a ModifiedSpanTrigger which
// indicates the node liveness record that the range leader should
// gossip on commit.
func (nl *NodeLiveness) updateLiveness(
	ctx context.Context,
	nodeID roachpb.NodeID,
	newLiveness *Liveness,
	oldLiveness *Liveness,
	handleCondFailed func(actual Liveness),
) error {
	if err := nl.db.Txn(ctx, func(txn *client.Txn) error {
		b := txn.NewBatch()
		key := keys.NodeLivenessKey(nodeID)
		// The batch interface requires interface{}(nil), not *Liveness(nil).
		if oldLiveness == nil {
			b.CPut(key, newLiveness, nil)
		} else {
			b.CPut(key, newLiveness, oldLiveness)
		}
		b.AddRawRequest(&roachpb.EndTransactionRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				ModifiedSpanTrigger: &roachpb.ModifiedSpanTrigger{
					NodeLivenessSpan: &roachpb.Span{
						Key:    key,
						EndKey: key.Next(),
					},
				},
			},
		})
		return txn.Run(b)
	}); err != nil {
		if cErr, ok := err.(*roachpb.ConditionFailedError); ok && handleCondFailed != nil {
			var actualLiveness Liveness
			if err := cErr.ActualValue.GetProto(&actualLiveness); err != nil {
				return errors.Wrapf(err, "couldn't update node liveness from CPut actual value")
			}
			handleCondFailed(actualLiveness)
			return nil
		}
		return err
	}
	return nil
}

// livenessGossipUpdate is the gossip callback used to keep the
// in-memory liveness info up to date.
func (nl *NodeLiveness) livenessGossipUpdate(key string, content roachpb.Value) {
	var liveness Liveness
	if err := content.GetProto(&liveness); err != nil {
		log.Error(context.TODO(), err)
		return
	}

	// If there's an existing liveness record, only update the received
	// timestamp if this is our first receipt of this node's liveness
	// or if the expiration or epoch was advanced.
	nl.mu.Lock()
	defer nl.mu.Unlock()
	exLiveness, ok := nl.mu.nodes[liveness.NodeID]
	if !ok || exLiveness.Expiration.Less(liveness.Expiration) || exLiveness.Epoch < liveness.Epoch {
		nl.mu.nodes[liveness.NodeID] = liveness
	}
}
