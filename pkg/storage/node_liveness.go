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
	"sync/atomic"
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
)

var (
	// ErrNoLivenessRecord is returned when asking for liveness information
	// about a node for which nothing is known.
	ErrNoLivenessRecord = errors.New("node not in the liveness table")

	// errSkippedHeartbeat is returned when a heartbeat request fails because
	// the underlying liveness record has had its epoch incremented.
	errSkippedHeartbeat = errors.New("heartbeat failed on epoch increment")
)

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
	ambientCtx        log.AmbientContext
	clock             *hlc.Clock
	db                *client.DB
	gossip            *gossip.Gossip
	livenessThreshold time.Duration
	heartbeatInterval time.Duration
	pauseHeartbeat    atomic.Value
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
	ambient log.AmbientContext,
	clock *hlc.Clock,
	db *client.DB,
	g *gossip.Gossip,
	livenessThreshold time.Duration,
	heartbeatInterval time.Duration,
) *NodeLiveness {
	nl := &NodeLiveness{
		ambientCtx:        ambient,
		clock:             clock,
		db:                db,
		gossip:            g,
		livenessThreshold: livenessThreshold,
		heartbeatInterval: heartbeatInterval,
		metrics: LivenessMetrics{
			HeartbeatSuccesses: metric.NewCounter(metaHeartbeatSuccesses),
			HeartbeatFailures:  metric.NewCounter(metaHeartbeatFailures),
			EpochIncrements:    metric.NewCounter(metaEpochIncrements),
		},
	}
	nl.pauseHeartbeat.Store(false)
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
	log.VEventf(ctx, 1, "starting liveness heartbeat")

	stopper.RunWorker(func() {
		ambient := nl.ambientCtx
		ambient.AddLogTag("hb", nil)
		ticker := time.NewTicker(nl.heartbeatInterval)
		defer ticker.Stop()
		for {
			if !nl.pauseHeartbeat.Load().(bool) {
				ctx, sp := ambient.AnnotateCtxWithSpan(context.Background(), "heartbeat")
				// Retry heartbeat in the event the conditional put fails.
				for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
					liveness, err := nl.Self()
					if err != nil && err != ErrNoLivenessRecord {
						log.Errorf(ctx, "unexpected error getting liveness: %v", err)
					}
					if err := nl.heartbeat(ctx, liveness); err != nil {
						if err == errSkippedHeartbeat {
							continue
						}
						log.Errorf(ctx, "failed liveness heartbeat: %v", err)
					}
					break
				}
				sp.Finish()
			}
			select {
			case <-ticker.C:
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// PauseHeartbeat stops or restarts the periodic heartbeat depending
// on the pause parameter.
func (nl *NodeLiveness) PauseHeartbeat(pause bool) {
	nl.pauseHeartbeat.Store(pause)
}

// Heartbeat triggers a heartbeat outside of the normal periodic
// heartbeat loop. Used for unittesting.
func (nl *NodeLiveness) Heartbeat(ctx context.Context, liveness *Liveness) error {
	return nl.heartbeat(ctx, liveness)
}

// heartbeat is called to update a node's expiration timestamp. This
// method does a conditional put on the node liveness record, and if
// successful, stores the updated liveness record in the nodes map.
func (nl *NodeLiveness) heartbeat(ctx context.Context, liveness *Liveness) error {
	nodeID := nl.gossip.NodeID.Get()
	var newLiveness Liveness
	if liveness == nil {
		newLiveness = Liveness{
			NodeID: nodeID,
			Epoch:  1,
		}
	} else {
		newLiveness = *liveness
	}
	newLiveness.Expiration = nl.clock.Now().Add(nl.livenessThreshold.Nanoseconds(), 0)
	err := nl.updateLiveness(ctx, &newLiveness, liveness, func(actual Liveness) error {
		// Set actual liveness on unexpected mismatch.
		newLiveness = actual
		// If the actual liveness is further ahead than expected, we're done.
		if !actual.Expiration.Less(newLiveness.Expiration) {
			return nil
		}
		// Otherwise, return error.
		return errSkippedHeartbeat
	})

	log.VEventf(ctx, 1, "heartbeat %+v", newLiveness.Expiration)
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.mu.self = newLiveness
	if err != nil {
		nl.metrics.HeartbeatFailures.Inc(1)
		return err
	}
	nl.metrics.HeartbeatSuccesses.Inc(1)
	return nil
}

// Self returns the liveness record for this node. ErrNoLivenessRecord
// is returned in the event that the node has neither heartbeat its
// liveness record successfully, nor received a gossip message containing
// a former liveness update on restart.
func (nl *NodeLiveness) Self() (*Liveness, error) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	return nl.getLivenessLocked(nl.gossip.NodeID.Get())
}

// GetLiveness returns the liveness record for the specified nodeID.
// ErrNoLivenessRecord is returned in the event that nothing is yet
// known about nodeID via liveness gossip.
func (nl *NodeLiveness) GetLiveness(nodeID roachpb.NodeID) (*Liveness, error) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	return nl.getLivenessLocked(nodeID)
}

func (nl *NodeLiveness) getLivenessLocked(nodeID roachpb.NodeID) (*Liveness, error) {
	if nodeID != 0 && nodeID == nl.mu.self.NodeID {
		copySelf := nl.mu.self
		return &copySelf, nil
	}
	l, ok := nl.mu.nodes[nodeID]
	if !ok {
		return nil, ErrNoLivenessRecord
	}
	return &l, nil
}

// IncrementEpoch is called to increment the current liveness epoch,
// thereby invalidating anything relying on the liveness of the
// previous epoch. This method does a conditional put on the node
// liveness record, and if successful, stores the updated liveness
// record in the nodes map. If this method is called on a node ID
// which is considered live according to the most recent information
// gathered through gossip, an error is returned.
//
// TODO(spencer): make sure calls to this method are captured in
// range lease metrics.
func (nl *NodeLiveness) IncrementEpoch(ctx context.Context, liveness *Liveness) error {
	if liveness.isLive(nl.clock) {
		return errors.Errorf("cannot increment epoch on live node: %+v", liveness)
	}
	newLiveness := *liveness
	newLiveness.Epoch++
	if err := nl.updateLiveness(ctx, &newLiveness, liveness, func(actual Liveness) error {
		if actual.Epoch > liveness.Epoch {
			newLiveness = actual
			return nil
		} else if actual.Epoch < liveness.Epoch {
			return errors.Errorf("unexpected liveness epoch %d; expected >= %d", actual.Epoch, liveness.Epoch)
		}
		nl.mu.Lock()
		defer nl.mu.Unlock()
		nl.mu.nodes[actual.NodeID] = actual
		return errors.Errorf("mismatch incrementing epoch for %+v; actual is %+v", liveness, actual)
	}); err != nil {
		return err
	}

	log.VEventf(ctx, 1, "incremented node %d liveness epoch to %d",
		newLiveness.NodeID, newLiveness.Epoch)
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.mu.nodes[newLiveness.NodeID] = newLiveness
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
	newLiveness *Liveness,
	oldLiveness *Liveness,
	handleCondFailed func(actual Liveness) error,
) error {
	if err := nl.db.Txn(ctx, func(txn *client.Txn) error {
		b := txn.NewBatch()
		key := keys.NodeLivenessKey(newLiveness.NodeID)
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
			if cErr.ActualValue == nil {
				return handleCondFailed(Liveness{})
			}
			var actualLiveness Liveness
			if err := cErr.ActualValue.GetProto(&actualLiveness); err != nil {
				return errors.Wrapf(err, "couldn't update node liveness from CPut actual value")
			}
			return handleCondFailed(actualLiveness)
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
