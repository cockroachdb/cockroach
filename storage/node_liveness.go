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

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
)

const (
	// LivenessThreshold is the threshold after which a node will be
	// considered unavailable.
	LivenessThreshold = 9 * time.Second
	// LivenessHeartbeatInterval is the interval for periodic heartbeats
	// to a node's liveness record.
	LivenessHeartbeatInterval = 7200 * time.Millisecond
)

// ErrNoLivenessRecord is returned when asking for liveness information
// about a node for which nothing is known.
var ErrNoLivenessRecord = errors.New("node not in the liveness table")

type livenessRecord struct {
	liveness Liveness
	received hlc.Timestamp
}

func (lr livenessRecord) isLive(nl *NodeLiveness) bool {
	// Compute expiration depending on whether or not the node is
	// considering itself or another node. For itself, we measure
	// expiration from the LastHeartbeat of the Liveness record.  For
	// another node, expiration is from the time at which the liveness
	// record was received via gossip.
	var expiration hlc.Timestamp
	if lr.liveness.NodeID == nl.gossip.GetNodeID() {
		expiration = lr.liveness.LastHeartbeat.Add(LivenessThreshold.Nanoseconds(), 0)
	} else {
		expiration = lr.received.Add(LivenessThreshold.Nanoseconds(), 0)
	}
	return nl.clock.Now().Less(expiration)
}

// NodeLiveness encapsulates information on node liveness and provides
// an API for querying, updating, and invalidating node
// liveness. Nodes periodically "heartbeat" the range holding the node
// liveness system table to indicate that they're available. The
// resulting liveness information is used to ignore unresponsive nodes
// while making range quiescense decisions, as well as for efficient,
// node liveness epoch-based range leader leases.
type NodeLiveness struct {
	clock           *hlc.Clock
	db              *client.DB
	gossip          *gossip.Gossip
	manualHeartbeat chan struct{}
	mu              struct {
		syncutil.RWMutex
		nodes map[roachpb.NodeID]livenessRecord
	}
}

// NewNodeLiveness returns a new instance of NodeLiveness configured
// with the specified gossip instance.
func NewNodeLiveness(clock *hlc.Clock, db *client.DB, g *gossip.Gossip) *NodeLiveness {
	nl := &NodeLiveness{
		clock:           clock,
		db:              db,
		gossip:          g,
		manualHeartbeat: make(chan struct{}, 1),
	}
	nl.mu.nodes = map[roachpb.NodeID]livenessRecord{}

	livenessRegex := gossip.MakePrefixPattern(gossip.KeyNodeLivenessPrefix)
	nl.gossip.RegisterCallback(livenessRegex, nl.livenessGossipUpdate)

	return nl
}

// IsLive returns whether or not the specified node is considered live
// based on the last receipt of a liveness update via gossip. It is an
// error if the specified node is not in the local liveness table.
func (nl *NodeLiveness) IsLive(nodeID roachpb.NodeID) (bool, error) {
	lr, err := nl.getLiveness(nodeID)
	if err != nil {
		return false, err
	}
	return lr.isLive(nl), nil
}

// StartHeartbeat starts a periodic heartbeat to refresh this node's
// last heartbeat in the canonical node liveness table.
func (nl *NodeLiveness) StartHeartbeat(ctx context.Context, stopper *stop.Stopper) {
	log.VEventf(1, ctx, "starting liveness heartbeat for node %d", nl.gossip.GetNodeID())
	stopper.RunWorker(func() {
		ticker := time.NewTicker(LivenessHeartbeatInterval)
		defer ticker.Stop()
		for {
			if err := nl.heartbeat(ctx); err != nil {
				log.Errorf(ctx, "failed liveness heartbeat for node %d: %s", nl.gossip.GetNodeID(), err)
			}
			select {
			case <-ticker.C:
			case <-nl.manualHeartbeat:
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// ManualHeartbeat triggers a heartbeat outside of the normal periodic
// heartbeat loop. Used for unittesting.
func (nl *NodeLiveness) ManualHeartbeat() {
	nl.manualHeartbeat <- struct{}{}
}

// heartbeat is called to update a node's last heartbeat. This method
// does a conditional put on the node liveness record, and if
// successful, stores the updated liveness record in the nodes map.
func (nl *NodeLiveness) heartbeat(ctx context.Context) error {
	nodeID := nl.gossip.GetNodeID()

	var newLiveness Liveness
	var oldLiveness *Liveness
	lr, err := nl.getLiveness(nodeID)
	if err == nil {
		oldLiveness = &lr.liveness
		newLiveness = lr.liveness
	} else {
		newLiveness = Liveness{
			NodeID: nodeID,
			Epoch:  1,
		}
	}

	// Retry heartbeat in the event the conditional put fails.
	for {
		newLiveness.LastHeartbeat = nl.clock.Now()
		tryAgain := false
		if err := nl.updateLiveness(ctx, nodeID, &newLiveness, oldLiveness, func(actual Liveness) {
			oldLiveness = &actual
			newLiveness = actual
			tryAgain = true
		}); err != nil {
			return err
		}
		if !tryAgain {
			break
		}
	}

	log.VEventf(1, ctx, "heartbeat node %d liveness at %s", nodeID, newLiveness.LastHeartbeat)
	nl.mu.Lock()
	defer nl.mu.Unlock()
	lr.liveness = newLiveness
	lr.received = newLiveness.LastHeartbeat
	nl.mu.nodes[nodeID] = lr

	return nil
}

// GetLiveness returns the liveness record for the specified nodeID.
// ErrNoLivenessRecord is returned in the event that nothing is yet
// known about nodeID via liveness gossip.
func (nl *NodeLiveness) GetLiveness(nodeID roachpb.NodeID) (Liveness, error) {
	lr, err := nl.getLiveness(nodeID)
	if err != nil {
		return Liveness{}, err
	}
	return lr.liveness, nil
}

func (nl *NodeLiveness) getLiveness(nodeID roachpb.NodeID) (livenessRecord, error) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	lr, ok := nl.mu.nodes[nodeID]
	if !ok {
		return livenessRecord{}, ErrNoLivenessRecord
	}
	return lr, nil
}

// IncrementEpoch is called by nodes on other nodes in order to
// increment the current liveness epoch, thereby invalidating anything
// relying on the liveness of the previous epoch. This method does a
// conditional put on the node liveness record, and if successful,
// stores the updated liveness record in the nodes map.
func (nl *NodeLiveness) IncrementEpoch(ctx context.Context, nodeID roachpb.NodeID) error {
	lr, err := nl.getLiveness(nodeID)
	if err != nil {
		return err
	}
	if lr.isLive(nl) {
		return errors.Errorf("cannot increment epoch on live node: %+v", lr)
	}
	newLiveness := lr.liveness
	newLiveness.Epoch++
	if err := nl.updateLiveness(ctx, nodeID, &newLiveness, &lr.liveness, nil); err != nil {
		return err
	}

	log.VEventf(1, ctx, "incremented node %d liveness epoch to %d", nodeID, newLiveness.Epoch)
	nl.mu.Lock()
	defer nl.mu.Unlock()
	lr.liveness = newLiveness
	nl.mu.nodes[nodeID] = lr

	return nil
}

// updateLiveness does a conditional put on the node liveness record
// for the node specified by nodeID. In the event that the conditional
// put fails, the handleCondFailed callback is invoked with the actual
// node liveness record. The conditional put is done as a 1PC
// transaction with a ModifiedSpanTrigger which indicates the node
// liveness record that the range leader should gossip on commit.
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
				return errors.Errorf("couldn't update node liveness from CPut actual value: %s", err)
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
		log.Error(context.Background(), err)
		return
	}

	// If there's an existing liveness record, only update the received
	// timestamp if the last heartbeat was advanced.
	lr, ok := nl.mu.nodes[liveness.NodeID]
	if ok {
		if lr.liveness.LastHeartbeat.Less(liveness.LastHeartbeat) {
			lr.received = nl.clock.Now()
		}
	} else {
		// Otherwise, since no previously existing record was available, set
		// received timestamp.
		lr.received = nl.clock.Now()
	}
	lr.liveness = liveness

	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.mu.nodes[liveness.NodeID] = lr
}
