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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	metaLiveNodes = metric.Metadata{
		Name: "liveness.livenodes",
		Help: "Number of live nodes in the cluster (will be 0 if this node is not itself live)",
	}
	metaHeartbeatSuccesses = metric.Metadata{
		Name: "liveness.heartbeatsuccesses",
		Help: "Number of successful node liveness heartbeats from this node",
	}
	metaHeartbeatFailures = metric.Metadata{
		Name: "liveness.heartbeatfailures",
		Help: "Number of failed node liveness heartbeats from this node",
	}
	metaEpochIncrements = metric.Metadata{
		Name: "liveness.epochincrements",
		Help: "Number of times this node has incremented its liveness epoch",
	}
)

func (l *Liveness) isLive(now hlc.Timestamp, maxOffset time.Duration) bool {
	expiration := l.Expiration.Add(-maxOffset.Nanoseconds(), 0)
	return now.Less(expiration)
}

// LivenessMetrics holds metrics for use with node liveness activity.
type LivenessMetrics struct {
	LiveNodes          *metric.Gauge
	HeartbeatSuccesses *metric.Counter
	HeartbeatFailures  *metric.Counter
	EpochIncrements    *metric.Counter
}

// IsLiveCallback is invoked when a node's IsLive state changes to true.
// Callbacks can be registered via NodeLiveness.RegisterCallback().
type IsLiveCallback func(nodeID roachpb.NodeID)

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
	pauseHeartbeat    atomic.Value // contains a bool
	incrementSem      chan struct{}
	heartbeatSem      chan struct{}
	metrics           LivenessMetrics

	mu struct {
		syncutil.Mutex
		self      Liveness
		callbacks []IsLiveCallback
		nodes     map[roachpb.NodeID]Liveness
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
	renewalDuration time.Duration,
) *NodeLiveness {
	nl := &NodeLiveness{
		ambientCtx:        ambient,
		clock:             clock,
		db:                db,
		gossip:            g,
		livenessThreshold: livenessThreshold,
		heartbeatInterval: livenessThreshold - renewalDuration,
		incrementSem:      make(chan struct{}, 1),
		heartbeatSem:      make(chan struct{}, 1),
	}
	nl.metrics = LivenessMetrics{
		LiveNodes:          metric.NewFunctionalGauge(metaLiveNodes, nl.numLiveNodes),
		HeartbeatSuccesses: metric.NewCounter(metaHeartbeatSuccesses),
		HeartbeatFailures:  metric.NewCounter(metaHeartbeatFailures),
		EpochIncrements:    metric.NewCounter(metaEpochIncrements),
	}
	nl.pauseHeartbeat.Store(false)
	nl.mu.nodes = map[roachpb.NodeID]Liveness{}

	livenessRegex := gossip.MakePrefixPattern(gossip.KeyNodeLivenessPrefix)
	nl.gossip.RegisterCallback(livenessRegex, nl.livenessGossipUpdate)

	return nl
}

// GetLivenessThreshold returns the maximum duration between heartbeats
// before a node is considered not-live.
func (nl *NodeLiveness) GetLivenessThreshold() time.Duration {
	return nl.livenessThreshold
}

// IsLive returns whether or not the specified node is considered live
// based on the last receipt of a liveness update via gossip. It is an
// error if the specified node is not in the local liveness table.
func (nl *NodeLiveness) IsLive(nodeID roachpb.NodeID) (bool, error) {
	liveness, err := nl.GetLiveness(nodeID)
	if err != nil {
		return false, err
	}
	return liveness.isLive(nl.clock.Now(), nl.clock.MaxOffset()), nil
}

// StartHeartbeat starts a periodic heartbeat to refresh this node's
// last heartbeat in the node liveness table.
func (nl *NodeLiveness) StartHeartbeat(ctx context.Context, stopper *stop.Stopper) {
	log.VEventf(ctx, 1, "starting liveness heartbeat")
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()

	stopper.RunWorker(func() {
		ambient := nl.ambientCtx
		ambient.AddLogTag("hb", nil)
		ticker := time.NewTicker(nl.heartbeatInterval)
		defer ticker.Stop()
		for i := 0; ; i++ {
			if !nl.pauseHeartbeat.Load().(bool) {
				ctx, sp := ambient.AnnotateCtxWithSpan(context.Background(), "heartbeat")
				ctx, cancel := context.WithTimeout(ctx, nl.heartbeatInterval)
				// Retry heartbeat in the event the conditional put fails.
				for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
					liveness, err := nl.Self()
					if err != nil && err != ErrNoLivenessRecord {
						log.Errorf(ctx, "unexpected error getting liveness: %v", err)
					}
					if err := nl.Heartbeat(ctx, liveness); err != nil {
						if err == errSkippedHeartbeat {
							continue
						}
						log.Errorf(ctx, "failed liveness heartbeat: %v", err)
					}
					// On first heartbeat, increment the epoch. This ensures that all
					// leases previously held by replicas on this node are renewed.
					if i == 0 {
						nl.mu.Lock()
						liveness := nl.mu.self // copy
						nl.mu.Unlock()
						if err := nl.IncrementEpoch(ctx, &liveness); err != nil {
							log.Errorf(ctx, "failed initial liveness epoch increment: %v", err)
							continue
						}
					}
					break
				}
				cancel()
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

var errNodeAlreadyLive = errors.New("node already live")

// Heartbeat is called to update a node's expiration timestamp. This
// method does a conditional put on the node liveness record, and if
// successful, stores the updated liveness record in the nodes map.
func (nl *NodeLiveness) Heartbeat(ctx context.Context, liveness *Liveness) error {
	defer func(start time.Time) {
		if dur := timeutil.Now().Sub(start); dur > time.Second {
			log.Warningf(ctx, "slow heartbeat took %0.1fs", dur.Seconds())
		}
	}(timeutil.Now())

	// Allow only one heartbeat at a time.
	select {
	case nl.heartbeatSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-nl.heartbeatSem
	}()

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
	// We need to add the maximum clock offset to the expiration because it's
	// used when determining liveness for a node.
	newLiveness.Expiration = nl.clock.Now().Add(
		(nl.livenessThreshold + nl.clock.MaxOffset()).Nanoseconds(), 0)
	if err := nl.updateLiveness(ctx, &newLiveness, liveness, func(actual Liveness) error {
		// Update liveness to actual value on mismatch.
		nl.mu.Lock()
		nl.mu.self = actual
		nl.mu.Unlock()
		// If the actual liveness is different than expected, but is
		// considered live, treat the heartbeat as a success. This can
		// happen when the periodic heartbeater races with a concurrent
		// lease acquisition.
		if actual.isLive(nl.clock.Now(), nl.clock.MaxOffset()) {
			return errNodeAlreadyLive
		}
		// Otherwise, return error.
		return errSkippedHeartbeat
	}); err != nil {
		if err == errNodeAlreadyLive {
			return nil
		}
		nl.metrics.HeartbeatFailures.Inc(1)
		return err
	}

	log.VEventf(ctx, 1, "heartbeat %+v", newLiveness.Expiration)
	nl.mu.Lock()
	nl.mu.self = newLiveness
	nl.mu.Unlock()
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

// GetLivenessMap returns a map of nodeID to liveness.
func (nl *NodeLiveness) GetLivenessMap() map[roachpb.NodeID]bool {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	lMap := map[roachpb.NodeID]bool{}
	now := nl.clock.Now()
	maxOffset := nl.clock.MaxOffset()
	for nID, l := range nl.mu.nodes {
		lMap[nID] = l.isLive(now, maxOffset)
	}
	if nl.mu.self.NodeID != 0 {
		lMap[nl.mu.self.NodeID] = nl.mu.self.isLive(now, maxOffset)
	}
	return lMap
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
	if l, ok := nl.mu.nodes[nodeID]; ok {
		return &l, nil
	}
	return nil, ErrNoLivenessRecord
}

var errEpochAlreadyIncremented = errors.New("epoch already incremented")

// IncrementEpoch is called to increment the current liveness epoch,
// thereby invalidating anything relying on the liveness of the
// previous epoch. This method does a conditional put on the node
// liveness record, and if successful, stores the updated liveness
// record in the nodes map. If this method is called on a node ID
// which is considered live according to the most recent information
// gathered through gossip, an error is returned.
func (nl *NodeLiveness) IncrementEpoch(ctx context.Context, liveness *Liveness) error {
	// Allow only one increment at a time.
	select {
	case nl.incrementSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-nl.incrementSem
	}()

	if liveness.isLive(nl.clock.Now(), nl.clock.MaxOffset()) {
		return errors.Errorf("cannot increment epoch on live node: %+v", liveness)
	}
	newLiveness := *liveness
	newLiveness.Epoch++
	update := func(l Liveness) {
		nl.mu.Lock()
		defer nl.mu.Unlock()
		if nodeID := nl.gossip.NodeID.Get(); nodeID == l.NodeID {
			nl.mu.self = l
		} else {
			nl.mu.nodes[l.NodeID] = l
		}
	}
	if err := nl.updateLiveness(ctx, &newLiveness, liveness, func(actual Liveness) error {
		defer update(actual)
		if actual.Epoch > liveness.Epoch {
			return errEpochAlreadyIncremented
		} else if actual.Epoch < liveness.Epoch {
			return errors.Errorf("unexpected liveness epoch %d; expected >= %d", actual.Epoch, liveness.Epoch)
		}
		return errors.Errorf("mismatch incrementing epoch for %+v; actual is %+v", liveness, actual)
	}); err != nil {
		if err == errEpochAlreadyIncremented {
			return nil
		}
		return err
	}

	log.VEventf(ctx, 1, "incremented node %d liveness epoch to %d",
		newLiveness.NodeID, newLiveness.Epoch)
	update(newLiveness)
	nl.metrics.EpochIncrements.Inc(1)
	return nil
}

// Metrics returns a struct which contains metrics related to node
// liveness activity.
func (nl *NodeLiveness) Metrics() LivenessMetrics {
	return nl.metrics
}

// RegisterCallback registers a callback to be invoked any time a
// node's IsLive() state changes to true.
func (nl *NodeLiveness) RegisterCallback(cb IsLiveCallback) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.mu.callbacks = append(nl.mu.callbacks, cb)
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
	// First check the existing liveness map to avoid known conditional
	// put failures.
	if l, err := nl.GetLiveness(newLiveness.NodeID); err == nil &&
		(oldLiveness == nil || *l != *oldLiveness) {
		return handleCondFailed(*l)
	}

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
	var callbacks []IsLiveCallback
	nl.mu.Lock()
	exLiveness, ok := nl.mu.nodes[liveness.NodeID]
	if !ok || exLiveness.Expiration.Less(liveness.Expiration) || exLiveness.Epoch < liveness.Epoch {
		nl.mu.nodes[liveness.NodeID] = liveness

		// If isLive status is now true, but previously false, invoke any registered callbacks.
		now, offset := nl.clock.Now(), nl.clock.MaxOffset()
		if !exLiveness.isLive(now, offset) && liveness.isLive(now, offset) {
			callbacks = append(callbacks, nl.mu.callbacks...)
		}
	}
	nl.mu.Unlock()

	// Invoke any "is live" callbacks after releasing lock.
	for _, cb := range callbacks {
		cb(liveness.NodeID)
	}
}

// numLiveNodes is used to populate a metric that tracks the number of live
// nodes in the cluster. Returns 0 if this node is not itself live, to avoid
// reporting potentially inaccurate data.
// We export this metric from every live node rather than a single particular
// live node because liveness information is gossiped and thus may be stale.
// That staleness could result in no nodes reporting the metric or multiple
// nodes reporting the metric, so it's simplest to just have all live nodes
// report it.
func (nl *NodeLiveness) numLiveNodes() int64 {
	selfID := nl.gossip.NodeID.Get()
	if selfID == 0 {
		return 0
	}

	nl.mu.Lock()
	defer nl.mu.Unlock()

	// If this node isn't live, we don't want to report its view of node liveness
	// because it's more likely to be inaccurate than the view of a live node.
	now := nl.clock.Now()
	maxOffset := nl.clock.MaxOffset()
	if !nl.mu.self.isLive(now, maxOffset) {
		return 0
	}

	var liveNodes int64
	for _, l := range nl.mu.nodes {
		if l.isLive(now, maxOffset) {
			liveNodes++
		}
	}
	return liveNodes
}
