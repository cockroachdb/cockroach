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

package storage

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var (
	// ErrNoLivenessRecord is returned when asking for liveness information
	// about a node for which nothing is known.
	ErrNoLivenessRecord = errors.New("node not in the liveness table")

	errChangeDecommissioningFailed = errors.New("failed to change the decommissioning status")

	// ErrEpochIncremented is returned when a heartbeat request fails because
	// the underlying liveness record has had its epoch incremented.
	ErrEpochIncremented = errors.New("heartbeat failed on epoch increment")
)

type errRetryLiveness struct {
	error
}

func (e *errRetryLiveness) Cause() error {
	return e.error
}

func (e *errRetryLiveness) Error() string {
	return fmt.Sprintf("%T: %s", *e, e.error)
}

// Node liveness metrics counter names.
var (
	metaLiveNodes = metric.Metadata{
		Name: "liveness.livenodes",
		Help: "Number of live nodes in the cluster (will be 0 if this node is not itself live)"}
	metaHeartbeatSuccesses = metric.Metadata{
		Name: "liveness.heartbeatsuccesses",
		Help: "Number of successful node liveness heartbeats from this node"}
	metaHeartbeatFailures = metric.Metadata{
		Name: "liveness.heartbeatfailures",
		Help: "Number of failed node liveness heartbeats from this node"}
	metaEpochIncrements = metric.Metadata{
		Name: "liveness.epochincrements",
		Help: "Number of times this node has incremented its liveness epoch"}
	metaHeartbeatLatency = metric.Metadata{
		Name: "liveness.heartbeatlatency",
		Help: "Node liveness heartbeat latency in nanoseconds"}
)

// IsLive returns whether the node is considered live at the given time with the
// given clock offset.
func (l *Liveness) IsLive(now hlc.Timestamp, maxOffset time.Duration) bool {
	if maxOffset == timeutil.ClocklessMaxOffset {
		// When using clockless reads, we're live without a buffer period.
		maxOffset = 0
	}
	expiration := hlc.Timestamp(l.Expiration).Add(-maxOffset.Nanoseconds(), 0)
	return now.Less(expiration)
}

// IsDead returns whether the node is considered dead at the given time with the
// given threshold.
func (l *Liveness) IsDead(now hlc.Timestamp, threshold time.Duration) bool {
	deadAsOf := hlc.Timestamp(l.Expiration).GoTime().Add(threshold)
	return !now.GoTime().Before(deadAsOf)
}

// LivenessStatus returns a NodeLivenessStatus enumeration value for this liveness
// based on the provided timestamp, threshold, and clock max offset.
func (l *Liveness) LivenessStatus(
	now time.Time, threshold, maxOffset time.Duration,
) NodeLivenessStatus {
	nowHlc := hlc.Timestamp{WallTime: now.UnixNano()}
	if l.IsDead(nowHlc, threshold) {
		if l.Decommissioning {
			return NodeLivenessStatus_DECOMMISSIONED
		}
		return NodeLivenessStatus_DEAD
	}
	if l.Decommissioning {
		return NodeLivenessStatus_DECOMMISSIONING
	}
	if l.Draining {
		return NodeLivenessStatus_UNAVAILABLE
	}
	if l.IsLive(nowHlc, maxOffset) {
		return NodeLivenessStatus_LIVE
	}
	return NodeLivenessStatus_UNAVAILABLE
}

// LivenessMetrics holds metrics for use with node liveness activity.
type LivenessMetrics struct {
	LiveNodes          *metric.Gauge
	HeartbeatSuccesses *metric.Counter
	HeartbeatFailures  *metric.Counter
	EpochIncrements    *metric.Counter
	HeartbeatLatency   *metric.Histogram
}

// IsLiveCallback is invoked when a node's IsLive state changes to true.
// Callbacks can be registered via NodeLiveness.RegisterCallback().
type IsLiveCallback func(nodeID roachpb.NodeID)

// HeartbeatCallback is invoked whenever this node updates its own liveness status,
// indicating that it is alive.
type HeartbeatCallback func(context.Context)

// NodeLiveness encapsulates information on node liveness and provides
// an API for querying, updating, and invalidating node
// liveness. Nodes periodically "heartbeat" the range holding the node
// liveness system table to indicate that they're available. The
// resulting liveness information is used to ignore unresponsive nodes
// while making range quiescence decisions, as well as for efficient,
// node liveness epoch-based range leases.
type NodeLiveness struct {
	ambientCtx        log.AmbientContext
	clock             *hlc.Clock
	db                *client.DB
	engines           []engine.Engine
	gossip            *gossip.Gossip
	livenessThreshold time.Duration
	heartbeatInterval time.Duration
	pauseHeartbeat    atomic.Value // contains a bool
	selfSem           chan struct{}
	otherSem          chan struct{}
	triggerHeartbeat  chan struct{} // for testing
	metrics           LivenessMetrics

	mu struct {
		syncutil.Mutex
		callbacks         []IsLiveCallback
		nodes             map[roachpb.NodeID]Liveness
		heartbeatCallback HeartbeatCallback
	}
}

// NewNodeLiveness returns a new instance of NodeLiveness configured
// with the specified gossip instance.
func NewNodeLiveness(
	ambient log.AmbientContext,
	clock *hlc.Clock,
	db *client.DB,
	engines []engine.Engine,
	g *gossip.Gossip,
	livenessThreshold time.Duration,
	renewalDuration time.Duration,
	histogramWindow time.Duration,
) *NodeLiveness {
	nl := &NodeLiveness{
		ambientCtx:        ambient,
		clock:             clock,
		db:                db,
		engines:           engines,
		gossip:            g,
		livenessThreshold: livenessThreshold,
		heartbeatInterval: livenessThreshold - renewalDuration,
		selfSem:           make(chan struct{}, 1),
		otherSem:          make(chan struct{}, 1),
		triggerHeartbeat:  make(chan struct{}, 1),
	}
	nl.metrics = LivenessMetrics{
		LiveNodes:          metric.NewFunctionalGauge(metaLiveNodes, nl.numLiveNodes),
		HeartbeatSuccesses: metric.NewCounter(metaHeartbeatSuccesses),
		HeartbeatFailures:  metric.NewCounter(metaHeartbeatFailures),
		EpochIncrements:    metric.NewCounter(metaEpochIncrements),
		HeartbeatLatency:   metric.NewLatency(metaHeartbeatLatency, histogramWindow),
	}
	nl.pauseHeartbeat.Store(false)
	nl.mu.nodes = map[roachpb.NodeID]Liveness{}

	livenessRegex := gossip.MakePrefixPattern(gossip.KeyNodeLivenessPrefix)
	nl.gossip.RegisterCallback(livenessRegex, nl.livenessGossipUpdate)

	return nl
}

var errNodeDrainingSet = errors.New("node is already draining")

func (nl *NodeLiveness) sem(nodeID roachpb.NodeID) chan struct{} {
	if nodeID == nl.gossip.NodeID.Get() {
		return nl.selfSem
	}
	return nl.otherSem
}

// SetDraining attempts to update this node's liveness record to put itself
// into the draining state.
func (nl *NodeLiveness) SetDraining(ctx context.Context, drain bool) {
	ctx = nl.ambientCtx.AnnotateCtx(ctx)
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		liveness, err := nl.Self()
		if err != nil && err != ErrNoLivenessRecord {
			log.Errorf(ctx, "unexpected error getting liveness: %s", err)
		}
		if err := nl.setDrainingInternal(ctx, liveness, drain); err == nil {
			return
		}
	}
}

// SetDecommissioning runs a best-effort attempt of marking the the liveness
// record as decommissioning. It returns whether the function committed a
// transaction that updated the liveness record.
func (nl *NodeLiveness) SetDecommissioning(
	ctx context.Context, nodeID roachpb.NodeID, decommission bool,
) (changeCommitted bool, err error) {
	ctx = nl.ambientCtx.AnnotateCtx(ctx)

	attempt := func() (bool, error) {
		// Allow only one decommissioning attempt in flight per node at a time.
		// This is required for correct results since we may otherwise race with
		// concurrent `IncrementEpoch` calls and get stuck in a situation in
		// which the cached liveness is has decommissioning=false while it's
		// really true, and that means that SetDecommissioning becomes a no-op
		// (which is correct) but that our cached liveness never updates to
		// reflect that.
		//
		// See https://github.com/cockroachdb/cockroach/issues/17995.
		sem := nl.sem(nodeID)
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return false, ctx.Err()
		}
		defer func() {
			<-sem
		}()

		// We need the current liveness in each iteration.
		//
		// We ignore any liveness record in Gossip because we may have to fall back
		// to the KV store anyway. The scenario in which this is needed is:
		// - kill node 2 and stop node 1
		// - wait for node 2's liveness record's Gossip entry to expire on all surviving nodes
		// - restart node 1; it'll never see node 2 in `GetLiveness` unless the whole
		//   node liveness span gets regossiped (unlikely if it wasn't the lease holder
		//   for that span)
		// - can't decommission node 2 from node 1 without KV fallback.
		//
		// See #20863.
		//
		// NB: this also de-flakes TestNodeLivenessDecommissionAbsent; running
		// decommissioning commands in a tight loop on different nodes sometimes
		// results in unintentional no-ops (due to the Gossip lag); this could be
		// observed by users in principle, too.
		var oldLiveness Liveness
		if err := nl.db.GetProto(ctx, keys.NodeLivenessKey(nodeID), &oldLiveness); err != nil {
			return false, errors.Wrap(err, "unable to get liveness")
		}
		if (oldLiveness == Liveness{}) {
			return false, ErrNoLivenessRecord
		}
		// We may have discovered a Liveness not yet received via Gossip. Offer it
		// to make sure that when we actually try to update the liveness, the
		// previous view is correct. This, too, is required to de-flake
		// TestNodeLivenessDecommissionAbsent.
		nl.maybeUpdate(oldLiveness)

		return nl.setDecommissioningInternal(ctx, nodeID, &oldLiveness, decommission)
	}

	for {
		changeCommitted, err := attempt()
		if errors.Cause(err) == errChangeDecommissioningFailed {
			continue // expected when epoch incremented
		}
		return changeCommitted, err
	}
}

func (nl *NodeLiveness) setDrainingInternal(
	ctx context.Context, liveness *Liveness, drain bool,
) error {
	nodeID := nl.gossip.NodeID.Get()
	sem := nl.sem(nodeID)
	// Allow only one attempt to set the draining field at a time.
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-sem
	}()

	newLiveness := Liveness{
		NodeID: nodeID,
		Epoch:  1,
	}
	if liveness != nil {
		newLiveness = *liveness
	}
	newLiveness.Draining = drain
	if err := nl.updateLiveness(ctx, &newLiveness, liveness, func(actual Liveness) error {
		nl.maybeUpdate(actual)
		if actual.Draining == newLiveness.Draining {
			return errNodeDrainingSet
		}
		return errors.New("failed to update liveness record")
	}); err != nil {
		if err == errNodeDrainingSet {
			return nil
		}
		return err
	}
	nl.maybeUpdate(newLiveness)
	return nil
}

func (nl *NodeLiveness) setDecommissioningInternal(
	ctx context.Context, nodeID roachpb.NodeID, liveness *Liveness, decommission bool,
) (changeCommitted bool, err error) {
	newLiveness := Liveness{
		NodeID: nodeID,
		Epoch:  1,
	}
	if liveness != nil {
		newLiveness = *liveness
	}
	newLiveness.Decommissioning = decommission
	var conditionFailed bool
	if err := nl.updateLiveness(ctx, &newLiveness, liveness, func(actual Liveness) error {
		conditionFailed = true
		if actual.Decommissioning == newLiveness.Decommissioning {
			return nil
		}
		return errChangeDecommissioningFailed
	}); err != nil {
		return false, err
	}
	return !conditionFailed && liveness.Decommissioning != decommission, nil
}

// GetLivenessThreshold returns the maximum duration between heartbeats
// before a node is considered not-live.
func (nl *NodeLiveness) GetLivenessThreshold() time.Duration {
	return nl.livenessThreshold
}

// IsLive returns whether or not the specified node is considered live based on
// whether or not its liveness has expired regardless of the liveness status. It
// is an error if the specified node is not in the local liveness table.
func (nl *NodeLiveness) IsLive(nodeID roachpb.NodeID) (bool, error) {
	liveness, err := nl.GetLiveness(nodeID)
	if err != nil {
		return false, err
	}
	return liveness.IsLive(nl.clock.Now(), nl.clock.MaxOffset()), nil
}

// IsHealthy returns whether or not the specified node IsLive and is in a LIVE
// state, i.e. not draining, decommissioning, or otherwise unhealthy.
func (nl *NodeLiveness) IsHealthy(nodeID roachpb.NodeID) (bool, error) {
	liveness, err := nl.GetLiveness(nodeID)
	if err != nil {
		return false, err
	}
	ls := liveness.LivenessStatus(
		nl.clock.Now().GoTime(),
		nl.GetLivenessThreshold(),
		nl.clock.MaxOffset(),
	)
	return ls == NodeLivenessStatus_LIVE, nil
}

// StartHeartbeat starts a periodic heartbeat to refresh this node's
// last heartbeat in the node liveness table. The optionally provided
// HeartbeatCallback will be invoked whenever this node updates its own liveness.
func (nl *NodeLiveness) StartHeartbeat(
	ctx context.Context, stopper *stop.Stopper, alive HeartbeatCallback,
) {
	log.VEventf(ctx, 1, "starting liveness heartbeat")
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()

	nl.mu.Lock()
	nl.mu.heartbeatCallback = alive
	nl.mu.Unlock()

	stopper.RunWorker(ctx, func(context.Context) {
		ambient := nl.ambientCtx
		ambient.AddLogTag("hb", nil)
		ticker := time.NewTicker(nl.heartbeatInterval)
		defer ticker.Stop()
		incrementEpoch := true
		for {
			if !nl.pauseHeartbeat.Load().(bool) {
				func() {
					// Give the context a timeout approximately as long as the time we
					// have left before our liveness entry expires.
					ctx, cancel := context.WithTimeout(context.Background(), nl.livenessThreshold-nl.heartbeatInterval)
					ctx, sp := ambient.AnnotateCtxWithSpan(ctx, "liveness heartbeat loop")
					defer cancel()
					defer sp.Finish()

					// Retry heartbeat in the event the conditional put fails.
					for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
						liveness, err := nl.Self()
						if err != nil && err != ErrNoLivenessRecord {
							log.Errorf(ctx, "unexpected error getting liveness: %v", err)
						}
						if err := nl.heartbeatInternal(ctx, liveness, incrementEpoch); err != nil {
							if err == ErrEpochIncremented {
								log.Infof(ctx, "%s; retrying", err)
								continue
							}
							log.Warningf(ctx, "failed node liveness heartbeat: %v", err)
						} else {
							incrementEpoch = false // don't increment epoch after first heartbeat
						}
						break
					}
				}()
			}
			select {
			case <-ticker.C:
			case <-nl.triggerHeartbeat:
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// PauseHeartbeat stops or restarts the periodic heartbeat depending on the
// pause parameter. When unpausing, triggers an immediate heartbeat.
// This is only used by tests as of the 1.1 release, so be careful about using
// it in non-test code.
func (nl *NodeLiveness) PauseHeartbeat(pause bool) {
	nl.pauseHeartbeat.Store(pause)
	if !pause {
		select {
		case nl.triggerHeartbeat <- struct{}{}:
		default:
		}
	}
}

// DisableAllHeartbeatsForTest disables all node liveness heartbeats, including
// those triggered from outside the normal StartHeartbeat loop. Returns a
// closure to call to re-enable heartbeats. Only safe for use in tests.
func (nl *NodeLiveness) DisableAllHeartbeatsForTest() func() {
	nl.PauseHeartbeat(true)
	nl.selfSem <- struct{}{}
	nl.otherSem <- struct{}{}
	return func() {
		<-nl.selfSem
		<-nl.otherSem
	}
}

var errNodeAlreadyLive = errors.New("node already live")

// Heartbeat is called to update a node's expiration timestamp. This
// method does a conditional put on the node liveness record, and if
// successful, stores the updated liveness record in the nodes map.
func (nl *NodeLiveness) Heartbeat(ctx context.Context, liveness *Liveness) error {
	return nl.heartbeatInternal(ctx, liveness, false /* increment epoch */)
}

func (nl *NodeLiveness) heartbeatInternal(
	ctx context.Context, liveness *Liveness, incrementEpoch bool,
) error {
	ctx, finish := tracing.EnsureChildSpan(ctx, nl.ambientCtx.Tracer, "liveness heartbeat")
	defer finish()
	defer func(start time.Time) {
		dur := timeutil.Now().Sub(start)
		nl.metrics.HeartbeatLatency.RecordValue(dur.Nanoseconds())
		if dur > time.Second {
			log.Warningf(ctx, "slow heartbeat took %0.1fs", dur.Seconds())
		}
	}(timeutil.Now())

	// Allow only one heartbeat at a time.
	nodeID := nl.gossip.NodeID.Get()
	sem := nl.sem(nodeID)
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-sem
	}()

	newLiveness := Liveness{
		NodeID: nodeID,
		Epoch:  1,
	}
	if liveness != nil {
		newLiveness = *liveness
		if incrementEpoch {
			newLiveness.Epoch++
			// Clear draining field.
			newLiveness.Draining = false
		}
	}
	// We need to add the maximum clock offset to the expiration because it's
	// used when determining liveness for a node (unless we're configured for
	// clockless reads).
	{
		maxOffset := nl.clock.MaxOffset()
		if maxOffset == timeutil.ClocklessMaxOffset {
			maxOffset = 0
		}
		newLiveness.Expiration = hlc.LegacyTimestamp(
			nl.clock.Now().Add((nl.livenessThreshold + maxOffset).Nanoseconds(), 0))
		// This guards against the system clock moving backwards. As long
		// as the cockroach process is running, checks inside hlc.Clock
		// will ensure that the clock never moves backwards, but these
		// checks don't work across process restarts.
		if liveness != nil && newLiveness.Expiration.Less(liveness.Expiration) {
			return errors.Errorf("proposed liveness update expires earlier than previous record")
		}
	}
	if err := nl.updateLiveness(ctx, &newLiveness, liveness, func(actual Liveness) error {
		// Update liveness to actual value on mismatch.
		nl.maybeUpdate(actual)
		// If the actual liveness is different than expected, but is
		// considered live, treat the heartbeat as a success. This can
		// happen when the periodic heartbeater races with a concurrent
		// lease acquisition.
		if actual.IsLive(nl.clock.Now(), nl.clock.MaxOffset()) && !incrementEpoch {
			return errNodeAlreadyLive
		}
		// Otherwise, return error.
		return ErrEpochIncremented
	}); err != nil {
		if err == errNodeAlreadyLive {
			nl.metrics.HeartbeatSuccesses.Inc(1)
			return nil
		}
		nl.metrics.HeartbeatFailures.Inc(1)
		return err
	}

	log.VEventf(ctx, 1, "heartbeat %+v", newLiveness.Expiration)
	nl.maybeUpdate(newLiveness)
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

// GetIsLiveMap returns a map of nodeID to boolean liveness status of
// each node.
func (nl *NodeLiveness) GetIsLiveMap() map[roachpb.NodeID]bool {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	lMap := map[roachpb.NodeID]bool{}
	now := nl.clock.Now()
	maxOffset := nl.clock.MaxOffset()
	for nID, l := range nl.mu.nodes {
		lMap[nID] = l.IsLive(now, maxOffset)
	}
	return lMap
}

// GetLivenesses returns a slice containing the liveness status of every node
// on the cluster.
func (nl *NodeLiveness) GetLivenesses() []Liveness {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	livenesses := make([]Liveness, 0, len(nl.mu.nodes))
	for _, l := range nl.mu.nodes {
		livenesses = append(livenesses, l)
	}
	return livenesses
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
	sem := nl.sem(liveness.NodeID)
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-sem
	}()

	if liveness.IsLive(nl.clock.Now(), nl.clock.MaxOffset()) {
		return errors.Errorf("cannot increment epoch on live node: %+v", liveness)
	}
	newLiveness := *liveness
	newLiveness.Epoch++
	if err := nl.updateLiveness(ctx, &newLiveness, liveness, func(actual Liveness) error {
		defer nl.maybeUpdate(actual)
		if actual.Epoch > liveness.Epoch {
			return errEpochAlreadyIncremented
		} else if actual.Epoch < liveness.Epoch {
			return errors.Errorf("unexpected liveness epoch %d; expected >= %d", actual.Epoch, liveness.Epoch)
		}
		return errors.Errorf("mismatch incrementing epoch for %+v; actual is %+v", *liveness, actual)
	}); err != nil {
		if err == errEpochAlreadyIncremented {
			return nil
		}
		return err
	}

	log.Infof(ctx, "incremented n%d liveness epoch to %d", newLiveness.NodeID, newLiveness.Epoch)
	nl.maybeUpdate(newLiveness)
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

// updateLiveness does a conditional put on the node liveness record for the
// node specified by nodeID. In the event that the conditional put fails, and
// the handleCondFailed callback is not nil, it's invoked with the actual node
// liveness record and nil is returned for an error. If handleCondFailed is nil,
// any conditional put failure is returned as an error to the caller. The
// conditional put is done as a 1PC transaction with a ModifiedSpanTrigger which
// indicates the node liveness record that the range leader should gossip on
// commit.
//
// updateLiveness terminates certain errors that are expected to occur
// sporadically, such as TransactionStatusError (due to the 1PC requirement of
// the liveness txn, and ambiguous results).
func (nl *NodeLiveness) updateLiveness(
	ctx context.Context,
	newLiveness *Liveness,
	oldLiveness *Liveness,
	handleCondFailed func(actual Liveness) error,
) error {
	for {
		for _, eng := range nl.engines {
			// Synchronously writing to all disks before updating node liveness because
			// we don't want any excessively slow disks to prevent the lease from
			// shifting to other nodes. If the disk is slow, batch.Commit() will block.
			batch := eng.NewBatch()
			defer batch.Close()

			if err := batch.LogData(nil); err != nil {
				return errors.Wrapf(err, "couldn't update node liveness because LogData to disk fails")
			}

			if err := batch.Commit(true /* sync */); err != nil {
				return errors.Wrapf(err, "couldn't update node liveness because Commit to disk fails")
			}
		}

		if err := nl.updateLivenessAttempt(ctx, newLiveness, oldLiveness, handleCondFailed); err != nil {
			// Intentionally don't errors.Cause() the error, or we'd hop past errRetryLiveness.
			if _, ok := err.(*errRetryLiveness); ok {
				log.Infof(ctx, "retrying liveness update after %s", err)
				continue
			}
			return err
		}
		return nil
	}
}

func (nl *NodeLiveness) updateLivenessAttempt(
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

	if err := nl.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		key := keys.NodeLivenessKey(newLiveness.NodeID)
		// The batch interface requires interface{}(nil), not *Liveness(nil).
		if oldLiveness == nil {
			b.CPut(key, newLiveness, nil)
		} else {
			b.CPut(key, newLiveness, oldLiveness)
		}
		// Use a trigger on EndTransaction to indicate that node liveness should
		// be re-gossiped. Further, require that this transaction complete as a
		// one phase commit to eliminate the possibility of leaving write intents.
		b.AddRawRequest(&roachpb.EndTransactionRequest{
			Commit:     true,
			Require1PC: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				ModifiedSpanTrigger: &roachpb.ModifiedSpanTrigger{
					NodeLivenessSpan: &roachpb.Span{
						Key:    key,
						EndKey: key.Next(),
					},
				},
			},
		})
		return txn.Run(ctx, b)
	}); err != nil {
		switch tErr := errors.Cause(err).(type) {
		case *roachpb.ConditionFailedError:
			if handleCondFailed != nil {
				if tErr.ActualValue == nil {
					return handleCondFailed(Liveness{})
				}
				var actualLiveness Liveness
				if err := tErr.ActualValue.GetProto(&actualLiveness); err != nil {
					return errors.Wrapf(err, "couldn't update node liveness from CPut actual value")
				}
				return handleCondFailed(actualLiveness)
			}
		case *roachpb.TransactionStatusError:
			return &errRetryLiveness{err}
		case *roachpb.AmbiguousResultError:
			return &errRetryLiveness{err}
		}
		return err
	}

	nl.mu.Lock()
	cb := nl.mu.heartbeatCallback
	nl.mu.Unlock()
	if cb != nil {
		cb(ctx)
	}
	return nil
}

// maybeUpdate replaces the liveness (if it appears newer) and invokes the
// registered callbacks if the node became live in the process.
func (nl *NodeLiveness) maybeUpdate(new Liveness) {
	nl.mu.Lock()
	// Note that this works fine even if `old` is empty.
	old := nl.mu.nodes[new.NodeID]
	should := shouldReplaceLiveness(old, new)
	var callbacks []IsLiveCallback
	if should {
		nl.mu.nodes[new.NodeID] = new
		callbacks = append(callbacks, nl.mu.callbacks...)
	}
	nl.mu.Unlock()

	if !should {
		return
	}

	now, offset := nl.clock.Now(), nl.clock.MaxOffset()
	if !old.IsLive(now, offset) && new.IsLive(now, offset) {
		for _, fn := range callbacks {
			fn(new.NodeID)
		}
	}
}

func shouldReplaceLiveness(old, new Liveness) bool {
	if (old == Liveness{}) {
		return true
	}

	// Compare first Epoch, and no change there, Expiration.
	if old.Epoch != new.Epoch {
		return old.Epoch < new.Epoch
	}
	if old.Expiration != new.Expiration {
		return old.Expiration.Less(new.Expiration)
	}

	// If Epoch and Expiration are unchanged, assume that the update is newer
	// when its draining or decommissioning field changed.
	//
	// This has false positives (in which case we're clobbering the liveness). A
	// better way to handle liveness updates in general is to add a sequence
	// number.
	//
	// See #18219.
	return old.Draining != new.Draining || old.Decommissioning != new.Decommissioning
}

// livenessGossipUpdate is the gossip callback used to keep the
// in-memory liveness info up to date.
func (nl *NodeLiveness) livenessGossipUpdate(key string, content roachpb.Value) {
	var liveness Liveness
	if err := content.GetProto(&liveness); err != nil {
		log.Error(context.TODO(), err)
		return
	}

	nl.maybeUpdate(liveness)
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
	ctx := nl.ambientCtx.AnnotateCtx(context.Background())

	selfID := nl.gossip.NodeID.Get()
	if selfID == 0 {
		return 0
	}

	now := nl.clock.Now()
	maxOffset := nl.clock.MaxOffset()

	nl.mu.Lock()
	defer nl.mu.Unlock()

	self, err := nl.getLivenessLocked(selfID)
	if err == ErrNoLivenessRecord {
		return 0
	}
	if err != nil {
		log.Warningf(ctx, "looking up own liveness: %s", err)
		return 0
	}
	// If this node isn't live, we don't want to report its view of node liveness
	// because it's more likely to be inaccurate than the view of a live node.
	if !self.IsLive(now, maxOffset) {
		return 0
	}
	var liveNodes int64
	for _, l := range nl.mu.nodes {
		if l.IsLive(now, maxOffset) {
			liveNodes++
		}
	}
	return liveNodes
}
