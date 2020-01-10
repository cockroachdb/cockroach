// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

var (
	// ErrNoLivenessRecord is returned when asking for liveness information
	// about a node for which nothing is known.
	ErrNoLivenessRecord = errors.New("node not in the liveness table")

	errChangeDecommissioningFailed = errors.New("failed to change the decommissioning status")

	// ErrEpochIncremented is returned when a heartbeat request fails because
	// the underlying liveness record has had its epoch incremented.
	ErrEpochIncremented = errors.New("heartbeat failed on epoch increment")

	// ErrEpochAlreadyIncremented is returned by IncrementEpoch when
	// someone else has already incremented the epoch to the desired
	// value.
	ErrEpochAlreadyIncremented = errors.New("epoch already incremented")

	errLiveClockNotLive = errors.New("not live")
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
		Name:        "liveness.livenodes",
		Help:        "Number of live nodes in the cluster (will be 0 if this node is not itself live)",
		Measurement: "Nodes",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatSuccesses = metric.Metadata{
		Name:        "liveness.heartbeatsuccesses",
		Help:        "Number of successful node liveness heartbeats from this node",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatFailures = metric.Metadata{
		Name:        "liveness.heartbeatfailures",
		Help:        "Number of failed node liveness heartbeats from this node",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaEpochIncrements = metric.Metadata{
		Name:        "liveness.epochincrements",
		Help:        "Number of times this node has incremented its liveness epoch",
		Measurement: "Epochs",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatLatency = metric.Metadata{
		Name:        "liveness.heartbeatlatency",
		Help:        "Node liveness heartbeat latency",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

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

// NodeLiveness is a centralized failure detector that coordinates
// with the epoch-based range system to provide for leases of
// indefinite length (replacing frequent per-range lease renewals with
// heartbeats to the liveness system).
//
// It is also used as a general-purpose failure detector, but it is
// not ideal for this purpose. It is inefficient due to the use of
// replicated durable writes, and is not very sensitive (it primarily
// tests connectivity from the node to the liveness range; a node with
// a failing disk could still be considered live by this system).
//
// The persistent state of node liveness is stored in the KV layer,
// near the beginning of the keyspace. These are normal MVCC keys,
// written by CPut operations in 1PC transactions (the use of
// transactions and MVCC is regretted because it means that the
// liveness span depends on MVCC GC and can get overwhelmed if GC is
// not working. Transactions were used only to piggyback on the
// transaction commit trigger). The leaseholder of the liveness range
// gossips its contents whenever they change (only the changed
// portion); other nodes rarely read from this range directly.
//
// The use of conditional puts is crucial to maintain the guarantees
// needed by epoch-based leases. Both the Heartbeat and IncrementEpoch
// on this type require an expected value to be passed in; see
// comments on those methods for more.
//
// TODO(bdarnell): Also document interaction with draining and decommissioning.
type NodeLiveness struct {
	ambientCtx        log.AmbientContext
	clock             *hlc.Clock
	db                *client.DB
	engines           []engine.Engine
	gossip            *gossip.Gossip
	livenessThreshold time.Duration
	heartbeatInterval time.Duration
	selfSem           chan struct{}
	st                *cluster.Settings
	otherSem          chan struct{}
	// heartbeatPaused contains an atomically-swapped number representing a bool
	// (1 or 0). heartbeatToken is a channel containing a token which is taken
	// when heartbeating or when pausing the heartbeat. Used for testing.
	heartbeatPaused uint32
	heartbeatToken  chan struct{}
	metrics         LivenessMetrics

	mu struct {
		syncutil.RWMutex
		callbacks         []IsLiveCallback
		nodes             map[roachpb.NodeID]storagepb.Liveness
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
	st *cluster.Settings,
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
		st:                st,
		otherSem:          make(chan struct{}, 1),
		heartbeatToken:    make(chan struct{}, 1),
	}
	nl.metrics = LivenessMetrics{
		LiveNodes:          metric.NewFunctionalGauge(metaLiveNodes, nl.numLiveNodes),
		HeartbeatSuccesses: metric.NewCounter(metaHeartbeatSuccesses),
		HeartbeatFailures:  metric.NewCounter(metaHeartbeatFailures),
		EpochIncrements:    metric.NewCounter(metaEpochIncrements),
		HeartbeatLatency:   metric.NewLatency(metaHeartbeatLatency, histogramWindow),
	}
	nl.mu.nodes = map[roachpb.NodeID]storagepb.Liveness{}
	nl.heartbeatToken <- struct{}{}

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
			log.Errorf(ctx, "unexpected error getting liveness: %+v", err)
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
		//
		// TODO(bdarnell): This is the one place where a range other than
		// the leaseholder reads from this range. Should this read from
		// gossip instead? (I have vague concerns about concurrent reads
		// and timestamp cache pushes causing problems here)
		var oldLiveness storagepb.Liveness
		if err := nl.db.GetProto(ctx, keys.NodeLivenessKey(nodeID), &oldLiveness); err != nil {
			return false, errors.Wrap(err, "unable to get liveness")
		}
		if (oldLiveness == storagepb.Liveness{}) {
			return false, ErrNoLivenessRecord
		}

		// We may have discovered a Liveness not yet received via Gossip. Offer it
		// to make sure that when we actually try to update the liveness, the
		// previous view is correct. This, too, is required to de-flake
		// TestNodeLivenessDecommissionAbsent.
		nl.maybeUpdate(oldLiveness)

		return nl.setDecommissioningInternal(ctx, nodeID, oldLiveness, decommission)
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
	ctx context.Context, liveness storagepb.Liveness, drain bool,
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

	update := livenessUpdate{
		Liveness: storagepb.Liveness{
			NodeID: nodeID,
			Epoch:  1,
		},
	}
	if liveness != (storagepb.Liveness{}) {
		update.Liveness = liveness
	}
	update.Draining = drain
	update.ignoreCache = true

	if err := nl.updateLiveness(ctx, update, liveness, func(actual storagepb.Liveness) error {
		nl.maybeUpdate(actual)
		if actual.Draining == update.Draining {
			return errNodeDrainingSet
		}
		return errors.New("failed to update liveness record")
	}); err != nil {
		if err == errNodeDrainingSet {
			return nil
		}
		return err
	}
	nl.maybeUpdate(update.Liveness)
	return nil
}

type livenessUpdate struct {
	storagepb.Liveness
	// When ignoreCache is set, we won't assume that our in-memory cached version
	// of the liveness record is accurate and will use a CPut on the liveness
	// table with whatever the client supplied. This is used for operations that
	// don't want to deal with the inconsistencies of using the cache.
	ignoreCache bool
}

func (nl *NodeLiveness) setDecommissioningInternal(
	ctx context.Context, nodeID roachpb.NodeID, liveness storagepb.Liveness, decommission bool,
) (changeCommitted bool, err error) {
	update := livenessUpdate{
		Liveness: storagepb.Liveness{
			NodeID: nodeID,
			Epoch:  1,
		},
	}
	if liveness != (storagepb.Liveness{}) {
		update.Liveness = liveness
	}
	update.Decommissioning = decommission
	update.ignoreCache = true

	var conditionFailed bool
	if err := nl.updateLiveness(ctx, update, liveness, func(actual storagepb.Liveness) error {
		conditionFailed = true
		if actual.Decommissioning == update.Decommissioning {
			return nil
		}
		return errChangeDecommissioningFailed
	}); err != nil {
		return false, err
	}
	committed := !conditionFailed && liveness.Decommissioning != decommission
	return committed, nil
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
	return liveness.IsLive(nl.clock.Now()), nil
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

	nl.mu.RLock()
	nl.mu.heartbeatCallback = alive
	nl.mu.RUnlock()

	stopper.RunWorker(ctx, func(context.Context) {
		ambient := nl.ambientCtx
		ambient.AddLogTag("liveness-hb", nil)
		ctx, cancel := stopper.WithCancelOnStop(context.Background())
		defer cancel()
		ctx, sp := ambient.AnnotateCtxWithSpan(ctx, "liveness heartbeat loop")
		defer sp.Finish()

		incrementEpoch := true
		ticker := time.NewTicker(nl.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-nl.heartbeatToken:
			case <-stopper.ShouldStop():
				return
			}
			// Give the context a timeout approximately as long as the time we
			// have left before our liveness entry expires.
			if err := contextutil.RunWithTimeout(ctx, "node liveness heartbeat", nl.livenessThreshold-nl.heartbeatInterval,
				func(ctx context.Context) error {
					// Retry heartbeat in the event the conditional put fails.
					for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
						liveness, err := nl.Self()
						if err != nil && err != ErrNoLivenessRecord {
							log.Errorf(ctx, "unexpected error getting liveness: %+v", err)
						}
						if err := nl.heartbeatInternal(ctx, liveness, incrementEpoch); err != nil {
							if err == ErrEpochIncremented {
								log.Infof(ctx, "%s; retrying", err)
								continue
							}
							return err
						}
						incrementEpoch = false // don't increment epoch after first heartbeat
						break
					}
					return nil
				}); err != nil {
				log.Warningf(ctx, "failed node liveness heartbeat: %+v", err)
			}

			nl.heartbeatToken <- struct{}{}
			select {
			case <-ticker.C:
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// PauseHeartbeat stops or restarts the periodic heartbeat depending on the
// pause parameter. When pause is true, waits until it acquires the heartbeatToken
// (unless heartbeat was already paused); this ensures that no heartbeats happen
// after this is called. This function is only safe for use in tests.
func (nl *NodeLiveness) PauseHeartbeat(pause bool) {
	if pause {
		if swapped := atomic.CompareAndSwapUint32(&nl.heartbeatPaused, 0, 1); swapped {
			<-nl.heartbeatToken
		}
	} else {
		if swapped := atomic.CompareAndSwapUint32(&nl.heartbeatPaused, 1, 0); swapped {
			nl.heartbeatToken <- struct{}{}
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
//
// The liveness argument is the expected previous value of this node's
// liveness.
//
// If this method returns nil, the node's liveness has been extended,
// relative to the previous value. It may or may not still be alive
// when this method returns.
//
// On failure, this method returns ErrEpochIncremented, although this
// may not necessarily mean that the epoch was actually incremented.
// TODO(bdarnell): Fix error semantics here.
//
// This method is rarely called directly; heartbeats are normally sent
// by the StartHeartbeat loop.
// TODO(bdarnell): Should we just remove this synchronous heartbeat completely?
func (nl *NodeLiveness) Heartbeat(ctx context.Context, liveness storagepb.Liveness) error {
	return nl.heartbeatInternal(ctx, liveness, false /* increment epoch */)
}

func (nl *NodeLiveness) heartbeatInternal(
	ctx context.Context, liveness storagepb.Liveness, incrementEpoch bool,
) error {
	ctx, sp := tracing.EnsureChildSpan(ctx, nl.ambientCtx.Tracer, "liveness heartbeat")
	defer sp.Finish()
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

	update := livenessUpdate{
		Liveness: storagepb.Liveness{
			NodeID: nodeID,
			Epoch:  1,
		},
	}
	if liveness != (storagepb.Liveness{}) {
		update.Liveness = liveness
		if incrementEpoch {
			update.Epoch++
			// Clear draining field.
			update.Draining = false
		}
	}
	// We need to add the maximum clock offset to the expiration because it's
	// used when determining liveness for a node.
	{
		update.Expiration = hlc.LegacyTimestamp(
			nl.clock.Now().Add((nl.livenessThreshold).Nanoseconds(), 0))
		// This guards against the system clock moving backwards. As long
		// as the cockroach process is running, checks inside hlc.Clock
		// will ensure that the clock never moves backwards, but these
		// checks don't work across process restarts.
		if update.Expiration.Less(liveness.Expiration) {
			return errors.Errorf("proposed liveness update expires earlier than previous record")
		}
	}
	if err := nl.updateLiveness(ctx, update, liveness, func(actual storagepb.Liveness) error {
		// Update liveness to actual value on mismatch.
		nl.maybeUpdate(actual)
		// If the actual liveness is different than expected, but is
		// considered live, treat the heartbeat as a success. This can
		// happen when the periodic heartbeater races with a concurrent
		// lease acquisition.
		//
		// TODO(bdarnell): If things are very slow, the new liveness may
		// have already expired and we'd incorrectly return
		// ErrEpochIncremented. Is this check even necessary? The common
		// path through this method doesn't check whether the liveness
		// expired while in flight, so maybe we don't have to care about
		// that and only need to distinguish between same and different
		// epochs in our return value.
		if actual.IsLive(nl.clock.Now()) && !incrementEpoch {
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

	log.VEventf(ctx, 1, "heartbeat %+v", update.Expiration)
	nl.maybeUpdate(update.Liveness)
	nl.metrics.HeartbeatSuccesses.Inc(1)
	return nil
}

// Self returns the liveness record for this node. ErrNoLivenessRecord
// is returned in the event that the node has neither heartbeat its
// liveness record successfully, nor received a gossip message containing
// a former liveness update on restart.
func (nl *NodeLiveness) Self() (storagepb.Liveness, error) {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	return nl.getLivenessLocked(nl.gossip.NodeID.Get())
}

// IsLiveMapEntry encapsulates data about current liveness for a
// node.
type IsLiveMapEntry struct {
	IsLive bool
	Epoch  int64
}

// IsLiveMap is a type alias for a map from NodeID to IsLiveMapEntry.
type IsLiveMap map[roachpb.NodeID]IsLiveMapEntry

// GetIsLiveMap returns a map of nodeID to boolean liveness status of
// each node. This excludes nodes that were removed completely (dead +
// decommissioning).
func (nl *NodeLiveness) GetIsLiveMap() IsLiveMap {
	lMap := IsLiveMap{}
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	now := nl.clock.Now()
	for nID, l := range nl.mu.nodes {
		isLive := l.IsLive(now)
		if !isLive && l.Decommissioning {
			// This is a node that was completely removed. Skip over it.
			continue
		}
		lMap[nID] = IsLiveMapEntry{
			IsLive: isLive,
			Epoch:  l.Epoch,
		}
	}
	return lMap
}

// GetLivenesses returns a slice containing the liveness status of
// every node on the cluster known to gossip. Callers should consider
// calling (statusServer).NodesWithLiveness() instead where possible.
func (nl *NodeLiveness) GetLivenesses() []storagepb.Liveness {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	livenesses := make([]storagepb.Liveness, 0, len(nl.mu.nodes))
	for _, l := range nl.mu.nodes {
		livenesses = append(livenesses, l)
	}
	return livenesses
}

// GetLiveness returns the liveness record for the specified nodeID.
// ErrNoLivenessRecord is returned in the event that nothing is yet
// known about nodeID via liveness gossip.
func (nl *NodeLiveness) GetLiveness(nodeID roachpb.NodeID) (storagepb.Liveness, error) {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	return nl.getLivenessLocked(nodeID)
}

func (nl *NodeLiveness) getLivenessLocked(nodeID roachpb.NodeID) (storagepb.Liveness, error) {
	if l, ok := nl.mu.nodes[nodeID]; ok {
		return l, nil
	}
	return storagepb.Liveness{}, ErrNoLivenessRecord
}

// IncrementEpoch is called to attempt to revoke another node's
// current epoch, causing an expiration of all its leases. This method
// does a conditional put on the node liveness record, and if
// successful, stores the updated liveness record in the nodes map. If
// this method is called on a node ID which is considered live
// according to the most recent information gathered through gossip,
// an error is returned.
//
// The liveness argument is used as the expected value on the
// conditional put. If this method returns nil, there was a match and
// the epoch has been incremented. This means that the expiration time
// in the supplied liveness accurately reflects the time at which the
// epoch ended.
//
// If this method returns ErrEpochAlreadyIncremented, the epoch has
// already been incremented past the one in the liveness argument, but
// the conditional put did not find a match. This means that another
// node performed a successful IncrementEpoch, but we can't tell at
// what time the epoch actually ended. (Usually when multiple
// IncrementEpoch calls race, they're using the same expected value.
// But when there is a severe backlog, it's possible for one increment
// to get stuck in a queue long enough for the dead node to make
// another successful heartbeat, and a second increment to come in
// after that)
func (nl *NodeLiveness) IncrementEpoch(ctx context.Context, liveness storagepb.Liveness) error {
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

	if liveness.IsLive(nl.clock.Now()) {
		return errors.Errorf("cannot increment epoch on live node: %+v", liveness)
	}
	update := livenessUpdate{Liveness: liveness}
	update.Epoch++
	if err := nl.updateLiveness(ctx, update, liveness, func(actual storagepb.Liveness) error {
		defer nl.maybeUpdate(actual)
		if actual.Epoch > liveness.Epoch {
			return ErrEpochAlreadyIncremented
		} else if actual.Epoch < liveness.Epoch {
			return errors.Errorf("unexpected liveness epoch %d; expected >= %d", actual.Epoch, liveness.Epoch)
		}
		return errors.Errorf("mismatch incrementing epoch for %+v; actual is %+v", liveness, actual)
	}); err != nil {
		return err
	}

	log.Infof(ctx, "incremented n%d liveness epoch to %d", update.NodeID, update.Epoch)
	nl.maybeUpdate(update.Liveness)
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
	update livenessUpdate,
	oldLiveness storagepb.Liveness,
	handleCondFailed func(actual storagepb.Liveness) error,
) error {
	for {
		// Before each attempt, ensure that the context has not expired.
		if err := ctx.Err(); err != nil {
			return err
		}

		for _, eng := range nl.engines {
			// We synchronously write to all disks before updating liveness because we
			// don't want any excessively slow disks to prevent leases from being
			// shifted to other nodes. A slow/stalled disk would block here and cause
			// the node to lose its leases.
			if err := engine.WriteSyncNoop(ctx, eng); err != nil {
				return errors.Wrapf(err, "couldn't update node liveness because disk write failed")
			}
		}
		if err := nl.updateLivenessAttempt(ctx, update, oldLiveness, handleCondFailed); err != nil {
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
	update livenessUpdate,
	oldLiveness storagepb.Liveness,
	handleCondFailed func(actual storagepb.Liveness) error,
) error {
	// First check the existing liveness map to avoid known conditional
	// put failures.
	if !update.ignoreCache {
		if l, err := nl.GetLiveness(update.NodeID); err == nil && l != oldLiveness {
			return handleCondFailed(l)
		}
	}

	if err := nl.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		key := keys.NodeLivenessKey(update.NodeID)
		val := update.Liveness
		if oldLiveness == (storagepb.Liveness{}) {
			b.CPut(key, &val, nil)
		} else {
			expVal := oldLiveness
			b.CPutDeprecated(key, &val, &expVal)
		}
		// Use a trigger on EndTxn to indicate that node liveness should be
		// re-gossiped. Further, require that this transaction complete as a one
		// phase commit to eliminate the possibility of leaving write intents.
		b.AddRawRequest(&roachpb.EndTxnRequest{
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
					return handleCondFailed(storagepb.Liveness{})
				}
				var actualLiveness storagepb.Liveness
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
func (nl *NodeLiveness) maybeUpdate(new storagepb.Liveness) {
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

	now := nl.clock.Now()
	if !old.IsLive(now) && new.IsLive(now) {
		for _, fn := range callbacks {
			fn(new.NodeID)
		}
	}
}

func shouldReplaceLiveness(old, new storagepb.Liveness) bool {
	if (old == storagepb.Liveness{}) {
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
	var liveness storagepb.Liveness
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

	nl.mu.RLock()
	defer nl.mu.RUnlock()

	self, err := nl.getLivenessLocked(selfID)
	if err == ErrNoLivenessRecord {
		return 0
	}
	if err != nil {
		log.Warningf(ctx, "looking up own liveness: %+v", err)
		return 0
	}
	// If this node isn't live, we don't want to report its view of node liveness
	// because it's more likely to be inaccurate than the view of a live node.
	if !self.IsLive(now) {
		return 0
	}
	var liveNodes int64
	for _, l := range nl.mu.nodes {
		if l.IsLive(now) {
			liveNodes++
		}
	}
	return liveNodes
}

// AsLiveClock returns a closedts.LiveClockFn that takes a current timestamp off
// the clock and returns it only if node liveness indicates that the node is live
// at that timestamp and the returned epoch.
func (nl *NodeLiveness) AsLiveClock() closedts.LiveClockFn {
	return func(nodeID roachpb.NodeID) (hlc.Timestamp, ctpb.Epoch, error) {
		now := nl.clock.Now()
		liveness, err := nl.GetLiveness(nodeID)
		if err != nil {
			return hlc.Timestamp{}, 0, err
		}
		if !liveness.IsLive(now) {
			return hlc.Timestamp{}, 0, errLiveClockNotLive
		}
		return now, ctpb.Epoch(liveness.Epoch), nil
	}
}

// GetNodeCount returns a count of the number of nodes in the cluster,
// including dead nodes, but excluding decommissioning or decommissioned nodes.
func (nl *NodeLiveness) GetNodeCount() int {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	var count int
	for _, l := range nl.mu.nodes {
		if !l.Decommissioning {
			count++
		}
	}
	return count
}
