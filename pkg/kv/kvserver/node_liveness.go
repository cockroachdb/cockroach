// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var (
	// ErrNoLivenessRecord is returned when asking for liveness information
	// about a node for which nothing is known.
	ErrNoLivenessRecord = errors.New("node not in the liveness table")

	// errChangeMembershipStatusFailed is returned when we're not able to
	// conditionally write the target membership status. It's safe to retry
	// when encountering this error.
	errChangeMembershipStatusFailed = errors.New("failed to change the membership status")

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
	metaHeartbeatsInFlight = metric.Metadata{
		Name:        "liveness.heartbeatsinflight",
		Help:        "Number of in-flight liveness heartbeats from this node",
		Measurement: "Requests",
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
	HeartbeatsInFlight *metric.Gauge
	HeartbeatSuccesses *metric.Counter
	HeartbeatFailures  *metric.Counter
	EpochIncrements    *metric.Counter
	HeartbeatLatency   *metric.Histogram
}

// IsLiveCallback is invoked when a node's IsLive state changes to true.
// Callbacks can be registered via NodeLiveness.RegisterCallback().
type IsLiveCallback func(kvserverpb.Liveness)

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
	db                *kv.DB
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
		nodes             map[roachpb.NodeID]LivenessRecord
		heartbeatCallback HeartbeatCallback
		// Before heartbeating, we write to each of these engines to avoid
		// maintaining liveness when a local disks is stalled.
		engines []storage.Engine
	}
}

// LivenessRecord is a liveness record that has been read from the database,
// together with its database encoding. The encoding is useful for CPut-ing an
// update to the liveness record: the raw value will act as the expected value.
// This way the proto's encoding can change without the CPut failing.
type LivenessRecord struct {
	kvserverpb.Liveness
	// raw represents the raw bytes read from the database - suitable to pass to a
	// CPut. Nil if the value doesn't exist in the DB.
	raw []byte
}

// NewNodeLiveness returns a new instance of NodeLiveness configured
// with the specified gossip instance.
func NewNodeLiveness(
	ambient log.AmbientContext,
	clock *hlc.Clock,
	db *kv.DB,
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
		HeartbeatsInFlight: metric.NewGauge(metaHeartbeatsInFlight),
		HeartbeatSuccesses: metric.NewCounter(metaHeartbeatSuccesses),
		HeartbeatFailures:  metric.NewCounter(metaHeartbeatFailures),
		EpochIncrements:    metric.NewCounter(metaEpochIncrements),
		HeartbeatLatency:   metric.NewLatency(metaHeartbeatLatency, histogramWindow),
	}
	nl.mu.nodes = make(map[roachpb.NodeID]LivenessRecord)
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
//
// The reporter callback, if non-nil, is called on a best effort basis
// to report work that needed to be done and which may or may not have
// been done by the time this call returns. See the explanation in
// pkg/server/drain.go for details.
func (nl *NodeLiveness) SetDraining(
	ctx context.Context, drain bool, reporter func(int, redact.SafeString),
) {
	ctx = nl.ambientCtx.AnnotateCtx(ctx)
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		oldLivenessRec, err := nl.SelfEx()
		if err != nil && !errors.Is(err, ErrNoLivenessRecord) {
			log.Errorf(ctx, "unexpected error getting liveness: %+v", err)
		}
		err = nl.setDrainingInternal(ctx, oldLivenessRec, drain, reporter)
		if err != nil {
			if log.V(1) {
				log.Infof(ctx, "attempting to set liveness draining status to %v: %v", drain, err)
			}
			continue
		}
		return
	}
}

// SetMembershipStatus changes the liveness record to reflect the target
// membership status. It does so idempotently, and may retry internally until it
// observes its target state durably persisted. It returns whether it was able
// to change the membership status (as opposed to it returning early when
// finding the target status possibly set by another node).
func (nl *NodeLiveness) SetMembershipStatus(
	ctx context.Context, nodeID roachpb.NodeID, targetStatus kvserverpb.MembershipStatus,
) (statusChanged bool, err error) {
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
		var oldLiveness kvserverpb.Liveness
		kv, err := nl.db.Get(ctx, keys.NodeLivenessKey(nodeID))
		if err != nil {
			return false, errors.Wrap(err, "unable to get liveness")
		}
		if kv.Value == nil {
			return false, ErrNoLivenessRecord
		}
		if err := kv.Value.GetProto(&oldLiveness); err != nil {
			return false, errors.Wrap(err, "invalid liveness record")
		}

		oldLivenessRec := LivenessRecord{
			Liveness: oldLiveness,
			raw:      kv.Value.TagAndDataBytes(),
		}

		// We may have discovered a Liveness not yet received via Gossip.
		// Offer it to make sure that when we actually try to update the
		// liveness, the previous view is correct. This, too, is required to
		// de-flake TestNodeLivenessDecommissionAbsent.
		nl.maybeUpdate(oldLivenessRec)
		return nl.setMembershipStatusInternal(ctx, nodeID, oldLivenessRec, targetStatus)
	}

	for {
		statusChanged, err := attempt()
		if errors.Is(err, errChangeMembershipStatusFailed) {
			// Expected when epoch incremented, it's safe to retry.
			continue
		}
		return statusChanged, err
	}
}

func (nl *NodeLiveness) setDrainingInternal(
	ctx context.Context,
	oldLivenessRec LivenessRecord,
	drain bool,
	reporter func(int, redact.SafeString),
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

	// Let's compute what our new liveness record should be.
	var newLiveness kvserverpb.Liveness
	if oldLivenessRec.Liveness == (kvserverpb.Liveness{}) {
		// Liveness record didn't previously exist, so we create one.
		newLiveness = kvserverpb.Liveness{
			NodeID: nodeID,
			Epoch:  1,
		}
	} else {
		newLiveness = oldLivenessRec.Liveness
	}

	if reporter != nil && drain && !newLiveness.Draining {
		// Report progress to the Drain RPC.
		reporter(1, "liveness record")
	}
	newLiveness.Draining = drain

	update := livenessUpdate{
		oldLiveness: oldLivenessRec.Liveness,
		newLiveness: newLiveness,
		oldRaw:      oldLivenessRec.raw,
		ignoreCache: true,
	}
	written, err := nl.updateLiveness(ctx, update, func(actual LivenessRecord) error {
		nl.maybeUpdate(actual)

		if actual.Draining == update.newLiveness.Draining {
			return errNodeDrainingSet
		}
		return errors.New("failed to update liveness record because record has changed")
	})
	if err != nil {
		if log.V(1) {
			log.Infof(ctx, "updating liveness record: %v", err)
		}
		if errors.Is(err, errNodeDrainingSet) {
			return nil
		}
		return err
	}

	nl.maybeUpdate(written)
	return nil
}

// livenessUpdate contains the information for CPutting a new version of a
// liveness record. It has both the new and the old version of the proto.
type livenessUpdate struct {
	newLiveness kvserverpb.Liveness
	oldLiveness kvserverpb.Liveness
	// When ignoreCache is set, we won't assume that our in-memory cached version
	// of the liveness record is accurate and will use a CPut on the liveness
	// table with the old value supplied by the client (oldRaw). This is used for
	// operations that don't want to deal with the inconsistencies of using the
	// cache.
	//
	// When ignoreCache is not set, the state of the cache is checked against old and,
	// if they don't correspond, the CPut is considered to have failed.
	//
	// When ignoreCache is set, oldRaw needs to be set as well.
	ignoreCache bool
	// oldRaw is the raw value from which `old` was decoded. Used for CPuts as the
	// existing value. Note that we don't simply marshal `old` as that would break
	// if unmarshalling/marshaling doesn't round-trip. Nil means that a liveness
	// record for the respected node is not expected to exist in the database.
	//
	// oldRaw must not be set when ignoreCache is not set.
	oldRaw []byte
}

// CreateLivenessRecord creates a liveness record for the node specified by the
// given node ID. This is typically used when adding a new node to a running
// cluster, or when bootstrapping a cluster through a given node.
//
// This is a pared down version of StartHeartbeat; it exists only to durably
// persist a liveness to record the node's existence. Nodes will heartbeat their
// records after starting up, and incrementing to epoch=1 when doing so, at
// which point we'll set an appropriate expiration timestamp, gossip the
// liveness record, and update our in-memory representation of it.
//
// NB: An existing liveness record is not overwritten by this method, we return
// an error instead.
func (nl *NodeLiveness) CreateLivenessRecord(ctx context.Context, nodeID roachpb.NodeID) error {
	// We start off at epoch=0, entrusting the initial heartbeat to increment it
	// to epoch=1 to signal the very first time the node is up and running.
	liveness := kvserverpb.Liveness{NodeID: nodeID, Epoch: 0}

	// We skip adding an expiration, we only really care about the liveness
	// record existing within KV.

	v := new(roachpb.Value)
	if err := nl.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		key := keys.NodeLivenessKey(nodeID)
		if err := v.SetProto(&liveness); err != nil {
			log.Fatalf(ctx, "failed to marshall proto: %s", err)
		}
		// Given we're looking to create a new liveness record here, we don't
		// expect to find anything.
		b.CPut(key, v, nil)

		// We don't bother adding a gossip trigger, that'll happen with the
		// first heartbeat. We still keep it as a 1PC commit to avoid leaving
		// write intents.
		b.AddRawRequest(&roachpb.EndTxnRequest{
			Commit:     true,
			Require1PC: true,
		})
		return txn.Run(ctx, b)
	}); err != nil {
		return err
	}

	// We'll learn about this liveness record through gossip eventually, so we
	// don't bother updating our in-memory view of node liveness.

	log.Infof(ctx, "created liveness record for n%d", nodeID)
	return nil
}

func (nl *NodeLiveness) setMembershipStatusInternal(
	ctx context.Context,
	nodeID roachpb.NodeID,
	oldLivenessRec LivenessRecord,
	targetStatus kvserverpb.MembershipStatus,
) (statusChanged bool, err error) {
	// Let's compute what our new liveness record should be.
	var newLiveness kvserverpb.Liveness
	if oldLivenessRec.Liveness == (kvserverpb.Liveness{}) {
		// Liveness record didn't previously exist, so we create one.
		//
		// TODO(irfansharif): The above is now no longer possible. We always
		// create one (see CreateLivenessRecord, WriteInitialClusterData) when
		// adding a node to the cluster. We should clean up all this logic that
		// tries to work around the liveness record possibly not existing.
		newLiveness = kvserverpb.Liveness{
			NodeID: nodeID,
			Epoch:  1,
		}
	} else {
		// We start off with a copy of our existing liveness record.
		newLiveness = oldLivenessRec.Liveness
	}

	newLiveness.Membership = targetStatus
	if oldLivenessRec.Membership == newLiveness.Membership {
		// No-op. Return early.
		return false, nil
	} else if oldLivenessRec.Membership.Decommissioned() &&
		newLiveness.Membership.Decommissioning() {
		// Marking a decommissioned node for decommissioning is a no-op. We
		// return early.
		return false, nil
	}

	if oldLivenessRec.Liveness != (kvserverpb.Liveness{}) {
		err := kvserverpb.ValidateLivenessTransition(oldLivenessRec.Liveness, newLiveness)
		if err != nil {
			return false, err
		}
	}

	update := livenessUpdate{
		newLiveness: newLiveness,
		oldLiveness: oldLivenessRec.Liveness,
		oldRaw:      oldLivenessRec.raw,
		ignoreCache: true,
	}
	statusChanged = true
	if _, err := nl.updateLiveness(ctx, update, func(actual LivenessRecord) error {
		if actual.Membership != update.newLiveness.Membership {
			// We're racing with another attempt at updating the liveness
			// record, we error out in order to retry.
			return errChangeMembershipStatusFailed
		}
		// The found liveness membership status is the same as the target one,
		// so we consider our work done. We inform the caller that this attempt
		// was a no-op.
		statusChanged = false
		return nil
	}); err != nil {
		return false, err
	}

	return statusChanged, nil
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
	// NB: We use clock.Now().GoTime() instead of clock.PhysicalTime() in order to
	// consider clock signals from other nodes.
	return liveness.IsLive(nl.clock.Now().GoTime()), nil
}

// StartHeartbeat starts a periodic heartbeat to refresh this node's last
// heartbeat in the node liveness table. The optionally provided
// HeartbeatCallback will be invoked whenever this node updates its own
// liveness. The slice of engines will be written to before each heartbeat to
// avoid maintaining liveness in the presence of disk stalls.
func (nl *NodeLiveness) StartHeartbeat(
	ctx context.Context, stopper *stop.Stopper, engines []storage.Engine, alive HeartbeatCallback,
) {
	log.VEventf(ctx, 1, "starting liveness heartbeat")
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()

	if len(engines) == 0 {
		// Avoid silently forgetting to pass the engines. It happened before.
		log.Fatalf(ctx, "must supply at least one engine")
	}

	nl.mu.Lock()
	nl.mu.heartbeatCallback = alive
	nl.mu.engines = engines
	nl.mu.Unlock()

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
						oldLiveness, err := nl.Self()
						if err != nil && !errors.Is(err, ErrNoLivenessRecord) {
							log.Errorf(ctx, "unexpected error getting liveness: %+v", err)
						}
						if err := nl.heartbeatInternal(ctx, oldLiveness, incrementEpoch); err != nil {
							if errors.Is(err, ErrEpochIncremented) {
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
				log.Warningf(ctx, heartbeatFailureLogFormat, err)
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

const heartbeatFailureLogFormat = `failed node liveness heartbeat: %+v

An inability to maintain liveness will prevent a node from participating in a
cluster. If this problem persists, it may be a sign of resource starvation or
of network connectivity problems. For help troubleshooting, visit:

    https://www.cockroachlabs.com/docs/stable/cluster-setup-troubleshooting.html#node-liveness-issues

`

// PauseHeartbeatLoopForTest stops the periodic heartbeat. The function
// waits until it acquires the heartbeatToken (unless heartbeat was
// already paused); this ensures that no heartbeats happen after this is
// called. Returns a closure to call to re-enable the heartbeat loop.
// This function is only safe for use in tests.
func (nl *NodeLiveness) PauseHeartbeatLoopForTest() func() {
	if swapped := atomic.CompareAndSwapUint32(&nl.heartbeatPaused, 0, 1); swapped {
		<-nl.heartbeatToken
	}
	return func() {
		if swapped := atomic.CompareAndSwapUint32(&nl.heartbeatPaused, 1, 0); swapped {
			nl.heartbeatToken <- struct{}{}
		}
	}
}

// PauseSynchronousHeartbeatsForTest disables all node liveness
// heartbeats triggered from outside the normal StartHeartbeat loop.
// Returns a closure to call to re-enable synchronous heartbeats. Only
// safe for use in tests.
func (nl *NodeLiveness) PauseSynchronousHeartbeatsForTest() func() {
	nl.selfSem <- struct{}{}
	nl.otherSem <- struct{}{}
	return func() {
		<-nl.selfSem
		<-nl.otherSem
	}
}

// PauseAllHeartbeatsForTest disables all node liveness heartbeats,
// including those triggered from outside the normal StartHeartbeat
// loop. Returns a closure to call to re-enable heartbeats. Only safe
// for use in tests.
func (nl *NodeLiveness) PauseAllHeartbeatsForTest() func() {
	enableLoop := nl.PauseHeartbeatLoopForTest()
	enableSync := nl.PauseSynchronousHeartbeatsForTest()
	return func() {
		enableLoop()
		enableSync()
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
func (nl *NodeLiveness) Heartbeat(ctx context.Context, liveness kvserverpb.Liveness) error {
	return nl.heartbeatInternal(ctx, liveness, false /* increment epoch */)
}

func (nl *NodeLiveness) heartbeatInternal(
	ctx context.Context, oldLiveness kvserverpb.Liveness, incrementEpoch bool,
) (err error) {
	ctx, sp := tracing.EnsureChildSpan(ctx, nl.ambientCtx.Tracer, "liveness heartbeat")
	defer sp.Finish()
	defer func(start time.Time) {
		dur := timeutil.Now().Sub(start)
		nl.metrics.HeartbeatLatency.RecordValue(dur.Nanoseconds())
		if dur > time.Second {
			log.Warningf(ctx, "slow heartbeat took %s; err=%v", dur, err)
		}
	}(timeutil.Now())

	// Collect a clock reading from before we begin queuing on the heartbeat
	// semaphore. This method (attempts to, see [*]) guarantees that, if
	// successful, the liveness record's expiration will be at least the
	// liveness threshold above the time that the method was called.
	// Collecting this clock reading before queuing allows us to enforce
	// this while avoiding redundant liveness heartbeats during thundering
	// herds without needing to explicitly coalesce heartbeats.
	//
	// [*]: see TODO below about how errNodeAlreadyLive handling does not
	//      enforce this guarantee.
	beforeQueueTS := nl.clock.Now()
	minExpiration := hlc.LegacyTimestamp(
		beforeQueueTS.Add(nl.livenessThreshold.Nanoseconds(), 0))

	// Before queueing, record the heartbeat as in-flight.
	nl.metrics.HeartbeatsInFlight.Inc(1)
	defer nl.metrics.HeartbeatsInFlight.Dec(1)

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

	// If we are not intending to increment the node's liveness epoch, detect
	// whether this heartbeat is needed anymore. It is possible that we queued
	// for long enough on the semaphore such that other heartbeat attempts ahead
	// of us already incremented the expiration past what we wanted. Note that
	// if we allowed the heartbeat to proceed in this case, we know that it
	// would hit a ConditionFailedError and return a errNodeAlreadyLive down
	// below.
	if !incrementEpoch {
		curLiveness, err := nl.Self()
		if err == nil && minExpiration.Less(curLiveness.Expiration) {
			return nil
		}
	}

	// Let's compute what our new liveness record should be.
	var newLiveness kvserverpb.Liveness
	if oldLiveness != (kvserverpb.Liveness{}) {
		// Start off with our existing view of liveness.
		newLiveness = oldLiveness
	} else {
		// We don't yet know about our own liveness record (which does exist, we
		// maintain the invariant that there's always a liveness record for
		// every given node). Let's retrieve it from KV before proceeding.
		//
		// If we didn't previously know about our liveness record, it indicates
		// that we're heartbeating for the very first time.
		kv, err := nl.db.Get(ctx, keys.NodeLivenessKey(nodeID))
		if err != nil {
			return errors.Wrap(err, "unable to get liveness")
		}

		if kv.Value != nil {
			// This is the happy path. Let's unpack the liveness record we found
			// within KV, and use that to inform what our new liveness should
			// be.
			if err := kv.Value.GetProto(&oldLiveness); err != nil {
				return errors.Wrap(err, "invalid liveness record")
			}

			oldLivenessRec := LivenessRecord{
				Liveness: oldLiveness,
				raw:      kv.Value.TagAndDataBytes(),
			}

			// Update our cache with the liveness record we just found.
			nl.maybeUpdate(oldLivenessRec)

			newLiveness = oldLiveness
		} else {
			// This is a "should basically never happen" scenario given our
			// invariant around always persisting liveness records on node
			// startup. But that was a change we added in 20.2. Though unlikely,
			// it's possible to get into the following scenario:
			//
			// - v20.1 node gets added to v20.1 cluster, and is quickly removed
			//   before being able to persist its liveness record.
			// - The cluster is upgraded to v20.2.
			// - The node from earlier is rolled into v20.2, and re-added to the
			//   cluster.
			// - It's never able to successfully heartbeat (it didn't join
			//   through the join rpc, bootstrap, or gossip). Welp.
			//
			// Given this possibility, we'll just fall back to creating the
			// liveness record here as we did in v20.1 code.
			//
			// TODO(irfansharif): Remove this once v20.2 is cut.
			log.Warningf(ctx, "missing liveness record for n%d; falling back to creating it in-place", nodeID)
			newLiveness = kvserverpb.Liveness{
				NodeID: nodeID,
				Epoch:  0, // incremented to epoch=1 below as needed
			}
		}
	}

	if incrementEpoch {
		newLiveness.Epoch++
		newLiveness.Draining = false // clear draining field
	}

	// Grab a new clock reading to compute the new expiration time,
	// since we may have queued on the semaphore for a while.
	afterQueueTS := nl.clock.Now()
	newLiveness.Expiration = hlc.LegacyTimestamp(
		afterQueueTS.Add(nl.livenessThreshold.Nanoseconds(), 0))
	// This guards against the system clock moving backwards. As long
	// as the cockroach process is running, checks inside hlc.Clock
	// will ensure that the clock never moves backwards, but these
	// checks don't work across process restarts.
	if newLiveness.Expiration.Less(oldLiveness.Expiration) {
		return errors.Errorf("proposed liveness update expires earlier than previous record")
	}

	update := livenessUpdate{
		oldLiveness: oldLiveness,
		newLiveness: newLiveness,
	}
	written, err := nl.updateLiveness(ctx, update, func(actual LivenessRecord) error {
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
		//
		// TODO(nvanbenschoten): Unlike the early return above, this doesn't
		// guarantee that the resulting expiration is past minExpiration,
		// only that it's different than our oldLiveness. Is that ok? It
		// hasn't caused issues so far, but we might want to detect this
		// case and retry, at least in the case of the liveness heartbeat
		// loop. The downside of this is that a heartbeat that's intending
		// to bump the expiration of a record out 9s into the future may
		// return a success even if the expiration is only 5 seconds in the
		// future. The next heartbeat will then start with only 0.5 seconds
		// before expiration.
		if actual.IsLive(nl.clock.Now().GoTime()) && !incrementEpoch {
			return errNodeAlreadyLive
		}
		// Otherwise, return error.
		return ErrEpochIncremented
	})
	if err != nil {
		if errors.Is(err, errNodeAlreadyLive) {
			nl.metrics.HeartbeatSuccesses.Inc(1)
			return nil
		}
		nl.metrics.HeartbeatFailures.Inc(1)
		return err
	}

	log.VEventf(ctx, 1, "heartbeat %+v", written.Expiration)
	nl.maybeUpdate(written)
	nl.metrics.HeartbeatSuccesses.Inc(1)
	return nil
}

// Self returns the liveness record for this node. ErrNoLivenessRecord
// is returned in the event that the node has neither heartbeat its
// liveness record successfully, nor received a gossip message containing
// a former liveness update on restart.
func (nl *NodeLiveness) Self() (kvserverpb.Liveness, error) {
	rec, err := nl.SelfEx()
	if err != nil {
		return kvserverpb.Liveness{}, err
	}
	return rec.Liveness, nil
}

// SelfEx is like Self, but returns the raw, encoded value that the database has
// for this liveness record in addition to the decoded liveness proto.
func (nl *NodeLiveness) SelfEx() (LivenessRecord, error) {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	return nl.getLivenessLocked(nl.gossip.NodeID.Get())
}

// IsLiveMapEntry encapsulates data about current liveness for a
// node.
type IsLiveMapEntry struct {
	kvserverpb.Liveness
	IsLive bool
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
	now := nl.clock.Now().GoTime()
	for nID, l := range nl.mu.nodes {
		isLive := l.IsLive(now)
		if !isLive && !l.Membership.Active() {
			// This is a node that was completely removed. Skip over it.
			continue
		}
		lMap[nID] = IsLiveMapEntry{
			Liveness: l.Liveness,
			IsLive:   isLive,
		}
	}
	return lMap
}

// GetLivenesses returns a slice containing the liveness status of
// every node on the cluster known to gossip. Callers should consider
// calling (statusServer).NodesWithLiveness() instead where possible.
func (nl *NodeLiveness) GetLivenesses() []kvserverpb.Liveness {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	livenesses := make([]kvserverpb.Liveness, 0, len(nl.mu.nodes))
	for _, l := range nl.mu.nodes {
		livenesses = append(livenesses, l.Liveness)
	}
	return livenesses
}

// GetLiveness returns the liveness record for the specified nodeID.
// ErrNoLivenessRecord is returned in the event that nothing is yet known about
// nodeID via liveness gossip. The record returned also includes the raw,
// encoded value that the database has for this liveness record in addition to
// the decoded liveness proto.
func (nl *NodeLiveness) GetLiveness(nodeID roachpb.NodeID) (LivenessRecord, error) {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	return nl.getLivenessLocked(nodeID)
}

func (nl *NodeLiveness) getLivenessLocked(nodeID roachpb.NodeID) (LivenessRecord, error) {
	if l, ok := nl.mu.nodes[nodeID]; ok {
		return l, nil
	}
	return LivenessRecord{}, ErrNoLivenessRecord
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
func (nl *NodeLiveness) IncrementEpoch(ctx context.Context, liveness kvserverpb.Liveness) error {
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

	if liveness.IsLive(nl.clock.Now().GoTime()) {
		return errors.Errorf("cannot increment epoch on live node: %+v", liveness)
	}

	update := livenessUpdate{
		newLiveness: liveness,
		oldLiveness: liveness,
	}
	update.newLiveness.Epoch++

	written, err := nl.updateLiveness(ctx, update, func(actual LivenessRecord) error {
		nl.maybeUpdate(actual)

		if actual.Epoch > liveness.Epoch {
			return ErrEpochAlreadyIncremented
		} else if actual.Epoch < liveness.Epoch {
			return errors.Errorf("unexpected liveness epoch %d; expected >= %d", actual.Epoch, liveness.Epoch)
		}
		return errors.Errorf("mismatch incrementing epoch for %+v; actual is %+v", liveness, actual)
	})
	if err != nil {
		return err
	}

	log.Infof(ctx, "incremented n%d liveness epoch to %d", written.NodeID, written.Epoch)
	nl.maybeUpdate(written)
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
// node specified by nodeID. In the event that the conditional put fails, the
// handleCondFailed callback is invoked with the actual node liveness record;
// the error returned by the callback replaces the ConditionFailedError as the
// retval, and an empty LivenessRecord is returned.
//
// The conditional put is done as a 1PC transaction with a ModifiedSpanTrigger
// which indicates the node liveness record that the range leader should gossip
// on commit.
//
// updateLiveness terminates certain errors that are expected to occur
// sporadically, such as TransactionStatusError (due to the 1PC requirement of
// the liveness txn, and ambiguous results).
//
// If the CPut is successful (i.e. no error is returned and handleCondFailed is
// not called), the value that has been written is returned as a LivenessRecord.
// This includes the encoded bytes, and it can be used to update the local
// cache.
func (nl *NodeLiveness) updateLiveness(
	ctx context.Context, update livenessUpdate, handleCondFailed func(actual LivenessRecord) error,
) (LivenessRecord, error) {
	for {
		// Before each attempt, ensure that the context has not expired.
		if err := ctx.Err(); err != nil {
			return LivenessRecord{}, err
		}

		nl.mu.RLock()
		engines := nl.mu.engines
		nl.mu.RUnlock()
		for _, eng := range engines {
			// We synchronously write to all disks before updating liveness because we
			// don't want any excessively slow disks to prevent leases from being
			// shifted to other nodes. A slow/stalled disk would block here and cause
			// the node to lose its leases.
			if err := storage.WriteSyncNoop(ctx, eng); err != nil {
				return LivenessRecord{}, errors.Wrapf(err, "couldn't update node liveness because disk write failed")
			}
		}
		written, err := nl.updateLivenessAttempt(ctx, update, handleCondFailed)
		if err != nil {
			if errors.HasType(err, (*errRetryLiveness)(nil)) {
				log.Infof(ctx, "retrying liveness update after %s", err)
				continue
			}
			return LivenessRecord{}, err
		}
		return written, nil
	}
}

func (nl *NodeLiveness) updateLivenessAttempt(
	ctx context.Context, update livenessUpdate, handleCondFailed func(actual LivenessRecord) error,
) (LivenessRecord, error) {
	var oldRaw []byte
	if update.ignoreCache {
		// If ignoreCache is set, the caller is manually providing the previous
		// value in update.oldRaw.
		oldRaw = update.oldRaw
	} else {
		// Check the existing liveness map to avoid known conditional put
		// failures. The raw value from the map also helps us in doing the CPut.
		if update.oldRaw != nil {
			log.Fatalf(ctx, "unexpected oldRaw when ignoreCache not specified")
		}

		l, err := nl.GetLiveness(update.newLiveness.NodeID)
		if err != nil && !errors.Is(err, ErrNoLivenessRecord) {
			return LivenessRecord{}, err
		}
		if err == nil && l.Liveness != update.oldLiveness {
			return LivenessRecord{}, handleCondFailed(l)
		}
		oldRaw = l.raw
	}

	v := new(roachpb.Value)
	if err := nl.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		key := keys.NodeLivenessKey(update.newLiveness.NodeID)
		if err := v.SetProto(&update.newLiveness); err != nil {
			log.Fatalf(ctx, "failed to marshall proto: %s", err)
		}
		b.CPut(key, v, oldRaw)
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
		if tErr := (*roachpb.ConditionFailedError)(nil); errors.As(err, &tErr) {
			if tErr.ActualValue == nil {
				return LivenessRecord{}, handleCondFailed(LivenessRecord{})
			}
			var actualLiveness kvserverpb.Liveness
			if err := tErr.ActualValue.GetProto(&actualLiveness); err != nil {
				return LivenessRecord{}, errors.Wrapf(err, "couldn't update node liveness from CPut actual value")
			}
			return LivenessRecord{}, handleCondFailed(LivenessRecord{Liveness: actualLiveness, raw: tErr.ActualValue.TagAndDataBytes()})
		} else if errors.HasType(err, (*roachpb.TransactionStatusError)(nil)) ||
			errors.HasType(err, (*roachpb.AmbiguousResultError)(nil)) {
			return LivenessRecord{}, &errRetryLiveness{err}
		}
		return LivenessRecord{}, err
	}

	nl.mu.RLock()
	cb := nl.mu.heartbeatCallback
	nl.mu.RUnlock()
	if cb != nil {
		cb(ctx)
	}
	return LivenessRecord{Liveness: update.newLiveness, raw: v.TagAndDataBytes()}, nil
}

// maybeUpdate replaces the liveness (if it appears newer) and invokes the
// registered callbacks if the node became live in the process.
func (nl *NodeLiveness) maybeUpdate(new LivenessRecord) {
	// An empty new means that we haven't updated anything.
	if new.Liveness == (kvserverpb.Liveness{}) {
		return
	}

	nl.mu.Lock()
	old := nl.mu.nodes[new.NodeID]

	should := shouldReplaceLiveness(old.Liveness, new.Liveness)
	var callbacks []IsLiveCallback
	if should {
		nl.mu.nodes[new.NodeID] = new
		callbacks = append(callbacks, nl.mu.callbacks...)
	}
	nl.mu.Unlock()

	if !should {
		return
	}

	now := nl.clock.Now().GoTime()
	if !old.IsLive(now) && new.IsLive(now) {
		for _, fn := range callbacks {
			fn(new.Liveness)
		}
	}
}

// shouldReplaceLiveness checks to see if the new liveness is in fact newer
// than the old liveness.
func shouldReplaceLiveness(old, new kvserverpb.Liveness) bool {
	if (old == kvserverpb.Liveness{}) {
		return true
	}

	// Compare liveness information. If old < new, replace.
	if cmp := old.Compare(new); cmp != 0 {
		return cmp < 0
	}

	// If Epoch and Expiration are unchanged, assume that the update is newer
	// when its draining or decommissioning field changed.
	//
	// This has false positives (in which case we're clobbering the liveness). A
	// better way to handle liveness updates in general is to add a sequence
	// number.
	//
	// See #18219.
	return old.Draining != new.Draining ||
		old.Membership != new.Membership
}

// livenessGossipUpdate is the gossip callback used to keep the
// in-memory liveness info up to date.
func (nl *NodeLiveness) livenessGossipUpdate(_ string, content roachpb.Value) {
	var liveness kvserverpb.Liveness
	if err := content.GetProto(&liveness); err != nil {
		log.Errorf(context.TODO(), "%v", err)
		return
	}

	nl.maybeUpdate(LivenessRecord{Liveness: liveness, raw: content.TagAndDataBytes()})
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

	nl.mu.RLock()
	defer nl.mu.RUnlock()

	self, err := nl.getLivenessLocked(selfID)
	if errors.Is(err, ErrNoLivenessRecord) {
		return 0
	}
	if err != nil {
		log.Warningf(ctx, "looking up own liveness: %+v", err)
		return 0
	}
	now := nl.clock.Now().GoTime()
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
		if !liveness.IsLive(now.GoTime()) {
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
		if l.Membership.Active() {
			count++
		}
	}
	return count
}
