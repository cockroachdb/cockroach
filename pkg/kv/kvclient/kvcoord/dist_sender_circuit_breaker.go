// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// DistSenderCircuitBreakersMode controls if and to what level we trip circuit
// breakers for replicas in the DistSender when they are failed or stalled.
type DistSenderCircuitBreakersMode int64

const (
	// DistSenderCircuitBreakersNoRanges indicates we should never trip circuit
	// breakers.
	DistSenderCircuitBreakersNoRanges DistSenderCircuitBreakersMode = iota
	// DistSenderCircuitBreakersLivenessRangeOnly indicates we should only trip
	// circuit breakers if a replica belonging to node liveness experiences a
	// failure or stall.
	//
	// Typically, a replica belonging to node liveness has a high potential to
	// disrupt a cluster. All nodes need to write to the liveness range -- either
	// to heartbeat their liveness record to keep their leases, or to increment
	// another node's epoch before acquiring theirs. As such, a stalled or failed
	// replica belonging to the liveness range that these requests get stuck on is
	// no good -- nothing in the cluster will make progress.
	DistSenderCircuitBreakersLivenessRangeOnly
	// DistSenderCircuitBreakersAllRanges indicates that we should trip circuit
	// breakers for any replica that experiences a failure or a stall, regardless
	// of the range it belongs to.
	//
	// Tripping a circuit breaker involves launching a probe to eventually un-trip
	// the thing. This per-replica probe launches a goroutine. As such, a node
	// level issue that causes a large number of circuit breakers to be tripped on
	// a client has the potential of causing a big goroutine spike on the client.
	// We currently don't have good scalability numbers to measure the impact of
	// this -- once we do, we can revaluate the default mode we run in if the
	// numbers look good. Else we may want to make these things more scalable.
	DistSenderCircuitBreakersAllRanges
)

var (
	CircuitBreakersMode = settings.RegisterEnumSetting(
		settings.ApplicationLevel,
		"kv.dist_sender.circuit_breakers.mode",
		"set of ranges to trip circuit breakers for failing or stalled replicas",
		"liveness range only",
		map[DistSenderCircuitBreakersMode]string{
			DistSenderCircuitBreakersNoRanges:          "no ranges",
			DistSenderCircuitBreakersLivenessRangeOnly: "liveness range only",
			DistSenderCircuitBreakersAllRanges:         "all ranges",
		},
		settings.WithPublic,
	)

	CircuitBreakerProbeThreshold = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"kv.dist_sender.circuit_breaker.probe.threshold",
		"duration of errors or stalls after which a replica will be probed",
		3*time.Second,
		settings.WithPublic,
	)

	CircuitBreakerProbeInterval = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"kv.dist_sender.circuit_breaker.probe.interval",
		"interval between replica probes",
		3*time.Second,
		settings.WithPublic,
	)

	CircuitBreakerProbeTimeout = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"kv.dist_sender.circuit_breaker.probe.timeout",
		"timeout for replica probes",
		3*time.Second,
		settings.WithPublic,
	)

	CircuitBreakerCancellation = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"kv.dist_sender.circuit_breaker.cancellation.enabled",
		"when enabled, in-flight requests will be cancelled when the circuit breaker trips",
		true,
		settings.WithPublic,
	)

	CircuitBreakerCancellationWriteGracePeriod = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"kv.dist_sender.circuit_breaker.cancellation.write_grace_period",
		"how long after the circuit breaker trips to cancel write requests "+
			"(these can't retry internally, so should be long enough to allow quorum/lease recovery)",
		10*time.Second,
		settings.WithPublic,
		settings.WithValidateDuration(func(t time.Duration) error {
			// This prevents probes from exiting when idle, which can lead to buildup
			// of probe goroutines, so cap it at 1 minute.
			if t > time.Minute {
				return errors.New("grace period can't be more than 1 minute")
			}
			return nil
		}),
	)
)

const (
	// cbGCThreshold is the threshold after which an idle replica's circuit
	// breaker will be garbage collected, even when tripped.
	cbGCThreshold = 20 * time.Minute

	// cbGCInterval is the interval between garbage collection scans.
	cbGCInterval = time.Minute

	// cbProbeIdleTimeout is the interval with no client requests after which a
	// failing probe should exit. It will be relaunched on the next request.
	cbProbeIdleTimeout = 10 * time.Second
)

// cbRequestCancellationPolicy classifies a batch request.
type cbRequestCancellationPolicy int

const (
	cbCancelImmediately cbRequestCancellationPolicy = iota
	cbCancelAfterGracePeriod
	cbNumRequestKinds // must be last in list
)

func (k cbRequestCancellationPolicy) String() string {
	switch k {
	case cbCancelImmediately:
		return "immediately"
	case cbCancelAfterGracePeriod:
		return "after grace period"
	default:
		panic(errors.AssertionFailedf("unknown request kind %d", k))
	}
}

func cbRequestCancellationPolicyFromBatch(
	ba *kvpb.BatchRequest, withCommit bool,
) cbRequestCancellationPolicy {
	// If the batch request is writing or is part of a transaction commit, we
	// can't automatically retry it without risking an ambiguous error, so we
	// cancel it after a grace period. Otherwise, we cancel it immediately and
	// allow DistSender to retry.
	// TODO(nvanbenschoten): a batch request that is writing and is not part of a
	// transaction commit can be retried. Do we need the IsWrite condition here?
	if ba.IsWrite() || withCommit {
		return cbCancelAfterGracePeriod
	}
	return cbCancelImmediately
}

// cbKey is a key in the DistSender replica circuit breakers map.
type cbKey struct {
	rangeID   roachpb.RangeID
	replicaID roachpb.ReplicaID
}

// DistSenderCircuitBreakers manages circuit breakers for replicas. Their
// primary purpose is to prevent the DistSender getting stuck on non-functional
// replicas. The DistSender relies on receiving a NLHE from the replica to
// update its range cache and try other replicas, otherwise it will keep sending
// requests to the same broken replica which will continue to get stuck, giving
// the appearance of an unavailable range. This can happen if:
//
//   - The replica stalls, e.g. with a disk stall or mutex deadlock.
//
//   - Clients time out before the replica lease acquisition attempt times out,
//     e.g. if the replica is partitioned away from the leader.
//
// Each replica has its own circuit breaker. The circuit breaker will probe the
// replica if:
//
// - It has only returned errors in the past probe threshold.
//   - Checked after each error.
//   - Send/network errors are ignored, and handled by RPC circuit breakers.
//   - NLHE with a known lease is not considered an error.
//   - Client timeouts and context cancellations count as errors. Consider e.g.
//     a stalled replica which continually causes client timeouts.
//
// - It has potentially stalled, with no responses in the past probe threshold.
//   - Checked via an asynchronous loop.
//   - Any response from the replica resets the timer (even br.Error).
//   - Only if there are still in-flight requests.
//
// The breaker is only tripped once the probe fails (never in response to user
// request failures alone). If enabled, in-flight reads are cancelled
// immediately when the breaker trips, and writes are cancelled after a grace
// period (since they can't be automatically retried).
//
// The probe sends a LeaseInfo request and expects either a successful response
// (if it is the leaseholder) or a NLHE (if it knows a leaseholder or leader
// exists elsewhere) before the probe timeout. Otherwise, it will trip the
// circuit breaker. In particular, this will fail if the replica is unable to
// acquire or detect a lease, e.g. because it is partitioned away from the
// leader. With a tripped breaker, a new probe is sent every probe interval as
// long as the replica keeps seeing recent client traffic, otherwise a new one
// is launched on the next request.
//
// We don't try too hard to interpret errors from the replica, since this can be
// brittle. Instead, we assume that most functional replicas will have a mix of
// errors and successes. If we get this wrong (e.g. if a replica sees a steady
// stream of failing requests), we'll send a (successful) probe every 3 seconds,
// which is likely ok since this case is likely rare.
//
// Stale circuit breakers are removed if they haven't seen any traffic for the
// past GC threshold.
//
// TODO(erikgrinaker): we can extend this to also manage range-level circuit
// breakers, but for now we focus exclusively on replica-level circuit breakers.
// This avoids the overhead of maintaining and accessing a multi-level
// structure.
//
// TODO(erikgrinaker): this needs comprehensive testing.
type DistSenderCircuitBreakers struct {
	ambientCtx       log.AmbientContext
	stopper          *stop.Stopper
	settings         *cluster.Settings
	transportFactory TransportFactory
	metrics          DistSenderMetrics
	replicas         syncutil.Map[cbKey, ReplicaCircuitBreaker]
}

// NewDistSenderCircuitBreakers creates new DistSender circuit breakers.
func NewDistSenderCircuitBreakers(
	ambientCtx log.AmbientContext,
	stopper *stop.Stopper,
	settings *cluster.Settings,
	transportFactory TransportFactory,
	metrics DistSenderMetrics,
) *DistSenderCircuitBreakers {
	return &DistSenderCircuitBreakers{
		ambientCtx:       ambientCtx,
		stopper:          stopper,
		settings:         settings,
		transportFactory: transportFactory,
		metrics:          metrics,
	}
}

// Start starts the circuit breaker manager, and runs it until the stopper
// stops. It only returns an error if the server is already stopping.
func (d *DistSenderCircuitBreakers) Start() error {
	ctx := d.ambientCtx.AnnotateCtx(context.Background())
	err := d.stopper.RunAsyncTask(ctx, "distsender-circuit-breakers-stall-probe", d.probeStallLoop)
	if err != nil {
		return err
	}
	err = d.stopper.RunAsyncTask(ctx, "distsender-circuit-breakers-gc", d.gcLoop)
	if err != nil {
		return err
	}
	return nil
}

// probeStallLoop periodically scans replica circuit breakers to detect stalls
// and launch probes.
func (d *DistSenderCircuitBreakers) probeStallLoop(ctx context.Context) {
	// We use the probe interval as the scan interval, since we can sort of
	// consider this to be probing the replicas for a stall.
	var timer timeutil.Timer
	defer timer.Stop()
	timer.Reset(CircuitBreakerProbeInterval.Get(&d.settings.SV))

	for {
		select {
		case <-timer.C:
			timer.Read = true
			// Eagerly reset the timer, to avoid skewing the interval.
			timer.Reset(CircuitBreakerProbeInterval.Get(&d.settings.SV))
		case <-d.stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		}

		// Don't do anything if circuit breakers have been disabled.
		if d.Mode() == DistSenderCircuitBreakersNoRanges {
			continue
		}

		// Probe replicas for a stall if we haven't seen a response from them in the
		// past probe threshold.
		now := crtime.NowMono()
		probeThreshold := CircuitBreakerProbeThreshold.Get(&d.settings.SV)

		d.replicas.Range(func(_ cbKey, cb *ReplicaCircuitBreaker) bool {
			// Don't probe if the breaker is already tripped. It will be probed in
			// response to user traffic, to reduce the number of concurrent probes.
			if cb.stallDuration(now) >= probeThreshold && !cb.isTripped() {
				cb.breaker.Probe()
			}

			return true
		})
	}
}

// gcLoop periodically GCs replica circuit breakers that haven't seen traffic
// for the past GC threshold.
//
// We use this simple scheme both to avoid tracking replicas that aren't
// being used, and also to clean up after replicas that no longer exist.
// This is much simpler and less error-prone than eagerly removing them in
// response to errors and synchronizing with range descriptor updates,
// which would also risk significant churn to create and destroy circuit
// breakers if the DistSender keeps sending requests to them for some
// reason.
func (d *DistSenderCircuitBreakers) gcLoop(ctx context.Context) {
	ticker := time.NewTicker(cbGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-d.stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		}

		now := crtime.NowMono()

		var cbs, gced int
		d.replicas.Range(func(key cbKey, cb *ReplicaCircuitBreaker) bool {
			cbs++

			if idleDuration := cb.lastRequestDuration(now); idleDuration >= cbGCThreshold {
				// Check if we raced with a concurrent delete or replace. We don't
				// expect to, since only this loop removes circuit breakers.
				if cb2, ok := d.replicas.LoadAndDelete(key); ok {
					cb = cb2

					d.metrics.CircuitBreaker.Replicas.Dec(1)
					gced++

					// We don't expect a probe to run, since the replica is idle, but we
					// may race with a probe launch or there may be a long-running one (if
					// e.g. the probe timeout or interval has increased).
					//
					// Close closedC to stop any running probes and prevent new probes
					// from launching. Only we close it, due to the atomic map delete.
					close(cb.closedC)

					// The circuit breaker may be tripped, and reported as such in
					// metrics. A concurrent probe may also be about to trip/untrip it.
					// We let the probe's OnProbeDone() be responsible for managing the
					// ReplicasTripped gauge to avoid metrics leaks, by untripping the
					// breaker when closedC has been closed. To synchronize with a
					// concurrent probe, we attempt to launch a new one. Either:
					//
					// a) no probe is running: we launch a noop probe which will
					//    immediately call OnProbeDone() and clean up. All future
					//    probes are noops.
					//
					// b) a concurrent probe is running: the Probe() call is a noop, but
					//    when the running probe shuts down in response to closedC,
					//    OnProbeDone() will clean up.
					cb.breaker.Probe()
				}
			}
			return true
		})

		log.VEventf(ctx, 2, "garbage collected %d/%d DistSender replica circuit breakers", gced, cbs)
	}
}

// ForReplica returns a circuit breaker for a given replica.
func (d *DistSenderCircuitBreakers) ForReplica(
	rangeDesc *roachpb.RangeDescriptor, replDesc *roachpb.ReplicaDescriptor,
) *ReplicaCircuitBreaker {
	// If circuit breakers are disabled, return a nil breaker.
	if d.Mode() == DistSenderCircuitBreakersNoRanges {
		return nil
	}

	// If circuit breakers are only enabled for the liveness range, don't check
	// circuit breakers. If the mode changes for a tripped circuit breaker, the
	// range will eventually idle and be GC'ed since we will stop tracking new
	// requests to the range.
	// NB: We may allow the liveness range to split in the future so the overlap
	// check is safer.
	if d.Mode() == DistSenderCircuitBreakersLivenessRangeOnly {
		if !keys.NodeLivenessSpan.Overlaps(rangeDesc.KeySpan().AsRawSpanWithNoLocals()) {
			return nil
		}
	}

	key := cbKey{rangeID: rangeDesc.RangeID, replicaID: replDesc.ReplicaID}

	// Fast path: use existing circuit breaker.
	if cb, ok := d.replicas.Load(key); ok {
		return cb
	}

	// Slow path: construct a new replica circuit breaker and insert it. If we
	// race with a concurrent insert, return it instead.
	cb, loaded := d.replicas.LoadOrStore(key, newReplicaCircuitBreaker(d, rangeDesc, replDesc))
	if !loaded {
		d.metrics.CircuitBreaker.Replicas.Inc(1)
	}
	return cb
}

func (d *DistSenderCircuitBreakers) Mode() DistSenderCircuitBreakersMode {
	return CircuitBreakersMode.Get(&d.settings.SV)
}

// ReplicaCircuitBreaker is a circuit breaker for an individual replica.
type ReplicaCircuitBreaker struct {
	d        *DistSenderCircuitBreakers
	rangeID  roachpb.RangeID
	startKey roachpb.Key
	desc     roachpb.ReplicaDescriptor
	breaker  *circuit.Breaker

	// inflightReqs tracks the number of in-flight requests.
	inflightReqs atomic.Int32

	// lastRequest contains the last request timestamp, for garbage collection.
	lastRequest crtime.AtomicMono

	// errorSince is the timestamp when the current streak of errors began. Set on
	// an initial error, and cleared on successful responses.
	errorSince crtime.AtomicMono

	// stallSince is the timestamp when the current potential stall began. It is
	// set on every first in-flight request (inflightReqs==1) and moved forward on
	// every response from the replica (even errors).
	//
	// It is not reset to zero when inflightReqs==0, to avoid synchronization with
	// inflightReqs. To determine whether a replica is stalled, it is therefore
	// also necessary to check inflightReqs>0.
	stallSince crtime.AtomicMono

	// closedC is closed when the circuit breaker has been GCed. This will shut
	// down a running probe, and prevent new probes from launching.
	closedC chan struct{}

	mu struct {
		syncutil.Mutex

		// cancelFns contains context cancellation functions for all in-flight
		// requests, segmented by request type. Reads can be retried by the
		// DistSender, so we cancel these immediately when the breaker trips.
		// Writes can't automatically retry, and will return ambiguous result errors
		// to clients, so we only cancel them after a grace period.
		//
		// Only tracked if cancellation is enabled.
		cancelFns [cbNumRequestKinds]map[*kvpb.BatchRequest]context.CancelCauseFunc
	}
}

// newReplicaCircuitBreaker creates a new DistSender replica circuit breaker.
//
// TODO(erikgrinaker): consider pooling these.
func newReplicaCircuitBreaker(
	d *DistSenderCircuitBreakers,
	rangeDesc *roachpb.RangeDescriptor,
	replDesc *roachpb.ReplicaDescriptor,
) *ReplicaCircuitBreaker {
	r := &ReplicaCircuitBreaker{
		d:        d,
		rangeID:  rangeDesc.RangeID,
		startKey: rangeDesc.StartKey.AsRawKey(), // immutable
		desc:     *replDesc,
		closedC:  make(chan struct{}),
	}
	r.breaker = circuit.NewBreaker(circuit.Options{
		Name:         r.id(),
		AsyncProbe:   r.launchProbe,
		EventHandler: r,
	})
	for i := range r.mu.cancelFns {
		r.mu.cancelFns[i] = map[*kvpb.BatchRequest]context.CancelCauseFunc{}
	}
	return r
}

// replicaCircuitBreakerToken carries request-scoped state between Track() and
// done().
type replicaCircuitBreakerToken struct {
	// r is the circuit breaker reference. nil if circuit breakers were disabled
	// when we began tracking the request.
	r *ReplicaCircuitBreaker

	// ctx is the client's original context, to determine if it has gone away.
	ctx context.Context

	// cancelCtx is the child context used to cancel the request. nil if
	// cancellation is disabled.
	cancelCtx context.Context

	// ba is the batch request being tracked.
	ba *kvpb.BatchRequest

	// withCommit denotes whether the request is part of a transaction commit.
	withCommit bool
}

// Done records the result of the request and untracks it. If the request was
// cancelled by the circuit breaker, an appropriate context cancellation error
// is returned.
func (t replicaCircuitBreakerToken) Done(
	br *kvpb.BatchResponse, sendErr error, now crtime.Mono,
) error {
	return t.r.done(t.ctx, t.cancelCtx, t.ba, t.withCommit, br, sendErr, now)
}

// id returns a string identifier for the replica.
func (r *ReplicaCircuitBreaker) id() redact.RedactableString {
	return redact.Sprintf("r%d/%d:(n%d,s%d)",
		r.rangeID, r.desc.ReplicaID, r.desc.NodeID, r.desc.StoreID)
}

// errorDuration returns the error duration relative to now.
func (r *ReplicaCircuitBreaker) errorDuration(now crtime.Mono) time.Duration {
	errorSince := r.errorSince.Load()
	if errorSince == 0 || errorSince > now {
		return 0
	}
	return now.Sub(errorSince)
}

// stallDuration returns the stall duration relative to now.
func (r *ReplicaCircuitBreaker) stallDuration(now crtime.Mono) time.Duration {
	stallSince := r.stallSince.Load()
	// The replica is only stalled if there are in-flight requests.
	if r.inflightReqs.Load() == 0 || stallSince > now {
		return 0
	}
	return now.Sub(stallSince)
}

// lastRequestDuration returns the last request duration relative to now.
func (r *ReplicaCircuitBreaker) lastRequestDuration(now crtime.Mono) time.Duration {
	lastRequest := r.lastRequest.Load()
	if lastRequest == 0 || lastRequest > now {
		return 0
	}
	return now.Sub(lastRequest)
}

// Err returns the circuit breaker error if it is tripped.
//
// NB: if the breaker is tripped, this will also launch an async probe if one
// isn't already running. Use isTripped() instead to avoid this.
func (r *ReplicaCircuitBreaker) Err() error {
	if r == nil {
		return nil // circuit breakers disabled
	}
	// TODO(erikgrinaker): this is a bit more expensive than necessary, consider
	// optimizing it.
	return r.breaker.Signal().Err()
}

// isTripped returns true if the circuit breaker is currently tripped. Unlike
// Err(), this will not launch an async probe when tripped.
func (r *ReplicaCircuitBreaker) isTripped() bool {
	if r == nil {
		return false // circuit breakers disabled
	}
	return r.breaker.Signal().IsTripped()
}

// isClosed returns true if this circuit breaker has been closed and GCed.
func (r *ReplicaCircuitBreaker) isClosed() bool {
	if r == nil {
		return true // circuit breakers disabled
	}
	select {
	case <-r.closedC:
		return true
	default:
		return false
	}
}

// Track attempts to start tracking a request with the circuit breaker. If the
// breaker is tripped, returns an error. Otherwise, returns the context to use
// for the send and a token which the caller must call Done() on with the result
// of the request.
func (r *ReplicaCircuitBreaker) Track(
	ctx context.Context, ba *kvpb.BatchRequest, withCommit bool, now crtime.Mono,
) (context.Context, replicaCircuitBreakerToken, error) {
	if r == nil {
		return ctx, replicaCircuitBreakerToken{}, nil // circuit breakers disabled
	}

	// Record the request timestamp.
	r.lastRequest.Store(now)

	// Check if the breaker is tripped. If it is, this will also launch a probe if
	// one isn't already running.
	if err := r.Err(); err != nil {
		log.VErrEventf(ctx, 2, "request rejected by tripped circuit breaker for %s: %s", r.id(), err)
		r.d.metrics.CircuitBreaker.ReplicasRequestsRejected.Inc(1)
		return nil, replicaCircuitBreakerToken{}, errors.Wrapf(err,
			"%s is unavailable (circuit breaker tripped)", r.id())
	}

	// Set up the request token.
	token := replicaCircuitBreakerToken{
		r:          r,
		ctx:        ctx,
		ba:         ba,
		withCommit: withCommit,
	}

	// Record in-flight requests. If this is the only request, tentatively start
	// tracking a stall.
	if inflightReqs := r.inflightReqs.Add(1); inflightReqs == 1 {
		r.stallSince.Store(now)
	} else if inflightReqs < 0 {
		log.Fatalf(ctx, "inflightReqs %d < 0", inflightReqs) // overflow
	}

	// If enabled, create a send context that can be used to cancel in-flight
	// requests if the breaker trips.
	//
	// TODO(erikgrinaker): we should try to make the map lock-free. WithCancel()
	// also allocates. Ideally, it should be possible to propagate cancellation of
	// a single replica-scoped context onto all request contexts, but this
	// requires messing with Go internals.
	if CircuitBreakerCancellation.Get(&r.d.settings.SV) {
		// If the request already has a timeout that is below the probe threshold
		// and probe timeout, there is no point in us cancelling it (only relevant
		// with replica stalls). This is the common case when using statement
		// timeouts, and avoids the overhead.
		if deadline, hasTimeout := ctx.Deadline(); !hasTimeout ||
			crtime.MonoFromTime(deadline).Sub(now) >
				CircuitBreakerProbeThreshold.Get(&r.d.settings.SV)+CircuitBreakerProbeTimeout.Get(&r.d.settings.SV) {
			sendCtx, cancel := context.WithCancelCause(ctx)
			token.cancelCtx = sendCtx

			reqKind := cbRequestCancellationPolicyFromBatch(ba, withCommit)
			r.mu.Lock()
			r.mu.cancelFns[reqKind][ba] = cancel
			r.mu.Unlock()
			return sendCtx, token, nil
		}
	}

	return ctx, token, nil
}

// done records the result of a tracked request and untracks it. It is called
// via replicaCircuitBreakerToken.Done().
//
// If the request was cancelled by the circuit breaker, an appropriate context
// cancellation error is returned.
func (r *ReplicaCircuitBreaker) done(
	ctx context.Context,
	cancelCtx context.Context,
	ba *kvpb.BatchRequest,
	withCommit bool,
	br *kvpb.BatchResponse,
	sendErr error,
	now crtime.Mono,
) error {
	if r == nil {
		return nil // circuit breakers disabled when we began tracking the request
	}

	// Untrack the request.
	if inflightReqs := r.inflightReqs.Add(-1); inflightReqs < 0 {
		log.Fatalf(ctx, "inflightReqs %d < 0", inflightReqs)
	}

	// Detect if the circuit breaker cancelled the request, and prepare a
	// cancellation error to return to the caller.
	var cancelErr error
	if cancelCtx != nil {
		if sendErr != nil || br.Error != nil {
			if cancelErr = cancelCtx.Err(); cancelErr != nil && ctx.Err() == nil { // check ctx last
				log.VErrEventf(ctx, 2,
					"request cancelled by tripped circuit breaker for %s: %s", r.id(), cancelErr)
				cancelErr = errors.Wrapf(cancelErr, "%s is unavailable (circuit breaker tripped)", r.id())
			}
		}

		// Clean up the cancel function.
		reqKind := cbRequestCancellationPolicyFromBatch(ba, withCommit)
		r.mu.Lock()
		cancel := r.mu.cancelFns[reqKind][ba]
		delete(r.mu.cancelFns[reqKind], ba) // nolint:deferunlockcheck
		r.mu.Unlock()
		if cancel != nil {
			cancel(nil)
		}
	}

	// If this was a local send error, i.e. sendErr != nil, we rely on RPC circuit
	// breakers to fail fast. There is no need for us to launch a probe as well.
	// This includes the case where either the remote or local node has been
	// decommissioned.
	//
	// However, if the sender's context is cancelled, pessimistically assume this
	// is a timeout and fall through to the error handling below to potentially
	// launch a probe. Even though this may simply be the client going away, we
	// can't know if this was because of a client timeout or not, so we assume
	// there may be a problem with the replica. We will typically see recent
	// successful responses too if that isn't the case.
	if sendErr != nil && ctx.Err() == nil {
		return cancelErr
	}

	// If we got a response from the replica (even a br.Error), it isn't stalled.
	// Bump the stall timestamp to the current response timestamp, in case a
	// concurrent request has stalled.
	//
	// NB: we don't reset this to 0 when inflightReqs==0 to avoid unnecessary
	// synchronization.
	if sendErr == nil {
		r.stallSince.Store(now)
	}

	// Record error responses, by setting err non-nil. Otherwise, the response is
	// recorded as a success.
	err := sendErr
	if sendErr == nil && br.Error != nil {
		switch tErr := br.Error.GetDetail().(type) {
		case *kvpb.NotLeaseHolderError:
			// Consider NLHE a success if it contains a lease record, as the replica
			// appears functional. If there is no lease record, the replica was unable
			// to acquire a lease and has no idea who the leaseholder might be, likely
			// because it is disconnected from the leader or there is no quorum.
			if tErr.Lease == nil || *tErr.Lease == (roachpb.Lease{}) {
				err = tErr
			}
		case *kvpb.RangeNotFoundError, *kvpb.RangeKeyMismatchError, *kvpb.StoreNotFoundError:
			// If the replica no longer exists, we don't need to probe. The DistSender
			// will stop using the replica soon enough.
		case *kvpb.ReplicaUnavailableError:
			// If the replica's circuit breaker is tripped, defer to it. No need for
			// us to also probe.
		default:
			// Record all other errors.
			//
			// NB: this pessimistically assumes that any other error response may
			// indicate a replica problem. That's generally not true for most errors.
			// However, we will generally also see successful responses. If we only
			// see errors, it seems worthwhile to probe the replica and check, rather
			// than explicitly listing error types and possibly missing some. In the
			// worst case, this means launcing a goroutine and sending a cheap probe
			// every few seconds for each failing replica (which could be bad enough
			// across a large number of replicas).
			err = br.Error.GoError()
		}
	}

	if err == nil {
		// On success, reset the error tracking.
		r.errorSince.Store(0)
	} else if errorDuration := r.errorDuration(now); errorDuration == 0 {
		// If this is the first error we've seen, record it. We'll launch a probe on
		// a later error if necessary.
		r.errorSince.Store(now)
	} else if errorDuration >= CircuitBreakerProbeThreshold.Get(&r.d.settings.SV) {
		// The replica has been failing for the past probe threshold, probe it.
		r.breaker.Probe()
	}

	// Return the client cancellation error (if any).
	return cancelErr
}

// launchProbe spawns an async replica probe that sends LeaseInfo requests to
// the replica and trips/untrips the breaker as appropriate.
//
// While the breaker is tripped, the probe keeps running as long as there have
// been requests to the replica in the past few probe intervals. Otherwise, the
// probe exits, and a new one will be launched on the next request to the
// replica.  This limits the number of probe goroutines to the number of active
// replicas with a tripped circuit breaker, which should be small in the common
// case (in particular, leases will often move to a different replica once the
// current lease expires).
//
// TODO(erikgrinaker): consider batching LeaseInfo requests for many/all
// replicas on the same node/store. However, this needs server-side timeout
// handling such that if 1 out of 1000 replicas are stalled we won't fail the
// entire batch.
func (r *ReplicaCircuitBreaker) launchProbe(report func(error), done func()) {
	// If this circuit breaker has been closed, don't launch further probes. This
	// acts as a synchronization point with circuit breaker GC.
	if r.isClosed() {
		done()
		return
	}

	ctx := r.d.ambientCtx.AnnotateCtx(context.Background())

	name := fmt.Sprintf("distsender-replica-probe-%s", r.id())
	err := r.d.stopper.RunAsyncTask(ctx, name, func(ctx context.Context) {
		defer done()

		ctx, cancel := r.d.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		// Prepare the probe transport, using SystemClass to avoid RPC latency.
		//
		// We construct a bare replica slice without any locality information, since
		// we're only going to contact this replica.
		replicas := ReplicaSlice{{ReplicaDescriptor: r.desc}}
		opts := SendOptions{
			class:                  rpc.SystemClass,
			metrics:                &r.d.metrics,
			dontConsiderConnHealth: true,
		}
		transport := r.d.transportFactory(opts, replicas)
		defer transport.Release()

		// Start the write grace timer. Unlike reads, writes can't automatically be
		// retried by the DistSender, so we don't cancel them immediately when the
		// breaker trips but only after it has remained tripped for a grace period.
		// This should be long enough to wait out a Raft election timeout and lease
		// interval and then repropose the write, in case the range is temporarily
		// unavailable (e.g. following leaseholder loss).
		//
		// If the breaker is already tripped, the previous probe already waited out
		// the grace period, so we don't have to. The grace timer channel is set to
		// nil when there is no timer running.
		//
		// NB: lease requests aren't subject to the write grace period, despite
		// being write requests, since they are submitted directly to the local
		// replica instead of via the DistSender.
		var writeGraceTimer timeutil.Timer
		defer writeGraceTimer.Stop()
		if period := CircuitBreakerCancellationWriteGracePeriod.Get(&r.d.settings.SV); period > 0 {
			if !r.isTripped() {
				writeGraceTimer.Reset(period)
			}
		}

		// Continually probe the replica until it succeeds or the replica stops
		// seeing traffic. We probe immediately since we only trip the breaker on
		// probe failure.
		var timer timeutil.Timer
		defer timer.Stop()

		for {
			// Untrip the breaker and stop probing if circuit breakers are disabled.
			if r.d.Mode() == DistSenderCircuitBreakersNoRanges {
				report(nil)
				return
			}

			// Start the interval before sending the probe, to avoid skewing the
			// interval, instead preferring frequent probes.
			timer.Reset(CircuitBreakerProbeInterval.Get(&r.d.settings.SV))

			// Probe the replica.
			err := r.sendProbe(ctx, transport)

			// If the context (with no timeout) failed, we're shutting down. Just exit
			// the probe without reporting the result (which could trip the breaker).
			if ctx.Err() != nil {
				return
			}

			// Report the probe result.
			report(err)
			if err == nil {
				// On a successful probe, record the success and stop probing.
				r.stallSince.Store(crtime.NowMono())
				r.errorSince.Store(0)
				return
			}

			// Cancel in-flight read requests on failure, and write requests if the
			// grace timer has expired. We do this on every failure, and also remove
			// the cancel functions from the map (even though done() will also clean
			// them up), in case another request makes it in after the breaker trips.
			// There should typically never be any contention here.
			cancelRequests := func(reqKind cbRequestCancellationPolicy) {
				r.mu.Lock()
				defer r.mu.Unlock()

				if l := len(r.mu.cancelFns[reqKind]); l > 0 {
					log.VEventf(ctx, 2, "cancelling %d requests %s for %s", l, reqKind, r.id())
				}
				for ba, cancel := range r.mu.cancelFns[reqKind] {
					delete(r.mu.cancelFns[reqKind], ba)
					cancel(errors.Wrapf(err, "%s is unavailable (circuit breaker tripped)", r.id()))
					r.d.metrics.CircuitBreaker.ReplicasRequestsCancelled.Inc(1)
				}
			}

			cancelRequests(cbCancelImmediately)
			if writeGraceTimer.C == nil {
				cancelRequests(cbCancelAfterGracePeriod)
			}

			for !timer.Read { // select until probe interval timer fires
				select {
				case <-timer.C:
					timer.Read = true
				case <-writeGraceTimer.C:
					cancelRequests(cbCancelAfterGracePeriod)
					writeGraceTimer.Read = true
					writeGraceTimer.Stop() // sets C = nil
				case <-r.closedC:
					// The circuit breaker has been GCed, exit. We could cancel the context
					// instead to also abort an in-flight probe, but that requires extra
					// synchronization with circuit breaker GC (a probe may be launching but
					// haven't yet installed its cancel function). This is simpler.
					return
				case <-r.d.stopper.ShouldQuiesce():
					return
				case <-ctx.Done():
					return
				}
			}

			// If there haven't been any recent requests, stop probing but keep the
			// breaker tripped. A new probe will be launched on the next request.
			//
			// NB: we check this after waiting out the probe interval above, to avoid
			// frequently spawning new probe goroutines, instead waiting to see if any
			// requests come in.
			if r.lastRequestDuration(crtime.NowMono()) >= cbProbeIdleTimeout {
				// Keep probing if the write grace timer hasn't expired yet, since we
				// need to cancel pending writes first.
				if writeGraceTimer.C == nil {
					return
				}
			}
		}
	})
	if err != nil {
		done()
	}
}

// sendProbe probes the replica by sending a LeaseInfo request. It returns an
// error if the circuit breaker should trip, or nil if it should untrip and
// stop probing.
//
// Note that this may return nil even though the request itself fails. The
// typical example is a NLHE, which indicates that the replica is functional but
// not the leaseholder, but there are other cases too. See below.
//
// We use a LeaseInfo request as a makeshift health check because:
//
//   - It is cheap (only reads in-memory state).
//   - It does not take out any latches.
//   - It requires a lease, so it will either attempt to acquire a lease or
//     return NLHE if it knows about a potential leaseholder elsewhere. This is
//     important, because if the replica is not connected to a quorum it will wait
//     for lease acquisition, and clients with low timeouts may cancel their
//     requests before a NLHE is returned, causing the DistSender to get stuck on
//     these replicas.
func (r *ReplicaCircuitBreaker) sendProbe(ctx context.Context, transport Transport) error {
	// We don't use timeutil.RunWithTimeout() because we need to be able to
	// differentiate whether the context timed out.
	timeout := CircuitBreakerProbeTimeout.Get(&r.d.settings.SV)
	ctx, cancel := context.WithTimeout(ctx, timeout) // nolint:context
	defer cancel()

	transport.Reset()

	ba := &kvpb.BatchRequest{}
	ba.RangeID = r.rangeID
	ba.Replica = transport.NextReplica()
	ba.Add(&kvpb.LeaseInfoRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: r.startKey,
		},
	})

	log.VEventf(ctx, 2, "sending probe to %s: %s", r.id(), ba)
	br, err := transport.SendNext(ctx, ba)
	log.VEventf(ctx, 2, "probe result from %s: br=%v err=%v", r.id(), br, err)

	// Handle local send errors.
	if err != nil {
		// If the context timed out, fail. The caller will handle the case where
		// we're shutting down.
		if err := ctx.Err(); err != nil {
			return errors.Wrapf(err, "probe timed out")
		}

		// Any other local error is likely a networking/gRPC issue. This includes if
		// either the remote node or the local node has been decommissioned. We
		// rely on RPC circuit breakers to fail fast for these, so there's no point
		// in us probing individual replicas. Stop probing.
		return nil // nolint:returnerrcheck
	}

	// Handle error responses.
	if br.Error != nil {
		switch tErr := br.Error.GetDetail().(type) {
		case *kvpb.NotLeaseHolderError:
			// If we get a NLHE back with a lease record, the replica is healthy
			// enough to know who the leaseholder is. Otherwise, we have to trip the
			// breaker such that the DistSender will try other replicas and discover
			// the leaseholder -- this may otherwise never happen if clients time out
			// before the replica returns the NLHE.
			if tErr.Lease == nil || *tErr.Lease == (roachpb.Lease{}) {
				err = br.Error.GoError()
			}
		case *kvpb.RangeNotFoundError, *kvpb.RangeKeyMismatchError, *kvpb.StoreNotFoundError:
			// If the replica no longer exists, stop probing.
		case *kvpb.ReplicaUnavailableError:
			// If the replica's circuit breaker is tripped, defer to it. No need for
			// us to also probe.
		default:
			// On any other error, trip the breaker.
			err = br.Error.GoError()
		}
	}

	return errors.Wrapf(err, "probe failed")
}

// OnTrip implements circuit.EventHandler.
func (r *ReplicaCircuitBreaker) OnTrip(b *circuit.Breaker, prev, cur error) {
	if cur == nil {
		return
	}
	// OnTrip() is called every time the probe reports an error, regardless of
	// whether the breaker was already tripped. Record each probe failure, but
	// only record tripped breakers when it wasn't already tripped.
	r.d.metrics.CircuitBreaker.ReplicasProbesFailure.Inc(1)
	if prev == nil {
		// TODO(erikgrinaker): consider rate limiting these with log.Every, but for
		// now we want to know which ones trip for debugging.
		ctx := r.d.ambientCtx.AnnotateCtx(context.Background())
		now := crtime.NowMono()
		stallSince := r.stallDuration(now).Truncate(time.Millisecond)
		errorSince := r.errorDuration(now).Truncate(time.Millisecond)
		log.Errorf(ctx, "%s circuit breaker tripped: %s (stalled for %s, erroring for %s)",
			r.id(), cur, stallSince, errorSince)

		r.d.metrics.CircuitBreaker.ReplicasTripped.Inc(1)
		r.d.metrics.CircuitBreaker.ReplicasTrippedEvents.Inc(1)
	}
}

// OnReset implements circuit.EventHandler.
func (r *ReplicaCircuitBreaker) OnReset(b *circuit.Breaker, prev error) {
	// If the circuit breaker has been GCed, we don't need to log or record the
	// probe success. We do need to decrement ReplicasTripped if we're actually
	// tripped though, to avoid metrics leaks. This may be happen either in
	// response to an actual probe success, or a noop probe during GC.
	if r.isClosed() {
		if prev != nil {
			r.d.metrics.CircuitBreaker.ReplicasTripped.Dec(1)
		}
		return
	}

	// OnReset() is called every time the probe reports a success, regardless
	// of whether the breaker was already tripped. Record each probe success,
	// but only record untripped breakers when it was already tripped.
	r.d.metrics.CircuitBreaker.ReplicasProbesSuccess.Inc(1)
	if prev != nil {
		// TODO(erikgrinaker): consider rate limiting these with log.Every, but for
		// now we want to know which ones reset for debugging.
		ctx := r.d.ambientCtx.AnnotateCtx(context.Background())
		log.Infof(ctx, "%s circuit breaker reset", r.id())

		r.d.metrics.CircuitBreaker.ReplicasTripped.Dec(1)
	}
}

// OnProbeLaunched implements circuit.EventHandler.
func (r *ReplicaCircuitBreaker) OnProbeLaunched(b *circuit.Breaker) {
	r.d.metrics.CircuitBreaker.ReplicasProbesRunning.Inc(1)

	// If the circuit breaker has been GCed, don't log the probe launch since we
	// don't actually spawn a goroutine. We still increment ProbesRunning above to
	// avoid metrics leaks when decrementing in OnProbeDone().
	if r.isClosed() {
		return
	}

	ctx := r.d.ambientCtx.AnnotateCtx(context.Background())
	now := crtime.NowMono()
	stallSince := r.stallDuration(now).Truncate(time.Millisecond)
	errorSince := r.errorDuration(now).Truncate(time.Millisecond)
	tripped := r.breaker.Signal().IsTripped()
	log.VEventf(ctx, 2, "launching circuit breaker probe for %s (tripped=%t stall=%s error=%s)",
		r.id(), tripped, stallSince, errorSince)
}

// OnProbeDone implements circuit.EventHandler.
func (r *ReplicaCircuitBreaker) OnProbeDone(b *circuit.Breaker) {
	r.d.metrics.CircuitBreaker.ReplicasProbesRunning.Dec(1)

	// If the circuit breaker has been GCed, don't log the probe stopping. We
	// still decrement ProbesRunning above to avoid metrics leaks (we don't know
	// if the circuit breaker was GCed when OnProbeLaunched was called).
	//
	// We must also reset the breaker if it's tripped, to avoid ReplicasTripped
	// metric gauge leaks. This can either be in response to an already-running
	// probe shutting down, or a noop probe launched by GC -- it doesn't matter.
	// A concurrent request may then use the untripped breaker, but that's ok
	// since it would also use an untripped breaker if it arrived after GC.
	if r.isClosed() {
		if r.isTripped() {
			r.breaker.Reset()
		}
		return
	}

	ctx := r.d.ambientCtx.AnnotateCtx(context.Background())
	now := crtime.NowMono()
	tripped := r.breaker.Signal().IsTripped()
	lastRequest := r.lastRequestDuration(now).Truncate(time.Millisecond)
	log.VEventf(ctx, 2, "stopping circuit breaker probe for %s (tripped=%t lastRequest=%s)",
		r.id(), tripped, lastRequest)
}
