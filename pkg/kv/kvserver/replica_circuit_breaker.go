// Copyright 2021 The Cockroach Authors.
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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"go.etcd.io/etcd/raft/v3"
)

type replicaInCircuitBreaker interface {
	Clock() *hlc.Clock
	Desc() *roachpb.RangeDescriptor
	Send(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
	slowReplicationThreshold(ba *roachpb.BatchRequest) (time.Duration, bool)
	replicaUnavailableError() error
}

var defaultReplicaCircuitBreakerSlowReplicationThreshold = envutil.EnvOrDefaultDuration(
	"COCKROACH_REPLICA_CIRCUIT_BREAKER_SLOW_REPLICATION_THRESHOLD", 0,
)

var replicaCircuitBreakerSlowReplicationThreshold = settings.RegisterPublicDurationSettingWithExplicitUnit(
	settings.SystemOnly,
	"kv.replica_circuit_breaker.slow_replication_threshold",
	"duration after which slow proposals trip the per-Replica circuit breaker (zero duration disables breakers)",
	defaultReplicaCircuitBreakerSlowReplicationThreshold,
	func(d time.Duration) error {
		// Setting the breaker duration too low could be very dangerous to cluster
		// health (breaking things to the point where the cluster setting can't be
		// changed), so enforce a sane minimum.
		const min = 500 * time.Millisecond
		if d == 0 {
			return nil
		}
		if d <= min {
			return errors.Errorf("must specify a minimum of %s", min)
		}
		return nil
	},
)

// Telemetry counter to count number of trip events.
var telemetryTripAsync = telemetry.GetCounterOnce("kv.replica_circuit_breaker.num_tripped_events")

// replicaCircuitBreaker is a wrapper around *circuit.Breaker that makes it
// convenient for use as a per-Replica circuit breaker.
type replicaCircuitBreaker struct {
	ambCtx  log.AmbientContext
	stopper *stop.Stopper
	r       replicaInCircuitBreaker
	st      *cluster.Settings
	cancels CancelStorage
	wrapped *circuit.Breaker

	versionIsActive int32 // atomic
}

// Register takes a cancelable context and its cancel function (which the caller
// must cancel when the request has finished), and registers them with the
// circuit breaker. If the breaker is already tripped, its error is returned
// immediately and the caller should not continue processing the request.
// Otherwise, the cancel function is invoked if the breaker trips. The caller is
// provided with a token and signaller for use in a call to
// UnregisterAndAdjustError upon request completion. That method also takes the
// error (if any) resulting from the request to ensure that in the case of a
// tripped breaker, the error reflects this fact.
func (br *replicaCircuitBreaker) Register(
	ctx context.Context, cancel func(),
) (_token interface{}, _ signaller, _ error) {
	brSig := br.Signal()

	// TODO(tbg): we may want to exclude more requests from this check, or allow
	// requests to exclude themselves from the check (via their header). This
	// latter mechanism could also replace isCircuitBreakerProbe.
	if isCircuitBreakerProbe(ctx) {
		// NB: brSig.C() == nil.
		brSig = neverTripSignaller{}
	}

	if brSig.C() == nil {
		// Circuit breakers are disabled and/or this is a probe request, so don't do
		// any work registering the context. UnregisterAndAdjustError will know that we didn't
		// since it checks the same brSig for a nil C().
		return ctx, brSig, nil
	}

	// NB: it might be tempting to check the breaker error first to avoid the call
	// to Set below if the breaker is tripped at this point. However, the ordering
	// here, subtly, is required to avoid situations in which the cancel is still
	// in the map despite the probe having shut down (in which case cancel will
	// not be invoked until the probe is next triggered, which maybe "never").
	//
	// To see this, consider the case in which the breaker is initially not
	// tripped when we check, but then trips immediately and has the probe fail
	// (and terminate). Since the probe is in charge of cancelling all tracked
	// requests, we must ensure that this probe sees our request. Adding the
	// request prior to calling Signal() means that if we see an untripped
	// breaker, no probe is running - consequently should the breaker then trip,
	// it will observe our cancel, thus avoiding a leak. If we observe a tripped
	// breaker, we also need to remove our own cancel, as the probe may already
	// have passed the point at which it iterates through the cancels prior to us
	// inserting it. The cancel may be invoked twice, but that's ok.
	//
	// See TestReplicaCircuitBreaker_NoCancelRace.
	tok := br.cancels.Set(ctx, cancel)
	if err := brSig.Err(); err != nil {
		br.cancels.Del(tok)
		cancel()
		return nil, nil, err
	}

	return tok, brSig, nil
}

// UnregisterAndAdjustError releases a tracked cancel function upon request
// completion. The error resulting from the request is passed in to allow
// decorating it in case the breaker tripped while the request was in-flight.
//
// See Register.
func (br *replicaCircuitBreaker) UnregisterAndAdjustError(
	tok interface{}, sig signaller, pErr *roachpb.Error,
) *roachpb.Error {
	if sig.C() == nil {
		// Breakers were disabled and we never put the cancel in the registry.
		return pErr
	}

	br.cancels.Del(tok)

	brErr := sig.Err()
	if pErr == nil || brErr == nil {
		return pErr
	}

	// The breaker tripped and the command is returning an error. Make sure the
	// error reflects the tripped breaker.

	err := pErr.GoError()
	if ae := (&roachpb.AmbiguousResultError{}); errors.As(err, &ae) {
		// The breaker tripped while a command was inflight, so we have to
		// propagate an ambiguous result. We don't want to replace it, but there
		// is a way to stash an Error in it so we use that.
		//
		// TODO(tbg): could also wrap it; there is no other write to WrappedErr
		// in the codebase and it might be better to remove it. Nested *Errors
		// are not a good idea.
		wrappedErr := brErr
		if ae.WrappedErr != nil {
			wrappedErr = errors.Wrapf(brErr, "%v", ae.WrappedErr)
		}
		ae.WrappedErr = roachpb.NewError(wrappedErr)
		return roachpb.NewError(ae)
	} else if le := (&roachpb.NotLeaseHolderError{}); errors.As(err, &le) {
		// When a lease acquisition triggered by this request is short-circuited
		// by the breaker, it will return an opaque NotLeaseholderError, which we
		// replace with the breaker's error.
		return roachpb.NewError(errors.CombineErrors(brErr, le))
	}
	return pErr
}

func (br *replicaCircuitBreaker) cancelAllTrackedContexts() {
	br.cancels.Visit(func(ctx context.Context, cancel func()) (remove bool) {
		cancel()
		return true // remove
	})
}

func (br *replicaCircuitBreaker) canEnable() bool {
	b := atomic.LoadInt32(&br.versionIsActive) == 1
	if b {
		return true // fast path
	}
	// IsActive is mildly expensive since it has to unmarshal
	// a protobuf.
	if br.st.Version.IsActive(context.Background(), clusterversion.ProbeRequest) {
		atomic.StoreInt32(&br.versionIsActive, 1)
		return true
	}
	return false // slow path
}

func (br *replicaCircuitBreaker) enabled() bool {
	return replicaCircuitBreakerSlowReplicationThreshold.Get(&br.st.SV) > 0 && br.canEnable()
}

func (br *replicaCircuitBreaker) newError() error {
	return br.r.replicaUnavailableError()
}

func (br *replicaCircuitBreaker) TripAsync() {
	if !br.enabled() {
		return
	}

	_ = br.stopper.RunAsyncTask(
		br.ambCtx.AnnotateCtx(context.Background()), "trip-breaker",
		func(ctx context.Context) {
			br.wrapped.Report(br.newError())
		},
	)
}

type signaller interface {
	Err() error
	C() <-chan struct{}
}

type neverTripSignaller struct{}

func (s neverTripSignaller) Err() error         { return nil }
func (s neverTripSignaller) C() <-chan struct{} { return nil }

func (br *replicaCircuitBreaker) Signal() signaller {
	if !br.enabled() {
		return neverTripSignaller{}
	}
	return br.wrapped.Signal()
}

func newReplicaCircuitBreaker(
	cs *cluster.Settings,
	stopper *stop.Stopper,
	ambientCtx log.AmbientContext,
	r replicaInCircuitBreaker,
	s CancelStorage,
	onTrip func(),
	onReset func(),
) *replicaCircuitBreaker {
	br := &replicaCircuitBreaker{
		stopper: stopper,
		ambCtx:  ambientCtx,
		r:       r,
		st:      cs,
	}
	br.cancels = s
	br.cancels.Reset()
	br.wrapped = circuit.NewBreaker(circuit.Options{
		Name:       "breaker", // log bridge has ctx tags
		AsyncProbe: br.asyncProbe,
		EventHandler: &replicaCircuitBreakerLogger{
			EventHandler: &circuit.EventLogger{
				Log: func(buf redact.StringBuilder) {
					log.Infof(ambientCtx.AnnotateCtx(context.Background()), "%s", buf)
				},
			},
			onTrip:  onTrip,
			onReset: onReset,
		},
	})

	return br
}

type replicaCircuitBreakerLogger struct {
	circuit.EventHandler
	onTrip  func()
	onReset func()
}

func (r replicaCircuitBreakerLogger) OnTrip(br *circuit.Breaker, prev, cur error) {
	if prev == nil {
		r.onTrip()
	}
	r.EventHandler.OnTrip(br, prev, cur)
}

func (r replicaCircuitBreakerLogger) OnReset(br *circuit.Breaker) {
	r.onReset()
	r.EventHandler.OnReset(br)
}

type probeKey struct{}

func isCircuitBreakerProbe(ctx context.Context) bool {
	return ctx.Value(probeKey{}) != nil
}

func withCircuitBreakerProbeMarker(ctx context.Context) context.Context {
	return context.WithValue(ctx, probeKey{}, probeKey{})
}

func (br *replicaCircuitBreaker) asyncProbe(report func(error), done func()) {
	bgCtx := br.ambCtx.AnnotateCtx(context.Background())
	if err := br.stopper.RunAsyncTask(bgCtx, "replica-probe", func(ctx context.Context) {
		defer done()

		if !br.enabled() {
			report(nil)
			return
		}

		// First, tell all current requests to fail fast. Note that clients insert
		// first, then check the breaker (and remove themselves if breaker already
		// tripped then). This prevents any cancels from sneaking in after the probe
		// gets past this point, which could otherwise leave cancels hanging until
		// "something" triggers the next probe (which may be never if no more traffic
		// arrives at the Replica). See Register.
		br.cancelAllTrackedContexts()
		err := sendProbe(ctx, br.r)
		report(err)
	}); err != nil {
		done()
	}
}

func sendProbe(ctx context.Context, r replicaInCircuitBreaker) error {
	ctx = withCircuitBreakerProbeMarker(ctx)
	desc := r.Desc()
	if !desc.IsInitialized() {
		return nil
	}
	ba := roachpb.BatchRequest{}
	ba.Timestamp = r.Clock().Now()
	ba.RangeID = r.Desc().RangeID
	probeReq := &roachpb.ProbeRequest{}
	probeReq.Key = desc.StartKey.AsRawKey()
	ba.Add(probeReq)
	thresh, ok := r.slowReplicationThreshold(&ba)
	if !ok {
		// Breakers are disabled now.
		return nil
	}
	if err := contextutil.RunWithTimeout(ctx, "probe", thresh,
		func(ctx context.Context) error {
			_, pErr := r.Send(ctx, ba)
			return pErr.GoError()
		},
	); err != nil {
		return errors.CombineErrors(r.replicaUnavailableError(), err)
	}
	return nil
}

func replicaUnavailableError(
	desc *roachpb.RangeDescriptor,
	replDesc roachpb.ReplicaDescriptor,
	lm liveness.IsLiveMap,
	rs *raft.Status,
) error {
	nonLiveRepls := roachpb.MakeReplicaSet(nil)
	for _, rDesc := range desc.Replicas().Descriptors() {
		if lm[rDesc.NodeID].IsLive {
			continue
		}
		nonLiveRepls.AddReplica(rDesc)
	}

	canMakeProgress := desc.Replicas().CanMakeProgress(
		func(replDesc roachpb.ReplicaDescriptor) bool {
			return lm[replDesc.NodeID].IsLive
		},
	)

	// Ensure good redaction.
	var _ redact.SafeFormatter = nonLiveRepls
	var _ redact.SafeFormatter = desc
	var _ redact.SafeFormatter = replDesc

	err := roachpb.NewReplicaUnavailableError(desc, replDesc)
	err = errors.Wrapf(
		err,
		"raft status: %+v", redact.Safe(rs), // raft status contains no PII
	)
	if len(nonLiveRepls.AsProto()) > 0 {
		err = errors.Wrapf(err, "replicas on non-live nodes: %v (lost quorum: %t)", nonLiveRepls, !canMakeProgress)
	}

	return err
}

func (r *Replica) replicaUnavailableError() error {
	desc := r.Desc()
	replDesc, _ := desc.GetReplicaDescriptor(r.store.StoreID())

	var isLiveMap liveness.IsLiveMap
	if nl := r.store.cfg.NodeLiveness; nl != nil { // exclude unit test
		isLiveMap = nl.GetIsLiveMap()
	}
	return replicaUnavailableError(desc, replDesc, isLiveMap, r.RaftStatus())
}
