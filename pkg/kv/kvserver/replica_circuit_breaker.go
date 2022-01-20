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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	settings.NonNegativeDuration,
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
	cancels struct {
		syncutil.Mutex
		m map[context.Context]func()
	}
	wrapped *circuit.Breaker
}

// Register takes a cancelable context and its cancel function. The
// context is cancelled when the circuit breaker trips. If the breaker is
// already tripped, its error is returned immediately and the caller should not
// continue processing the request. Otherwise, the caller is provided with a
// signaller for use in a deferred call to maybeAdjustWithBreakerError, which
// will annotate the outgoing error in the event of the breaker tripping while
// the request is processing.
func (br *replicaCircuitBreaker) Register(
	ctx context.Context, cancel func(),
) (_token interface{}, _ signaller, _ error) {
	// We intentionally lock over the Signal() call and Err() check to avoid races
	// where the breaker trips but the cancel() function is not invoked. Both of
	// these calls are cheap and do not allocate.
	br.cancels.Lock()
	defer br.cancels.Unlock()

	brSig := br.Signal()
	if isCircuitBreakerProbe(ctx) {
		brSig = neverTripSignaller{}
	}

	if err := brSig.Err(); err != nil {
		// TODO(tbg): we may want to exclude some requests from this check, or allow
		// requests to exclude themselves from the check (via their header).
		cancel()
		return nil, nil, err
	}
	br.cancels.m[ctx] = cancel

	// The token is the context, saving allocations.
	return ctx, brSig, nil
}

func (br *replicaCircuitBreaker) Unregister(
	tok interface{}, sig signaller, pErr *roachpb.Error,
) *roachpb.Error {
	brErr := sig.Err()
	if sig.C() == nil {
		// Breakers were disabled and we never put the cancel in the registry.
		return pErr
	}

	br.cancels.Lock()
	delete(br.cancels.m, tok.(context.Context))
	br.cancels.Unlock()

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

func (br *replicaCircuitBreaker) visitCancels(f func(context.Context)) {
	br.cancels.Lock()
	for ctx := range br.cancels.m {
		f(ctx)
	}
	br.cancels.Unlock()
}

func (br *replicaCircuitBreaker) cancelAllTrackedProposals() error {
	br.cancels.Lock()
	defer br.cancels.Unlock()
	// NB: this intentionally consults the wrapped Signal(), which does not check
	// the breaker cluster setting.
	if br.wrapped.Signal().Err() == nil {
		// TODO(tbg): in the future, we may want to trigger the probe even if
		// the breaker is not tripped. For example, if we *suspect* that there
		// might be something wrong with the range but we're not quite sure,
		// we would like to let the probe decide whether to trip. Once/if we
		// do this, this code may need to become more permissive and we'll
		// have to re-work where the cancellation actually occurs.
		return errors.AssertionFailedf("asked to cancel all proposals, but breaker is not tripped")
	}
	for _, cancel := range br.cancels.m {
		cancel()
	}
	br.cancels.m = map[context.Context]func(){}
	return nil
}

func (br *replicaCircuitBreaker) enabled() bool {
	return replicaCircuitBreakerSlowReplicationThreshold.Get(&br.st.SV) > 0 &&
		br.st.Version.IsActive(context.Background(), clusterversion.ProbeRequest)
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

func (br *replicaCircuitBreaker) Signal() signaller { // TODO(tbg): unexport?
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
	onTrip func(),
	onReset func(),
) *replicaCircuitBreaker {
	br := &replicaCircuitBreaker{
		stopper: stopper,
		ambCtx:  ambientCtx,
		r:       r,
		st:      cs,
	}
	br.cancels.m = map[context.Context]func(){}
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

		if err := br.cancelAllTrackedProposals(); err != nil {
			log.Errorf(ctx, "%s", err)
			// Proceed.
		}
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
