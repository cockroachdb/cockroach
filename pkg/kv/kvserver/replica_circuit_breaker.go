// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type replicaInCircuitBreaker interface {
	Clock() *hlc.Clock
	Desc() *roachpb.RangeDescriptor
	Send(context.Context, *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
	slowReplicationThreshold(ba *kvpb.BatchRequest) (time.Duration, bool)
	replicaUnavailableError(err error) error
	poisonInflightLatches(err error)
	IsDestroyed() (DestroyReason, error)
}

var defaultReplicaCircuitBreakerSlowReplicationThreshold = envutil.EnvOrDefaultDuration(
	"COCKROACH_REPLICA_CIRCUIT_BREAKER_SLOW_REPLICATION_THRESHOLD",
	// SlowRequestThreshold is used in various places to log warnings on slow
	// request phases. We are even more conservative about the circuit breakers,
	// i.e. multiply by a factor. This is mainly defense in depth; at time of
	// writing the slow request threshold is 15s which *should* also be good
	// enough for circuit breakers since it's already fairly conservative.
	4*base.SlowRequestThreshold,
)

var replicaCircuitBreakerSlowReplicationThreshold = settings.RegisterDurationSettingWithExplicitUnit(
	settings.SystemOnly,
	"kv.replica_circuit_breaker.slow_replication_threshold",
	"duration after which slow proposals trip the per-Replica circuit breaker (zero duration disables breakers)",
	defaultReplicaCircuitBreakerSlowReplicationThreshold,
	settings.WithPublic,
	// Setting the breaker duration too low could be very dangerous to cluster
	// health (breaking things to the point where the cluster setting can't be
	// changed), so enforce a sane minimum.
	settings.DurationWithMinimumOrZeroDisable(500*time.Millisecond),
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
	wrapped *circuit.Breaker
}

func (br *replicaCircuitBreaker) HasMark(err error) bool {
	return br.wrapped.HasMark(err)
}

func (br *replicaCircuitBreaker) enabled() bool {
	return replicaCircuitBreakerSlowReplicationThreshold.Get(&br.st.SV) > 0
}

func (br *replicaCircuitBreaker) TripAsync(err error) {
	if !br.enabled() {
		return
	}

	_ = br.stopper.RunAsyncTask(
		br.ambCtx.AnnotateCtx(context.Background()), "trip-breaker",
		func(ctx context.Context) {
			br.tripSync(err)
		},
	)
}

func (br *replicaCircuitBreaker) tripSync(err error) {
	br.wrapped.Report(br.r.replicaUnavailableError(err))
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
	onTrip func(),
	onReset func(),
) *replicaCircuitBreaker {
	br := &replicaCircuitBreaker{
		stopper: stopper,
		ambCtx:  ambientCtx,
		r:       r,
		st:      cs,
	}
	br.wrapped = circuit.NewBreaker(circuit.Options{
		Name:       "breaker", // log bridge has ctx tags
		AsyncProbe: br.asyncProbe,
		EventHandler: &replicaCircuitBreakerLogger{
			ambientCtx: ambientCtx,
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
	ambientCtx log.AmbientContext
	onTrip     func()
	onReset    func()
}

func (r replicaCircuitBreakerLogger) OnTrip(b *circuit.Breaker, prev, cur error) {
	if prev == nil {
		r.onTrip()
	}
	// Log directly from this method via log.Errorf.
	var buf redact.StringBuilder
	circuit.EventFormatter{}.OnTrip(b, prev, cur, &buf)
	log.Errorf(r.ambientCtx.AnnotateCtx(context.Background()), "%s", buf)
}

func (r replicaCircuitBreakerLogger) OnReset(br *circuit.Breaker, prev error) {
	r.onReset()
	r.EventHandler.OnReset(br, prev)
}

func (br *replicaCircuitBreaker) asyncProbe(report func(error), done func()) {
	bgCtx := br.ambCtx.AnnotateCtx(context.Background())
	if err := br.stopper.RunAsyncTask(bgCtx, "replica-probe", func(ctx context.Context) {
		defer done()
		ctx, cancel := br.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		if !br.enabled() {
			report(nil)
			return
		}

		brErr := br.Signal().Err()
		if brErr == nil {
			// This shouldn't happen, but if we're not even tripped, don't do
			// anything.
			return
		}

		// Poison any inflight latches. Note that any new request that is added in
		// while the probe is running but after poisonInflightLatches has been
		// invoked will remain untouched. We rely on the replica to periodically
		// access the circuit breaker to trigger additional probes in that case.
		// (This happens in refreshProposalsLocked).
		br.r.poisonInflightLatches(brErr)
		err := sendProbe(ctx, br.r)
		report(err)
	}); err != nil {
		done()
	}
}

func sendProbe(ctx context.Context, r replicaInCircuitBreaker) error {
	// NB: ProbeRequest has the bypassesCircuitBreaker flag. If in the future we
	// enhance the probe, we may need to allow any additional requests we send to
	// chose to bypass the circuit breaker explicitly.
	desc := r.Desc()
	// Untrip the breaker if the replica is destroyed or not initialized.
	if reason, _ := r.IsDestroyed(); !desc.IsInitialized() || reason == destroyReasonRemoved {
		return nil
	}
	ba := &kvpb.BatchRequest{}
	ba.Timestamp = r.Clock().Now()
	ba.RangeID = r.Desc().RangeID
	probeReq := &kvpb.ProbeRequest{}
	probeReq.Key = desc.StartKey.AsRawKey()
	ba.Add(probeReq)
	_, ok := r.slowReplicationThreshold(ba)
	if !ok {
		// Breakers are disabled now.
		return nil
	}
	_, pErr := r.Send(ctx, ba)
	if err := pErr.GoError(); err != nil {
		return r.replicaUnavailableError(err)
	}
	return nil
}

func replicaUnavailableError(
	err error,
	desc *roachpb.RangeDescriptor,
	replDesc roachpb.ReplicaDescriptor,
	lm livenesspb.IsLiveMap,
	rs *raft.Status,
	closedTS hlc.Timestamp,
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

	var buf redact.StringBuilder
	if !canMakeProgress {
		buf.Printf("lost quorum (down: %v); ", nonLiveRepls)
	}
	buf.Printf(
		"closed timestamp: %s (%s); raft status: %+v",
		closedTS,
		redact.Safe(timeutil.Unix(0, closedTS.WallTime).UTC().Format("2006-01-02 15:04:05")),
		redact.Safe(rs), /* raft status contains no PII */
	)

	return kvpb.NewReplicaUnavailableError(errors.Wrapf(err, "%s", buf), desc, replDesc)
}

func (r *Replica) replicaUnavailableError(err error) error {
	desc := r.Desc()
	replDesc, _ := desc.GetReplicaDescriptor(r.store.StoreID())

	isLiveMap, _ := r.store.livenessMap.Load().(livenesspb.IsLiveMap)
	ct := r.GetCurrentClosedTimestamp(context.Background())
	return replicaUnavailableError(err, desc, replDesc, isLiveMap, r.RaftStatus(), ct)
}
