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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
)

type replicaInCircuitBreaker interface {
	Clock() *hlc.Clock
	Desc() *roachpb.RangeDescriptor
	Send(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
	slowReplicationThreshold(ba *roachpb.BatchRequest) time.Duration
	rangeUnavailableError() error
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
		if d < 0 {
			return errors.New("duration must be non-negative")
		}
		return nil
	},
)

type replicaCircuitBreaker struct {
	ambCtx  log.AmbientContext
	stopper *stop.Stopper
	r       replicaInCircuitBreaker
	st      *cluster.Settings
	wrapped *circuit.Breaker
}

func (br *replicaCircuitBreaker) enabled() bool {
	return replicaCircuitBreakerSlowReplicationThreshold.Get(&br.st.SV) > 0 &&
		br.st.Version.IsActive(context.Background(), clusterversion.ProbeRequest)
}

func (br *replicaCircuitBreaker) newError() error {
	return br.r.rangeUnavailableError()
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

type neverTripSignaller struct{}

func (s neverTripSignaller) Err() error         { return nil }
func (s neverTripSignaller) C() <-chan struct{} { return nil }

func (br *replicaCircuitBreaker) Signal() interface {
	Err() error
	C() <-chan struct{}
} {
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
) *replicaCircuitBreaker {
	br := &replicaCircuitBreaker{
		stopper: stopper,
		ambCtx:  ambientCtx,
		r:       r,
		st:      cs,
	}

	br.wrapped = circuit.NewBreaker(circuit.Options{
		Name:       redact.Sprintf("breaker"), // log bridge has ctx tags
		AsyncProbe: br.asyncProbe,
		EventHandler: &circuit.EventLogger{
			Log: func(buf redact.StringBuilder) {
				log.Infof(ambientCtx.AnnotateCtx(context.Background()), "%s", buf)
			},
		},
	})

	return br
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
	if err := br.stopper.RunAsyncTask(
		bgCtx,
		"replica-probe",
		func(ctx context.Context) {
			defer done()

			if !br.enabled() {
				report(nil)
				return
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
	if err := contextutil.RunWithTimeout(ctx, "probe", r.slowReplicationThreshold(&ba),
		func(ctx context.Context) error {
			_, pErr := r.Send(ctx, ba)
			return pErr.GoError()
		},
	); err != nil {
		return errors.CombineErrors(r.rangeUnavailableError(), err)
	}
	return nil
}
