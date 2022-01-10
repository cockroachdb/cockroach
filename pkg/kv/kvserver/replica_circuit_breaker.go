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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type replicaInCircuitBreaker interface {
	Clock() *hlc.Clock
	Desc() *roachpb.RangeDescriptor
	Send(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
	rangeUnavailableError() error
}

var replicaCircuitBreakersEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.replica_circuit_breakers.enabled",
	"fail-fast requests to unavailable replicas",
	true,
)

type replicaCircuitBreaker struct {
	ambCtx  log.AmbientContext
	stopper *stop.Stopper
	r       replicaInCircuitBreaker
	st      *cluster.Settings
	wrapped *circuit.Breaker
}

func (br *replicaCircuitBreaker) enabled() bool {
	return replicaCircuitBreakersEnabled.Get(&br.st.SV)
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

func (br *replicaCircuitBreaker) asyncProbe(report func(error), done func()) {
	bgCtx := br.ambCtx.AnnotateCtx(context.Background())
	if err := br.stopper.RunAsyncTask(
		withCircuitBreakerProbeMarker(bgCtx),
		"replica-probe",
		func(ctx context.Context) {
			// TODO(tbg): timeout for the probe.
			defer done()

			if !br.enabled() {
				report(nil)
				return
			}

			ctx, finishAndGet := tracing.ContextWithRecordingSpan(ctx, br.ambCtx.Tracer, "probe")
			defer finishAndGet()

			// TODO(tbg): plumb r.slowReplicationThreshold here.
			// TODO(tbg): generate a nicer error on timeout (the common case). Right now it will
			// just say "timed out after 10s: context canceled".
			const probeTimeout = 10 * time.Second
			err := contextutil.RunWithTimeout(ctx, "probe", probeTimeout, func(ctx context.Context) error {
				if err := checkShouldUntripBreaker(ctx, br.r); err != nil {
					return errors.CombineErrors(br.r.rangeUnavailableError(), err)
				}
				return nil
			})
			report(err)
			if err != nil {
				rec := finishAndGet()
				// NB: can't use `ctx` any more; that's a use-after-finish.
				log.Infof(bgCtx, "probe failed %s", rec.String())
			}
		}); err != nil {
		done()
	}
}

func checkShouldUntripBreaker(ctx context.Context, r replicaInCircuitBreaker) error {
	desc := r.Desc()
	if !desc.IsInitialized() {
		return nil
	}
	ba := roachpb.BatchRequest{}
	ba.Timestamp = r.Clock().Now()
	ba.RangeID = r.Desc().RangeID
	// TODO(tbg): need a version gate here.
	probeReq := &roachpb.ProbeRequest{}
	probeReq.Key = desc.StartKey.AsRawKey()
	ba.Add(probeReq)
	_, pErr := r.Send(ctx, ba)
	if pErr != nil {
		return pErr.GoError()
	}
	// TODO(tbg): if the breaker is now untripped, probe should also
	// verify whether lease can be acquired / NotLeaseholderError with a
	// reference to another node can be obtained.
	// It could be better to also special case lease requests triggered
	// by the probe so that they bypass the breaker. Then we could do
	// this before reporting success.
	return nil
}
