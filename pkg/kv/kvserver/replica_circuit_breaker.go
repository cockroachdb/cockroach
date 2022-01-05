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
	"github.com/cockroachdb/redact"
)

type replicaInCircuitBreaker interface {
	Clock() *hlc.Clock
	Desc() *roachpb.RangeDescriptor
	Send(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
}

var replicaCircuitBreakersEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.replica_circuit_breakers.enabled",
	"fail-fast requests to unavailable replicas",
	true,
)

type replicaCircuitBreaker struct {
	st      *cluster.Settings
	wrapped *circuit.Breaker
}

func (br *replicaCircuitBreaker) enabled() bool {
	return replicaCircuitBreakersEnabled.Get(&br.st.SV)
}

func (br *replicaCircuitBreaker) Report(err error) {
	if !br.enabled() {
		return
	}
	br.wrapped.Report(err)
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
	bgCtx := ambientCtx.AnnotateCtx(context.Background())
	wrapped := circuit.NewBreaker(circuit.Options{
		Name: redact.Sprintf("breaker"), // log bridge has ctx tags
		AsyncProbe: func(report func(error), done func()) {
			if err := stopper.RunAsyncTask(
				withCircuitBreakerProbeMarker(bgCtx),
				"replica-probe",
				func(ctx context.Context) {
					// TODO(tbg): timeout for the probe.
					defer done()

					ctx, finishAndGet := tracing.ContextWithRecordingSpan(ctx, ambientCtx.Tracer, "probe")
					defer finishAndGet()

					// TODO(tbg): plumb r.slowReplicationThreshold here.
					const probeTimeout = 10 * time.Second
					err := contextutil.RunWithTimeout(ctx, "probe", probeTimeout, func(ctx context.Context) error {
						return checkShouldUntripBreaker(ctx, r)
					})
					report(err)
					if err != nil {
						rec := finishAndGet()
						// TODO
						log.Infof(ctx, "TBG %s", rec.String())
					}
				}); err != nil {
				done()
			}
		},
		EventHandler: &circuit.EventLogger{
			Log: func(buf redact.StringBuilder) {
				log.Infof(ambientCtx.AnnotateCtx(context.Background()), "%s", buf)
			},
		},
	})

	return &replicaCircuitBreaker{
		st:      cs,
		wrapped: wrapped,
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
