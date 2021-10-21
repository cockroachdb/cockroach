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
	rangeUnavailableError() error
}

var defaultReplicaCircuitBreakersEnabled = !envutil.EnvOrDefaultBool("COCKROACH_DISABLE_REPLICA_CIRCUIT_BREAKERS", true)

var replicaCircuitBreakersEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.replica_circuit_breakers.enabled",
	"fail-fast requests to unavailable replicas",
	defaultReplicaCircuitBreakersEnabled,
)

type replicaCircuitBreaker struct {
	ambCtx  log.AmbientContext
	stopper *stop.Stopper
	r       replicaInCircuitBreaker
	st      *cluster.Settings
	wrapped *circuit.Breaker
}

func (br *replicaCircuitBreaker) enabled() bool {
	return replicaCircuitBreakersEnabled.Get(&br.st.SV) &&
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

func (br *replicaCircuitBreaker) asyncProbe(report func(error), done func()) {
	bgCtx := br.ambCtx.AnnotateCtx(context.Background())
	if err := br.stopper.RunAsyncTask(
		withCircuitBreakerProbeMarker(bgCtx),
		"replica-probe",
		func(ctx context.Context) {
			defer done()

			if !br.enabled() {
				report(nil)
				return
			}

			// TODO(tbg): plumb r.slowReplicationThreshold here.
			// TODO(tbg): generate a nicer error on timeout (the common case). Right now it will
			// just say "operation probe timed out after 10s: context canceled"; this isn't helpful.
			// Might be better to wrap with an error that indicates the last attempt at un-breaking,
			// or when the range first tripped, or not to report a new error at all.
			const probeTimeout = 10 * time.Second
			err := contextutil.RunWithTimeout(ctx, "probe", probeTimeout, func(ctx context.Context) error {
				if err := checkShouldUntripBreaker(ctx, br.r); err != nil {
					return errors.CombineErrors(br.r.rangeUnavailableError(), err)
				}
				return nil
			})
			report(err)
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
	probeReq := &roachpb.ProbeRequest{}
	probeReq.Key = desc.StartKey.AsRawKey()
	ba.Add(probeReq)
	_, pErr := r.Send(ctx, ba)
	if pErr != nil {
		return pErr.GoError()
	}
	return nil
}
