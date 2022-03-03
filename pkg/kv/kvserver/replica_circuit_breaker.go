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
	poisonInflightLatches(err error)
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
	wrapped *circuit.Breaker

	versionIsActive int32 // atomic
}

func (br *replicaCircuitBreaker) HasMark(err error) bool {
	return br.wrapped.HasMark(err)
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

func (br *replicaCircuitBreaker) asyncProbe(report func(error), done func()) {
	bgCtx := br.ambCtx.AnnotateCtx(context.Background())
	if err := br.stopper.RunAsyncTask(bgCtx, "replica-probe", func(ctx context.Context) {
		defer done()

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
