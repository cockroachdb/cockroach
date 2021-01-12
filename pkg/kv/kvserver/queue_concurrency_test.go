// Copyright 2019 The Cockroach Authors.
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
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

func constantTimeoutFunc(d time.Duration) func(*cluster.Settings, replicaInQueue) time.Duration {
	return func(*cluster.Settings, replicaInQueue) time.Duration { return d }
}

// TestBaseQueueConcurrent verifies that under concurrent adds/removes of ranges
// to the queue including purgatory errors and regular errors, the queue
// invariants are upheld. The test operates on fake ranges and a mock queue
// impl, which are defined at the end of the file.
func TestBaseQueueConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// We'll use this many ranges, each of which is added a few times to the
	// queue and maybe removed as well.
	const num = 1000

	cfg := queueConfig{
		maxSize:              num / 2,
		maxConcurrency:       4,
		acceptsUnsplitRanges: true,
		processTimeoutFunc:   constantTimeoutFunc(time.Millisecond),
		// We don't care about these, but we don't want to crash.
		successes:       metric.NewCounter(metric.Metadata{Name: "processed"}),
		failures:        metric.NewCounter(metric.Metadata{Name: "failures"}),
		pending:         metric.NewGauge(metric.Metadata{Name: "pending"}),
		processingNanos: metric.NewCounter(metric.Metadata{Name: "processingnanos"}),
		purgatory:       metric.NewGauge(metric.Metadata{Name: "purgatory"}),
	}

	// Set up a fake store with just exactly what the code calls into. Ideally
	// we'd set up an interface against the *Store as well, similar to
	// replicaInQueue, but this isn't an ideal world. Deal with it.
	store := &Store{
		cfg: StoreConfig{
			Clock:             hlc.NewClock(hlc.UnixNano, time.Second),
			AmbientCtx:        log.AmbientContext{Tracer: tracing.NewTracer()},
			DefaultZoneConfig: zonepb.DefaultZoneConfigRef(),
		},
	}

	// Set up a queue impl that will return random results from processing.
	impl := fakeQueueImpl{
		pr: func(context.Context, *Replica, *config.SystemConfig) (bool, error) {
			n := rand.Intn(4)
			if n == 0 {
				return true, nil
			} else if n == 1 {
				return false, errors.New("injected regular error")
			} else if n == 2 {
				return false, &benignError{errors.New("injected benign error")}
			}
			return false, &testPurgatoryError{}
		},
	}
	bq := newBaseQueue("test", impl, store, nil /* Gossip */, cfg)
	bq.getReplica = func(id roachpb.RangeID) (replicaInQueue, error) {
		return &fakeReplica{rangeID: id}, nil
	}
	bq.Start(stopper)

	var g errgroup.Group
	for i := 1; i <= num; i++ {
		r := &fakeReplica{rangeID: roachpb.RangeID(i)}
		for j := 0; j < 5; j++ {
			g.Go(func() error {
				_, err := bq.testingAdd(ctx, r, 1.0)
				return err
			})
		}
		if rand.Intn(5) == 0 {
			g.Go(func() error {
				bq.MaybeRemove(r.rangeID)
				return nil
			})
		}
		g.Go(func() error {
			bq.assertInvariants()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	for done := false; !done; {
		bq.mu.Lock()
		done = len(bq.mu.replicas) == 0
		bq.mu.Unlock()
		runtime.Gosched()
	}
}

type fakeQueueImpl struct {
	pr func(context.Context, *Replica, *config.SystemConfig) (processed bool, err error)
}

func (fakeQueueImpl) shouldQueue(
	context.Context, hlc.ClockTimestamp, *Replica, *config.SystemConfig,
) (shouldQueue bool, priority float64) {
	return rand.Intn(5) != 0, 1.0
}

func (fq fakeQueueImpl) process(
	ctx context.Context, repl *Replica, cfg *config.SystemConfig,
) (bool, error) {
	return fq.pr(ctx, repl, cfg)
}

func (fakeQueueImpl) timer(time.Duration) time.Duration {
	return time.Nanosecond
}

func (fakeQueueImpl) purgatoryChan() <-chan time.Time {
	return time.After(time.Nanosecond)
}

type fakeReplica struct {
	rangeID   roachpb.RangeID
	replicaID roachpb.ReplicaID
}

func (fr *fakeReplica) AnnotateCtx(ctx context.Context) context.Context { return ctx }
func (fr *fakeReplica) StoreID() roachpb.StoreID {
	return 1
}
func (fr *fakeReplica) GetRangeID() roachpb.RangeID         { return fr.rangeID }
func (fr *fakeReplica) ReplicaID() roachpb.ReplicaID        { return fr.replicaID }
func (fr *fakeReplica) IsInitialized() bool                 { return true }
func (fr *fakeReplica) IsDestroyed() (DestroyReason, error) { return destroyReasonAlive, nil }
func (fr *fakeReplica) Desc() *roachpb.RangeDescriptor {
	return &roachpb.RangeDescriptor{RangeID: fr.rangeID, EndKey: roachpb.RKey("z")}
}
func (fr *fakeReplica) maybeInitializeRaftGroup(context.Context) {}
func (fr *fakeReplica) redirectOnOrAcquireLease(
	context.Context,
) (kvserverpb.LeaseStatus, *roachpb.Error) {
	// baseQueue only checks that the returned error is nil.
	return kvserverpb.LeaseStatus{}, nil
}
func (fr *fakeReplica) LeaseStatusAt(context.Context, hlc.ClockTimestamp) kvserverpb.LeaseStatus {
	return kvserverpb.LeaseStatus{}
}
