// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfiller

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/sync/errgroup"
)

// NewPeriodicProgressFlusher returns a PeriodicProgressFlusher that
// will flush at the given intervals.
func NewPeriodicProgressFlusher(
	checkpointIntervalFn func() time.Duration, fractionIntervalFn func() time.Duration,
) scexec.PeriodicProgressFlusher {
	return &periodicProgressFlusher{
		clock:              timeutil.DefaultTimeSource{},
		checkpointInterval: checkpointIntervalFn,
		fractionInterval:   fractionIntervalFn,
	}
}

// NewPeriodicProgressFlusherForIndexBackfill returns a PeriodicProgressFlusher
// that will flush according to the intervals defined in the cluster settings.
func NewPeriodicProgressFlusherForIndexBackfill(
	settings *cluster.Settings,
) scexec.PeriodicProgressFlusher {
	return NewPeriodicProgressFlusher(
		func() time.Duration {
			return backfill.IndexBackfillCheckpointInterval.Get(&settings.SV)

		},
		func() time.Duration {
			// fractionInterval is copied from the logic in existing backfill code.
			// TODO(ajwerner): Add a cluster setting to control this.
			const fractionInterval = 10 * time.Second
			return fractionInterval
		},
	)
}

type periodicProgressFlusher struct {
	clock                                timeutil.TimeSource
	checkpointInterval, fractionInterval func() time.Duration
}

func (p *periodicProgressFlusher) StartPeriodicUpdates(
	ctx context.Context, tracker scexec.BackfillerProgressFlusher,
) (stop func()) {
	stopCh := make(chan struct{})
	runPeriodicWrite := func(
		ctx context.Context,
		write func(context.Context) error,
		interval func() time.Duration,
	) error {
		timer := p.clock.NewTimer()
		defer timer.Stop()
		for {
			timer.Reset(interval())
			select {
			case <-stopCh:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.Ch():
				timer.MarkRead()
				if err := write(ctx); err != nil {
					log.Warningf(ctx, "could not flush progress: %v", err)
				}
			}
		}
	}
	var g errgroup.Group
	g.Go(func() error {
		return runPeriodicWrite(
			ctx, tracker.FlushFractionCompleted, p.fractionInterval)
	})
	g.Go(func() error {
		return runPeriodicWrite(
			ctx, tracker.FlushCheckpoint, p.checkpointInterval)
	})
	toClose := stopCh // make the returned function idempotent
	return func() {
		if toClose != nil {
			close(toClose)
			toClose = nil
		}
		if err := g.Wait(); err != nil {
			log.Warningf(ctx, "waiting for progress flushing goroutines: %v", err)
		}
	}
}
