// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterstats

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// ProcessStatEventFn processes a collection of stat events.
type ProcessStatEventFn func([]StatEvent) bool

// StatStreamer collects registered point-in-time stats at an interval and
// reports them for processing.
type StatStreamer interface {
	// Register adds a stat to be collected.
	Register(...ClusterStat)
	// Run collects cluster statistics, attributable to by nodes in the cluster.
	// This collection is done at an interval defined in the stat streamer, whilst
	// the query time is increased bt the collecter interval. The results are
	// passed the provided processTickFn. This function will run indefinitely,
	// until either:
	//   (1) the context cancellation
	//   (2) processTickFn returns true, indicating that it has finished.
	Run(context.Context, *logger.Logger, time.Time) error
	// SetInterval updates the streaming interval (default 10s) to the duration
	// given.
	SetInterval(time.Duration)
}

// clusterStatStreamer collects registered point-in-time stats at an interval
// and reports them to processTickFn.
type clusterStatStreamer struct {
	statCollector      StatCollector
	processStatEventFn ProcessStatEventFn
	registered         []ClusterStat
	interval           time.Duration
	// NB: testingZeroTime is used to set the run loop tick scrape frequency to
	// zero in unit tests. In all other cases, the interval is used.
	testingZeroTime bool
	errs            []error
}

// NewClusterStatStreamer returns an implementation of StatsStreamer.
func NewClusterStatStreamer(
	statCollector StatCollector, processStatEventFn ProcessStatEventFn,
) StatStreamer {
	return newClusterStatStreamer(statCollector, processStatEventFn)
}

func newClusterStatStreamer(
	statCollector StatCollector, processStatEventFn ProcessStatEventFn,
) *clusterStatStreamer {
	return &clusterStatStreamer{
		statCollector:      statCollector,
		registered:         make([]ClusterStat, 0, 1),
		processStatEventFn: processStatEventFn,
		interval:           defaultScrapeInterval,
		errs:               make([]error, 0, 1),
	}
}

// SetInterval updates the streaming interval (default 10s) to the duration
// given.
func (css *clusterStatStreamer) SetInterval(interval time.Duration) {
	css.interval = interval
}

// Register adds a stat to be collected.
func (css *clusterStatStreamer) Register(stats ...ClusterStat) {
	css.registered = append(css.registered, stats...)
}

// StatEvent represents a prometheus statistic point-in-time result, for a
// query.
type StatEvent struct {
	Stat  ClusterStat
	Value map[string]StatPoint
}

// Run collects cluster statistics, attributable to by nodes in the cluster.
// This collection is done at an interval defined in the stat streamer, whilst
// the query time is increased bt the collecter interval. The results are
// passed the provided processTickFn. This function will run indefinitely,
// until either:
//
//	(1) the context cancellation
//	(2) processTickFn returns true, indicating that it has finished.
func (css *clusterStatStreamer) Run(
	ctx context.Context, l *logger.Logger, startTime time.Time,
) error {
	var statsTimer timeutil.Timer
	defer statsTimer.Stop()
	queryTime := startTime

	// updateTimer resets the timer controlling the streaming loop iteration.
	// If testingZeroTime is set, the interval is ignored and the loop runs
	// with 0 delay.
	updateTimer := func() {
		if css.testingZeroTime {
			statsTimer.Reset(0)
		} else {
			statsTimer.Reset(css.interval)
		}
	}

	updateTimer()
	for {
		select {
		case <-ctx.Done():
			return css.err()
		case <-statsTimer.C:
			eventBuffer := make([]StatEvent, 0, 1)
			statsTimer.Read = true
			for _, stat := range css.registered {
				values, err := css.statCollector.CollectPoint(ctx, l, queryTime, stat.Query)
				if err != nil {
					l.PrintfCtx(ctx, "Unable to collect registered stat %s: %s", stat.Query, err)
					css.writeErr(ctx, l, err)
					continue
				}
				eventBuffer = append(eventBuffer, StatEvent{
					Stat:  stat,
					Value: values[stat.LabelName],
				})
			}
			updateTimer()
			queryTime = queryTime.Add(css.interval)
			if finish := css.processStatEventFn(eventBuffer); finish {
				return css.err()
			}
		}
	}
}

func (css *clusterStatStreamer) writeErr(ctx context.Context, l *logger.Logger, err error) {
	l.PrintfCtx(ctx, "error during stat streaming: %v", err)
	css.errs = append(css.errs, err)
}

func (css *clusterStatStreamer) err() error {
	var err error
	for i := range css.errs {
		if i == 0 {
			err = css.errs[i]
		} else {
			err = errors.CombineErrors(err, css.errs[i])
		}
	}
	return err
}
