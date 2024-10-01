// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/codahale/hdrhistogram"
)

type jobInfo interface {
	GetHighWater() time.Time
	GetFinishedTime() time.Time
	GetStatus() string
	GetError() string
}

type jobFetcher func(db *gosql.DB, jobID int) (jobInfo, error)

type latencyVerifier struct {
	name                     string
	statementTime            time.Time
	targetSteadyLatency      time.Duration
	targetInitialScanLatency time.Duration
	tolerateErrors           bool
	logger                   *logger.Logger
	jobFetcher               jobFetcher
	setTestStatus            func(...interface{})

	initialScanHighwater time.Time
	initialScanLatency   time.Duration

	catchupScanEveryN roachtestutil.EveryN

	maxSeenSteadyLatency time.Duration
	maxSeenSteadyEveryN  roachtestutil.EveryN
	latencyBecameSteady  bool

	latencyHist *hdrhistogram.Histogram
}

func makeLatencyVerifier(
	name string,
	targetInitialScanLatency time.Duration,
	targetSteadyLatency time.Duration,
	l *logger.Logger,
	jobFetcher jobFetcher,
	setTestStatus func(...interface{}),
	tolerateErrors bool,
) *latencyVerifier {
	const (
		sigFigs    = 1
		minLatency = 100 * time.Microsecond
		maxLatency = 100 * time.Second
	)
	hist := hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
	return &latencyVerifier{
		name:                     name,
		targetInitialScanLatency: targetInitialScanLatency,
		targetSteadyLatency:      targetSteadyLatency,
		logger:                   l,
		jobFetcher:               jobFetcher,
		setTestStatus:            setTestStatus,
		latencyHist:              hist,
		tolerateErrors:           tolerateErrors,
		maxSeenSteadyEveryN:      roachtestutil.Every(10 * time.Second),
		catchupScanEveryN:        roachtestutil.Every(2 * time.Second),
	}
}

func (lv *latencyVerifier) noteHighwater(highwaterTime time.Time) {
	// Highwater timestamps received before the statement time indicate an initial
	// scan is taking place.
	if highwaterTime.Before(lv.statementTime) {
		return
	}
	if lv.initialScanLatency == 0 && lv.targetInitialScanLatency != 0 {
		lv.initialScanLatency = timeutil.Since(lv.statementTime)
		lv.initialScanHighwater = highwaterTime
		lv.logger.Printf("%s: initial scan completed: latency %s, highwater %s\n", lv.name, lv.initialScanLatency, lv.initialScanHighwater)
		return
	}

	latency := timeutil.Since(highwaterTime)

	// We assume that the highwater starts far behind and eventually catches up to
	// within lv.targetSteadyLatency of the present time, which is valid. We
	// assert that the lag between then highwater and present decreases until it
	// is below lv.targetSteadyLatency and remains below lv.targetSteadyLatency.
	// This has the implicit assumption of the lag decreasing, which is only valid
	// if the highwater progresses faster than real time. If we measure the lag at
	// two different times without updating the highwater, we will observe an
	// increase in lag.
	//
	// Catchup scans do not update the highwater. If a catchup scan takes longer
	// than lv.initialScanHighwater, we will observe that the lag is higher than
	// lv.targetSteadyLatency, violating the above assertion. For this reason, we
	// wait for catchup scans to finish before asserting latency.
	if highwaterTime.Equal(lv.initialScanHighwater) {
		if lv.catchupScanEveryN.ShouldLog() {
			lv.logger.Printf("%s: catchup scan: latency %s\n", lv.name, latency.Truncate(time.Millisecond))
		}
		return
	}

	if lv.targetSteadyLatency == 0 || latency < lv.targetSteadyLatency/2 {
		lv.latencyBecameSteady = true
	}
	if !lv.latencyBecameSteady {
		// Before we have RangeFeed, the polls just get
		// progressively smaller after the initial one. Start
		// tracking the max latency once we seen a latency
		// that's less than the max allowed. Verify at the end
		// of the test that this happens at some point.
		if lv.maxSeenSteadyEveryN.ShouldLog() {
			update := fmt.Sprintf(
				"%s: end-to-end latency %s not yet below target steady latency %s",
				lv.name, latency.Truncate(time.Millisecond), lv.targetSteadyLatency.Truncate(time.Millisecond))
			lv.setTestStatus(update)
		}
		return
	}
	if err := lv.latencyHist.RecordValue(latency.Nanoseconds()); err != nil {
		lv.logger.Printf("%s: could not record value %s: %s\n", lv.name, latency, err)
	}
	if latency > lv.maxSeenSteadyLatency {
		lv.maxSeenSteadyLatency = latency
	}
	if lv.maxSeenSteadyEveryN.ShouldLog() {
		update := fmt.Sprintf(
			"%s: end-to-end steady latency %s; max steady latency so far %s; highwater %s",
			lv.name, latency.Truncate(time.Millisecond), lv.maxSeenSteadyLatency.Truncate(time.Millisecond), highwaterTime)
		lv.setTestStatus(update)
	}
}

// pollLatencyUntilJobSucceeds polls the changefeed latency until it is
// signalled to stop or the job completes.
func (lv *latencyVerifier) pollLatencyUntilJobSucceeds(
	ctx context.Context, db *gosql.DB, jobID int, interval time.Duration, stopper chan struct{},
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stopper:
			return nil
		case <-time.After(interval):
		}

		info, err := lv.jobFetcher(db, jobID)
		if err != nil {
			if lv.tolerateErrors {
				lv.logger.Printf("%s: error getting info: %s", lv.name, err)
				continue
			}
			return err
		}
		status := info.GetStatus()
		if status == "succeeded" {
			lv.noteHighwater(info.GetFinishedTime())
			lv.logger.Printf("%s: latency poller shutting down due to changefeed completion", lv.name)
			return nil
		} else if status == "running" {
			lv.noteHighwater(info.GetHighWater())
		} else {
			lv.logger.Printf("%s: unexpected status: %s, error: %s", lv.name, status, info.GetError())
			return errors.Errorf("%s: unexpected status: %s", lv.name, status)
		}
		if lv.targetSteadyLatency != 0 && lv.maxSeenSteadyLatency > lv.targetSteadyLatency {
			return errors.Errorf("%s: max latency was more than allowed: %s vs %s",
				lv.name, lv.maxSeenSteadyLatency, lv.targetSteadyLatency)
		}
	}
}

func (lv *latencyVerifier) assertValid(t test.Test) {
	if lv.targetInitialScanLatency != 0 && lv.initialScanLatency == 0 {
		t.Fatalf("%s: initial scan did not complete", lv.name)
	}
	if lv.targetInitialScanLatency != 0 && lv.initialScanLatency > lv.targetInitialScanLatency {
		t.Fatalf("%s: initial scan latency was more than target: %s vs %s",
			lv.name, lv.initialScanLatency, lv.targetInitialScanLatency)
	}
	if lv.targetSteadyLatency != 0 && !lv.latencyBecameSteady {
		t.Fatalf("%s: latency never dropped to acceptable steady level: %s", lv.name, lv.targetSteadyLatency)
	}
	if lv.targetSteadyLatency != 0 && lv.maxSeenSteadyLatency > lv.targetSteadyLatency {
		t.Fatalf("%s: max latency was more than allowed: %s vs %s",
			lv.name, lv.maxSeenSteadyLatency, lv.targetSteadyLatency)
	}
}

func (lv *latencyVerifier) maybeLogLatencyHist() {
	if lv.latencyHist == nil {
		return
	}
	lv.logger.Printf(
		"%s: end-to-end __avg(ms)__p50(ms)__p75(ms)__p90(ms)__p95(ms)__p99(ms)_pMax(ms)\n", lv.name)
	lv.logger.Printf("%s end-to-end  %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
		lv.name,
		time.Duration(lv.latencyHist.Mean()).Seconds()*1000,
		time.Duration(lv.latencyHist.ValueAtQuantile(50)).Seconds()*1000,
		time.Duration(lv.latencyHist.ValueAtQuantile(75)).Seconds()*1000,
		time.Duration(lv.latencyHist.ValueAtQuantile(90)).Seconds()*1000,
		time.Duration(lv.latencyHist.ValueAtQuantile(95)).Seconds()*1000,
		time.Duration(lv.latencyHist.ValueAtQuantile(99)).Seconds()*1000,
		time.Duration(lv.latencyHist.ValueAtQuantile(100)).Seconds()*1000,
	)
}
