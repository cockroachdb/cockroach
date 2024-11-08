// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type tpccChaosEventProcessor struct {
	workloadInstances []workloadInstance
	workloadNodeIP    string
	ops               []string
	ch                chan ChaosEvent
	promClient        prometheus.Client
	errs              []error

	// allowZeroSuccessDuringUptime allows 0 successes during an uptime event.
	// Otherwise, we expect success rates to be strictly increasing during
	// uptime.
	allowZeroSuccessDuringUptime bool
	// maxErrorsDuringUptime dictates the number of errors to accept during
	// uptime.
	maxErrorsDuringUptime int
}

func (ep *tpccChaosEventProcessor) checkUptime(
	ctx context.Context,
	l *logger.Logger,
	op string,
	w workloadInstance,
	from time.Time,
	to time.Time,
) error {
	return ep.checkMetrics(
		ctx,
		l,
		op,
		w,
		from,
		to,
		func(from, to model.SampleValue) error {
			if to > from {
				return nil
			}
			// We allow to == from if allowZeroSuccessDuringUptime is set.
			if ep.allowZeroSuccessDuringUptime && to == from {
				return nil
			}
			return errors.Newf("expected successes to be increasing, found from %f, to %f", from, to)
		},
		func(from, to model.SampleValue) error {
			// Allow up to maxErrorsDuringUptime errors during uptime.
			if to <= from+model.SampleValue(ep.maxErrorsDuringUptime) {
				return nil
			}
			return errors.Newf("expected <=%d errors, found from %f, to %f", ep.maxErrorsDuringUptime, from, to)
		},
	)
}

func (ep *tpccChaosEventProcessor) checkDowntime(
	ctx context.Context,
	l *logger.Logger,
	op string,
	w workloadInstance,
	from time.Time,
	to time.Time,
) error {
	return ep.checkMetrics(
		ctx,
		l,
		op,
		w,
		from,
		to,
		func(from, to model.SampleValue) error {
			if to == from {
				return nil
			}
			return errors.Newf("expected successes to not increase, found from %f, to %f", from, to)
		},
		func(from, to model.SampleValue) error {
			if to > from {
				return nil
			}
			return errors.Newf("expected errors, found from %f, to %f", from, to)
		},
	)
}

func (ep *tpccChaosEventProcessor) checkMetrics(
	ctx context.Context,
	l *logger.Logger,
	op string,
	w workloadInstance,
	fromTime time.Time,
	toTime time.Time,
	successCheckFn func(from, to model.SampleValue) error,
	errorCheckFn func(from, to model.SampleValue) error,
) error {
	// Add an extra interval to fromTime to account for the first data point
	// which may include a node not being fully shutdown or restarted.
	fromTime = fromTime.Add(prometheus.DefaultScrapeInterval)
	// Similarly, scale back the toTime to account for the data point
	// potentially already having data of a node which may have already
	// started restarting or shutting down.
	toTime = toTime.Add(-prometheus.DefaultScrapeInterval)
	if !toTime.After(fromTime) {
		l.PrintfCtx(
			ctx,
			"to %s < from %s, skipping",
			toTime.Format(time.RFC3339),
			fromTime.Format(time.RFC3339),
		)
		return nil
	}

	for _, check := range []struct {
		metricType string
		checkFn    func(from, to model.SampleValue) error
	}{
		{
			metricType: "success",
			checkFn:    successCheckFn,
		},
		{
			metricType: "error",
			checkFn:    errorCheckFn,
		},
	} {
		// Verify all nodes had successes and minimal errors.
		q := fmt.Sprintf(
			`workload_tpcc_%s_%s_total{instance="%s:%d"}`,
			op,
			check.metricType,
			ep.workloadNodeIP,
			w.prometheusPort,
		)
		fromVal, err := ep.queryPrometheus(ctx, l, q, fromTime)
		if err != nil {
			return err
		}
		toVal, err := ep.queryPrometheus(ctx, l, q, toTime)
		if err != nil {
			return err
		}

		// Results are a vector with 1 element, so deserialize accordingly.
		fromVec := fromVal.(model.Vector)
		if len(fromVec) == 0 {
			return errors.Newf("unexpected empty fromVec for %s @ %s", q, fromTime.Format(time.RFC3339))
		}
		from := fromVec[0].Value
		toVec := toVal.(model.Vector)
		if len(toVec) == 0 {
			return errors.Newf("unexpected empty toVec for %s @ %s", q, toTime.Format(time.RFC3339))
		}
		to := toVec[0].Value

		l.PrintfCtx(
			ctx,
			"metric %s, from %f @ %s, to %f @ %s\n",
			q,
			from,
			fromTime.Format(time.RFC3339),
			to,
			toTime.Format(time.RFC3339),
		)

		if err := check.checkFn(from, to); err != nil {
			return errors.Wrapf(
				err,
				"error at from %s, to %s on metric %s",
				fromTime.Format(time.RFC3339),
				toTime.Format(time.RFC3339),
				q,
			)
		}
	}
	return nil
}

func (ep *tpccChaosEventProcessor) queryPrometheus(
	ctx context.Context, l *logger.Logger, q string, ts time.Time,
) (val model.Value, err error) {
	rOpts := base.DefaultRetryOptions()
	rOpts.MaxRetries = 5
	var warnings promv1.Warnings
	for r := retry.Start(rOpts); r.Next(); {
		val, warnings, err = ep.promClient.Query(
			ctx,
			q,
			ts,
		)
		if err == nil {
			break
		}
		l.Printf("error querying prometheus, retrying: %+v", err)
	}
	if len(warnings) > 0 {
		return nil, errors.Newf("found warnings querying prometheus: %s", warnings)
	}
	return val, err
}

func (ep *tpccChaosEventProcessor) writeErr(ctx context.Context, l *logger.Logger, err error) {
	l.PrintfCtx(ctx, "error during chaos: %v", err)
	ep.errs = append(ep.errs, err)
}

func (ep *tpccChaosEventProcessor) err() error {
	var err error
	for i := range ep.errs {
		if i == 0 {
			err = ep.errs[i]
		} else {
			err = errors.CombineErrors(err, ep.errs[i])
		}
	}
	return err
}

func (ep *tpccChaosEventProcessor) listen(ctx context.Context, t task.Tasker, l *logger.Logger) {
	t.Go(func(context.Context, *logger.Logger) error {
		var prevTime time.Time
		started := false
		for ev := range ep.ch {
			switch ev.Type {
			case ChaosEventTypeStart,
				ChaosEventTypeEnd,
				ChaosEventTypeShutdownComplete,
				ChaosEventTypeStartupComplete:
				// Do nothing - just need time to be marked.
			case ChaosEventTypePreShutdown:
				// We cannot assert the first time as the first Start event may not yet have traffic.
				if !started {
					started = true
					break
				}
				for _, op := range ep.ops {
					for _, w := range ep.workloadInstances {
						if err := ep.checkUptime(
							ctx,
							l,
							op,
							w,
							prevTime,
							ev.Time,
						); err != nil {
							ep.writeErr(ctx, l, err)
						}
					}
				}
			case ChaosEventTypePreStartup:
				for _, op := range ep.ops {
					for _, w := range ep.workloadInstances {
						if w.nodes.Equals(ev.Target) {
							// If target was subject to shutdown, expect downtime.
							if err := ep.checkDowntime(
								ctx,
								l,
								op,
								w,
								prevTime,
								ev.Time,
							); err != nil {
								ep.writeErr(ctx, l, err)
							}
						} else if err := ep.checkUptime(
							ctx,
							l,
							op,
							w,
							prevTime,
							ev.Time,
						); err != nil {
							ep.writeErr(ctx, l, err)
						}
					}
				}
			}
			prevTime = ev.Time
		}
		return nil
	})
}
