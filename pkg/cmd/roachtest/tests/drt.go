// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

//go:generate mockgen -source drt.go -package tests -destination drt_generated.go

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/prometheus"
	"github.com/cockroachdb/errors"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type promClient interface {
	Query(ctx context.Context, query string, ts time.Time) (model.Value, promv1.Warnings, error)
}

type tpccChaosEventProcessor struct {
	workloadInstances []workloadInstance
	workloadNodeIP    string
	ops               []string
	ch                chan ChaosEvent
	promClient        promClient
	errs              []error
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
			return errors.Newf("expected successes to be increasing, found from %f, to %f", from, to)
		},
		func(from, to model.SampleValue) error {
			if to == from {
				return nil
			}
			return errors.Newf("expected 0 errors, found from %f, to %f", from, to)
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
		// Add an extra interval to fromTime to account for the first data point
		// which may include a node not being fully shutdown or restarted.
		fromTime := fromTime.Add(prometheus.DefaultScrapeInterval)
		if !toTime.After(fromTime) {
			l.PrintfCtx(
				ctx,
				"to %s < from %s, skipping",
				toTime.Format(time.RFC3339),
				fromTime.Format(time.RFC3339),
			)
			continue
		}

		// Verify all nodes had successes and minimal errors.
		q := fmt.Sprintf(
			`workload_tpcc_%s_%s_total{instance="%s:%d"}`,
			op,
			check.metricType,
			ep.workloadNodeIP,
			w.prometheusPort,
		)
		fromVal, warnings, err := ep.promClient.Query(
			ctx,
			q,
			fromTime,
		)
		if err != nil {
			return err
		}
		if len(warnings) > 0 {
			return errors.Newf("found warnings querying prometheus: %s", warnings)
		}
		toVal, warnings, err := ep.promClient.Query(
			ctx,
			q,
			toTime,
		)
		if err != nil {
			return err
		}
		if len(warnings) > 0 {
			return errors.Newf("found warnings querying prometheus: %s", warnings)
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

func (ep *tpccChaosEventProcessor) listen(ctx context.Context, l *logger.Logger) {
	go func() {
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
	}()
}
