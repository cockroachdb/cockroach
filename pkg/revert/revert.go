// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revert

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

var maxRevertSpanNumWorkers = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.revert.max_span_parallelism",
	"the maximum number of workers used to issue RevertRange request",
	8,
	settings.PositiveInt,
)

// RevertSpansFanout calls RevertSpans in parallel. The span is
// divided using DistSQL's PartitionSpans.
//
// We do this to get parallel execution of RevertRange even in the
// case of a non-zero batch size. DistSender will not parallelize
// requests with non-zero MaxSpanRequestKeys set.
func RevertSpansFanout(
	ctx context.Context,
	db *kv.DB,
	rsCtx sql.JobExecContext,
	spans []roachpb.Span,
	targetTime hlc.Timestamp,
	ignoreGCThreshold bool,
	batchSize int64,
	onCompletedCallback func(context.Context, roachpb.Span) error,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "sql.RevertSpansFanout")
	defer sp.Finish()

	execCfg := rsCtx.ExecCfg()
	maxWorkerCount := int(maxRevertSpanNumWorkers.Get(execCfg.SV()))
	if maxWorkerCount == 1 {
		return RevertSpans(ctx, db, spans, targetTime, ignoreGCThreshold, batchSize, onCompletedCallback)
	}

	dsp := rsCtx.DistSQLPlanner()
	planCtx, _, err := dsp.SetupAllNodesPlanning(ctx, rsCtx.ExtendedEvalContext(), execCfg)
	if err != nil {
		return err
	}

	spanPartitions, err := dsp.PartitionSpans(ctx, planCtx, spans, sql.PartitionSpansBoundDefault)
	if err != nil {
		return err
	}

	var workerPartitions []sql.SpanPartition
	if len(spanPartitions) <= maxWorkerCount {
		workerPartitions = spanPartitions
	} else {
		workerPartitions = make([]sql.SpanPartition, maxWorkerCount)
		for i, sp := range spanPartitions {
			idx := i % maxWorkerCount
			workerPartitions[idx].Spans = append(workerPartitions[idx].Spans, sp.Spans...)
		}
	}

	var callback func(context.Context, roachpb.Span) error
	if onCompletedCallback != nil {
		// If we have an onCompletedCallback arrange for it to
		// be called serially so that callers don't need to
		// worry about making it concurrency safe.
		var callbackMutex syncutil.Mutex
		callback = func(ctx context.Context, completed roachpb.Span) error {
			callbackMutex.Lock()
			defer callbackMutex.Unlock()
			return onCompletedCallback(ctx, completed)
		}
	}

	errGroup, workerCtx := errgroup.WithContext(ctx)
	for i := range workerPartitions {
		workerIdx := i
		errGroup.Go(func() error {
			spans := workerPartitions[workerIdx].Spans
			return RevertSpans(workerCtx, db, spans,
				targetTime, ignoreGCThreshold, batchSize, callback)
		})
	}
	return errGroup.Wait()
}

// RevertSpans reverts the passed span to the target time, which must be above
// the GC threshold for every range (unless the flag ignoreGCThreshold is passed
// which should be done with care -- see RevertRangeRequest.IgnoreGCThreshold).
//
// The onCompletedSpan is called after each response to a RevertRange
// request.
func RevertSpans(
	ctx context.Context,
	db *kv.DB,
	spans []roachpb.Span,
	targetTime hlc.Timestamp,
	ignoreGCThreshold bool,
	batchSize int64,
	onCompletedSpan func(ctx context.Context, completed roachpb.Span) error,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "sql.RevertSpans")
	defer sp.Finish()

	for _, sp := range spans {
		span := &sp
		for span != nil {
			var b kv.Batch
			b.AddRawRequest(&kvpb.RevertRangeRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    span.Key,
					EndKey: span.EndKey,
				},
				TargetTime:        targetTime,
				IgnoreGcThreshold: ignoreGCThreshold,
			})
			// TODO(ssd): We should probably be setting an
			// admission header here has well.
			b.Header.MaxSpanRequestKeys = batchSize

			log.VEventf(ctx, 2, "RevertRange %s %s", span, targetTime)
			if err := db.Run(ctx, &b); err != nil {
				return err
			}

			if l := len(b.RawResponse().Responses); l != 1 {
				return errors.AssertionFailedf("expected single response, got %d", l)
			}

			resp := b.RawResponse().Responses[0].GetRevertRange()
			if resp == nil {
				return errors.AssertionFailedf("expected RevertRangeResponse, got: %v", resp)
			}

			completed := *span
			span = resp.ResumeSpan

			if resp.ResumeSpan != nil {
				if !resp.ResumeSpan.Valid() {
					return errors.Errorf("invalid resume span: %s", resp.ResumeSpan)
				}
				completed.EndKey = resp.ResumeSpan.Key
			}

			if onCompletedSpan != nil {
				if err := onCompletedSpan(ctx, completed); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// RevertDefaultBatchSize is the default batch size for reverting spans.
// This only needs to be small enough to keep raft/pebble happy -- there is no
// reply size to worry about.
// TODO(dt): tune this via experimentation.
const RevertDefaultBatchSize = 500000
