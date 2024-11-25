// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// TODO (msbutler): tune these
var rollbackBatchSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.import.predicate_delete_range_batch_size",
	"the number of ranges to include in a single Predicate Based DeleteRange request",
	10)

var predicateDeleteRangeNumWorkers = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.import.predicate_delete_range_parallelism",
	"the number of workers used to issue Predicate Based DeleteRange request",
	4)

var maxRevertSpanNumWorkers = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.revert.max_span_parallelism",
	"the maximum number of workers used to issue RevertRange request",
	8,
	settings.PositiveInt,
)

// RevertTableDefaultBatchSize is the default batch size for reverting tables.
// This only needs to be small enough to keep raft/rocks happy -- there is no
// reply size to worry about.
// TODO(dt): tune this via experimentation.
const RevertTableDefaultBatchSize = 500000

// RevertTables reverts the passed table to the target time, which must be above
// the GC threshold for every range (unless the flag ignoreGCThreshold is passed
// which should be done with care -- see RevertRangeRequest.IgnoreGCThreshold).
func RevertTables(
	ctx context.Context,
	db *kv.DB,
	execCfg *ExecutorConfig,
	tables []catalog.TableDescriptor,
	targetTime hlc.Timestamp,
	ignoreGCThreshold bool,
	batchSize int64,
) error {
	spans := make([]roachpb.Span, 0, len(tables))

	// Check that all the tables are revertable -- i.e. offline.
	for i := range tables {
		if tables[i].GetState() != descpb.DescriptorState_OFFLINE {
			return errors.New("only offline tables can be reverted")
		}

		if !tables[i].IsPhysicalTable() {
			return errors.Errorf("cannot revert virtual table %s", tables[i].GetName())
		}
		spans = append(spans, tables[i].TableSpan(execCfg.Codec))
	}

	for i := range tables {
		// This is a) rare and b) probably relevant if we are looking at logs so it
		// probably makes sense to log it without a verbosity filter.
		log.Infof(ctx, "reverting table %s (%d) to time %v", tables[i].GetName(), tables[i].GetID(), targetTime)
	}

	return RevertSpans(ctx, db, spans, targetTime, ignoreGCThreshold, batchSize, nil)
}

// RevertSpansContext provides the execution environment for a call to
// RevertSpansFanout.
type RevertSpansContext interface {
	ExtendedEvalContext() *extendedEvalContext
	ExecCfg() *ExecutorConfig
	DistSQLPlanner() *DistSQLPlanner
}

// RevertSpansFanout calls RevertSpans in parallel. The span is
// divided using DistSQL's PartitionSpans.
//
// We do this to get parallel execution of RevertRange even in the
// case of a non-zero batch size. DistSender will not parallelize
// requests with non-zero MaxSpanRequestKeys set.
func RevertSpansFanout(
	ctx context.Context,
	db *kv.DB,
	rsCtx RevertSpansContext,
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

	spanPartitions, err := dsp.PartitionSpans(ctx, planCtx, spans)
	if err != nil {
		return err
	}

	var workerPartitions []SpanPartition
	if len(spanPartitions) <= maxWorkerCount {
		workerPartitions = spanPartitions
	} else {
		workerPartitions = make([]SpanPartition, maxWorkerCount)
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

// DeleteTableWithPredicate issues a series of point and range tombstones over a subset of keys in a
// table that match the passed-in predicate.
//
// This function will error, without a resume span, if it encounters an intent
// in the span. The caller should resolve these intents by retrying the
// function. To prevent errors, the caller should only pass a table that will
// not see new writes during this bulk delete operation (e.g. on a span that's
// part of an import rollback).
//
// NOTE: this function will issue tombstones on keys with versions that match
// the predicate, in contrast to RevertRange, which rolls back a table to a
// certain timestamp. For example, if a key gets a single update after the
// predicate.startTime, DeleteTableWithPredicate would delete that key, while
// RevertRange would revert that key to its state before the update.
func DeleteTableWithPredicate(
	ctx context.Context,
	db *kv.DB,
	codec keys.SQLCodec,
	sv *settings.Values,
	distSender *kvcoord.DistSender,
	table catalog.TableDescriptor,
	predicates kvpb.DeleteRangePredicates,
	batchSize int64,
) error {

	log.Infof(ctx, "deleting data for table %d with predicate %s", table.GetID(), predicates.String())
	tableKey := roachpb.RKey(codec.TablePrefix(uint32(table.GetID())))
	tableSpan := roachpb.RSpan{Key: tableKey, EndKey: tableKey.PrefixEnd()}

	// To process the table in parallel, spin up a few workers and partition the
	// inputted span such that each span partition contains a few ranges of data.
	// The partitions are sent to the workers via the spansToDo channel.
	//
	// TODO (msbutler): tune these
	rangesPerBatch := rollbackBatchSize.Get(sv)
	numWorkers := int(predicateDeleteRangeNumWorkers.Get(sv))

	spansToDo := make(chan *roachpb.Span, 1)

	// Create a cancellable context to prevent the worker goroutines below from
	// leaking once the parent goroutine returns.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	grp := ctxgroup.WithContext(ctx)
	grp.GoCtx(func(ctx context.Context) error {
		return ctxgroup.GroupWorkers(ctx, numWorkers, func(ctx context.Context, _ int) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case span, ok := <-spansToDo:
					if !ok {
						return nil
					}
					// If the kvserver returns a resume span, shadow the initial span with
					// the resume span and rerun the request until no resume span is
					// returned.
					resumeCount := 1
					for span != nil {
						admissionHeader := kvpb.AdmissionHeader{
							Priority:                 int32(admissionpb.BulkNormalPri),
							CreateTime:               timeutil.Now().UnixNano(),
							Source:                   kvpb.AdmissionHeader_FROM_SQL,
							NoMemoryReservedAtSource: true,
						}
						delRangeRequest := &kvpb.DeleteRangeRequest{
							RequestHeader: kvpb.RequestHeader{
								Key:    span.Key,
								EndKey: span.EndKey,
							},
							UseRangeTombstone: true,
							Predicates:        predicates,
						}
						log.VEventf(ctx, 2, "deleting range %s - %s; attempt %v", span.Key, span.EndKey, resumeCount)

						rawResp, err := kv.SendWrappedWithAdmission(
							ctx,
							db.NonTransactionalSender(),
							kvpb.Header{MaxSpanRequestKeys: batchSize},
							admissionHeader,
							delRangeRequest)

						if err != nil {
							log.Errorf(ctx, "delete range %s - %s failed: %v", span.Key, span.EndKey, err)
							return errors.Wrapf(err.GoError(), "delete range %s - %s", span.Key, span.EndKey)
						}
						span = nil
						resp := rawResp.(*kvpb.DeleteRangeResponse)
						if resp.ResumeSpan != nil {
							if !resp.ResumeSpan.Valid() {
								return errors.Errorf("invalid resume span: %s", resp.ResumeSpan)
							}
							span = resp.ResumeSpan
							resumeCount++
						}
					}
				}
			}
		})
	})

	var n int64
	lastKey := tableSpan.Key
	ri := kvcoord.MakeRangeIterator(distSender)
	for ri.Seek(ctx, tableSpan.Key, kvcoord.Ascending); ; ri.Next(ctx) {
		if !ri.Valid() {
			return ri.Error()
		}
		if n++; n >= rangesPerBatch || !ri.NeedAnother(tableSpan) {
			endKey := ri.Desc().EndKey
			if tableSpan.EndKey.Less(endKey) {
				endKey = tableSpan.EndKey
			}
			spansToDo <- &roachpb.Span{Key: lastKey.AsRawKey(), EndKey: endKey.AsRawKey()}
			n = 0
			lastKey = endKey
		}

		if !ri.NeedAnother(tableSpan) {
			break
		}
	}
	close(spansToDo)
	return grp.Wait()
}
