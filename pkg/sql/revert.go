// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TODO (msbutler): tune these
var rollbackBatchSize = settings.RegisterIntSetting(
	settings.TenantWritable,
	"bulkio.import.predicate_delete_range_batch_size",
	"the number of ranges to include in a single Predicate Based DeleteRange request",
	10)

var predicateDeleteRangeNumWorkers = settings.RegisterIntSetting(
	settings.TenantWritable,
	"bulkio.import.predicate_delete_range_parallelism",
	"the number of workers used to issue Predicate Based DeleteRange request",
	4)

// RevertTableDefaultBatchSize is the default batch size for reverting tables.
// This only needs to be small enough to keep raft/rocks happy -- there is no
// reply size to worry about.
// TODO(dt): tune this via experimentation.
const RevertTableDefaultBatchSize = 500000

// RevertTables reverts the passed table to the target time, which much be above
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

	// TODO(dt): pre-split requests up using a rangedesc cache and run batches in
	// parallel (since we're passing a key limit, distsender won't do its usual
	// splitting/parallel sending to separate ranges).
	for len(spans) != 0 {
		var b kv.Batch
		for _, span := range spans {
			b.AddRawRequest(&roachpb.RevertRangeRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    span.Key,
					EndKey: span.EndKey,
				},
				TargetTime:                          targetTime,
				IgnoreGcThreshold:                   ignoreGCThreshold,
				EnableTimeBoundIteratorOptimization: true, // NB: Must set for 22.1 compatibility.
			})
		}
		b.Header.MaxSpanRequestKeys = batchSize

		if err := db.Run(ctx, &b); err != nil {
			return err
		}

		spans = spans[:0]
		for _, raw := range b.RawResponse().Responses {
			r := raw.GetRevertRange()
			if r.ResumeSpan != nil {
				if !r.ResumeSpan.Valid() {
					return errors.Errorf("invalid resume span: %s", r.ResumeSpan)
				}
				spans = append(spans, *r.ResumeSpan)
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
	predicates roachpb.DeleteRangePredicates,
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
						admissionHeader := roachpb.AdmissionHeader{
							Priority:                 int32(admissionpb.BulkNormalPri),
							CreateTime:               timeutil.Now().UnixNano(),
							Source:                   roachpb.AdmissionHeader_FROM_SQL,
							NoMemoryReservedAtSource: true,
						}
						delRangeRequest := &roachpb.DeleteRangeRequest{
							RequestHeader: roachpb.RequestHeader{
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
							roachpb.Header{MaxSpanRequestKeys: batchSize},
							admissionHeader,
							delRangeRequest)

						if err != nil {
							log.Errorf(ctx, "delete range %s - %s failed: %s", span.Key, span.EndKey, err.String())
							return errors.Wrapf(err.GoError(), "delete range %s - %s", span.Key, span.EndKey)
						}
						span = nil
						resp := rawResp.(*roachpb.DeleteRangeResponse)
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
