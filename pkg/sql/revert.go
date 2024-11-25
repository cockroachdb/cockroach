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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

const RevertTableDefaultBatchSize = 500000

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
	tableID catid.DescID,
	predicates kvpb.DeleteRangePredicates,
	batchSize int64,
) error {

	log.Infof(ctx, "deleting data for table %d with predicate %s", tableID, predicates.String())
	tableKey := roachpb.RKey(codec.TablePrefix(uint32(tableID)))
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
