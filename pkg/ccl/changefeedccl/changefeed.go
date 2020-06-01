// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvfeed"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	jsonMetaSentinel = `__crdb__`
)

type emitEntry struct {
	// row, if not the zero value, represents a changed row to be emitted.
	row encodeRow

	// resolved, if non-nil, is a guarantee for the associated
	// span that no previously unseen entries with a lower or equal updated
	// timestamp will be emitted.
	resolved *jobspb.ResolvedSpan

	// bufferGetTimestamp is the time this entry came out of the buffer.
	bufferGetTimestamp time.Time
}

// kvsToRows gets changed kvs from a closure and converts them into sql rows. It
// returns a closure that may be repeatedly called to advance the changefeed.
// The returned closure is not threadsafe.
func kvsToRows(
	codec keys.SQLCodec,
	leaseMgr *lease.Manager,
	details jobspb.ChangefeedDetails,
	inputFn func(context.Context) (kvfeed.Event, error),
) func(context.Context) ([]emitEntry, error) {
	_, withDiff := details.Opts[changefeedbase.OptDiff]
	rfCache := newRowFetcherCache(codec, leaseMgr)

	var kvs row.SpanKVFetcher
	appendEmitEntryForKV := func(
		ctx context.Context,
		output []emitEntry,
		kv roachpb.KeyValue,
		prevVal roachpb.Value,
		schemaTimestamp hlc.Timestamp,
		prevSchemaTimestamp hlc.Timestamp,
		bufferGetTimestamp time.Time,
	) ([]emitEntry, error) {

		desc, err := rfCache.TableDescForKey(ctx, kv.Key, schemaTimestamp)
		if err != nil {
			return nil, err
		}
		if _, ok := details.Targets[desc.ID]; !ok {
			// This kv is for an interleaved table that we're not watching.
			if log.V(3) {
				log.Infof(ctx, `skipping key from unwatched table %s: %s`, desc.Name, kv.Key)
			}
			return nil, nil
		}

		rf, err := rfCache.RowFetcherForTableDesc(desc)
		if err != nil {
			return nil, err
		}

		// Get new value.
		var r emitEntry
		r.bufferGetTimestamp = bufferGetTimestamp
		{
			// TODO(dan): Handle tables with multiple column families.
			// Reuse kvs to save allocations.
			kvs.KVs = kvs.KVs[:0]
			kvs.KVs = append(kvs.KVs, kv)
			if err := rf.StartScanFrom(ctx, &kvs); err != nil {
				return nil, err
			}

			r.row.datums, r.row.tableDesc, _, err = rf.NextRow(ctx)
			if err != nil {
				return nil, err
			}
			if r.row.datums == nil {
				return nil, errors.AssertionFailedf("unexpected empty datums")
			}
			r.row.datums = append(sqlbase.EncDatumRow(nil), r.row.datums...)
			r.row.deleted = rf.RowIsDeleted()
			r.row.updated = schemaTimestamp

			// Assert that we don't get a second row from the row.Fetcher. We
			// fed it a single KV, so that would be surprising.
			var nextRow emitEntry
			nextRow.row.datums, nextRow.row.tableDesc, _, err = rf.NextRow(ctx)
			if err != nil {
				return nil, err
			}
			if nextRow.row.datums != nil {
				return nil, errors.AssertionFailedf("unexpected non-empty datums")
			}
		}

		// Get prev value, if necessary.
		if withDiff {
			prevRF := rf
			if prevSchemaTimestamp != schemaTimestamp {
				// If the previous value is being interpreted under a different
				// version of the schema, fetch the correct table descriptor and
				// create a new row.Fetcher with it.
				prevDesc, err := rfCache.TableDescForKey(ctx, kv.Key, prevSchemaTimestamp)
				if err != nil {
					return nil, err
				}

				prevRF, err = rfCache.RowFetcherForTableDesc(prevDesc)
				if err != nil {
					return nil, err
				}
			}

			prevKV := roachpb.KeyValue{Key: kv.Key, Value: prevVal}
			// TODO(dan): Handle tables with multiple column families.
			// Reuse kvs to save allocations.
			kvs.KVs = kvs.KVs[:0]
			kvs.KVs = append(kvs.KVs, prevKV)
			if err := prevRF.StartScanFrom(ctx, &kvs); err != nil {
				return nil, err
			}
			r.row.prevDatums, r.row.prevTableDesc, _, err = prevRF.NextRow(ctx)
			if err != nil {
				return nil, err
			}
			if r.row.prevDatums == nil {
				return nil, errors.AssertionFailedf("unexpected empty datums")
			}
			r.row.prevDatums = append(sqlbase.EncDatumRow(nil), r.row.prevDatums...)
			r.row.prevDeleted = prevRF.RowIsDeleted()

			// Assert that we don't get a second row from the row.Fetcher. We
			// fed it a single KV, so that would be surprising.
			var nextRow emitEntry
			nextRow.row.prevDatums, nextRow.row.prevTableDesc, _, err = prevRF.NextRow(ctx)
			if err != nil {
				return nil, err
			}
			if nextRow.row.prevDatums != nil {
				return nil, errors.AssertionFailedf("unexpected non-empty datums")
			}
		}

		output = append(output, r)
		return output, nil
	}

	var output []emitEntry
	return func(ctx context.Context) ([]emitEntry, error) {
		// Reuse output to save allocations.
		output = output[:0]
		for {
			input, err := inputFn(ctx)
			if err != nil {
				return nil, err
			}
			switch input.Type() {
			case kvfeed.KVEvent:
				kv := input.KV()
				if log.V(3) {
					log.Infof(ctx, "changed key %s %s", kv.Key, kv.Value.Timestamp)
				}
				schemaTimestamp := kv.Value.Timestamp
				prevSchemaTimestamp := schemaTimestamp
				if backfillTs := input.BackfillTimestamp(); backfillTs != (hlc.Timestamp{}) {
					schemaTimestamp = backfillTs
					prevSchemaTimestamp = schemaTimestamp.Prev()
				}
				output, err = appendEmitEntryForKV(
					ctx, output, kv, input.PrevValue(),
					schemaTimestamp, prevSchemaTimestamp,
					input.BufferGetTimestamp())
				if err != nil {
					return nil, err
				}
			case kvfeed.ResolvedEvent:
				output = append(output, emitEntry{
					resolved:           input.Resolved(),
					bufferGetTimestamp: input.BufferGetTimestamp(),
				})
			}
			if output != nil {
				return output, nil
			}
		}
	}
}

// emitEntries connects to a sink, receives rows from a closure, and repeatedly
// emits them to the sink. It returns a closure that may be repeatedly called to
// advance the changefeed and which returns span-level resolved timestamp
// updates. The returned closure is not threadsafe. Note that rows read from
// `inputFn` which precede or equal the Frontier of `sf` will not be emitted
// because they're provably duplicates.
func emitEntries(
	settings *cluster.Settings,
	details jobspb.ChangefeedDetails,
	cursor hlc.Timestamp,
	sf *span.Frontier,
	encoder Encoder,
	sink Sink,
	inputFn func(context.Context) ([]emitEntry, error),
	knobs TestingKnobs,
	metrics *Metrics,
) func(context.Context) ([]jobspb.ResolvedSpan, error) {
	var scratch bufalloc.ByteAllocator
	emitRowFn := func(ctx context.Context, row encodeRow) error {
		// Ensure that row updates are strictly newer than the least resolved timestamp
		// being tracked by the local span frontier. The poller should not be forwarding
		// row updates that have timestamps less than or equal to any resolved timestamp
		// it's forwarded before.
		// TODO(dan): This should be an assertion once we're confident this can never
		// happen under any circumstance.
		if row.updated.LessEq(sf.Frontier()) && !row.updated.Equal(cursor) {
			log.Errorf(ctx, "cdc ux violation: detected timestamp %s that is less than "+
				"or equal to the local frontier %s.", cloudStorageFormatTime(row.updated),
				cloudStorageFormatTime(sf.Frontier()))
			return nil
		}
		var keyCopy, valueCopy []byte
		encodedKey, err := encoder.EncodeKey(ctx, row)
		if err != nil {
			return err
		}
		scratch, keyCopy = scratch.Copy(encodedKey, 0 /* extraCap */)
		encodedValue, err := encoder.EncodeValue(ctx, row)
		if err != nil {
			return err
		}
		scratch, valueCopy = scratch.Copy(encodedValue, 0 /* extraCap */)

		if knobs.BeforeEmitRow != nil {
			if err := knobs.BeforeEmitRow(ctx); err != nil {
				return err
			}
		}
		if err := sink.EmitRow(
			ctx, row.tableDesc, keyCopy, valueCopy, row.updated,
		); err != nil {
			return err
		}
		if log.V(3) {
			log.Infof(ctx, `row %s: %s -> %s`, row.tableDesc.Name, keyCopy, valueCopy)
		}
		return nil
	}

	var lastFlush time.Time
	// TODO(dan): We could keep these in `sf` to eliminate dups.
	var resolvedSpans []jobspb.ResolvedSpan

	return func(ctx context.Context) ([]jobspb.ResolvedSpan, error) {
		inputs, err := inputFn(ctx)
		if err != nil {
			return nil, err
		}
		var boundaryReached bool
		for _, input := range inputs {
			if input.bufferGetTimestamp == (time.Time{}) {
				// We could gracefully handle this instead of panic'ing, but
				// we'd really like to be able to reason about this data, so
				// instead we're defensive. If this is ever seen in prod without
				// breaking a unit test, then we have a pretty severe test
				// coverage issue.
				panic(`unreachable: bufferGetTimestamp is set by all codepaths`)
			}
			processingNanos := timeutil.Since(input.bufferGetTimestamp).Nanoseconds()
			metrics.ProcessingNanos.Inc(processingNanos)

			if input.row.datums != nil {
				if err := emitRowFn(ctx, input.row); err != nil {
					return nil, err
				}
			}
			if input.resolved != nil {
				boundaryReached = boundaryReached || input.resolved.BoundaryReached
				_ = sf.Forward(input.resolved.Span, input.resolved.Timestamp)
				resolvedSpans = append(resolvedSpans, *input.resolved)
			}
		}

		// If the resolved timestamp frequency is specified, use it as a rough
		// approximation of how latency-sensitive the changefeed user is. If it's
		// not, fall back to a default of 5s
		//
		// With timeBetweenFlushes and changefeedPollInterval both set to 1s, TPCC
		// was seeing about 100x more time spent emitting than flushing when tested
		// with low-latency sinks like Kafka. However when using cloud-storage
		// sinks, flushes can take much longer and trying to flush too often can
		// thus end up spending too much time flushing and not enough in emitting to
		// keep up with the feed. If a user does not specify a 'resolved' time, we
		// instead default to 5s, which is hopefully long enough to account for most
		// possible sink latencies we could see without falling behind.
		//
		// NB: As long as we periodically get new span-level resolved timestamps
		// from the poller (which should always happen, even if the watched data is
		// not changing), then this is sufficient and we don't have to do anything
		// fancy with timers.
		var timeBetweenFlushes time.Duration
		if r, ok := details.Opts[changefeedbase.OptResolvedTimestamps]; ok && r != `` {
			var err error
			if timeBetweenFlushes, err = time.ParseDuration(r); err != nil {
				return nil, err
			}
		} else {
			timeBetweenFlushes = time.Second * 5
		}
		if len(resolvedSpans) == 0 ||
			(timeutil.Since(lastFlush) < timeBetweenFlushes && !boundaryReached) {
			return nil, nil
		}

		// Make sure to flush the sink before forwarding resolved spans,
		// otherwise, we could lose buffered messages and violate the
		// at-least-once guarantee. This is also true for checkpointing the
		// resolved spans in the job progress.
		if err := sink.Flush(ctx); err != nil {
			return nil, err
		}
		lastFlush = timeutil.Now()
		if knobs.AfterSinkFlush != nil {
			if err := knobs.AfterSinkFlush(); err != nil {
				return nil, err
			}
		}
		ret := append([]jobspb.ResolvedSpan(nil), resolvedSpans...)
		resolvedSpans = resolvedSpans[:0]
		return ret, nil
	}
}

// emitResolvedTimestamp emits a changefeed-level resolved timestamp to the
// sink.
func emitResolvedTimestamp(
	ctx context.Context, encoder Encoder, sink Sink, resolved hlc.Timestamp,
) error {
	// TODO(dan): Emit more fine-grained (table level) resolved
	// timestamps.
	if err := sink.EmitResolvedTimestamp(ctx, encoder, resolved); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, `resolved %s`, resolved)
	}
	return nil
}

// createProtectedTimestampRecord will create a record to protect the spans for
// this changefeed at the resolved timestamp. The progress struct will be
// updated to refer to this new protected timestamp record.
func createProtectedTimestampRecord(
	ctx context.Context,
	pts protectedts.Storage,
	txn *kv.Txn,
	jobID int64,
	targets jobspb.ChangefeedTargets,
	resolved hlc.Timestamp,
	progress *jobspb.ChangefeedProgress,
) error {
	progress.ProtectedTimestampRecord = uuid.MakeV4()
	log.VEventf(ctx, 2, "creating protected timestamp %v at %v",
		progress.ProtectedTimestampRecord, resolved)
	spansToProtect := makeSpansToProtect(targets)
	rec := jobsprotectedts.MakeRecord(
		progress.ProtectedTimestampRecord, jobID, resolved, spansToProtect)
	return pts.Protect(ctx, txn, rec)
}

func makeSpansToProtect(targets jobspb.ChangefeedTargets) []roachpb.Span {
	// NB: We add 1 because we're also going to protect system.descriptors.
	// We protect system.descriptors because a changefeed needs all of the history
	// of table descriptors to version data.
	spansToProtect := make([]roachpb.Span, 0, len(targets)+1)
	addTablePrefix := func(id uint32) {
		tablePrefix := keys.TODOSQLCodec.TablePrefix(id)
		spansToProtect = append(spansToProtect, roachpb.Span{
			Key:    tablePrefix,
			EndKey: tablePrefix.PrefixEnd(),
		})
	}
	for t := range targets {
		addTablePrefix(uint32(t))
	}
	addTablePrefix(keys.DescriptorTableID)
	return spansToProtect
}

// initialScanFromOptions returns whether or not the options indicate the need
// for an initial scan on the first run.
func initialScanFromOptions(opts map[string]string) bool {
	_, cursor := opts[changefeedbase.OptCursor]
	_, initialScan := opts[changefeedbase.OptInitialScan]
	_, noInitialScan := opts[changefeedbase.OptNoInitialScan]
	return (cursor && initialScan) || (!cursor && !noInitialScan)
}
