// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var changefeedPollInterval = settings.RegisterDurationSetting(
	"changefeed.experimental_poll_interval",
	"polling interval for the prototype changefeed implementation",
	1*time.Second,
)

func init() {
	changefeedPollInterval.Hide()
}

const (
	jsonMetaSentinel = `__crdb__`
)

type emitRow struct {
	// datums is the new value of a changed table row.
	datums tree.Datums
	// timestamp is the mvcc timestamp corresponding to the latest update in
	// `row`.
	timestamp hlc.Timestamp
	// deleted is true if row is a deletion. In this case, only the primary
	// key columns are guaranteed to be set in `datums`.
	deleted bool
	// tableDesc is a TableDescriptor for the table containing `datums`.
	// It's valid for interpreting the row at `timestamp`.
	tableDesc *sqlbase.TableDescriptor
}

type emitEntry struct {
	// row, if datums is non-nil, represents a changed row to be emitted.
	row emitRow

	// resolved, if non-nil, is a guarantee for the associated
	// span that no previously unseen entries with a lower or equal updated
	// timestamp will be emitted.
	resolved *jobspb.ResolvedSpan
}

func runChangefeedFlow(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details jobspb.ChangefeedDetails,
	progress jobspb.Progress,
	resultsCh chan<- tree.Datums,
	progressedFn func(context.Context, jobs.HighWaterProgressedFn) error,
) error {
	details, err := validateDetails(details)
	if err != nil {
		return err
	}
	var highWater hlc.Timestamp
	if h := progress.GetHighWater(); h != nil {
		highWater = *h
	}
	trackedSpans, err := fetchSpansForTargets(ctx, execCfg.DB, details.Targets, highWater)
	if err != nil {
		return err
	}

	sink, err := getSink(details.SinkURI, resultsCh)
	if err != nil {
		return err
	}
	defer func() {
		if err := sink.Close(); err != nil {
			log.Warningf(ctx, "failed to close changefeed sink: %+v", err)
		}
	}()

	if _, ok := sink.(*channelSink); !ok {
		// For every sink but channelSink (which uses resultsCh to stream the
		// changes), we abuse the job's results channel to make CREATE
		// CHANGEFEED wait for this before returning to the user to ensure the
		// setup went okay. Job resumption doesn't have the same hack, but at
		// the moment ignores results and so is currently okay. Return nil
		// instead of anything meaningful so that if we start doing anything
		// with the results returned by resumed jobs, then it breaks instead of
		// returning nonsense.
		resultsCh <- tree.Datums(nil)
	}

	// The changefeed flow is intentionally structured as a pull model so it's
	// easy to later make it into a DistSQL processor.
	//
	// TODO(dan): Make this into a DistSQL flow.
	buf := makeBuffer()
	poller := makePoller(execCfg, details, trackedSpans, highWater, buf)
	rowsFn := kvsToRows(execCfg, details, buf.Get)
	emitEntriesFn := emitEntries(details, sink, rowsFn)
	if err != nil {
		return err
	}

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(poller.Run)

	sf := makeSpanFrontier(trackedSpans...)
	g.GoCtx(func(ctx context.Context) error {
		for {
			resolvedSpans, err := emitEntriesFn(ctx)
			if err != nil {
				return err
			}
			newFrontier := false
			for _, resolvedSpan := range resolvedSpans {
				if sf.Forward(resolvedSpan.Span, resolvedSpan.Timestamp) {
					newFrontier = true
				}
			}
			if newFrontier {
				if err := emitResolvedTimestamp(ctx, details, sink, progressedFn, sf); err != nil {
					return err
				}
			}
		}
	})

	return g.Wait()
}

// kvsToRows gets changed kvs from a closure and converts them into sql rows. It
// returns a closure that may be repeatedly called to advance the changefeed.
// The returned closure is not threadsafe.
func kvsToRows(
	execCfg *sql.ExecutorConfig,
	details jobspb.ChangefeedDetails,
	inputFn func(context.Context) (bufferEntry, error),
) func(context.Context) ([]emitEntry, error) {
	rfCache := newRowFetcherCache(execCfg.LeaseManager)

	var kvs sqlbase.SpanKVFetcher
	appendEmitEntryForKV := func(
		ctx context.Context, output []emitEntry, kv roachpb.KeyValue,
	) ([]emitEntry, error) {
		// Reuse kvs to save allocations.
		kvs.KVs = kvs.KVs[:0]

		desc, err := rfCache.TableDescForKey(ctx, kv.Key, kv.Value.Timestamp)
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
		// TODO(dan): Handle tables with multiple column families.
		kvs.KVs = append(kvs.KVs, kv)
		if err := rf.StartScanFrom(ctx, &kvs); err != nil {
			return nil, err
		}

		for {
			var r emitEntry
			r.row.datums, r.row.tableDesc, _, err = rf.NextRowDecoded(ctx)
			if err != nil {
				return nil, err
			}
			if r.row.datums == nil {
				break
			}
			r.row.datums = append(tree.Datums(nil), r.row.datums...)

			r.row.deleted = rf.RowIsDeleted()
			r.row.timestamp = kv.Value.Timestamp
			output = append(output, r)
		}
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
			if input.kv.Key != nil {
				if log.V(3) {
					log.Infof(ctx, "changed key %s %s", input.kv.Key, input.kv.Value.Timestamp)
				}
				output, err = appendEmitEntryForKV(ctx, output, input.kv)
				if err != nil {
					return nil, err
				}
			}
			if input.resolved != nil {
				output = append(output, emitEntry{resolved: input.resolved})
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
// updates. The returned closure is not threadsafe.
func emitEntries(
	details jobspb.ChangefeedDetails, sink Sink, inputFn func(context.Context) ([]emitEntry, error),
) func(context.Context) ([]jobspb.ResolvedSpan, error) {
	var scratch bufalloc.ByteAllocator
	var key, value bytes.Buffer
	emitRowFn := func(ctx context.Context, row emitRow) error {
		key.Reset()
		value.Reset()

		keyColumns := row.tableDesc.PrimaryIndex.ColumnNames
		jsonKeyRaw := make([]interface{}, len(keyColumns))
		jsonValueRaw := make(map[string]interface{}, len(row.datums))
		if _, ok := details.Opts[optTimestamps]; ok {
			jsonValueRaw[jsonMetaSentinel] = map[string]interface{}{
				`updated`: tree.TimestampToDecimal(row.timestamp).Decimal.String(),
			}
		}
		for i := range row.datums {
			var err error
			jsonValueRaw[row.tableDesc.Columns[i].Name], err = tree.AsJSON(row.datums[i])
			if err != nil {
				return err
			}
		}
		for i, columnName := range keyColumns {
			jsonKeyRaw[i] = jsonValueRaw[columnName]
		}

		jsonKey, err := json.MakeJSON(jsonKeyRaw)
		if err != nil {
			return err
		}
		jsonKey.Format(&key)
		if !row.deleted && envelopeType(details.Opts[optEnvelope]) == optEnvelopeRow {
			jsonValue, err := json.MakeJSON(jsonValueRaw)
			if err != nil {
				return err
			}
			jsonValue.Format(&value)
		}

		var keyCopy, valueCopy []byte
		scratch, keyCopy = scratch.Copy(key.Bytes(), 0 /* extraCap */)
		scratch, valueCopy = scratch.Copy(value.Bytes(), 0 /* extraCap */)
		if err := sink.EmitRow(ctx, row.tableDesc.Name, keyCopy, valueCopy); err != nil {
			return err
		}
		if log.V(3) {
			log.Infof(ctx, `row %s: %s -> %s`, row.tableDesc.Name, keyCopy, valueCopy)
		}
		return nil
	}

	return func(ctx context.Context) ([]jobspb.ResolvedSpan, error) {
		inputs, err := inputFn(ctx)
		if err != nil {
			return nil, err
		}
		var resolvedSpans []jobspb.ResolvedSpan
		for _, input := range inputs {
			if input.row.datums != nil {
				if err := emitRowFn(ctx, input.row); err != nil {
					return nil, err
				}
			}
			if input.resolved != nil {
				resolvedSpans = append(resolvedSpans, *input.resolved)
			}
		}
		if len(resolvedSpans) > 0 {
			// Make sure to flush the sink before forwarding resolved spans,
			// otherwise, we could lose buffered messages and violate the
			// at-least-once guarantee. This is also true for checkpointing the
			// resolved spans in the job progress.
			//
			// TODO(dan): We'll probably want some rate limiting on these
			// flushes.
			if err := sink.Flush(ctx); err != nil {
				return nil, err
			}
		}
		return resolvedSpans, nil
	}
}

// emitResolvedTimestamp emits a changefeed-level resolved timestamp to the sink
// and checkpoints it and the span-level resolved timestamps to the job record.
func emitResolvedTimestamp(
	ctx context.Context,
	details jobspb.ChangefeedDetails,
	sink Sink,
	jobProgressedFn func(context.Context, jobs.HighWaterProgressedFn) error,
	sf *spanFrontier,
) error {
	resolved := sf.Frontier()
	var resolvedSpans []jobspb.ResolvedSpan
	sf.Entries(func(span roachpb.Span, ts hlc.Timestamp) {
		resolvedSpans = append(resolvedSpans, jobspb.ResolvedSpan{
			Span: span, Timestamp: ts,
		})
	})

	// Some benchmarks want to skip the job progress update for a bit more
	// isolation.
	//
	// NB: To minimize the chance that a user sees duplicates from below
	// this resolved timestamp, keep this update of the high-water mark
	// before emitting the resolved timestamp to the sink.
	if jobProgressedFn != nil {
		progressedClosure := func(ctx context.Context, d jobspb.ProgressDetails) hlc.Timestamp {
			d.(*jobspb.Progress_Changefeed).Changefeed.ResolvedSpans = resolvedSpans
			return resolved
		}
		if err := jobProgressedFn(ctx, progressedClosure); err != nil {
			return err
		}
	}

	if _, ok := details.Opts[optTimestamps]; ok {
		resolvedMetaRaw := map[string]interface{}{
			jsonMetaSentinel: map[string]interface{}{
				`resolved`: tree.TimestampToDecimal(resolved).Decimal.String(),
			},
		}
		resolvedMeta, err := gojson.Marshal(resolvedMetaRaw)
		if err != nil {
			return err
		}

		// TODO(dan): Emit more fine-grained (table level) resolved
		// timestamps.
		if err := sink.EmitResolvedTimestamp(ctx, resolvedMeta); err != nil {
			return err
		}
	}
	if log.V(2) {
		log.Infof(ctx, `resolved %s`, resolved)
	}
	return nil
}
