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
	"net/url"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
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
	// row is the new value of a changed table row.
	row tree.Datums
	// rowTimestamp is the mvcc timestamp corresponding to the latest update in
	// `row`.
	rowTimestamp hlc.Timestamp
	// deleted is true if row is a deletion. In this case, only the primary key
	// columns are guaranteed to be set in `row`.
	deleted bool
	// tableDesc is a TableDescriptor for the table containing `row`. It's valid
	// for interpreting the row at `rowTimestamp`.
	tableDesc *sqlbase.TableDescriptor
	// resolved, if non-zero, is a guarantee that all key values in subsequent
	// changedKVs will have an equal or higher timestamp.
	resolved hlc.Timestamp
}

func runChangefeedFlow(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details jobspb.ChangefeedDetails,
	progress jobspb.Progress,
	resultsCh chan<- tree.Datums,
	progressedFn func(context.Context, jobs.HighWaterProgressedFn) error,
) error {
	// Grab the current version of each table and fail fast if any are a virtual
	// table, view, etc.
	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// Note that all targets are currently guaranteed to be tables.
		for tableID := range details.Targets {
			tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
			if err != nil {
				return err
			}
			if err := validateChangefeedTable(tableDesc); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	details, err := validateDetails(details)
	if err != nil {
		return err
	}
	var highWater hlc.Timestamp
	if h := progress.GetHighWater(); h != nil {
		highWater = *h
	}

	jobProgressedFn := func(ctx context.Context, highWater hlc.Timestamp) error {
		// Some benchmarks want to skip the job progress update for a bit more
		// isolation.
		if progressedFn == nil {
			return nil
		}
		return progressedFn(ctx, func(ctx context.Context, details jobspb.ProgressDetails) hlc.Timestamp {
			return highWater
		})
	}

	// The changefeed flow is intentionally structured as a pull model so it's
	// easy to later make it into a DistSQL processor.
	//
	// TODO(dan): Make this into a DistSQL flow.
	buf := makeBuffer()
	poller, err := makePoller(ctx, execCfg, details, highWater, buf)
	if err != nil {
		return err
	}
	rowsFn := kvsToRows(execCfg, details, buf.Get)
	emitRowsFn, closeFn, err := emitRows(details, jobProgressedFn, rowsFn, resultsCh)
	if err != nil {
		return err
	}
	defer func() {
		if err := closeFn(); err != nil {
			log.Warningf(ctx, "failed to close changefeed sink: %+v", err)
		}
	}()

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(poller.Run)
	g.GoCtx(func(ctx context.Context) error {
		for {
			if err := emitRowsFn(ctx); err != nil {
				return err
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
) func(context.Context) ([]emitRow, error) {
	rfCache := newRowFetcherCache(execCfg.LeaseManager)

	var kvs sqlbase.SpanKVFetcher
	appendEmitRowsForKV := func(
		ctx context.Context, output []emitRow, kv roachpb.KeyValue,
	) ([]emitRow, error) {
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
			var r emitRow
			r.row, r.tableDesc, _, err = rf.NextRowDecoded(ctx)
			if err != nil {
				return nil, err
			}
			if r.row == nil {
				break
			}
			r.row = append(tree.Datums(nil), r.row...)

			r.deleted = rf.RowIsDeleted()
			r.rowTimestamp = kv.Value.Timestamp
			output = append(output, r)
		}
		return output, nil
	}

	var output []emitRow
	return func(ctx context.Context) ([]emitRow, error) {
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
				output, err = appendEmitRowsForKV(ctx, output, input.kv)
				if err != nil {
					return nil, err
				}
			}
			if input.resolved != (hlc.Timestamp{}) {
				output = append(output, emitRow{resolved: input.resolved})
			}
			if output != nil {
				return output, nil
			}
		}
	}
}

// emitRows connects to a sink, receives rows from a closure, and repeatedly
// emits them and close notifications to the sink. It returns a closure that may
// be repeatedly called to advance the changefeed. The returned closure is not
// threadsafe.
func emitRows(
	details jobspb.ChangefeedDetails,
	jobProgressedFn func(context.Context, hlc.Timestamp) error,
	inputFn func(context.Context) ([]emitRow, error),
	resultsCh chan<- tree.Datums,
) (emitFn func(context.Context) error, closeFn func() error, err error) {
	var sink Sink

	sinkURI, err := url.Parse(details.SinkURI)
	if err != nil {
		return nil, nil, err
	}
	switch sinkURI.Scheme {
	case sinkSchemeChannel:
		sink = &channelSink{resultsCh: resultsCh}
		closeFn = sink.Close
	case sinkSchemeKafka:
		kafkaTopicPrefix := sinkURI.Query().Get(sinkParamTopicPrefix)
		sink, err = getKafkaSink(kafkaTopicPrefix, sinkURI.Host)
		if err != nil {
			return nil, nil, err
		}
		closeFn = sink.Close

		// We abuse the job's results channel to make CREATE CHANGEFEED wait for
		// this before returning to the user to ensure the setup went okay. Job
		// resumption doesn't have the same hack, but at the moment ignores results
		// and so is currently okay. Return nil instead of anything meaningful so
		// that if we start doing anything with the results returned by resumed
		// jobs, then it breaks instead of returning nonsense.
		resultsCh <- tree.Datums(nil)
	default:
		return nil, nil, errors.Errorf(`unsupported sink: %s`, sinkURI.Scheme)
	}

	var scratch bufalloc.ByteAllocator
	var key, value bytes.Buffer
	return func(ctx context.Context) error {
		inputs, err := inputFn(ctx)
		if err != nil {
			return err
		}
		for _, input := range inputs {
			if input.row != nil {
				key.Reset()
				value.Reset()

				keyColumns := input.tableDesc.PrimaryIndex.ColumnNames
				jsonKeyRaw := make([]interface{}, len(keyColumns))
				jsonValueRaw := make(map[string]interface{}, len(input.row))
				if _, ok := details.Opts[optTimestamps]; ok {
					jsonValueRaw[jsonMetaSentinel] = map[string]interface{}{
						`updated`: tree.TimestampToDecimal(input.rowTimestamp).Decimal.String(),
					}
				}
				for i := range input.row {
					jsonValueRaw[input.tableDesc.Columns[i].Name], err = tree.AsJSON(input.row[i])
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
				if !input.deleted && envelopeType(details.Opts[optEnvelope]) == optEnvelopeRow {
					jsonValue, err := json.MakeJSON(jsonValueRaw)
					if err != nil {
						return err
					}
					jsonValue.Format(&value)
				}

				var keyCopy, valueCopy []byte
				scratch, keyCopy = scratch.Copy(key.Bytes(), 0 /* extraCap */)
				scratch, valueCopy = scratch.Copy(value.Bytes(), 0 /* extraCap */)
				if err := sink.EmitRow(ctx, input.tableDesc.Name, keyCopy, valueCopy); err != nil {
					return err
				}
				if log.V(2) {
					log.Infof(ctx, `row %s: %s -> %s`, input.tableDesc.Name, keyCopy, valueCopy)
				}
			}
			if input.resolved != (hlc.Timestamp{}) {
				// Make sure to flush the sink before saving the job progress,
				// otherwise, we could lost any buffered messages and violate
				// the at-least-once guarantee.
				if err := sink.Flush(ctx); err != nil {
					return err
				}

				// NB: To minimize the chance that a user sees duplicates from
				// below this resolved timestamp, keep this update of the
				// high-water mark before emitting the resolved timestamp to the
				// sink.
				if err := jobProgressedFn(ctx, input.resolved); err != nil {
					return err
				}

				if _, ok := details.Opts[optTimestamps]; ok {
					resolvedMetaRaw := map[string]interface{}{
						jsonMetaSentinel: map[string]interface{}{
							`resolved`: tree.TimestampToDecimal(input.resolved).Decimal.String(),
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
			}
		}
		return nil
	}, closeFn, nil
}
