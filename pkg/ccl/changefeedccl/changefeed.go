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
	"net/url"
	"time"

	"github.com/Shopify/sarama"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

type changedKVs struct {
	// sst, if non-nil, is an sstable with mvcc key values as returned by
	// ExportRequest.
	sst []byte
	// resolved, if non-zero, is a guarantee that all key values in subsequent
	// changedKVs will have an equal or higher timestamp.
	resolved hlc.Timestamp
}

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
	details jobs.ChangefeedDetails,
	progress jobs.ChangefeedProgress,
	startedCh chan<- tree.Datums,
	progressedFn func(context.Context, jobs.ProgressedFn) error,
) error {
	details, err := validateChangefeed(details)
	if err != nil {
		return err
	}

	jobProgressedFn := func(ctx context.Context, highwater hlc.Timestamp) error {
		// Some benchmarks want to skip the job progress update for a bit more
		// isolation.
		if progressedFn == nil {
			return nil
		}
		return progressedFn(ctx, func(ctx context.Context, details jobs.ProgressDetails) float32 {
			cfDetails := details.(*jobs.Progress_Changefeed).Changefeed
			cfDetails.Highwater = highwater
			// TODO(dan): Having this stuck at 0% forever is bad UX. Revisit.
			return 0.0
		})
	}

	// The changefeed flow is intentionally structured as a pull model so it's
	// easy to later make it into a DistSQL processor.
	//
	// TODO(dan): Make this into a DistSQL flow.
	changedKVsFn := exportRequestPoll(execCfg, details, progress)
	rowsFn := kvsToRows(execCfg, details, changedKVsFn)
	emitRowsFn, closeFn, err := emitRows(details, jobProgressedFn, rowsFn)
	if err != nil {
		return err
	}
	defer func() {
		if err := closeFn(); err != nil {
			log.Warningf(ctx, "failed to close changefeed sink: %+v", err)
		}
	}()

	// We abuse the job's results channel to make CREATE CHANGEFEED wait for
	// this before returning to the user to ensure the setup went okay. Job
	// resumption doesn't have the same hack, but at the moment ignores results
	// and so is currently okay. Return nil instead of anything meaningful so
	// that if we start doing anything with the results returned by resumed
	// jobs, then it breaks instead of returning nonsense.
	startedCh <- tree.Datums(nil)

	for {
		if err := emitRowsFn(ctx); err != nil {
			return err
		}
	}
}

// exportRequestPoll uses ExportRequest with the `ReturnSST` to fetch every kvs
// that changed between a set of timestamps. It returns a closure that may be
// repeatedly called to pull new changes. The returned closure is not
// threadsafe.
//
// Changes are looked up for every relevant span in a batch and buffered.
// Whenever the returned closure is called, changed kvs are returned from the
// buffer if it's non-empty. If the buffer is empty, then the closure blocks on
// a synchronous fetch to fill it. After all the changed kvs for a given fetch
// are returned, the timestamp used for the fetch is returned as resolved.
//
// The fetches are rate limited to be no more often than the
// `changefeed.experimental_poll_interval` setting.
func exportRequestPoll(
	execCfg *sql.ExecutorConfig, details jobs.ChangefeedDetails, progress jobs.ChangefeedProgress,
) func(context.Context) (changedKVs, error) {
	sender := execCfg.DB.GetSender()
	var spans []roachpb.Span
	for _, tableDesc := range details.TableDescs {
		spans = append(spans, tableDesc.PrimaryIndexSpan())
	}

	var buffer changefeedBuffer
	highwater := progress.Highwater
	return func(ctx context.Context) (changedKVs, error) {
		if ret, ok := buffer.get(); ok {
			return ret, nil
		}

		pollDuration := changefeedPollInterval.Get(&execCfg.Settings.SV)
		pollDuration = pollDuration - timeutil.Since(timeutil.Unix(0, highwater.WallTime))
		if pollDuration > 0 {
			log.VEventf(ctx, 1, `sleeping for %s`, pollDuration)
			select {
			case <-ctx.Done():
				return changedKVs{}, ctx.Err()
			case <-time.After(pollDuration):
			}
		}

		nextHighwater := execCfg.Clock.Now()
		log.VEventf(ctx, 1, `changefeed poll [%s,%s): %s`,
			highwater, nextHighwater, time.Duration(nextHighwater.WallTime-highwater.WallTime))

		// TODO(dan): Send these out in parallel.
		for _, span := range spans {
			header := roachpb.Header{Timestamp: nextHighwater}
			req := &roachpb.ExportRequest{
				RequestHeader: roachpb.RequestHeaderFromSpan(span),
				StartTime:     highwater,
				MVCCFilter:    roachpb.MVCCFilter_Latest,
				ReturnSST:     true,
			}
			res, pErr := client.SendWrappedWith(ctx, sender, header, req)
			if pErr != nil {
				return changedKVs{}, errors.Wrapf(
					pErr.GoError(), `fetching changes for [%s,%s)`, span.Key, span.EndKey)
			}
			for _, file := range res.(*roachpb.ExportResponse).Files {
				buffer.append(changedKVs{sst: file.SST})
			}
		}
		log.VEventf(ctx, 2, `poll took %s`,
			time.Duration(execCfg.Clock.Now().WallTime-nextHighwater.WallTime))

		// There is guaranteed to be at least one entry in buffer because we
		// always append the resolved timestamp.
		highwater = nextHighwater
		buffer.append(changedKVs{resolved: highwater})
		ret, _ := buffer.get()
		return ret, nil
	}
}

// kvsToRows gets changed kvs from a closure and converts them into sql rows. It
// returns a closure that may be repeatedly called to advance the changefeed.
// The returned closure is not threadsafe.
func kvsToRows(
	execCfg *sql.ExecutorConfig,
	details jobs.ChangefeedDetails,
	inputFn func(context.Context) (changedKVs, error),
) func(context.Context) ([]emitRow, error) {
	rfCache := newRowFetcherCache(execCfg.LeaseManager)

	var output []emitRow
	var kvs sqlbase.SpanKVFetcher
	var scratch bufalloc.ByteAllocator
	return func(ctx context.Context) ([]emitRow, error) {
		// Reuse output, kvs, scratch to save allocations.
		output, kvs.KVs, scratch = output[:0], kvs.KVs[:0], scratch[:0]

		input, err := inputFn(ctx)
		if err != nil {
			return nil, err
		}
		if input.sst != nil {
			it, err := engineccl.NewMemSSTIterator(input.sst, false /* verify */)
			if err != nil {
				return nil, err
			}
			defer it.Close()
			for it.Seek(engine.NilKey); ; it.Next() {
				if ok, err := it.Valid(); err != nil {
					return nil, err
				} else if !ok {
					break
				}

				unsafeKey := it.UnsafeKey()
				rf, err := rfCache.RowFetcherForKey(ctx, unsafeKey)
				if err != nil {
					return nil, err
				}
				if log.V(3) {
					log.Infof(ctx, "changed key %s", unsafeKey)
				}
				var key, value []byte
				scratch, key = scratch.Copy(unsafeKey.Key, 0 /* extraCap */)
				scratch, value = scratch.Copy(it.UnsafeValue(), 0 /* extraCap */)
				// TODO(dan): Handle tables with multiple column families.
				kvs.KVs = append(kvs.KVs, roachpb.KeyValue{
					Key: key,
					Value: roachpb.Value{
						Timestamp: unsafeKey.Timestamp,
						RawBytes:  value,
					},
				})
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
					r.rowTimestamp = unsafeKey.Timestamp
					output = append(output, r)
				}
			}
		}
		if input.resolved != (hlc.Timestamp{}) {
			output = append(output, emitRow{resolved: input.resolved})
		}
		return output, nil
	}
}

// emitRows connects to a sink, receives rows from a closure, and repeatedly
// emits them and close notifications to the sink. It returns a closure that may
// be repeatedly called to advance the changefeed. The returned closure is not
// threadsafe.
func emitRows(
	details jobs.ChangefeedDetails,
	jobProgressedFn func(context.Context, hlc.Timestamp) error,
	inputFn func(context.Context) ([]emitRow, error),
) (emitFn func(context.Context) error, closeFn func() error, err error) {
	var kafkaTopicPrefix string
	var producer sarama.SyncProducer

	sinkURI, err := url.Parse(details.SinkURI)
	if err != nil {
		return nil, nil, err
	}
	switch sinkURI.Scheme {
	case sinkSchemeKafka:
		kafkaTopicPrefix = sinkURI.Query().Get(sinkParamTopicPrefix)
		producer, err = getKafkaProducer(sinkURI.Host)
		if err != nil {
			return nil, nil, err
		}
		closeFn = producer.Close
	default:
		return nil, nil, errors.Errorf(`unsupported sink: %s`, sinkURI.Scheme)
	}

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
				if log.V(2) {
					log.Infof(ctx, `row %s -> %s`, key.String(), value.String())
				}

				message := &sarama.ProducerMessage{
					Topic: kafkaTopicPrefix + input.tableDesc.Name,
					Key:   sarama.ByteEncoder(key.Bytes()),
					Value: sarama.ByteEncoder(value.Bytes()),
				}
				if _, _, err := producer.SendMessage(message); err != nil {
					return errors.Wrapf(err, `sending message to kafka topic %s`, message.Topic)
				}
			}
			if input.resolved != (hlc.Timestamp{}) {
				if err := jobProgressedFn(ctx, input.resolved); err != nil {
					return err
				}

				// TODO(dan): HACK for testing. We call SendMessages with nil to
				// indicate to the test that a full poll finished. Figure out
				// something better.
				if err := producer.SendMessages(nil); err != nil {
					return err
				}
			}
		}
		return nil
	}, closeFn, nil
}
