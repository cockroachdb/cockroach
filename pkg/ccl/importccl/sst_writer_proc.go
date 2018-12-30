// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/bulk"
	"github.com/cockroachdb/cockroach/pkg/storage/diskmap"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

var sstOutputTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_STRING},
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
}

func newSSTWriterProcessor(
	flowCtx *distsqlrun.FlowCtx,
	processorID int32,
	spec distsqlpb.SSTWriterSpec,
	input distsqlrun.RowSource,
	output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	sp := &sstWriter{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		input:       input,
		output:      output,
		tempStorage: flowCtx.TempStorage,
		settings:    flowCtx.Settings,
		registry:    flowCtx.JobRegistry,
		progress:    spec.Progress,
		db:          flowCtx.EvalCtx.Txn.DB(),
	}
	if err := sp.out.Init(&distsqlpb.PostProcessSpec{}, sstOutputTypes, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return sp, nil
}

type sstWriter struct {
	flowCtx     *distsqlrun.FlowCtx
	processorID int32
	spec        distsqlpb.SSTWriterSpec
	input       distsqlrun.RowSource
	out         distsqlrun.ProcOutputHelper
	output      distsqlrun.RowReceiver
	tempStorage diskmap.Factory
	settings    *cluster.Settings
	registry    *jobs.Registry
	progress    distsqlpb.JobProgress
	db          *client.DB
}

var _ distsqlrun.Processor = &sstWriter{}

func (sp *sstWriter) OutputTypes() []sqlbase.ColumnType {
	return sstOutputTypes
}

func (sp *sstWriter) Run(ctx context.Context) {
	sp.input.Start(ctx)

	ctx, span := tracing.ChildSpan(ctx, "sstWriter")
	defer tracing.FinishSpan(span)

	err := func() error {
		job, err := sp.registry.LoadJob(ctx, sp.progress.JobID)
		if err != nil {
			return err
		}
		samples := job.Details().(jobspb.ImportDetails).Samples

		// Sort incoming KVs, which will be from multiple spans, into a single
		// RocksDB instance.
		types := sp.input.OutputTypes()
		input := distsqlrun.MakeNoMetadataRowSource(sp.input, sp.output)
		alloc := &sqlbase.DatumAlloc{}
		store := sp.tempStorage.NewSortedDiskMultiMap()
		defer store.Close(ctx)
		batch := store.NewBatchWriter()
		var key, val []byte
		for {
			row, err := input.NextRow()
			if err != nil {
				return err
			}
			if row == nil {
				break
			}
			if len(row) != 2 {
				return errors.Errorf("expected 2 datums, got %d", len(row))
			}
			for i, ed := range row {
				if err := ed.EnsureDecoded(&types[i], alloc); err != nil {
					return err
				}
				datum := ed.Datum.(*tree.DBytes)
				b := []byte(*datum)
				switch i {
				case 0:
					key = b
				case 1:
					val = b
				}
			}
			if err := batch.Put(key, val); err != nil {
				return err
			}
		}
		if err := batch.Close(ctx); err != nil {
			return err
		}

		// Fetch all the keys in each span and write them to storage.
		iter := store.NewIterator()
		defer iter.Close()
		iter.Rewind()
		maxSize := storageccl.MaxImportBatchSize(sp.settings)
		for i, span := range sp.spec.Spans {
			// Since we sampled the CSVs, it is possible for an SST to end up larger
			// than the max raft command size. Split them up into correctly sized chunks.
			contentCh := make(chan sstContent)
			group := ctxgroup.WithContext(ctx)
			group.GoCtx(func(ctx context.Context) error {
				defer close(contentCh)
				return makeSSTs(ctx, iter, maxSize, contentCh, sp.spec.WalltimeNanos, span.End, nil)
			})
			group.GoCtx(func(ctx context.Context) error {
				chunk := -1
				for sst := range contentCh {
					chunk++

					var checksum []byte
					name := span.Name

					if chunk > 0 {
						name = fmt.Sprintf("%d-%s", chunk, name)
					}
					end := span.End
					if sst.more {
						end = sst.span.EndKey
					}

					if sp.spec.Destination == "" {
						if err := sp.db.AdminSplit(ctx, end, end); err != nil {
							return err
						}

						log.VEventf(ctx, 1, "scattering key %s", roachpb.PrettyPrintKey(nil, end))
						scatterReq := &roachpb.AdminScatterRequest{
							RequestHeader: roachpb.RequestHeaderFromSpan(sst.span),
						}
						if _, pErr := client.SendWrapped(ctx, sp.db.NonTransactionalSender(), scatterReq); pErr != nil {
							// TODO(dan): Unfortunately, Scatter is still too unreliable to
							// fail the IMPORT when Scatter fails. I'm uncomfortable that
							// this could break entirely and not start failing the tests,
							// but on the bright side, it doesn't affect correctness, only
							// throughput.
							log.Errorf(ctx, "failed to scatter span %s: %s", roachpb.PrettyPrintKey(nil, end), pErr)
						}
						if err := bulk.AddSSTable(ctx, sp.db, sst.span.Key, sst.span.EndKey, sst.data); err != nil {
							return err
						}
					} else {
						checksum, err = storageccl.SHA512ChecksumData(sst.data)
						if err != nil {
							return err
						}
						conf, err := storageccl.ExportStorageConfFromURI(sp.spec.Destination)
						if err != nil {
							return err
						}
						es, err := storageccl.MakeExportStorage(ctx, conf, sp.settings)
						if err != nil {
							return err
						}
						err = es.WriteFile(ctx, name, bytes.NewReader(sst.data))
						es.Close()
						if err != nil {
							return err
						}
					}

					countsBytes, err := protoutil.Marshal(&sst.counts)
					if err != nil {
						return err
					}

					row := sqlbase.EncDatumRow{
						sqlbase.DatumToEncDatum(
							sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING},
							tree.NewDString(name),
						),
						sqlbase.DatumToEncDatum(
							sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
							tree.NewDBytes(tree.DBytes(countsBytes)),
						),
						sqlbase.DatumToEncDatum(
							sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
							tree.NewDBytes(tree.DBytes(checksum)),
						),
						sqlbase.DatumToEncDatum(
							sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
							tree.NewDBytes(tree.DBytes(sst.span.Key)),
						),
						sqlbase.DatumToEncDatum(
							sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
							tree.NewDBytes(tree.DBytes(end)),
						),
					}
					cs, err := sp.out.EmitRow(ctx, row)
					if err != nil {
						return err
					}
					if cs != distsqlrun.NeedMoreRows {
						return errors.New("unexpected closure of consumer")
					}
				}
				return nil
			})
			if err := group.Wait(); err != nil {
				return err
			}

			// There's no direct way to return an error from the Progressed()
			// callback when decoding the span.End key.
			var progressErr error
			if err := job.FractionProgressed(ctx, func(ctx context.Context, details jobspb.ProgressDetails) float32 {
				d := details.(*jobspb.Progress_Import).Import
				d.WriteProgress[sp.progress.Slot] = float32(i+1) / float32(len(sp.spec.Spans)) * sp.progress.Contribution

				// Fold the newly-completed span into existing progress.  Since
				// we know what all of the sampling points are and that they're
				// in sorted order, we can just find our known end-point and use
				// the previous key as the span-start.
				progressErr = func() error {
					// Binary-search for the index of span.End.
					idx := sort.Search(len(samples), func(i int) bool {
						key := roachpb.Key(samples[i])
						return key.Compare(span.End) >= 0
					})
					var finished roachpb.Span
					// Mark the processed span as done for resume. If it was the first or last
					// span, use min or max key. This is easier than trying to correctly determine
					// the table ID we imported and getting its start span because span.End
					// might be (in the case of an empty table) the start key of the next table.
					switch idx {
					case 0:
						finished = roachpb.Span{
							Key:    keys.MinKey,
							EndKey: span.End,
						}
					case len(samples):
						finished = roachpb.Span{
							Key:    samples[idx-1],
							EndKey: keys.MaxKey,
						}
					default:
						finished = roachpb.Span{
							Key:    samples[idx-1],
							EndKey: span.End,
						}
					}
					var sg roachpb.SpanGroup
					sg.Add(d.SpanProgress...)
					sg.Add(finished)
					d.SpanProgress = sg.Slice()
					return nil
				}()
				return d.Completed()
			}); err != nil {
				return err
			} else if progressErr != nil {
				return progressErr
			}
		}
		return nil
	}()
	distsqlrun.DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
}

type sstContent struct {
	data   []byte
	size   int64
	span   roachpb.Span
	more   bool
	counts roachpb.BulkOpSummary
}

const errSSTCreationMaybeDuplicateTemplate = "SST creation error at %s; this can happen when a primary or unique index has duplicate keys"

// makeSSTs creates SST files in memory of size maxSize and sent on
// contentCh. progressFn, if not nil, is periodically invoked with the number
// of KVs that have been written to SSTs and sent on contentCh. endKey,
// if not nil, will stop processing at the specified key.
func makeSSTs(
	ctx context.Context,
	it diskmap.SortedDiskMapIterator,
	sstMaxSize int64,
	contentCh chan<- sstContent,
	walltime int64,
	endKey roachpb.Key,
	progressFn func(int) error,
) error {
	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return err
	}
	defer sst.Close()

	var counts bulk.RowCounter
	var writtenKVs int
	writeSST := func(key, endKey roachpb.Key, more bool) error {
		data, err := sst.Finish()
		if err != nil {
			return err
		}
		counts.BulkOpSummary.DataSize = sst.DataSize
		sc := sstContent{
			data: data,
			size: sst.DataSize,
			span: roachpb.Span{
				Key:    key,
				EndKey: endKey,
			},
			more:   more,
			counts: counts.BulkOpSummary,
		}

		select {
		case contentCh <- sc:
		case <-ctx.Done():
			return ctx.Err()
		}
		counts.Reset()
		sst.Close()
		if progressFn != nil {
			if err := progressFn(writtenKVs); err != nil {
				return err
			}
		}
		return nil
	}

	var kv engine.MVCCKeyValue
	kv.Key.Timestamp.WallTime = walltime
	// firstKey is always the first key of the span. lastKey, if nil, means the
	// current SST hasn't yet filled up. Once the SST has filled up, lastKey is
	// set to the key at which to stop adding KVs. We have to do this because
	// all column families for a row must be in one SST and the SST may have
	// filled up with only some of the KVs from the column families being added.
	var firstKey, lastKey roachpb.Key

	if ok, err := it.Valid(); err != nil {
		return err
	} else if !ok {
		// Empty file.
		return nil
	}
	firstKey = it.Key()

	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		// Check this before setting kv.Key.Key because it is used below in the
		// final writeSST invocation.
		if endKey != nil && endKey.Compare(it.UnsafeKey()) <= 0 {
			// If we are at the end, break the loop and return the data. There is no
			// need to back up one key, because the iterator is pointing at the start
			// of the next block already, and Next won't be called until after the key
			// has been extracted again during the next call to this function.
			break
		}

		writtenKVs++

		kv.Key.Key = it.Key()
		kv.Value = it.UnsafeValue()

		if lastKey != nil {
			if kv.Key.Key.Compare(lastKey) >= 0 {
				if err := writeSST(firstKey, lastKey, true); err != nil {
					return err
				}
				firstKey = it.Key()
				lastKey = nil

				sst, err = engine.MakeRocksDBSstFileWriter()
				if err != nil {
					return err
				}
				defer sst.Close()
			}
		}

		if err := sst.Add(kv); err != nil {
			return errors.Wrapf(err, errSSTCreationMaybeDuplicateTemplate, kv.Key.Key)
		}
		if err := counts.Count(kv.Key.Key); err != nil {
			return errors.Wrapf(err, "failed to count key")
		}

		if sst.DataSize > sstMaxSize && lastKey == nil {
			// When we would like to split the file, proceed until we aren't in the
			// middle of a row. Start by finding the next safe split key.
			lastKey, err = keys.EnsureSafeSplitKey(kv.Key.Key)
			if err != nil {
				return err
			}
			lastKey = lastKey.PrefixEnd()
		}
	}

	if sst.DataSize > 0 {
		// Although we don't need to avoid row splitting here because there aren't any
		// more keys to read, we do still want to produce the same kind of lastKey
		// argument for the span as in the case above. lastKey <= the most recent
		// sst.Add call, but since we call PrefixEnd below, it will be guaranteed
		// to be > the most recent added key.
		lastKey, err = keys.EnsureSafeSplitKey(kv.Key.Key)
		if err != nil {
			return err
		}
		if err := writeSST(firstKey, lastKey.PrefixEnd(), false); err != nil {
			return err
		}
	}
	return nil
}

func init() {
	distsqlrun.NewSSTWriterProcessor = newSSTWriterProcessor
}
