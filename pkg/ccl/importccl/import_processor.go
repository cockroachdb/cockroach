// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var csvOutputTypes = []*types.T{
	types.Bytes,
	types.Bytes,
}

const readImportDataProcessorName = "readImportDataProcessor"

var importPKAdderBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.pk_buffer_size",
		"the initial size of the BulkAdder buffer handling primary index imports",
		32<<20,
	)
	return s
}()

var importPKAdderMaxBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.max_pk_buffer_size",
		"the maximum size of the BulkAdder buffer handling primary index imports",
		128<<20,
	)
	return s
}()

var importIndexAdderBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.index_buffer_size",
		"the initial size of the BulkAdder buffer handling secondary index imports",
		32<<20,
	)
	return s
}()

var importIndexAdderMaxBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.max_index_buffer_size",
		"the maximum size of the BulkAdder buffer handling secondary index imports",
		512<<20,
	)
	return s
}()

var importBufferIncrementSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.buffer_increment",
		"the size by which the BulkAdder attempts to grow its buffer before flushing",
		32<<20,
	)
	return s
}()

// ImportBufferConfigSizes determines the minimum, maximum and step size for the
// BulkAdder buffer used in import.
func importBufferConfigSizes(st *cluster.Settings, isPKAdder bool) (int64, func() int64, int64) {
	if isPKAdder {
		return importPKAdderBufferSize.Get(&st.SV),
			func() int64 { return importPKAdderMaxBufferSize.Get(&st.SV) },
			importBufferIncrementSize.Get(&st.SV)
	}
	return importIndexAdderBufferSize.Get(&st.SV),
		func() int64 { return importIndexAdderMaxBufferSize.Get(&st.SV) },
		importBufferIncrementSize.Get(&st.SV)
}

// readImportDataProcessor is a processor that does not take any inputs. It
// starts a worker goroutine in Start(), which emits progress updates over an
// internally maintained channel. Next() will read from this channel until
// exhausted and then emit the summary that the worker goroutine returns. The
// processor is built this way in order to manage parallelism internally.
type readImportDataProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.ReadImportDataSpec
	output  execinfra.RowReceiver

	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress

	seqChunkProvider *row.SeqChunkProvider

	importErr error
	summary   *roachpb.BulkOpSummary
}

var _ execinfra.Processor = &readImportDataProcessor{}
var _ execinfra.RowSource = &readImportDataProcessor{}

func newReadImportDataProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ReadImportDataSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	cp := &readImportDataProcessor{
		flowCtx: flowCtx,
		spec:    spec,
		output:  output,
		progCh:  make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress),
	}
	if err := cp.Init(cp, post, csvOutputTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			// This processor doesn't have any inputs to drain.
			InputsToDrain: nil,
		}); err != nil {
		return nil, err
	}

	// Load the import job running the import in case any of the columns have a
	// default expression which uses sequences. In this case we need to update the
	// job progress within the import processor.
	if cp.flowCtx.Cfg.JobRegistry != nil {
		cp.seqChunkProvider = &row.SeqChunkProvider{JobID: cp.spec.Progress.JobID,
			Registry: cp.flowCtx.Cfg.JobRegistry}
	}

	return cp, nil
}

// Start is part of the RowSource interface.
func (idp *readImportDataProcessor) Start(ctx context.Context) {
	ctx = idp.StartInternal(ctx, readImportDataProcessorName)
	// We don't have to worry about this go routine leaking because next we loop over progCh
	// which is closed only after the go routine returns.
	go func() {
		defer close(idp.progCh)
		idp.summary, idp.importErr = runImport(ctx, idp.flowCtx, &idp.spec, idp.progCh,
			idp.seqChunkProvider)
	}()
}

// Next is part of the RowSource interface.
func (idp *readImportDataProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if idp.State != execinfra.StateRunning {
		return nil, idp.DrainHelper()
	}

	for prog := range idp.progCh {
		p := prog
		return nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &p}
	}

	if idp.importErr != nil {
		idp.MoveToDraining(idp.importErr)
		return nil, idp.DrainHelper()
	}

	if idp.summary == nil {
		err := errors.Newf("no summary generated by %s", readImportDataProcessorName)
		idp.MoveToDraining(err)
		return nil, idp.DrainHelper()
	}

	// Once the import is done, send back to the controller the serialized
	// summary of the import operation. For more info see roachpb.BulkOpSummary.
	countsBytes, err := protoutil.Marshal(idp.summary)
	idp.MoveToDraining(err)
	if err != nil {
		return nil, idp.DrainHelper()
	}

	return rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(countsBytes))),
		rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes([]byte{}))),
	}, nil
}

func injectTimeIntoEvalCtx(ctx *tree.EvalContext, walltime int64) {
	sec := walltime / int64(time.Second)
	nsec := walltime % int64(time.Second)
	unixtime := timeutil.Unix(sec, nsec)
	ctx.StmtTimestamp = unixtime
	ctx.TxnTimestamp = unixtime
}

func makeInputConverter(
	ctx context.Context,
	spec *execinfrapb.ReadImportDataSpec,
	evalCtx *tree.EvalContext,
	kvCh chan row.KVBatch,
	seqChunkProvider *row.SeqChunkProvider,
) (inputConverter, error) {
	injectTimeIntoEvalCtx(evalCtx, spec.WalltimeNanos)
	var singleTable catalog.TableDescriptor
	var singleTableTargetCols tree.NameList
	if len(spec.Tables) == 1 {
		for _, table := range spec.Tables {
			singleTable = tabledesc.NewBuilder(table.Desc).BuildImmutableTable()
			singleTableTargetCols = make(tree.NameList, len(table.TargetCols))
			for i, colName := range table.TargetCols {
				singleTableTargetCols[i] = tree.Name(colName)
			}
		}
	}

	if format := spec.Format.Format; singleTable == nil && !isMultiTableFormat(format) {
		return nil, errors.Errorf("%s only supports reading a single, pre-specified table", format.String())
	}

	if singleTable != nil {
		if idx := catalog.FindDeletableNonPrimaryIndex(singleTable, func(idx catalog.Index) bool {
			return idx.IsPartial()
		}); idx != nil {
			return nil, unimplemented.NewWithIssue(50225, "cannot import into table with partial indexes")
		}

		// If we're using a format like CSV where data columns are not "named", and
		// therefore cannot be mapped to schema columns, then require the user to
		// use IMPORT INTO.
		//
		// We could potentially do something smarter here and check that only a
		// suffix of the columns are computed, and then expect the data file to have
		// #(visible columns) - #(computed columns).
		if len(singleTableTargetCols) == 0 && !formatHasNamedColumns(spec.Format.Format) {
			for _, col := range singleTable.VisibleColumns() {
				if col.IsComputed() {
					return nil, unimplemented.NewWithIssueDetail(56002, "import.computed",
						"to use computed columns, use IMPORT INTO")
				}
			}
		}
	}

	switch spec.Format.Format {
	case roachpb.IOFileFormat_CSV:
		isWorkload := true
		for _, file := range spec.Uri {
			if _, err := parseWorkloadConfig(file); err != nil {
				isWorkload = false
				break
			}
		}
		if isWorkload {
			return newWorkloadReader(kvCh, singleTable, evalCtx), nil
		}
		return newCSVInputReader(
			kvCh, spec.Format.Csv, spec.WalltimeNanos, int(spec.ReaderParallelism),
			singleTable, singleTableTargetCols, evalCtx, seqChunkProvider), nil
	case roachpb.IOFileFormat_MysqlOutfile:
		return newMysqloutfileReader(
			spec.Format.MysqlOut, kvCh, spec.WalltimeNanos,
			int(spec.ReaderParallelism), singleTable, singleTableTargetCols, evalCtx)
	case roachpb.IOFileFormat_Mysqldump:
		return newMysqldumpReader(ctx, kvCh, spec.WalltimeNanos, spec.Tables, evalCtx, spec.Format.MysqlDump)
	case roachpb.IOFileFormat_PgCopy:
		return newPgCopyReader(spec.Format.PgCopy, kvCh, spec.WalltimeNanos,
			int(spec.ReaderParallelism), singleTable, singleTableTargetCols, evalCtx)
	case roachpb.IOFileFormat_PgDump:
		return newPgDumpReader(ctx, int64(spec.Progress.JobID), kvCh, spec.Format.PgDump,
			spec.WalltimeNanos, spec.Tables, evalCtx)
	case roachpb.IOFileFormat_Avro:
		return newAvroInputReader(
			kvCh, singleTable, spec.Format.Avro, spec.WalltimeNanos,
			int(spec.ReaderParallelism), evalCtx)
	default:
		return nil, errors.Errorf(
			"Requested IMPORT format (%d) not supported by this node", spec.Format.Format)
	}
}

// ingestKvs drains kvs from the channel until it closes, ingesting them using
// the BulkAdder. It handles the required buffering/sorting/etc.
func ingestKvs(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	kvCh <-chan row.KVBatch,
) (*roachpb.BulkOpSummary, error) {
	ctx, span := tracing.ChildSpan(ctx, "ingestKVs")
	defer span.Finish()

	writeTS := hlc.Timestamp{WallTime: spec.WalltimeNanos}

	flushSize := func() int64 { return storageccl.MaxIngestBatchSize(flowCtx.Cfg.Settings) }

	// We create two bulk adders so as to combat the excessive flushing of small
	// SSTs which was observed when using a single adder for both primary and
	// secondary index kvs. The number of secondary index kvs are small, and so we
	// expect the indexAdder to flush much less frequently than the pkIndexAdder.
	//
	// It is highly recommended that the cluster setting controlling the max size
	// of the pkIndexAdder buffer be set below that of the indexAdder buffer.
	// Otherwise, as a consequence of filling up faster the pkIndexAdder buffer
	// will hog memory as it tries to grow more aggressively.
	minBufferSize, maxBufferSize, stepSize := importBufferConfigSizes(flowCtx.Cfg.Settings,
		true /* isPKAdder */)
	pkIndexAdder, err := flowCtx.Cfg.BulkAdder(ctx, flowCtx.Cfg.DB, writeTS, kvserverbase.BulkAdderOptions{
		Name:              "pkAdder",
		DisallowShadowing: true,
		SkipDuplicates:    true,
		MinBufferSize:     minBufferSize,
		MaxBufferSize:     maxBufferSize,
		StepBufferSize:    stepSize,
		SSTSize:           flushSize,
	})
	if err != nil {
		return nil, err
	}
	defer pkIndexAdder.Close(ctx)

	minBufferSize, maxBufferSize, stepSize = importBufferConfigSizes(flowCtx.Cfg.Settings,
		false /* isPKAdder */)
	indexAdder, err := flowCtx.Cfg.BulkAdder(ctx, flowCtx.Cfg.DB, writeTS, kvserverbase.BulkAdderOptions{
		Name:              "indexAdder",
		DisallowShadowing: true,
		SkipDuplicates:    true,
		MinBufferSize:     minBufferSize,
		MaxBufferSize:     maxBufferSize,
		StepBufferSize:    stepSize,
		SSTSize:           flushSize,
	})
	if err != nil {
		return nil, err
	}
	defer indexAdder.Close(ctx)

	// Setup progress tracking:
	//  - offsets maps source file IDs to offsets in the slices below.
	//  - writtenRow contains LastRow of batch most recently added to the buffer.
	//  - writtenFraction contains % of the input finished as of last batch.
	//  - pkFlushedRow contains `writtenRow` as of the last pk adder flush.
	//  - idxFlushedRow contains `writtenRow` as of the last index adder flush.
	// In pkFlushedRow, idxFlushedRow and writtenFaction values are written via
	// `atomic` so the progress reporting go goroutine can read them.
	writtenRow := make([]int64, len(spec.Uri))
	writtenFraction := make([]uint32, len(spec.Uri))

	pkFlushedRow := make([]int64, len(spec.Uri))
	idxFlushedRow := make([]int64, len(spec.Uri))

	// When the PK adder flushes, everything written has been flushed, so we set
	// pkFlushedRow to writtenRow. Additionally if the indexAdder is empty then we
	// can treat it as flushed as well (in case we're not adding anything to it).
	pkIndexAdder.SetOnFlush(func() {
		for i, emitted := range writtenRow {
			atomic.StoreInt64(&pkFlushedRow[i], emitted)
		}
		if indexAdder.IsEmpty() {
			for i, emitted := range writtenRow {
				atomic.StoreInt64(&idxFlushedRow[i], emitted)
			}
		}
	})
	indexAdder.SetOnFlush(func() {
		for i, emitted := range writtenRow {
			atomic.StoreInt64(&idxFlushedRow[i], emitted)
		}
	})

	// offsets maps input file ID to a slot in our progress tracking slices.
	offsets := make(map[int32]int, len(spec.Uri))
	var offset int
	for i := range spec.Uri {
		offsets[i] = offset
		offset++
	}

	pushProgress := func() {
		var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
		prog.ResumePos = make(map[int32]int64)
		prog.CompletedFraction = make(map[int32]float32)
		for file, offset := range offsets {
			pk := atomic.LoadInt64(&pkFlushedRow[offset])
			idx := atomic.LoadInt64(&idxFlushedRow[offset])
			// On resume we'll be able to skip up the last row for which both the
			// PK and index adders have flushed KVs.
			if idx > pk {
				prog.ResumePos[file] = pk
			} else {
				prog.ResumePos[file] = idx
			}
			prog.CompletedFraction[file] = math.Float32frombits(atomic.LoadUint32(&writtenFraction[offset]))
		}
		progCh <- prog
	}

	// stopProgress will be closed when there is no more progress to report.
	stopProgress := make(chan struct{})
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		tick := time.NewTicker(time.Second * 10)
		defer tick.Stop()
		done := ctx.Done()
		for {
			select {
			case <-done:
				return ctx.Err()
			case <-stopProgress:
				return nil
			case <-tick.C:
				pushProgress()
			}
		}
	})

	g.GoCtx(func(ctx context.Context) error {
		defer close(stopProgress)

		// We insert splits at every index span of the table above. Since the
		// BulkAdder is split aware when constructing SSTs, there is no risk of worst
		// case overlap behavior in the resulting AddSSTable calls.
		//
		// NB: We are getting rid of the pre-buffering stage which constructed
		// separate buckets for each table's primary data, and flushed to the
		// BulkAdder when the bucket was full. This is because, a tpcc 1k IMPORT would
		// OOM when maintaining this buffer. Two big wins we got from this
		// pre-buffering stage were:
		//
		// 1. We avoided worst case overlapping behavior in the AddSSTable calls as a
		// result of flushing keys with the same TableIDIndexID prefix, together.
		//
		// 2. Secondary index KVs which were few and filled the bucket infrequently
		// were flushed rarely, resulting in fewer L0 (and total) files.
		//
		// While we continue to achieve the first property as a result of the splits
		// mentioned above, the KVs sent to the BulkAdder are no longer grouped which
		// results in flushing a much larger number of small SSTs. This increases the
		// number of L0 (and total) files, but with a lower memory usage.
		for kvBatch := range kvCh {
			for _, kv := range kvBatch.KVs {
				_, _, indexID, indexErr := flowCtx.Codec().DecodeIndexPrefix(kv.Key)
				if indexErr != nil {
					return indexErr
				}

				// Decide which adder to send the KV to by extracting its index id.
				//
				// TODO(adityamaru): There is a potential optimization of plumbing the
				// different putters, and differentiating based on their type. It might be
				// more efficient than parsing every kv.
				if indexID == 1 {
					if err := pkIndexAdder.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
						if errors.HasType(err, (*kvserverbase.DuplicateKeyError)(nil)) {
							return errors.Wrap(err, "duplicate key in primary index")
						}
						return err
					}
				} else {
					if err := indexAdder.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
						if errors.HasType(err, (*kvserverbase.DuplicateKeyError)(nil)) {
							return errors.Wrap(err, "duplicate key in index")
						}
						return err
					}
				}
			}
			offset := offsets[kvBatch.Source]
			writtenRow[offset] = kvBatch.LastRow
			atomic.StoreUint32(&writtenFraction[offset], math.Float32bits(kvBatch.Progress))
			if flowCtx.Cfg.TestingKnobs.BulkAdderFlushesEveryBatch {
				_ = pkIndexAdder.Flush(ctx)
				_ = indexAdder.Flush(ctx)
				pushProgress()
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if err := pkIndexAdder.Flush(ctx); err != nil {
		if errors.HasType(err, (*kvserverbase.DuplicateKeyError)(nil)) {
			return nil, errors.Wrap(err, "duplicate key in primary index")
		}
		return nil, err
	}

	if err := indexAdder.Flush(ctx); err != nil {
		if errors.HasType(err, (*kvserverbase.DuplicateKeyError)(nil)) {
			return nil, errors.Wrap(err, "duplicate key in index")
		}
		return nil, err
	}

	addedSummary := pkIndexAdder.GetSummary()
	addedSummary.Add(indexAdder.GetSummary())
	return &addedSummary, nil
}

func init() {
	rowexec.NewReadImportDataProcessor = newReadImportDataProcessor
}
