// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var csvOutputTypes = []*types.T{
	types.Bytes,
	types.Bytes,
}

const readImportDataProcessorName = "readImportDataProcessor"

var progressUpdateInterval = time.Second * 10

var importPKAdderBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		settings.ApplicationLevel,
		"kv.bulk_ingest.pk_buffer_size",
		"the initial size of the BulkAdder buffer handling primary index imports",
		32<<20,
	)
	return s
}()

var importPKAdderMaxBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		settings.ApplicationLevel,
		"kv.bulk_ingest.max_pk_buffer_size",
		"the maximum size of the BulkAdder buffer handling primary index imports",
		128<<20,
	)
	return s
}()

var importIndexAdderBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		settings.ApplicationLevel,
		"kv.bulk_ingest.index_buffer_size",
		"the initial size of the BulkAdder buffer handling secondary index imports",
		32<<20,
	)
	return s
}()

var importIndexAdderMaxBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		settings.ApplicationLevel,
		"kv.bulk_ingest.max_index_buffer_size",
		"the maximum size of the BulkAdder buffer handling secondary index imports",
		512<<20,
	)
	return s
}()

var readerParallelismSetting = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.import.reader_parallelism",
	"number of parallel workers to use to convert read data for formats that support parallel conversion; 0 indicates number of cores",
	0,
	settings.NonNegativeInt,
)

// ImportBufferConfigSizes determines the minimum, maximum and step size for the
// BulkAdder buffer used in import.
func importBufferConfigSizes(st *cluster.Settings, isPKAdder bool) (int64, func() int64) {
	if isPKAdder {
		return importPKAdderBufferSize.Get(&st.SV),
			func() int64 { return importPKAdderMaxBufferSize.Get(&st.SV) }
	}
	return importIndexAdderBufferSize.Get(&st.SV),
		func() int64 { return importIndexAdderMaxBufferSize.Get(&st.SV) }
}

// readImportDataProcessor is a processor that does not take any inputs. It
// starts a worker goroutine in Start(), which emits progress updates over an
// internally maintained channel. Next() will read from this channel until
// exhausted and then emit the summary that the worker goroutine returns. The
// processor is built this way in order to manage parallelism internally.
type readImportDataProcessor struct {
	execinfra.ProcessorBase

	spec execinfrapb.ReadImportDataSpec

	cancel context.CancelFunc
	wg     ctxgroup.Group
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress

	seqChunkProvider *row.SeqChunkProvider

	importErr error
	summary   *kvpb.BulkOpSummary
}

var (
	_ execinfra.Processor = &readImportDataProcessor{}
	_ execinfra.RowSource = &readImportDataProcessor{}
)

func newReadImportDataProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ReadImportDataSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	idp := &readImportDataProcessor{
		spec:   spec,
		progCh: make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress),
	}
	if err := idp.Init(ctx, idp, post, csvOutputTypes, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			// This processor doesn't have any inputs to drain.
			InputsToDrain: nil,
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				idp.close()
				return nil
			},
		}); err != nil {
		return nil, err
	}

	// Load the import job running the import in case any of the columns have a
	// default expression which uses sequences. In this case we need to update the
	// job progress within the import processor.
	if idp.FlowCtx.Cfg.JobRegistry != nil {
		idp.seqChunkProvider = &row.SeqChunkProvider{
			JobID:    idp.spec.Progress.JobID,
			Registry: idp.FlowCtx.Cfg.JobRegistry,
			DB:       idp.FlowCtx.Cfg.DB,
		}
	}

	return idp, nil
}

// Start is part of the RowSource interface.
func (idp *readImportDataProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", idp.spec.JobID)
	ctx = idp.StartInternal(ctx, readImportDataProcessorName)

	grpCtx, cancel := context.WithCancel(ctx)
	idp.cancel = cancel
	idp.wg = ctxgroup.WithContext(grpCtx)
	idp.wg.GoCtx(func(ctx context.Context) error {
		defer close(idp.progCh)
		idp.summary, idp.importErr = runImport(ctx, idp.FlowCtx, &idp.spec, idp.progCh,
			idp.seqChunkProvider)
		return nil
	})
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
	// summary of the import operation. For more info see kvpb.BulkOpSummary.
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

func (idp *readImportDataProcessor) ConsumerClosed() {
	idp.close()
}

func (idp *readImportDataProcessor) close() {
	// ipd.Closed is set by idp.InternalClose().
	if idp.Closed {
		return
	}

	if idp.cancel != nil {
		idp.cancel()
	}
	_ = idp.wg.Wait()

	idp.InternalClose()
}

func injectTimeIntoEvalCtx(evalCtx *eval.Context, walltime int64) {
	sec := walltime / int64(time.Second)
	nsec := walltime % int64(time.Second)
	unixtime := timeutil.Unix(sec, nsec)
	evalCtx.StmtTimestamp = unixtime
	evalCtx.TxnTimestamp = unixtime
}

func makeInputConverter(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	spec *execinfrapb.ReadImportDataSpec,
	evalCtx *eval.Context,
	kvCh chan row.KVBatch,
	seqChunkProvider *row.SeqChunkProvider,
	db *kv.DB,
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

	readerParallelism := int(spec.ReaderParallelism)
	if readerParallelism <= 0 {
		readerParallelism = int(readerParallelismSetting.Get(&evalCtx.Settings.SV))
	}
	if readerParallelism <= 0 {
		readerParallelism = runtime.GOMAXPROCS(0)
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
			return newWorkloadReader(semaCtx, evalCtx, singleTable, kvCh, readerParallelism, db), nil
		}
		return newCSVInputReader(
			semaCtx, kvCh, spec.Format.Csv, spec.WalltimeNanos, readerParallelism,
			singleTable, singleTableTargetCols, evalCtx, seqChunkProvider, db), nil
	case roachpb.IOFileFormat_MysqlOutfile:
		return newMysqloutfileReader(
			semaCtx, spec.Format.MysqlOut, kvCh, spec.WalltimeNanos,
			readerParallelism, singleTable, singleTableTargetCols, evalCtx, db)
	case roachpb.IOFileFormat_Mysqldump:
		return newMysqldumpReader(ctx, semaCtx, kvCh, spec.WalltimeNanos, spec.Tables, evalCtx,
			spec.Format.MysqlDump, db)
	case roachpb.IOFileFormat_PgCopy:
		return newPgCopyReader(semaCtx, spec.Format.PgCopy, kvCh, spec.WalltimeNanos,
			readerParallelism, singleTable, singleTableTargetCols, evalCtx, db)
	case roachpb.IOFileFormat_PgDump:
		return newPgDumpReader(ctx, semaCtx, int64(spec.Progress.JobID), kvCh, spec.Format.PgDump,
			spec.WalltimeNanos, spec.Tables, evalCtx, db)
	case roachpb.IOFileFormat_Avro:
		return newAvroInputReader(
			semaCtx, kvCh, singleTable, spec.Format.Avro, spec.WalltimeNanos,
			readerParallelism, evalCtx, db)
	default:
		return nil, errors.Errorf(
			"Requested IMPORT format (%d) not supported by this node", spec.Format.Format)
	}
}

type tableAndIndex struct {
	tableID catid.DescID
	indexID catid.IndexID
}

// ingestKvs drains kvs from the channel until it closes, ingesting them using
// the BulkAdder. It handles the required buffering/sorting/etc.
func ingestKvs(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	kvCh <-chan row.KVBatch,
) (*kvpb.BulkOpSummary, error) {
	ctx, span := tracing.ChildSpan(ctx, "import-ingest-kvs")
	defer span.Finish()

	defer flowCtx.Cfg.JobRegistry.MarkAsIngesting(spec.Progress.JobID)()

	writeTS := hlc.Timestamp{WallTime: spec.WalltimeNanos}

	var pkAdderName, indexAdderName = "rows", "indexes"
	if len(spec.Tables) == 1 {
		for k := range spec.Tables {
			pkAdderName = fmt.Sprintf("%s rows", k)
			indexAdderName = fmt.Sprintf("%s indexes", k)
		}
	}

	isPK := make(map[tableAndIndex]bool, len(spec.Tables))
	for _, t := range spec.Tables {
		isPK[tableAndIndex{tableID: t.Desc.ID, indexID: t.Desc.PrimaryIndex.ID}] = true
	}

	// We create two bulk adders so as to combat the excessive flushing of small
	// SSTs which was observed when using a single adder for both primary and
	// secondary index kvs. The number of secondary index kvs are small, and so we
	// expect the indexAdder to flush much less frequently than the pkIndexAdder.
	//
	// It is highly recommended that the cluster setting controlling the max size
	// of the pkIndexAdder buffer be set below that of the indexAdder buffer.
	// Otherwise, as a consequence of filling up faster the pkIndexAdder buffer
	// will hog memory as it tries to grow more aggressively.
	minBufferSize, maxBufferSize := importBufferConfigSizes(flowCtx.Cfg.Settings,
		true /* isPKAdder */)

	var bulkAdderImportEpoch uint32
	for _, v := range spec.Tables {
		if bulkAdderImportEpoch == 0 {
			bulkAdderImportEpoch = v.Desc.ImportEpoch
		} else if bulkAdderImportEpoch != v.Desc.ImportEpoch {
			return nil, errors.AssertionFailedf("inconsistent import epoch on multi-table import")
		}
	}

	pkIndexAdder, err := flowCtx.Cfg.BulkAdder(ctx, flowCtx.Cfg.DB.KV(), writeTS, kvserverbase.BulkAdderOptions{
		Name:                     pkAdderName,
		DisallowShadowingBelow:   writeTS,
		SkipDuplicates:           true,
		MinBufferSize:            minBufferSize,
		MaxBufferSize:            maxBufferSize,
		InitialSplitsIfUnordered: int(spec.InitialSplits),
		WriteAtBatchTimestamp:    true,
		ImportEpoch:              bulkAdderImportEpoch,
	})
	if err != nil {
		return nil, err
	}
	defer pkIndexAdder.Close(ctx)

	minBufferSize, maxBufferSize = importBufferConfigSizes(flowCtx.Cfg.Settings,
		false /* isPKAdder */)
	indexAdder, err := flowCtx.Cfg.BulkAdder(ctx, flowCtx.Cfg.DB.KV(), writeTS, kvserverbase.BulkAdderOptions{
		Name:                     indexAdderName,
		DisallowShadowingBelow:   writeTS,
		SkipDuplicates:           true,
		MinBufferSize:            minBufferSize,
		MaxBufferSize:            maxBufferSize,
		InitialSplitsIfUnordered: int(spec.InitialSplits),
		WriteAtBatchTimestamp:    true,
		ImportEpoch:              bulkAdderImportEpoch,
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

	bulkSummaryMu := &struct {
		syncutil.Mutex
		summary kvpb.BulkOpSummary
	}{}

	// When the PK adder flushes, everything written has been flushed, so we set
	// pkFlushedRow to writtenRow. Additionally if the indexAdder is empty then we
	// can treat it as flushed as well (in case we're not adding anything to it).
	pkIndexAdder.SetOnFlush(func(summary kvpb.BulkOpSummary) {
		for i, emitted := range writtenRow {
			atomic.StoreInt64(&pkFlushedRow[i], emitted)
			bulkSummaryMu.Lock()
			bulkSummaryMu.summary.Add(summary)
			bulkSummaryMu.Unlock()
		}
		if indexAdder.IsEmpty() {
			for i, emitted := range writtenRow {
				atomic.StoreInt64(&idxFlushedRow[i], emitted)
			}
		}
	})
	indexAdder.SetOnFlush(func(summary kvpb.BulkOpSummary) {
		for i, emitted := range writtenRow {
			atomic.StoreInt64(&idxFlushedRow[i], emitted)
			bulkSummaryMu.Lock()
			bulkSummaryMu.summary.Add(summary)
			bulkSummaryMu.Unlock()
		}
	})

	// offsets maps input file ID to a slot in our progress tracking slices.
	offsets := make(map[int32]int, len(spec.Uri))
	var offset int
	for i := range spec.Uri {
		offsets[i] = offset
		offset++
	}

	pushProgress := func(ctx context.Context) {
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
			// Write down the summary of how much we've ingested since the last update.
			bulkSummaryMu.Lock()
			prog.BulkSummary = bulkSummaryMu.summary
			bulkSummaryMu.summary.Reset()
			bulkSummaryMu.Unlock()
		}
		select {
		case progCh <- prog:
		case <-ctx.Done():
		}

	}

	// stopProgress will be closed when there is no more progress to report.
	stopProgress := make(chan struct{})
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		tick := time.NewTicker(progressUpdateInterval)
		defer tick.Stop()
		done := ctx.Done()
		for {
			select {
			case <-done:
				return ctx.Err()
			case <-stopProgress:
				return nil
			case <-tick.C:
				pushProgress(ctx)
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
				_, tableID, indexID, indexErr := flowCtx.Codec().DecodeIndexPrefix(kv.Key)
				if indexErr != nil {
					return indexErr
				}

				// Decide which adder to send the KV to by extracting its index id.
				//
				// TODO(adityamaru): There is a potential optimization of plumbing the
				// different putters, and differentiating based on their type. It might be
				// more efficient than parsing every kv.
				if isPK[tableAndIndex{tableID: catid.DescID(tableID), indexID: catid.IndexID(indexID)}] {
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
				pushProgress(ctx)
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
