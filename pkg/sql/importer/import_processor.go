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

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
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

var distributedMergeOutputTypes = []*types.T{
	types.Bytes,
	types.Bytes,
	types.Bytes,
}

const readImportDataProcessorName = "readImportDataProcessor"

var progressUpdateInterval = time.Second * 10

var importPKAdderBufferSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"kv.bulk_ingest.pk_buffer_size",
	"the initial size of the BulkAdder buffer handling primary index imports",
	32<<20,
)

var importPKAdderMaxBufferSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"kv.bulk_ingest.max_pk_buffer_size",
	"the maximum size of the BulkAdder buffer handling primary index imports",
	128<<20,
)

var importIndexAdderBufferSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"kv.bulk_ingest.index_buffer_size",
	"the initial size of the BulkAdder buffer handling secondary index imports",
	32<<20,
)

var importIndexAdderMaxBufferSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"kv.bulk_ingest.max_index_buffer_size",
	"the maximum size of the BulkAdder buffer handling secondary index imports",
	512<<20,
)

var readerParallelismSetting = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.import.reader_parallelism",
	"number of parallel workers to use to convert read data for formats that support parallel conversion; 0 indicates number of cores",
	0,
	settings.NonNegativeInt,
)

// importBufferConfigSizes determines the minimum, maximum and step size for the
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
	files     *bulksst.SSTFiles
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
	outputTypes := csvOutputTypes
	if spec.UseDistributedMerge {
		outputTypes = distributedMergeOutputTypes
	}
	if err := idp.Init(ctx, idp, post, outputTypes, flowCtx, processorID, nil, /* memMonitor */
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
		idp.summary, idp.files, idp.importErr = runImport(ctx, idp.FlowCtx, &idp.spec, idp.progCh,
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

	// When using distributed merge, the processor will emit the SSTs and their
	// start and end keys.
	var fileDatums rowenc.EncDatumRow
	if idp.files != nil {
		bytes, err := protoutil.Marshal(idp.files)
		if err != nil {
			idp.MoveToDraining(err)
			return nil, idp.DrainHelper()
		}
		sstInfo := tree.NewDBytes(tree.DBytes(bytes))
		fileDatums = rowenc.EncDatumRow{
			rowenc.DatumToEncDatumUnsafe(types.Bytes, sstInfo),
		}
	}
	return append(rowenc.EncDatumRow{
		rowenc.DatumToEncDatumUnsafe(types.Bytes, tree.NewDBytes(tree.DBytes(countsBytes))),
		rowenc.DatumToEncDatumUnsafe(types.Bytes, tree.NewDBytes(tree.DBytes([]byte{}))),
	}, fileDatums...), nil
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
	if len(spec.Tables) > 1 {
		return nil, errors.AssertionFailedf("%s only supports reading a single, pre-specified table", spec.Format.Format.String())
	}

	injectTimeIntoEvalCtx(evalCtx, spec.WalltimeNanos)
	table := getTableFromSpec(spec)
	desc := tabledesc.NewBuilder(table.Desc).BuildImmutableTable()
	targetCols := make(tree.NameList, len(table.TargetCols))
	for i, colName := range table.TargetCols {
		targetCols[i] = tree.Name(colName)
	}

	// If we're using a format like CSV where data columns are not "named", and
	// therefore cannot be mapped to schema columns, then require the user to
	// use IMPORT INTO.
	//
	// We could potentially do something smarter here and check that only a
	// suffix of the columns are computed, and then expect the data file to have
	// #(visible columns) - #(computed columns).
	if len(targetCols) == 0 && !formatHasNamedColumns(spec.Format.Format) {
		for _, col := range desc.VisibleColumns() {
			if col.IsComputed() {
				return nil, unimplemented.NewWithIssueDetail(56002, "import.computed",
					"to use computed columns, use IMPORT INTO")
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
			return newWorkloadReader(semaCtx, evalCtx, desc, kvCh, readerParallelism, db), nil
		}
		return newCSVInputReader(
			semaCtx, kvCh, spec.Format.Csv, spec.WalltimeNanos, readerParallelism,
			desc, targetCols, evalCtx, seqChunkProvider, db), nil
	case roachpb.IOFileFormat_MysqlOutfile:
		return newMysqloutfileReader(
			semaCtx, spec.Format.MysqlOut, kvCh, spec.WalltimeNanos,
			readerParallelism, desc, targetCols, evalCtx, db)
	case roachpb.IOFileFormat_PgCopy:
		return newPgCopyReader(semaCtx, spec.Format.PgCopy, kvCh, spec.WalltimeNanos,
			readerParallelism, desc, targetCols, evalCtx, db)
	case roachpb.IOFileFormat_Avro:
		return newAvroInputReader(
			semaCtx, kvCh, desc, spec.Format.Avro, spec.WalltimeNanos,
			readerParallelism, evalCtx, db)
	default:
		return nil, errors.Errorf(
			"Requested IMPORT format (%d) not supported by this node", spec.Format.Format)
	}
}

var UseDistributedMergeForImport = settings.RegisterBoolSetting(settings.ApplicationLevel,
	"bulkio.import.distributed_merge.enabled",
	"enable distributed merge support for IMPORT",
	false)

// ingestKvs drains kvs from the channel until it closes, ingesting them using
// the BulkAdder. It handles the required buffering/sorting/etc.
func ingestKvs(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	tableName string,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	kvCh <-chan row.KVBatch,
) (*kvpb.BulkOpSummary, *bulksst.SSTFiles, error) {
	ctx, span := tracing.ChildSpan(ctx, "import-ingest-kvs")
	defer span.Finish()

	defer flowCtx.Cfg.JobRegistry.MarkAsIngesting(spec.Progress.JobID)()

	writeTS := hlc.Timestamp{WallTime: spec.WalltimeNanos}
	writtenRow := make([]int64, len(spec.Uri))

	// Setup external storage on node local for our generated SSTs.
	importAdder, err := makeIngestHelper(ctx, flowCtx, spec, writeTS, writtenRow)
	if err != nil {
		return nil, nil, err
	}
	defer importAdder.Close(ctx)

	// Setup progress tracking:
	//  - offsets maps source file IDs to offsets in the slices below.
	//  - writtenRow contains LastRow of batch most recently added to the buffer.
	//  - writtenFraction contains % of the input finished as of last batch.
	//  - pkFlushedRow contains `writtenRow` as of the last pk adder flush.
	//  - idxFlushedRow contains `writtenRow` as of the last index adder flush.
	// In pkFlushedRow, idxFlushedRow and writtenFaction values are written via
	// `atomic` so the progress reporting go goroutine can read them.
	writtenFraction := make([]uint32, len(spec.Uri))

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
			prog.ResumePos[file] = importAdder.GetResumePos(offset)
			prog.CompletedFraction[file] = math.Float32frombits(atomic.LoadUint32(&writtenFraction[offset]))
		}
		// Write down the summary of how much we've ingested since the last update.
		prog.BulkSummary = importAdder.GetProgress()
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
				_, _, indexID, indexErr := flowCtx.Codec().DecodeIndexPrefix(kv.Key)
				if indexErr != nil {
					return indexErr
				}
				importAdder.SetIndexID(catid.IndexID(indexID))
				if err := importAdder.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
					if errors.HasType(err, (*kvserverbase.DuplicateKeyError)(nil)) {
						return errors.Wrapf(err, "duplicate key in %s", importAdder.ErrTarget())
					}
					return err
				}
			}
			offset := offsets[kvBatch.Source]
			writtenRow[offset] = kvBatch.LastRow
			atomic.StoreUint32(&writtenFraction[offset], math.Float32bits(kvBatch.Progress))
			if flowCtx.Cfg.TestingKnobs.BulkAdderFlushesEveryBatch {
				_ = importAdder.Flush(ctx)
				pushProgress(ctx)
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	if err := importAdder.Flush(ctx); err != nil {
		if errors.HasType(err, (*kvserverbase.DuplicateKeyError)(nil)) {
			return nil, nil, errors.Wrapf(err, "duplicate key in %s", importAdder.ErrTarget())
		}
		return nil, nil, err
	}

	addedSummary := importAdder.GetSummary()
	return &addedSummary, importAdder.GetFileList(), nil
}

// ingestHelper abstracts the BulkAdder interfaces used by the importer so
// that we can have a unified codepath that encompasses import both with
// and without distributed merge.
type ingestHelper interface {
	kvserverbase.BulkAdder

	// Set the index to target with further operations.
	SetIndexID(indexID catid.IndexID)

	// Get the output SST file list, or nil if none.
	GetFileList() *bulksst.SSTFiles

	// Given a position in the progress tracking slice, get the resume position.
	GetResumePos(offset int) int64

	// Read the incremental progress of the import.
	GetProgress() kvpb.BulkOpSummary

	// In the event of an error in Add() or Flush(), this is the target that
	// errored.
	ErrTarget() string
}

// makeIngestHelper() creates a struct that abstracts the differences between
// import with and without distributed merge. The majority of this is making
// the dual BulkAdders of the non-distributed case look like a single
// BulkAdder. In the distributed case, we have just the single BulkAdder, so
// the implementation of the import helper is much simpler.
func makeIngestHelper(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	writeTS hlc.Timestamp,
	writtenRow []int64,
) (helper ingestHelper, err error) {
	table := getTableFromSpec(spec)

	baseProgress := &baseBulkAdderProgress{
		indexFlushedRow: make([]int64, len(spec.Uri)),
	}

	var indexAdder kvserverbase.BulkAdder
	if spec.UseDistributedMerge {
		uri := fmt.Sprintf("nodelocal://%d/job/%d/map/%s_rows/", flowCtx.Cfg.NodeID.SQLInstanceID(),
			spec.JobID, table.Desc.Name)
		rowStorage, err := flowCtx.Cfg.ExternalStorageFromURI(ctx, uri, spec.User())
		if err != nil {
			return nil, err
		}
		fileAllocator := bulksst.NewExternalFileAllocator(rowStorage, uri, flowCtx.Cfg.DB.KV().Clock())
		batcher := bulksst.NewUnsortedSSTBatcher(flowCtx.Cfg.Settings, fileAllocator)
		batcher.SetWriteTS(writeTS)

		helper = &mergeImportBulkAdder{
			Writer:                *batcher,
			rowStorage:            rowStorage,
			fileAllocator:         fileAllocator,
			baseBulkAdderProgress: baseProgress,
		}
		indexAdder = helper
	} else {
		// We create two bulk adders so as to combat the excessive flushing of small
		// SSTs which was observed when using a single adder for both primary and
		// secondary index kvs. The number of secondary index kvs are small, and so we
		// expect the indexAdder to flush much less frequently than the pkIndexAdder.
		//
		// It is highly recommended that the cluster setting controlling the max size
		// of the pkIndexAdder buffer be set below that of the indexAdder buffer.
		// Otherwise, as a consequence of filling up faster the pkIndexAdder buffer
		// will hog memory as it tries to grow more aggressively.
		minBufferSize, maxBufferSize := importBufferConfigSizes(flowCtx.Cfg.Settings, true /* isPKAdder */)

		var pkIndexAdder kvserverbase.BulkAdder
		pkIndexAdder, err = flowCtx.Cfg.BulkAdder(ctx, flowCtx.Cfg.DB.KV(), writeTS, kvserverbase.BulkAdderOptions{
			Name:                     fmt.Sprintf("%s_rows", table.Desc.Name),
			DisallowShadowingBelow:   writeTS,
			SkipDuplicates:           true,
			MinBufferSize:            minBufferSize,
			MaxBufferSize:            maxBufferSize,
			InitialSplitsIfUnordered: int(spec.InitialSplits),
			WriteAtBatchTimestamp:    true,
			ImportEpoch:              table.Desc.ImportEpoch,
		})
		if err != nil {
			return nil, err
		}

		indexAdder, err = flowCtx.Cfg.BulkAdder(ctx, flowCtx.Cfg.DB.KV(), writeTS, kvserverbase.BulkAdderOptions{
			Name:                     fmt.Sprintf("%s_indexes", table.Desc.Name),
			DisallowShadowingBelow:   writeTS,
			SkipDuplicates:           true,
			MinBufferSize:            minBufferSize,
			MaxBufferSize:            maxBufferSize,
			InitialSplitsIfUnordered: int(spec.InitialSplits),
			WriteAtBatchTimestamp:    true,
			ImportEpoch:              table.Desc.ImportEpoch,
		})
		if err != nil {
			pkIndexAdder.Close(ctx)
			return nil, err
		}

		adder := legacyImportBulkAdder{
			pkIndexID:             table.Desc.PrimaryIndex.ID,
			pkIndexAdder:          pkIndexAdder,
			indexAdder:            indexAdder,
			pkFlushedRow:          make([]int64, len(spec.Uri)),
			baseBulkAdderProgress: baseProgress,
		}

		// When the PK adder flushes, everything written has been flushed, so we set
		// pkFlushedRow to writtenRow. Additionally if the indexAdder is empty then we
		// can treat it as flushed as well (in case we're not adding anything to it).
		pkIndexAdder.SetOnFlush(func(summary kvpb.BulkOpSummary) {
			for i, emitted := range writtenRow {
				atomic.StoreInt64(&adder.pkFlushedRow[i], emitted)
			}
			baseProgress.mu.Lock()
			baseProgress.summary.Add(summary)
			baseProgress.mu.Unlock()

			if adder.indexAdder.IsEmpty() {
				for i, emitted := range writtenRow {
					atomic.StoreInt64(&baseProgress.indexFlushedRow[i], emitted)
				}
			}
		})
		helper = &adder
	}

	indexAdder.SetOnFlush(func(summary kvpb.BulkOpSummary) {
		for i, emitted := range writtenRow {
			atomic.StoreInt64(&baseProgress.indexFlushedRow[i], emitted)
		}
		baseProgress.mu.Lock()
		baseProgress.summary.Add(summary)
		baseProgress.mu.Unlock()
	})

	return helper, nil
}

// baseBulkAdderProgress provides a common implementation of GetProgress().
type baseBulkAdderProgress struct {
	indexFlushedRow []int64 // Accessed via atomics only.

	mu      syncutil.Mutex     // Protects summary.
	summary kvpb.BulkOpSummary // Incremental summary.
}

// GetProgress() implements the importHelper interface.
func (bbap *baseBulkAdderProgress) GetProgress() kvpb.BulkOpSummary {
	bbap.mu.Lock()
	summary := bbap.summary
	bbap.summary.Reset()
	bbap.mu.Unlock()
	return summary
}

// legacyImportBulkAdder implements the importHelper interface for the
// case where we're not using distributed merge and so want to have
// separate BulkAdders for the PK and non-PK indexes for performance
// reasons.
type legacyImportBulkAdder struct {
	targetID  catid.IndexID
	pkIndexID catid.IndexID

	pkIndexAdder kvserverbase.BulkAdder
	indexAdder   kvserverbase.BulkAdder

	pkFlushedRow []int64

	*baseBulkAdderProgress

	errTarget string
}

var _ ingestHelper = &legacyImportBulkAdder{}

// SetIndexID() implements the importHelper interface.
func (liba *legacyImportBulkAdder) SetIndexID(indexID catid.IndexID) {
	liba.targetID = indexID
}

// GetFileList() implements the importHelper interface.
func (liba *legacyImportBulkAdder) GetFileList() *bulksst.SSTFiles {
	return nil
}

// GetResumePos() implements the importHelper interface.
func (liba *legacyImportBulkAdder) GetResumePos(offset int) int64 {
	pk := atomic.LoadInt64(&liba.pkFlushedRow[offset])
	idx := atomic.LoadInt64(&liba.indexFlushedRow[offset])
	// On resume we'll be able to skip up the last row for which both the
	// PK and index adders have flushed KVs.
	if idx > pk {
		return pk
	} else {
		return idx
	}
}

// ErrTarget() implements the importHelper interface.
func (liba *legacyImportBulkAdder) ErrTarget() string {
	return liba.errTarget
}

// Add() implements the BulkAdder interface.
func (liba *legacyImportBulkAdder) Add(ctx context.Context, key roachpb.Key, value []byte) error {
	if liba.targetID == liba.pkIndexID {
		if err := liba.pkIndexAdder.Add(ctx, key, value); err != nil {
			liba.errTarget = "primary index"
			return err
		}
	} else {
		if err := liba.indexAdder.Add(ctx, key, value); err != nil {
			liba.errTarget = "index"
			return err
		}
	}
	return nil
}

// Flush() implements the BulkAdder interface.
func (liba *legacyImportBulkAdder) Flush(ctx context.Context) error {
	if err := liba.pkIndexAdder.Flush(ctx); err != nil {
		liba.errTarget = "primary index"
		return err
	}

	if err := liba.indexAdder.Flush(ctx); err != nil {
		liba.errTarget = "index"
		return err
	}

	return nil
}

// IsEmpty() implements the BulkAdder interface.
func (liba *legacyImportBulkAdder) IsEmpty() bool {
	return liba.pkIndexAdder.IsEmpty() && liba.indexAdder.IsEmpty()
}

// CurrentBufferFill() implements the BulkAdder interface.
func (liba *legacyImportBulkAdder) CurrentBufferFill() float32 {
	panic("unimplemented")
}

// GetSummary() implements the BulkAdder interface.
func (liba *legacyImportBulkAdder) GetSummary() kvpb.BulkOpSummary {
	summary := liba.pkIndexAdder.GetSummary()
	summary.Add(liba.indexAdder.GetSummary())
	return summary
}

// Close() implements the BulkAdder interface.
func (liba *legacyImportBulkAdder) Close(ctx context.Context) {
	liba.pkIndexAdder.Close(ctx)
	liba.indexAdder.Close(ctx)
}

// SetOnFlush() implements the BulkAdder interface.
func (liba *legacyImportBulkAdder) SetOnFlush(func(summary kvpb.BulkOpSummary)) {
	panic("unimplemented")
}

type mergeImportBulkAdder struct {
	bulksst.Writer

	rowStorage    cloud.ExternalStorage
	fileAllocator bulksst.FileAllocator

	*baseBulkAdderProgress
}

var _ ingestHelper = &mergeImportBulkAdder{}

// SetIndexID() implements the importHelper interface.
func (miba *mergeImportBulkAdder) SetIndexID(_ catid.IndexID) {}

// GetFileList() implements the importHelper interface.
func (miba *mergeImportBulkAdder) GetFileList() *bulksst.SSTFiles {
	return miba.fileAllocator.GetFileList()
}

// GetResumePos() implements the importHelper interface.
func (miba *mergeImportBulkAdder) GetResumePos(offset int) int64 {
	return atomic.LoadInt64(&miba.indexFlushedRow[offset])
}

// ErrTarget() implements the importHelper interface.
func (miba *mergeImportBulkAdder) ErrTarget() string {
	return "index"
}

func init() {
	rowexec.NewReadImportDataProcessor = newReadImportDataProcessor
}
