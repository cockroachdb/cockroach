// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	gogotypes "github.com/gogo/protobuf/types"
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

	spec        execinfrapb.ReadImportDataSpec
	processorID int32

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
		spec:        spec,
		processorID: processorID,
		progCh:      make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress),
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

		idp.summary, idp.importErr = runImport(ctx, idp.FlowCtx, &idp.spec, idp.processorID,
			idp.progCh, idp.seqChunkProvider)
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
		rowenc.DatumToEncDatumUnsafe(types.Bytes, tree.NewDBytes(tree.DBytes(countsBytes))),
		rowenc.DatumToEncDatumUnsafe(types.Bytes, tree.NewDBytes(tree.DBytes([]byte{}))),
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
	case roachpb.IOFileFormat_Parquet:
		return newParquetInputReader(
			semaCtx, kvCh, spec.WalltimeNanos, readerParallelism,
			desc, targetCols, evalCtx, seqChunkProvider, db, spec.Format.Parquet)
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
	processorID int32,
	tableName string,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	kvCh <-chan row.KVBatch,
) (*kvpb.BulkOpSummary, error) {
	ctx, span := tracing.ChildSpan(ctx, "import-ingest-kvs")
	defer span.Finish()
	defer flowCtx.Cfg.JobRegistry.MarkAsIngesting(spec.Progress.JobID)()

	// Protected state for progress tracking.
	progressTracker := newImportProgressTracker(flowCtx, spec)
	pushProgress := func(ctx context.Context) error {
		prog, err := progressTracker.formatProgress()
		if err != nil {
			return err
		}
		select {
		case progCh <- prog:
		case <-ctx.Done():
		}
		return nil
	}

	writeTS := hlc.Timestamp{WallTime: spec.WalltimeNanos}

	var pkIndexID descpb.IndexID
	var pkSink, indexSink bulksst.BulkSink
	var err error
	if spec.UseDistributedMerge {
		nodeID := flowCtx.NodeID.SQLInstanceID()
		prefix := fmt.Sprintf("nodelocal://%d/%s", nodeID, spec.DistributedMergeFilePrefix)
		pkSink, err = bulksst.NewSSTSink(
			ctx,
			flowCtx.Cfg.Settings,
			flowCtx.Cfg.ExternalStorageFromURI,
			flowCtx.Cfg.DB.KV().Clock(),
			prefix,
			writeTS,
			processorID,
			true, /*checkDuplicates */
		)
		if err != nil {
			return nil, err
		}
		defer pkSink.Close(ctx)
	} else {
		table := getTableFromSpec(spec)
		pkIndexID = table.Desc.PrimaryIndex.ID
		minBufferSize, maxBufferSize := importBufferConfigSizes(flowCtx.Cfg.Settings, true /* isPKAdder */)

		var adder kvserverbase.BulkAdder
		adder, err = flowCtx.Cfg.BulkAdder(ctx, flowCtx.Cfg.DB.KV(), writeTS, kvserverbase.BulkAdderOptions{
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
			return nil, err
		}
		indexSink = &bulksst.BulkAdderSink{BulkAdder: adder}
		defer indexSink.Close(ctx)
		progressTracker.registerSink(indexSink)

		adder, err = flowCtx.Cfg.BulkAdder(ctx, flowCtx.Cfg.DB.KV(), writeTS, kvserverbase.BulkAdderOptions{
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
		pkSink = &bulksst.BulkAdderSink{BulkAdder: adder}
		defer pkSink.Close(ctx)
	}
	progressTracker.registerSink(pkSink)

	flush := func() error {
		for _, s := range []bulksst.BulkSink{pkSink, indexSink} {
			if s == nil {
				continue
			}
			if err := s.Flush(ctx); err != nil {
				if errors.HasType(err, (*kvserverbase.DuplicateKeyError)(nil)) {
					return errors.Wrap(err, "duplicate key")
				}
				return err
			}
		}
		return nil
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
				if err := pushProgress(ctx); err != nil {
					return err
				}
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
				sink := pkSink
				if indexSink != nil && descpb.IndexID(indexID) != pkIndexID {
					sink = indexSink
				}
				if err := sink.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
					if errors.HasType(err, (*kvserverbase.DuplicateKeyError)(nil)) {
						return errors.Wrap(err, "duplicate key")
					}
					return err
				}
			}
			progressTracker.recordKVBatch(kvBatch)
			if flowCtx.Cfg.TestingKnobs.BulkAdderFlushesEveryBatch {
				if err := flush(); err != nil {
					log.Dev.Warningf(ctx, "%v", err)
				}
				if err := pushProgress(ctx); err != nil {
					return err
				}
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if err := flush(); err != nil {
		return nil, err
	}

	// Send final progress update to ensure all flush state is reported.
	if err := pushProgress(ctx); err != nil {
		return nil, err
	}

	finalSummary := progressTracker.fetchSummary()

	return &finalSummary, nil
}

// importProgressTracker centralizes progress tracking for import processors.
// It tracks per-file row positions and completion fractions, accumulates
// BulkOpSummary deltas from sink flushes, and collects SST manifests for
// the distributed merge path. Each registered BulkSink installs an OnFlush
// callback that updates flushedRows and accumulates summaries under the lock.
//
// Resume position is reported as the minimum flushed row across all sinks
// for each file, since a file can only be skipped on resume if all sinks
// have flushed its data.
type importProgressTracker struct {
	syncutil.Mutex

	// sinks holds references to each registered sink, used to check
	// IsEmpty() at progress reporting time.
	sinks []bulksst.BulkSink

	// flushedRows tracks the last flushed row position per file for each
	// registered sink. flushedRows[sinkIdx][fileOffset] is the row position
	// that sink has confirmed flushing for that file. Updated by OnFlush
	// callbacks.
	flushedRows [][]int64

	// summary accumulates BulkOpSummary deltas since the last call to
	// formatProgress. Reset after each progress report.
	summary kvpb.BulkOpSummary

	// totalSummary accumulates BulkOpSummary across the entire import,
	// never reset. Used to report the final result.
	totalSummary kvpb.BulkOpSummary

	// manifests collects SST file metadata from sink flushes for the
	// distributed merge path. Drained by formatProgress.
	manifests []jobspb.BulkSSTManifest

	// nodeID identifies this processor's node in progress reports.
	nodeID base.SQLInstanceID

	// offsets maps input file IDs (from spec.Uri keys) to contiguous
	// slot indices in the per-file tracking slices.
	offsets map[int32]int

	// unflushedRows tracks the most recent row position written (but not
	// necessarily flushed) per file slot. Updated by recordKVBatch.
	unflushedRows []int64

	// unflushedFraction tracks the completion fraction per file slot
	// as of the most recent KV batch. Updated by recordKVBatch.
	unflushedFraction []float32
}

func newImportProgressTracker(
	flowCtx *execinfra.FlowCtx, spec *execinfrapb.ReadImportDataSpec,
) *importProgressTracker {
	var ipt importProgressTracker
	ipt.flushedRows = make([][]int64, 0, 2)
	ipt.nodeID = flowCtx.Cfg.NodeID.SQLInstanceID()
	ipt.offsets = make(map[int32]int, len(spec.Uri))

	// ipt.offsets maps input file ID to a slot in our progress tracking slices.
	var off int
	for i := range spec.Uri {
		ipt.offsets[i] = off
		off++
	}

	ipt.unflushedRows = make([]int64, len(spec.Uri))
	ipt.unflushedFraction = make([]float32, len(spec.Uri))

	return &ipt
}

func (ipt *importProgressTracker) registerSink(sink bulksst.BulkSink) {
	ipt.Lock()
	defer ipt.Unlock()

	ipt.sinks = append(ipt.sinks, sink)
	thisSinksFlushedRows := make([]int64, len(ipt.unflushedRows))
	ipt.flushedRows = append(ipt.flushedRows, thisSinksFlushedRows)
	sink.SetOnFlush(func(summary kvpb.BulkOpSummary) {
		ipt.Lock()
		defer ipt.Unlock()

		copy(thisSinksFlushedRows, ipt.unflushedRows)
		ipt.summary.Add(summary)
		ipt.totalSummary.Add(summary)
		ipt.manifests = append(ipt.manifests, sink.ConsumeFlushManifests()...)
	})
}

func (ipt *importProgressTracker) formatProgress() (
	execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	error,
) {
	var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	prog.ResumePos = make(map[int32]int64)
	prog.CompletedFraction = make(map[int32]float32)

	ipt.Lock()
	defer ipt.Unlock()

	for file, offset := range ipt.offsets {
		// Report the resume position of the least advanced sink. If a sink's
		// buffer is empty, it has no un-flushed data and should not hold back
		// the resume position, so we use the current unflushedRows instead.
		prog.ResumePos[file] = ipt.flushedRowsForSink(0, offset)
		for s := 1; s < len(ipt.flushedRows); s++ {
			if pos := ipt.flushedRowsForSink(s, offset); prog.ResumePos[file] > pos {
				prog.ResumePos[file] = pos
			}
		}

		prog.CompletedFraction[file] = ipt.unflushedFraction[offset]
	}
	prog.BulkSummary = ipt.summary
	ipt.summary.Reset()

	prog.NodeID = ipt.nodeID
	if len(ipt.manifests) > 0 {
		manifests := append([]jobspb.BulkSSTManifest(nil), ipt.manifests...)
		ipt.manifests = nil
		mapProgress := execinfrapb.BulkMapProgress{SSTManifests: manifests}
		any, err := gogotypes.MarshalAny(&mapProgress)
		if err != nil {
			return prog, err
		}
		prog.ProgressDetails = *any
	}

	return prog, nil
}

// flushedRowsForSink returns the flushed row position for the given sink and
// file offset. If the sink's buffer is currently empty, it returns the current
// unflushedRows position instead, since an empty sink has no un-persisted data
// that could be lost on retry. Must be called with ipt.Mutex held.
func (ipt *importProgressTracker) flushedRowsForSink(sinkIdx, offset int) int64 {
	if ipt.sinks[sinkIdx].IsEmpty() {
		return ipt.unflushedRows[offset]
	}
	return ipt.flushedRows[sinkIdx][offset]
}

func (ipt *importProgressTracker) recordKVBatch(batch row.KVBatch) {
	ipt.Lock()
	defer ipt.Unlock()

	offset, ok := ipt.offsets[batch.Source]
	if !ok {
		panic(fmt.Sprintf("Unknown source %d!", batch.Source))
	}
	ipt.unflushedRows[offset] = batch.LastRow
	ipt.unflushedFraction[offset] = batch.Progress
}

func (ipt *importProgressTracker) fetchSummary() kvpb.BulkOpSummary {
	ipt.Lock()
	defer ipt.Unlock()

	return ipt.totalSummary.DeepCopy()
}

func init() {
	rowexec.NewReadImportDataProcessor = newReadImportDataProcessor
}
