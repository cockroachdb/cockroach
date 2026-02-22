// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backupsink"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	bulkutil "github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	gogotypes "github.com/gogo/protobuf/types"
)

// Progress is streamed to the coordinator through metadata.
var restoreDataOutputTypes = []*types.T{}

type restoreDataProcessor struct {
	execinfra.ProcessorBase

	spec  execinfrapb.RestoreDataSpec
	input execinfra.RowSource

	// numWorkers is the number of workers this processor should use. This
	// number is determined by the cluster setting and the amount of memory
	// available to be used by RESTORE. If the cluster setting or memory
	// allocation is updated, the job should be PAUSEd and RESUMEd for the new
	// worker count to take effect.
	numWorkers int

	// phaseGroup manages the phases of the restore:
	// 1) reading entries from the input
	// 2) ingesting the data associated with those entries in the concurrent
	// restore data workers.
	phaseGroup           ctxgroup.Group
	cancelWorkersAndWait func()

	// Metas from the input are forwarded to the output of this processor.
	metaCh chan *execinfrapb.ProducerMetadata
	// progress updates are accumulated on this channel. It is populated by the
	// concurrent workers and sent down the flow by the processor.
	progCh chan backuppb.RestoreProgress

	// Aggregator that aggregates StructuredEvents emitted in the
	// restoreDataProcessors' trace recording.
	agg      *tracing.TracingAggregator
	aggTimer timeutil.Timer

	// qp is a MemoryBackedQuotaPool that restricts the amount of memory that
	// can be used by this processor to open iterators on SSTs.
	qp *backuputils.MemoryBackedQuotaPool

	// progressMade is true if the processor has successfully processed a
	// restore span entry.
	progressMade bool
}

var (
	_ execinfra.Processor = &restoreDataProcessor{}
	_ execinfra.RowSource = &restoreDataProcessor{}
)

const restoreDataProcName = "restoreDataProcessor"

const maxConcurrentRestoreWorkers = 32

// minWorkerMemReservation is the minimum amount of memory reserved per restore
// data processor worker. It should be greater than
// sstReaderOverheadBytesPerFile and sstReaderEncryptedOverheadBytesPerFile to
// ensure that all workers at least can simultaneously process at least one
// file.
const minWorkerMemReservation = 15 << 20

var defaultNumWorkers = metamorphic.ConstantWithTestRange(
	"restore-worker-concurrency",
	func() int {
		// On low-CPU instances, a default value may still allow concurrent restore
		// workers to tie up all cores so cap default value at cores-1 when the
		// default value is higher.
		restoreWorkerCores := runtime.GOMAXPROCS(0) - 1
		if restoreWorkerCores < 1 {
			restoreWorkerCores = 1
		}
		return min(4, restoreWorkerCores)
	}(), /* defaultValue */
	1, /* metamorphic min */
	8, /* metamorphic max */
)

// TODO(pbardea): It may be worthwhile to combine this setting with the one that
// controls the number of concurrent AddSSTable requests if each restore worker
// spends all if its time sending AddSSTable requests.
//
// The maximum is not enforced since if the maximum is reduced in the future that
// may cause the cluster setting to fail.
var numRestoreWorkers = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"kv.bulk_io_write.restore_node_concurrency",
	fmt.Sprintf("the number of workers processing a restore per job per node; maximum %d",
		maxConcurrentRestoreWorkers),
	int64(defaultNumWorkers),
	settings.PositiveInt,
)

func newRestoreDataProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.RestoreDataSpec,
	post *execinfrapb.PostProcessSpec,
	input execinfra.RowSource,
) (execinfra.Processor, error) {
	rd := &restoreDataProcessor{
		input:  input,
		spec:   spec,
		progCh: make(chan backuppb.RestoreProgress, maxConcurrentRestoreWorkers),
	}

	rd.qp = backuputils.NewMemoryBackedQuotaPool(
		ctx, flowCtx.Cfg.BackupMonitor, "restore-mon", 0,
	)
	if err := rd.Init(ctx, rd, post, restoreDataOutputTypes, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{input},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				rd.ConsumerClosed()
				meta := &execinfrapb.ProducerMetadata{}
				if rd.agg != nil {
					meta = bulkutil.ConstructTracingAggregatorProducerMeta(ctx,
						rd.FlowCtx.NodeID.SQLInstanceID(), rd.FlowCtx.ID, rd.agg)
				}
				if clusterversion.V24_3.Version().LessEq(rd.spec.ResumeClusterVersion) {
					// Only send the completion message if the restore job started on a
					// 24.3 cluster or later.
					meta.BulkProcessorProgress = &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{Drained: true}
				}
				return []execinfrapb.ProducerMetadata{*meta}
			},
		}); err != nil {
		return nil, err
	}
	return rd, nil
}

// Start is part of the RowSource interface.
func (rd *restoreDataProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", rd.spec.JobID)
	rd.agg = tracing.TracingAggregatorForContext(ctx)
	// If the aggregator is nil, we do not want the timer to fire.
	if rd.agg != nil {
		rd.aggTimer.Reset(15 * time.Second)
	}

	ctx = rd.StartInternal(ctx, restoreDataProcName, rd.agg)
	rd.input.Start(ctx)

	ctx, cancel := context.WithCancel(ctx)
	rd.cancelWorkersAndWait = func() {
		defer func() {
			// A panic here will not have a useful stack trace. If the panic is an
			// error that contains a stack trace, we want those full details.
			// TODO(jeffswenson): improve panic recovery more generally.
			if p := recover(); p != nil {
				panic(fmt.Sprintf("restore worker hit panic: %+v", p))
			}
		}()
		cancel()
		_ = rd.phaseGroup.Wait()
	}

	// First we reserve minWorkerMemReservation for each restore worker, and
	// making sure that we always have enough memory for at least one worker. The
	// maximum number of workers is based on the cluster setting. If the cluster
	// setting is updated, the job should be PAUSEd and RESUMEd for the new
	// setting to take effect.
	numWorkers, err := reserveRestoreWorkerMemory(ctx, rd.FlowCtx.Cfg.Settings, rd.qp)
	if err != nil {
		log.Dev.Warningf(ctx, "cannot reserve restore worker memory: %v", err)
		rd.MoveToDraining(err)
		return
	}
	rd.numWorkers = numWorkers
	rd.metaCh = make(chan *execinfrapb.ProducerMetadata, numWorkers)

	rd.phaseGroup = ctxgroup.WithContext(ctx)
	log.Dev.Infof(ctx, "starting restore data processor with %d workers", rd.numWorkers)

	entries := make(chan execinfrapb.RestoreSpanEntry, rd.numWorkers)
	rd.phaseGroup.GoCtx(func(ctx context.Context) error {
		defer close(entries)
		return errors.Wrap(inputReader(ctx, rd.input, entries, rd.metaCh), "reading restore span entries")
	})

	rd.phaseGroup.GoCtx(func(ctx context.Context) error {
		defer close(rd.progCh)
		return errors.Wrap(rd.runRestoreWorkers(ctx, entries), "running restore workers")
	})
}

// inputReader reads the rows from its input in a single thread and converts the
// rows into either `entries` which are passed to the restore workers or
// ProducerMetadata which is passed to `Next`.
//
// The contract of Next does not guarantee that the EncDatumRow returned by Next
// remains valid after the following call to Next. This is why the input is
// consumed on a single thread, rather than consumed by each worker.
func inputReader(
	ctx context.Context,
	input execinfra.RowSource,
	entries chan execinfrapb.RestoreSpanEntry,
	metaCh chan *execinfrapb.ProducerMetadata,
) error {
	var alloc tree.DatumAlloc

	for {
		// We read rows from the SplitAndScatter processor. We expect each row to
		// contain 2 columns. The first is used to route the row to this processor,
		// and the second contains the RestoreSpanEntry that we're interested in.
		row, meta := input.Next()
		if meta != nil {
			if meta.Err != nil {
				return meta.Err
			}

			select {
			case metaCh <- meta:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		if row == nil {
			// Consumed all rows.
			return nil
		}

		if len(row) != 2 {
			return errors.New("expected input rows to have exactly 2 columns")
		}
		if err := row[1].EnsureDecoded(types.Bytes, &alloc); err != nil {
			return err
		}
		datum := row[1].Datum
		entryDatumBytes, ok := datum.(*tree.DBytes)
		if !ok {
			return errors.AssertionFailedf(`unexpected datum type %T: %+v`, datum, row)
		}

		var entry execinfrapb.RestoreSpanEntry
		if err := protoutil.Unmarshal([]byte(*entryDatumBytes), &entry); err != nil {
			return errors.Wrap(err, "un-marshaling restore span entry")
		}

		select {
		case entries <- entry:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type mergedSST struct {
	entry        execinfrapb.RestoreSpanEntry
	iter         storage.SimpleMVCCIterator
	cleanup      func()
	completeUpTo hlc.Timestamp
}

type resumeEntry struct {
	done bool
	idx  int
}

// openSSTs opens all files in entry starting from the resumeIdx and returns a
// multiplexed SST iterator over the files.
func (rd *restoreDataProcessor) openSSTs(
	ctx context.Context, entry execinfrapb.RestoreSpanEntry, resume *resumeEntry,
) (_ mergedSST, _ *resumeEntry, retErr error) {
	// TODO(msbutler): use a a map of external storage factories to avoid reopening the same dir
	// in a given restore span entry
	var dirs []cloud.ExternalStorage

	defer func() {
		if retErr != nil {
			for _, dir := range dirs {
				if err := dir.Close(); err != nil {
					log.Dev.Warningf(ctx, "close export storage failed %v", err)
				}
			}
		}
	}()

	// getIter returns a multiplexed iterator covering the currently accumulated
	// files over the channel.
	getIter := func(iter storage.SimpleMVCCIterator, dirsToSend []cloud.ExternalStorage, completeUpTo hlc.Timestamp) mergedSST {
		readAsOfIter := storage.NewReadAsOfIterator(iter, rd.spec.RestoreTime)

		cleanup := func() {
			log.Dev.VInfof(ctx, 1, "finished with and closing %d files in span %d [%s-%s)", len(entry.Files), entry.ProgressIdx, entry.Span.Key, entry.Span.EndKey)
			readAsOfIter.Close()

			for _, dir := range dirsToSend {
				if err := dir.Close(); err != nil {
					log.Dev.Warningf(ctx, "close export storage failed %v", err)
				}
			}
		}

		mSST := mergedSST{
			entry:        entry,
			iter:         readAsOfIter,
			cleanup:      cleanup,
			completeUpTo: completeUpTo,
		}

		return mSST
	}

	log.VEventf(ctx, 1, "ingesting %d files in span %d [%s-%s)", len(entry.Files), entry.ProgressIdx, entry.Span.Key, entry.Span.EndKey)

	storeFiles := make([]storage.StoreFile, 0, len(entry.Files))

	idx := 0
	if resume != nil {
		idx = resume.idx
	}

	for ; idx < len(entry.Files); idx++ {
		file := entry.Files[idx]

		log.VEventf(ctx, 2, "import file %s which starts at %s", file.Path, entry.Span.Key)
		dir, err := rd.FlowCtx.Cfg.ExternalStorage(ctx, file.Dir)
		if err != nil {
			return mergedSST{}, nil, err
		}
		dirs = append(dirs, dir)
		storeFiles = append(storeFiles, storage.StoreFile{Store: dir, FilePath: file.Path})
	}

	iterOpts := storage.IterOptions{
		RangeKeyMaskingBelow: rd.spec.RestoreTime,
		KeyTypes:             storage.IterKeyTypePointsAndRanges,
		LowerBound:           keys.LocalMax,
		UpperBound:           keys.MaxKey,
	}
	iter, err := storage.ExternalSSTReader(ctx, storeFiles, rd.spec.Encryption, iterOpts)
	if err != nil {
		return mergedSST{}, nil, err
	}

	mSST := getIter(iter, dirs, rd.spec.RestoreTime)
	res := &resumeEntry{
		idx:  idx,
		done: true,
	}
	return mSST, res, nil
}

// openSSTsForFiles opens the specified files and returns a multiplexed SST
// iterator. If emitDeletes is true, the iterator will emit tombstones instead
// of skipping them (needed when ingesting over linked layers).
func (rd *restoreDataProcessor) openSSTsForFiles(
	ctx context.Context,
	entry execinfrapb.RestoreSpanEntry,
	files []execinfrapb.RestoreFileSpec,
	emitDeletes bool,
) (_ mergedSST, retErr error) {
	var dirs []cloud.ExternalStorage

	defer func() {
		if retErr != nil {
			for _, dir := range dirs {
				if err := dir.Close(); err != nil {
					log.Dev.Warningf(ctx, "close export storage failed %v", err)
				}
			}
		}
	}()

	storeFiles := make([]storage.StoreFile, 0, len(files))
	for _, file := range files {
		dir, err := rd.FlowCtx.Cfg.ExternalStorage(ctx, file.Dir)
		if err != nil {
			return mergedSST{}, err
		}
		dirs = append(dirs, dir)
		storeFiles = append(storeFiles, storage.StoreFile{Store: dir, FilePath: file.Path})
	}

	iterOpts := storage.IterOptions{
		RangeKeyMaskingBelow: rd.spec.RestoreTime,
		KeyTypes:             storage.IterKeyTypePointsAndRanges,
		LowerBound:           keys.LocalMax,
		UpperBound:           keys.MaxKey,
	}
	iter, err := storage.ExternalSSTReader(ctx, storeFiles, rd.spec.Encryption, iterOpts)
	if err != nil {
		return mergedSST{}, err
	}

	var readAsOfIter *storage.ReadAsOfIterator
	if emitDeletes {
		readAsOfIter = storage.NewReadAsOfIteratorWithEmitDeletes(iter, rd.spec.RestoreTime)
	} else {
		readAsOfIter = storage.NewReadAsOfIterator(iter, rd.spec.RestoreTime)
	}

	cleanup := func() {
		log.Dev.VInfof(ctx, 1, "finished with and closing %d files in span %d [%s-%s)", len(files), entry.ProgressIdx, entry.Span.Key, entry.Span.EndKey)
		readAsOfIter.Close()
		for _, dir := range dirs {
			if err := dir.Close(); err != nil {
				log.Dev.Warningf(ctx, "close export storage failed %v", err)
			}
		}
	}

	return mergedSST{
		entry:        entry,
		iter:         readAsOfIter,
		cleanup:      cleanup,
		completeUpTo: rd.spec.RestoreTime,
	}, nil
}

func (rd *restoreDataProcessor) runRestoreWorkers(
	ctx context.Context, entries chan execinfrapb.RestoreSpanEntry,
) error {
	return ctxgroup.GroupWorkers(ctx, rd.numWorkers, func(ctx context.Context, worker int) error {
		ctx = logtags.AddTag(ctx, "restore-worker", worker)
		ctx, undo := pprofutil.SetProfilerLabelsFromCtxTags(ctx)
		defer undo()

		kr, err := MakeKeyRewriterFromRekeys(rd.FlowCtx.Codec(), rd.spec.TableRekeys, rd.spec.TenantRekeys,
			false /* restoreTenantFromStream */)
		if err != nil {
			return errors.Wrap(err, "creating key rewriter from rekeys")
		}

		for {
			done, err := func() (done bool, _ error) {
				entry, ok := <-entries
				if !ok {
					done = true
					return done, nil
				}

				ctx := logtags.AddTag(ctx, "restore-span", entry.ProgressIdx)
				ctx, undo := pprofutil.SetProfilerLabelsFromCtxTags(ctx)
				defer undo()
				ctx, sp := tracing.ChildSpan(ctx, "restore.processRestoreSpanEntry")
				defer sp.Finish()

				// Partition files into linkable and ingestable based on their UseLink flag.
				linkable, ingestable := partitionFilesByLinkability(entry.Files)

				if log.V(2) {
					log.Dev.Infof(ctx, "processing restore span entry %d: %d linkable files, %d ingestable files",
						entry.ProgressIdx, len(linkable), len(ingestable))
				}

				var summary kvpb.BulkOpSummary

				// First, link all linkable files.
				if len(linkable) > 0 {
					linkEntry := entry
					linkEntry.Files = linkable
					linkSummary, err := rd.linkFiles(ctx, kr, linkEntry)
					if err != nil {
						return done, errors.Wrap(err, "linking files")
					}
					summary.DataSize += linkSummary.DataSize
					for k, v := range linkSummary.EntryCounts {
						if summary.EntryCounts == nil {
							summary.EntryCounts = make(map[uint64]int64)
						}
						summary.EntryCounts[k] += v
					}
				}

				// Then, ingest files that cannot be linked.
				if len(ingestable) > 0 {
					// When there are linked files, we need to emit deletes to shadow
					// any keys in the linked layers that have been deleted.
					emitDeletes := len(linkable) > 0
					ingestEntry := entry
					ingestEntry.Files = ingestable

					sstIter, err := rd.openSSTsForFiles(ctx, ingestEntry, ingestable, emitDeletes)
					if err != nil {
						return done, errors.Wrap(err, "opening SSTs")
					}

					ingestSummary, err := rd.processRestoreSpanEntry(ctx, kr, sstIter)
					if err != nil {
						return done, errors.Wrap(err, "processing restore span entry")
					}
					summary.DataSize += ingestSummary.DataSize
					for k, v := range ingestSummary.EntryCounts {
						if summary.EntryCounts == nil {
							summary.EntryCounts = make(map[uint64]int64)
						}
						summary.EntryCounts[k] += v
					}
				}

				select {
				case rd.progCh <- makeProgressUpdate(summary, entry, rd.spec.PKIDs, rd.spec.RestoreTime):
				case <-ctx.Done():
					return done, errors.Wrap(ctx.Err(), "sending progress update")
				}
				return done, nil
			}()
			if err != nil {
				return err
			}

			if done {
				return nil
			}
		}
	})
}

// linkFiles links all files in the entry via LinkExternalSSTable instead of
// downloading and ingesting them. This is used for online restore when the
// UseLink flag is set on files.
func (rd *restoreDataProcessor) linkFiles(
	ctx context.Context, kr *KeyRewriter, entry execinfrapb.RestoreSpanEntry,
) (kvpb.BulkOpSummary, error) {
	ctx, sp := tracing.ChildSpan(ctx, "restore.linkFiles")
	defer sp.Finish()

	var summary kvpb.BulkOpSummary
	kvDB := rd.FlowCtx.Cfg.DB.KV()

	if err := assertCommonPrefix(entry.Span, entry.ElidedPrefix); err != nil {
		return summary, err
	}

	var currentLayer int32
	for i := range entry.Files {
		file := &entry.Files[i]
		if file.Layer < currentLayer {
			return summary, errors.AssertionFailedf("files not sorted by layer")
		}
		currentLayer = file.Layer
		if file.HasRangeKeys {
			return summary, errors.Wrapf(permanentRestoreError, "online restore of range keys not supported")
		}
		if err := assertCommonPrefix(file.BackupFileEntrySpan, entry.ElidedPrefix); err != nil {
			return summary, err
		}

		restoringSubspan := file.BackupFileEntrySpan.Intersect(entry.Span)
		if !restoringSubspan.Valid() {
			return summary, errors.AssertionFailedf("file %s with span %s has no overlap with restore span %s",
				file.Path,
				file.BackupFileEntrySpan,
				entry.Span,
			)
		}

		var rewriteErr error
		restoringSubspan, rewriteErr = rewriteSpan(kr, restoringSubspan.Clone(), entry.ElidedPrefix)
		if rewriteErr != nil {
			return summary, rewriteErr
		}

		log.VEventf(ctx, 2, "linking file %s (file span: %s) to span %s", file.Path, file.BackupFileEntrySpan, restoringSubspan)

		counts := file.BackupFileEntryCounts
		fileSize := file.ApproximatePhysicalSize
		if fileSize == 0 {
			fileSize = uint64(counts.DataSize)
		}
		if fileSize == 0 {
			fileSize = 16 << 20 // guess
		}

		// The synthetic prefix is computed from the REWRITTEN span key, not the
		// original file span key. This matches the old online restore path which
		// overwrites file.BackupFileEntrySpan before computing the prefix.
		syntheticPrefix, err := backupsink.ElidedPrefix(restoringSubspan.Key, entry.ElidedPrefix)
		if err != nil {
			return summary, err
		}

		fileStats := &enginepb.MVCCStats{
			ContainsEstimates: 1,
			KeyBytes:          counts.DataSize / 2,
			ValBytes:          counts.DataSize / 2,
			LiveBytes:         counts.DataSize,
			KeyCount:          counts.Rows + counts.IndexEntries,
			LiveCount:         counts.Rows + counts.IndexEntries,
		}

		var batchTimestamp hlc.Timestamp
		if writeAtBatchTS(ctx, restoringSubspan, kr.fromSystemTenant) {
			batchTimestamp = kvDB.Clock().Now()
		}

		loc := kvpb.LinkExternalSSTableRequest_ExternalFile{
			Locator:                 file.Dir.URI,
			Path:                    file.Path,
			ApproximatePhysicalSize: fileSize,
			BackingFileSize:         file.BackingFileSize,
			SyntheticPrefix:         syntheticPrefix,
			UseSyntheticSuffix:      batchTimestamp.IsSet(),
			MVCCStats:               fileStats,
		}

		if err := kvDB.LinkExternalSSTable(ctx, restoringSubspan, loc, batchTimestamp); err != nil {
			return summary, errors.Wrap(err, "linking external SSTable")
		}

		// Call testing knob after each file link, matching the old online restore path.
		if restoreKnobs, ok := rd.FlowCtx.TestingKnobs().BackupRestoreTestingKnobs.(*sql.BackupRestoreTestingKnobs); ok {
			if restoreKnobs.AfterAddRemoteSST != nil {
				if err := restoreKnobs.AfterAddRemoteSST(); err != nil {
					return summary, err
				}
			}
		}

		// Use ApproximatePhysicalSize for DataSize to match the coordinator path.
		summary.DataSize += int64(fileSize)

		// Populate EntryCounts so that countRows() can compute row counts for
		// progress tracking. We need to use a key that's in pkIDs for rows to be
		// counted correctly. Pick any key from pkIDs (the specific key doesn't
		// matter since we're just summing totals).
		if counts.Rows > 0 || counts.IndexEntries > 0 {
			if summary.EntryCounts == nil {
				summary.EntryCounts = make(map[uint64]int64)
			}
			// Use a pkID key for rows so countRows counts them as rows.
			for pkID := range rd.spec.PKIDs {
				summary.EntryCounts[pkID] += counts.Rows
				break // only need one key
			}
			// Use key 0 for index entries (won't be in pkIDs, so counted as index entries).
			if counts.IndexEntries > 0 {
				summary.EntryCounts[0] += counts.IndexEntries
			}
		}
	}

	rd.progressMade = true
	return summary, nil
}

var backupFileReadError = errors.New("error reading backup file")

func (rd *restoreDataProcessor) processRestoreSpanEntry(
	ctx context.Context, kr *KeyRewriter, sst mergedSST,
) (kvpb.BulkOpSummary, error) {
	db := rd.FlowCtx.Cfg.DB
	var summary kvpb.BulkOpSummary

	entry := sst.entry
	iter := sst.iter
	defer sst.cleanup()

	elidedPrefix, err := backupsink.ElidedPrefix(entry.Span.Key, sst.entry.ElidedPrefix)
	if err != nil {
		return summary, err
	}

	var batcher SSTBatcherExecutor
	if rd.spec.ValidateOnly {
		batcher = &sstBatcherNoop{}
	} else {
		writeAtBatchTS := writeAtBatchTS(ctx, entry.Span, kr.fromSystemTenant)

		// disallowShadowingBelow is set to an empty hlc.Timestamp in release builds
		// i.e. allow all shadowing without AddSSTable having to check for
		// overlapping keys. This is necessary since RESTORE can sometimes construct
		// SSTables that overwrite existing keys, in cases when there wasn't
		// sufficient memory to open an iterator for all files at once for a given
		// import span.
		//
		// NB: disallowShadowingBelow used to be unconditionally set to logical=1.
		// This permissive value would allow shadowing in case the RESTORE has to
		// retry ingestions but served to force evaluation of AddSSTable to check for
		// overlapping keys. It was believed that even across resumptions of a restore
		// job, `checkForKeyCollisions` would be inexpensive because of our frequent
		// job checkpointing. Further investigation in
		// https://github.com/cockroachdb/cockroach/issues/81116 revealed that our
		// progress checkpointing could significantly lag behind the spans we have
		// ingested, making a resumed restore spend a lot of time in
		// `checkForKeyCollisions` leading to severely degraded performance. We have
		// *never* seen a restore fail because of the invariant enforced by setting
		// `disallowShadowingBelow` to a non-empty value, and so we feel comfortable
		// disabling this check entirely. A future release will work on fixing our
		// progress checkpointing so that we do not have a buildup of un-checkpointed
		// work, at which point we can reassess reverting to logical=1.
		disallowShadowingBelow := hlc.Timestamp{}

		var err error
		batcher, err = bulk.MakeSSTBatcher(ctx,
			"restore",
			db.KV(),
			rd.FlowCtx.Cfg.Settings,
			disallowShadowingBelow,
			writeAtBatchTS,
			false, /* scatterSplitRanges */
			// TODO(rui): we can change this to the processor's bound account, but
			// currently there seems to be some accounting errors that will cause
			// tests to fail.
			rd.FlowCtx.Cfg.BackupMonitor.MakeConcurrentBoundAccount(),
			rd.FlowCtx.Cfg.BulkSenderLimiter,
			nil,
		)
		if err != nil {
			return summary, err
		}
	}
	defer batcher.Close(ctx)

	// Read log.V once first to avoid the vmodule mutex in the tight loop below.
	verbose := log.V(5)

	var keyScratch, valueScratch []byte

	startKeyMVCC, endKeyMVCC := storage.MVCCKey{Key: entry.Span.Key},
		storage.MVCCKey{Key: entry.Span.EndKey}

	if elidedPrefix != nil {
		startKeyMVCC.Key = bytes.TrimPrefix(startKeyMVCC.Key, elidedPrefix)
	}
	if verbose {
		log.Dev.Infof(ctx, "reading from %s to %s", startKeyMVCC, endKeyMVCC)
	}
	for iter.SeekGE(startKeyMVCC); ; iter.NextKey() {
		ok, err := iter.Valid()
		if err != nil {
			return summary, errors.Join(backupFileReadError, err)
		}
		if !ok {
			if verbose {
				log.Dev.Infof(ctx, "iterator exhausted")
			}
			break
		}

		key := iter.UnsafeKey()
		keyScratch = append(append(keyScratch[:0], elidedPrefix...), key.Key...)
		key.Key = keyScratch

		if !key.Less(endKeyMVCC) {
			if verbose {
				log.Dev.Infof(ctx, "iterator key %s exceeded end %s", key, endKeyMVCC)
			}
			break
		}

		v, err := iter.UnsafeValue()
		if err != nil {
			return summary, errors.Join(backupFileReadError, err)
		}
		valueScratch = append(valueScratch[:0], v...)
		value, err := storage.DecodeValueFromMVCCValue(valueScratch)
		if err != nil {
			return summary, errors.Join(backupFileReadError, err)
		}

		key.Key, ok, err = kr.RewriteKey(key.Key, key.Timestamp.WallTime)

		if err != nil {
			return summary, err
		}
		if !ok {
			// If the key rewriter didn't match this key, it's not data for the
			// table(s) we're interested in.
			//
			// As an example, keys from in-progress imports never get restored,
			// since the key's table gets restored to its pre-import state. Therefore,
			// we elide ingesting this key.
			if verbose {
				log.Dev.Infof(ctx, "skipping %s %s", key.Key, value.PrettyPrint())
			}
			continue
		}

		// Rewriting the key means the checksum needs to be updated.
		value.ClearChecksum()
		value.InitChecksum(key.Key)

		if verbose {
			log.Dev.Infof(ctx, "Put %s -> %s", key.Key, value.PrettyPrint())
		}

		// Using valueScratch here assumes that
		// DecodeValueFromMVCCValue, ClearChecksum, and
		// InitChecksum don't copy/reallocate the slice they
		// were given. We expect that value.ClearChecksum and
		// value.InitChecksum calls above have modified
		// valueScratch.
		if err := batcher.AddMVCCKey(ctx, key, valueScratch); err != nil {
			return summary, errors.Wrapf(err, "adding to batch: %s -> %s", key, value.PrettyPrint())
		}
	}
	// Flush out the last batch.
	if err := batcher.Flush(ctx); err != nil {
		return summary, err
	}

	if restoreKnobs, ok := rd.FlowCtx.TestingKnobs().BackupRestoreTestingKnobs.(*sql.BackupRestoreTestingKnobs); ok {
		if restoreKnobs.RunAfterProcessingRestoreSpanEntry != nil {
			if err := restoreKnobs.RunAfterProcessingRestoreSpanEntry(ctx, &entry); err != nil {
				return summary, err
			}
		}
	}

	return batcher.GetSummary(), nil
}

func makeProgressUpdate(
	summary kvpb.BulkOpSummary,
	entry execinfrapb.RestoreSpanEntry,
	pkIDs map[uint64]bool,
	completeUpTo hlc.Timestamp,
) (progDetails backuppb.RestoreProgress) {
	progDetails.Summary = countRows(summary, pkIDs)
	progDetails.ProgressIdx = entry.ProgressIdx
	progDetails.DataSpan = entry.Span
	progDetails.CompleteUpTo = completeUpTo
	return progDetails
}

// partitionFilesByLinkability splits files into those that can be linked
// (UseLink=true) and those that must be ingested (UseLink=false).
func partitionFilesByLinkability(
	files []execinfrapb.RestoreFileSpec,
) (linkable, ingestable []execinfrapb.RestoreFileSpec) {
	for i := range files {
		if files[i].UseLink {
			linkable = append(linkable, files[i])
		} else {
			ingestable = append(ingestable, files[i])
		}
	}
	return linkable, ingestable
}

// Next is part of the RowSource interface.
func (rd *restoreDataProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if rd.State != execinfra.StateRunning {
		return nil, rd.DrainHelper()
	}

	var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	select {
	case progDetails, ok := <-rd.progCh:
		if !ok {
			// Done. Check if any phase exited early with an error.
			err := rd.phaseGroup.Wait()
			rd.MoveToDraining(err)
			return nil, rd.DrainHelper()
		}

		details, err := gogotypes.MarshalAny(&progDetails)
		if err != nil {
			rd.MoveToDraining(err)
			return nil, rd.DrainHelper()
		}
		prog.ProgressDetails = *details
		rd.progressMade = true
		return nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &prog}
	case <-rd.aggTimer.C:
		rd.aggTimer.Reset(15 * time.Second)
		return nil, bulkutil.ConstructTracingAggregatorProducerMeta(rd.Ctx(),
			rd.FlowCtx.NodeID.SQLInstanceID(), rd.FlowCtx.ID, rd.agg)
	case meta := <-rd.metaCh:
		return nil, meta
	case <-rd.Ctx().Done():
		rd.MoveToDraining(rd.Ctx().Err())
		return nil, rd.DrainHelper()
	}
}

// ConsumerClosed is part of the RowSource interface.
func (rd *restoreDataProcessor) ConsumerClosed() {
	if rd.Closed {
		return
	}
	if rd.cancelWorkersAndWait != nil {
		rd.cancelWorkersAndWait()
	}

	rd.qp.Close(rd.Ctx())
	rd.aggTimer.Stop()
	rd.InternalClose()
}

func reserveRestoreWorkerMemory(
	ctx context.Context, settings *cluster.Settings, qmem *backuputils.MemoryBackedQuotaPool,
) (int, error) {
	maxRestoreWorkers := int(numRestoreWorkers.Get(&settings.SV))
	minRestoreWorkers := maxRestoreWorkers / 2
	if minRestoreWorkers < 1 {
		minRestoreWorkers = 1
	}

	numWorkers := 0
	for worker := 0; worker < maxRestoreWorkers; worker++ {
		if err := qmem.IncreaseCapacity(ctx, minWorkerMemReservation); err != nil {
			if worker >= minRestoreWorkers {
				break // no more memory to run workers
			}
			return 0, errors.Wrapf(err, "insufficient memory available to run restore with min %d workers", minRestoreWorkers)
		}

		numWorkers++
	}

	return numWorkers, nil
}

// SSTBatcherExecutor wraps the SSTBatcher methods, allowing a validation only restore to
// implement a mock SSTBatcher used purely for job progress tracking.
type SSTBatcherExecutor interface {
	AddMVCCKey(ctx context.Context, key storage.MVCCKey, value []byte) error
	Flush(ctx context.Context) error
	Close(ctx context.Context)
	GetSummary() kvpb.BulkOpSummary
}

type sstBatcherNoop struct {
	// totalRows written by the batcher
	totalRows storage.RowCounter
}

var _ SSTBatcherExecutor = &sstBatcherNoop{}

// AddMVCCKey merely increments the totalRow Counter. No key gets buffered or written.
func (b *sstBatcherNoop) AddMVCCKey(ctx context.Context, key storage.MVCCKey, value []byte) error {
	return b.totalRows.Count(key.Key)
}

// Flush noops.
func (b *sstBatcherNoop) Flush(ctx context.Context) error {
	return nil
}

// Close noops.
func (b *sstBatcherNoop) Close(ctx context.Context) {
}

// GetSummary returns this batcher's total added rows/bytes/etc.
func (b *sstBatcherNoop) GetSummary() kvpb.BulkOpSummary {
	return b.totalRows.BulkOpSummary
}

func init() {
	rowexec.NewRestoreDataProcessor = newRestoreDataProcessor
}
