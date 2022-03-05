// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"bytes"
	"context"
	"fmt"
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	gogotypes "github.com/gogo/protobuf/types"
)

// Progress is streamed to the coordinator through metadata.
var restoreDataOutputTypes = []*types.T{}

type restoreDataProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.RestoreDataSpec
	input   execinfra.RowSource
	output  execinfra.RowReceiver

	// numWorkers is the number of workers this processor should use. Initialized
	// at processor creation based on the cluster setting. If the cluster setting
	// is updated, the job should be PAUSEd and RESUMEd for the new setting to
	// take effect.
	numWorkers int

	// phaseGroup manages the phases of the restore:
	// 1) reading entries from the input
	// 2) ingesting the data associated with those entries in the concurrent
	// restore data workers.
	phaseGroup           ctxgroup.Group
	cancelWorkersAndWait func()

	// sstCh is a channel that holds SSTs opened by the processor, but not yet
	// ingested.
	sstCh chan mergedSST
	// Metas from the input are forwarded to the output of this processor.
	metaCh chan *execinfrapb.ProducerMetadata
	// progress updates are accumulated on this channel. It is populated by the
	// concurrent workers and sent down the flow by the processor.
	progCh chan RestoreProgress
}

var (
	_ execinfra.Processor = &restoreDataProcessor{}
	_ execinfra.RowSource = &restoreDataProcessor{}
)

const restoreDataProcName = "restoreDataProcessor"

const maxConcurrentRestoreWorkers = 32

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var defaultNumWorkers = util.ConstantWithMetamorphicTestRange(
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
	settings.TenantWritable,
	"kv.bulk_io_write.restore_node_concurrency",
	fmt.Sprintf("the number of workers processing a restore per job per node; maximum %d",
		maxConcurrentRestoreWorkers),
	int64(defaultNumWorkers),
	settings.PositiveInt,
)

var restoreAtNow = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"bulkio.restore_at_current_time.enabled",
	"write restored data at the current timestamp",
	true,
)

func newRestoreDataProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.RestoreDataSpec,
	post *execinfrapb.PostProcessSpec,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	sv := &flowCtx.Cfg.Settings.SV

	rd := &restoreDataProcessor{
		flowCtx:    flowCtx,
		input:      input,
		spec:       spec,
		output:     output,
		progCh:     make(chan RestoreProgress, maxConcurrentRestoreWorkers),
		metaCh:     make(chan *execinfrapb.ProducerMetadata, 1),
		numWorkers: int(numRestoreWorkers.Get(sv)),
	}

	if err := rd.Init(rd, post, restoreDataOutputTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{input},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				rd.ConsumerClosed()
				return nil
			},
		}); err != nil {
		return nil, err
	}
	return rd, nil
}

// Start is part of the RowSource interface.
func (rd *restoreDataProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", rd.spec.JobID)
	ctx = rd.StartInternal(ctx, restoreDataProcName)
	rd.input.Start(ctx)

	ctx, cancel := context.WithCancel(ctx)
	rd.cancelWorkersAndWait = func() {
		cancel()
		_ = rd.phaseGroup.Wait()
	}
	rd.phaseGroup = ctxgroup.WithContext(ctx)
	log.Infof(ctx, "starting restore data")

	entries := make(chan execinfrapb.RestoreSpanEntry, rd.numWorkers)
	rd.sstCh = make(chan mergedSST, rd.numWorkers)
	rd.phaseGroup.GoCtx(func(ctx context.Context) error {
		defer close(entries)
		return inputReader(ctx, rd.input, entries, rd.metaCh)
	})

	rd.phaseGroup.GoCtx(func(ctx context.Context) error {
		defer close(rd.sstCh)
		for entry := range entries {
			if err := rd.openSSTs(ctx, entry, rd.sstCh); err != nil {
				return err
			}
		}

		return nil
	})

	rd.phaseGroup.GoCtx(func(ctx context.Context) error {
		defer close(rd.progCh)
		return rd.runRestoreWorkers(ctx, rd.sstCh)
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
	entry   execinfrapb.RestoreSpanEntry
	iter    storage.SimpleMVCCIterator
	cleanup func()
}

func (rd *restoreDataProcessor) openSSTs(
	ctx context.Context, entry execinfrapb.RestoreSpanEntry, sstCh chan mergedSST,
) error {
	ctxDone := ctx.Done()

	// The sstables only contain MVCC data and no intents, so using an MVCC
	// iterator is sufficient.
	var iters []storage.SimpleMVCCIterator
	var dirs []cloud.ExternalStorage

	// If we bail early and haven't handed off responsibility of the dirs/iters to
	// the channel, close anything that we had open.
	defer func() {
		for _, iter := range iters {
			iter.Close()
		}

		for _, dir := range dirs {
			if err := dir.Close(); err != nil {
				log.Warningf(ctx, "close export storage failed %v", err)
			}
		}
	}()

	// sendIters sends all of the currently accumulated iterators over the
	// channel.
	sendIters := func(itersToSend []storage.SimpleMVCCIterator, dirsToSend []cloud.ExternalStorage) error {
		multiIter := storage.MakeMultiIterator(itersToSend)

		cleanup := func() {
			multiIter.Close()
			for _, iter := range itersToSend {
				iter.Close()
			}

			for _, dir := range dirsToSend {
				if err := dir.Close(); err != nil {
					log.Warningf(ctx, "close export storage failed %v", err)
				}
			}
		}

		mSST := mergedSST{
			entry:   entry,
			iter:    multiIter,
			cleanup: cleanup,
		}

		select {
		case sstCh <- mSST:
		case <-ctxDone:
			return ctx.Err()
		}

		iters = make([]storage.SimpleMVCCIterator, 0)
		dirs = make([]cloud.ExternalStorage, 0)
		return nil
	}

	log.VEventf(ctx, 1 /* level */, "ingesting span [%s-%s)", entry.Span.Key, entry.Span.EndKey)

	for _, file := range entry.Files {
		log.VEventf(ctx, 2, "import file %s which starts at %s", file.Path, entry.Span.Key)

		dir, err := rd.flowCtx.Cfg.ExternalStorage(ctx, file.Dir)
		if err != nil {
			return err
		}
		dirs = append(dirs, dir)

		// TODO(pbardea): When memory monitoring is added, send the currently
		// accumulated iterators on the channel if we run into memory pressure.
		iter, err := storageccl.ExternalSSTReader(ctx, dir, file.Path, rd.spec.Encryption)
		if err != nil {
			return err
		}
		iters = append(iters, iter)
	}

	return sendIters(iters, dirs)
}

func (rd *restoreDataProcessor) runRestoreWorkers(ctx context.Context, ssts chan mergedSST) error {
	return ctxgroup.GroupWorkers(ctx, rd.numWorkers, func(ctx context.Context, _ int) error {
		kr, err := makeKeyRewriterFromRekeys(rd.FlowCtx.Codec(), rd.spec.TableRekeys, rd.spec.TenantRekeys)
		if err != nil {
			return err
		}

		for {
			done, err := func() (done bool, _ error) {
				sstIter, ok := <-ssts
				if !ok {
					done = true
					return done, nil
				}

				summary, err := rd.processRestoreSpanEntry(ctx, kr, sstIter)
				if err != nil {
					return done, err
				}

				select {
				case rd.progCh <- makeProgressUpdate(summary, sstIter.entry, rd.spec.PKIDs):
				case <-ctx.Done():
					return done, ctx.Err()
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

func (rd *restoreDataProcessor) processRestoreSpanEntry(
	ctx context.Context, kr *KeyRewriter, sst mergedSST,
) (roachpb.BulkOpSummary, error) {
	db := rd.flowCtx.Cfg.DB
	evalCtx := rd.EvalCtx
	var summary roachpb.BulkOpSummary

	entry := sst.entry
	iter := sst.iter
	defer sst.cleanup()

	writeAtBatchTS := restoreAtNow.Get(&evalCtx.Settings.SV)
	if writeAtBatchTS && !evalCtx.Settings.Version.IsActive(ctx, clusterversion.MVCCAddSSTable) {
		return roachpb.BulkOpSummary{}, errors.Newf(
			"cannot use %s until version %s", restoreAtNow.Key(), clusterversion.MVCCAddSSTable.String(),
		)
	}

	// If the system tenant is restoring a guest tenant span, we don't want to
	// forward all the restored data to now, as there may be importing tables in
	// that span, that depend on the difference in timestamps on restored existing
	// vs importing keys to rollback.
	if writeAtBatchTS && kr.fromSystemTenant &&
		(bytes.HasPrefix(entry.Span.Key, keys.TenantPrefix) || bytes.HasPrefix(entry.Span.EndKey, keys.TenantPrefix)) {
		log.Warningf(ctx, "restoring span %s at its original timestamps because it is a tenant span", entry.Span)
		writeAtBatchTS = false
	}

	// "disallowing" shadowing of anything older than logical=1 is i.e. allow all
	// shadowing. We must allow shadowing in case the RESTORE has to retry any
	// ingestions, but setting a (permissive) disallow like this serves to force
	// evaluation of AddSSTable to check for overlapping keys. That in turn will
	// result in it maintaining exact MVCC stats rather than estimates. Of course
	// this comes at the cost of said overlap check, but in the common case of
	// non-overlapping ingestion into empty spans, that is just one seek.
	disallowShadowingBelow := hlc.Timestamp{Logical: 1}
	batcher, err := bulk.MakeSSTBatcher(ctx,
		db,
		evalCtx.Settings,
		disallowShadowingBelow,
		writeAtBatchTS,
	)
	if err != nil {
		return summary, err
	}
	defer batcher.Close()

	var keyScratch, valueScratch []byte

	startKeyMVCC, endKeyMVCC := storage.MVCCKey{Key: entry.Span.Key},
		storage.MVCCKey{Key: entry.Span.EndKey}

	for iter.SeekGE(startKeyMVCC); ; {
		ok, err := iter.Valid()
		if err != nil {
			return summary, err
		}
		if !ok {
			break
		}

		if !rd.spec.RestoreTime.IsEmpty() {
			// TODO(dan): If we have to skip past a lot of versions to find the
			// latest one before args.EndTime, then this could be slow.
			if rd.spec.RestoreTime.Less(iter.UnsafeKey().Timestamp) {
				iter.Next()
				continue
			}
		}

		if !ok || !iter.UnsafeKey().Less(endKeyMVCC) {
			break
		}
		if len(iter.UnsafeValue()) == 0 {
			// Value is deleted.
			iter.NextKey()
			continue
		}

		keyScratch = append(keyScratch[:0], iter.UnsafeKey().Key...)
		valueScratch = append(valueScratch[:0], iter.UnsafeValue()...)
		key := storage.MVCCKey{Key: keyScratch, Timestamp: iter.UnsafeKey().Timestamp}
		value := roachpb.Value{RawBytes: valueScratch}
		iter.NextKey()

		key.Key, ok, err = kr.RewriteKey(key.Key)
		if err != nil {
			return summary, err
		}
		if !ok {
			// If the key rewriter didn't match this key, it's not data for the
			// table(s) we're interested in.
			if log.V(5) {
				log.Infof(ctx, "skipping %s %s", key.Key, value.PrettyPrint())
			}
			continue
		}

		// Rewriting the key means the checksum needs to be updated.
		value.ClearChecksum()
		value.InitChecksum(key.Key)

		if log.V(5) {
			log.Infof(ctx, "Put %s -> %s", key.Key, value.PrettyPrint())
		}
		if err := batcher.AddMVCCKey(ctx, key, value.RawBytes); err != nil {
			return summary, errors.Wrapf(err, "adding to batch: %s -> %s", key, value.PrettyPrint())
		}
	}
	// Flush out the last batch.
	if err := batcher.Flush(ctx); err != nil {
		return summary, err
	}

	if restoreKnobs, ok := rd.flowCtx.TestingKnobs().BackupRestoreTestingKnobs.(*sql.BackupRestoreTestingKnobs); ok {
		if restoreKnobs.RunAfterProcessingRestoreSpanEntry != nil {
			restoreKnobs.RunAfterProcessingRestoreSpanEntry(ctx)
		}
	}

	return batcher.GetSummary(), nil
}

func makeProgressUpdate(
	summary roachpb.BulkOpSummary, entry execinfrapb.RestoreSpanEntry, pkIDs map[uint64]bool,
) (progDetails RestoreProgress) {
	progDetails.Summary = countRows(summary, pkIDs)
	progDetails.ProgressIdx = entry.ProgressIdx
	progDetails.DataSpan = entry.Span
	return
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
	case meta := <-rd.metaCh:
		return nil, meta
	case <-rd.Ctx.Done():
		rd.MoveToDraining(rd.Ctx.Err())
		return nil, rd.DrainHelper()
	}

	return nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &prog}
}

// ConsumerClosed is part of the RowSource interface.
func (rd *restoreDataProcessor) ConsumerClosed() {
	if rd.Closed {
		return
	}
	rd.cancelWorkersAndWait()
	if rd.sstCh != nil {
		// Cleanup all the remaining open SSTs that have not been consumed.
		for sst := range rd.sstCh {
			sst.cleanup()
		}
	}
	rd.InternalClose()
}

func init() {
	rowexec.NewRestoreDataProcessor = newRestoreDataProcessor
}
