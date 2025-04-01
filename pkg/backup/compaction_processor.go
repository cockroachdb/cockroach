// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/backup/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backupsink"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	gogotypes "github.com/gogo/protobuf/types"
)

const (
	compactBackupsProcessorName = "compactBackupsProcessor"
)

type compactBackupsProcessor struct {
	execinfra.ProcessorBase

	spec execinfrapb.CompactBackupsSpec

	progCh                 chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	cancelAndWaitForWorker func()
	compactionErr          error

	// completedSpans tracks how many spans have been successfully compacted by
	// the backup processor.
	completedSpans int32
}

var (
	_ execinfra.Processor = &compactBackupsProcessor{}
	_ execinfra.RowSource = &compactBackupsProcessor{}
)

func newCompactBackupsProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.CompactBackupsSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	processor := &compactBackupsProcessor{
		spec:   spec,
		progCh: make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress),
	}
	if err := processor.Init(ctx, processor, post, []*types.T{}, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: nil,
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				processor.close()
				return nil
			},
		}); err != nil {
		return nil, err
	}
	return processor, nil
}

func (p *compactBackupsProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", p.spec.JobID)

	p.StartInternal(ctx, compactBackupsProcessorName)
	ctx, cancel := context.WithCancel(ctx)
	p.cancelAndWaitForWorker = func() {
		cancel()
		for range p.progCh {
		}
	}
	log.Infof(ctx, "starting backup compaction")
	if err := p.FlowCtx.Stopper().RunAsyncTaskEx(ctx, stop.TaskOpts{
		TaskName: compactBackupsProcessorName + ".runCompactBackups",
		SpanOpt:  stop.ChildSpan,
	}, func(ctx context.Context) {
		p.compactionErr = runCompactBackups(ctx, p.FlowCtx, p.spec, p.progCh)
		cancel()
		close(p.progCh)
	}); err != nil {
		p.compactionErr = err
		cancel()
		close(p.progCh)
	}
}

func (p *compactBackupsProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if p.State != execinfra.StateRunning {
		return nil, p.DrainHelper()
	}

	prog, ok := <-p.progCh
	if !ok {
		p.MoveToDraining(p.compactionErr)
		return nil, p.DrainHelper()
	}
	return nil, p.constructProgressProducerMeta(prog)
}

func (p *compactBackupsProcessor) constructProgressProducerMeta(
	prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) *execinfrapb.ProducerMetadata {
	// Take a copy so that we can send the progress address to the output
	// processor.
	progCopy := prog
	progCopy.NodeID = p.FlowCtx.NodeID.SQLInstanceID()
	progCopy.FlowID = p.FlowCtx.ID

	progDetails := backuppb.BackupManifest_Progress{}
	if err := gogotypes.UnmarshalAny(&prog.ProgressDetails, &progDetails); err != nil {
		log.Warningf(p.Ctx(), "failed to unmarshal progress details: %v", err)
	} else {
		totalSpans := int32(len(p.spec.Spans))
		p.completedSpans += progDetails.CompletedSpans
		if totalSpans != 0 {
			if progCopy.CompletedFraction == nil {
				progCopy.CompletedFraction = make(map[int32]float32)
			}
			progCopy.CompletedFraction[p.ProcessorID] = float32(p.completedSpans) / float32(totalSpans)
		}
	}

	return &execinfrapb.ProducerMetadata{BulkProcessorProgress: &progCopy}
}

func (p *compactBackupsProcessor) close() {
	if p.Closed {
		return
	}
	if p.cancelAndWaitForWorker != nil {
		p.cancelAndWaitForWorker()
	}
	p.InternalClose()
}

// ConsumerClosed is part of the RowSource interface. We have to override the
// implementation provided by ProcessorBase.
func (p *compactBackupsProcessor) ConsumerClosed() {
	p.close()
}

func runCompactBackups(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec execinfrapb.CompactBackupsSpec,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	if len(spec.AssignedSpans) == 0 {
		return nil
	}
	user := spec.User()
	execCfg, ok := flowCtx.Cfg.ExecutorConfig.(*sql.ExecutorConfig)
	if !ok {
		return errors.New("executor config is not of type sql.ExecutorConfig")
	}
	defaultConf, err := cloud.ExternalStorageConfFromURI(spec.DefaultURI, user)
	if err != nil {
		return errors.Wrapf(err, "export configuration")
	}
	defaultStore, err := execCfg.DistSQLSrv.ExternalStorage(ctx, defaultConf)
	if err != nil {
		return errors.Wrapf(err, "external storage")
	}

	compactChain, encryption, err := compactionChainFromSpec(ctx, execCfg, spec, user)
	if err != nil {
		return err
	}

	intersectSpanCh := make(chan execinfrapb.RestoreSpanEntry, 1000)
	tasks := []func(context.Context) error{
		func(ctx context.Context) error {
			defer close(intersectSpanCh)
			return genIntersectingSpansForCompaction(ctx, execCfg, spec, compactChain, intersectSpanCh)
		},
		func(ctx context.Context) error {
			return processIntersectingSpanEntries(
				ctx, execCfg, spec, intersectSpanCh, encryption, progCh, defaultStore,
			)
		},
	}

	return ctxgroup.GoAndWait(ctx, tasks...)
}

func processIntersectingSpanEntries(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	spec execinfrapb.CompactBackupsSpec,
	entryCh chan execinfrapb.RestoreSpanEntry,
	encryption *jobspb.BackupEncryptionOptions,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	store cloud.ExternalStorage,
) error {
	var fileEncryption *kvpb.FileEncryptionOptions
	if encryption != nil {
		fileEncryption = &kvpb.FileEncryptionOptions{Key: encryption.Key}
	}
	sinkConf := backupsink.SSTSinkConf{
		ID:        execCfg.DistSQLSrv.NodeID.SQLInstanceID(),
		Enc:       fileEncryption,
		ProgCh:    progCh,
		Settings:  &execCfg.Settings.SV,
		ElideMode: spec.ElideMode,
	}
	sink, err := backupsink.MakeSSTSinkKeyWriter(sinkConf, store, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := sink.Flush(ctx); err != nil {
			log.Warningf(ctx, "failed to flush sink: %v", err)
			logClose(ctx, sink, "SST sink")
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case entry, ok := <-entryCh:
			if !ok {
				return nil
			}

			sstIter, err := openSSTs(ctx, execCfg, entry, fileEncryption, spec.EndTime)
			if err != nil {
				return errors.Wrap(err, "opening SSTs")
			}

			if err := compactSpanEntry(ctx, sstIter, sink); err != nil {
				return errors.Wrap(err, "compacting span entry")
			}
		}
	}
}

func compactionChainFromSpec(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	spec execinfrapb.CompactBackupsSpec,
	user username.SQLUsername,
) (compactionChain, *jobspb.BackupEncryptionOptions, error) {
	kmsEnv := backupencryption.MakeBackupKMSEnv(
		execCfg.Settings,
		&execCfg.ExternalIODirConfig,
		execCfg.InternalDB,
		user,
	)
	prevManifests, localityInfo, encryption, allIters, err := getBackupChain(
		ctx, execCfg, user, spec.Destination, spec.Encryption, spec.EndTime, &kmsEnv,
	)
	if err != nil {
		return compactionChain{}, nil, err
	}
	compactChain, err := newCompactionChain(
		prevManifests, spec.StartTime, spec.EndTime, localityInfo, allIters,
	)
	if err != nil {
		return compactionChain{}, nil, err
	}
	return compactChain, encryption, nil
}

func genIntersectingSpansForCompaction(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	spec execinfrapb.CompactBackupsSpec,
	compactChain compactionChain,
	intersectSpanCh chan execinfrapb.RestoreSpanEntry,
) error {
	backupLocalityMap, err := makeBackupLocalityMap(compactChain.compactedLocalityInfo, spec.User())
	if err != nil {
		return err
	}

	introducedSpanFrontier, err := createIntroducedSpanFrontier(
		compactChain.backupChain, compactChain.lastBackup().EndTime,
	)
	if err != nil {
		return err
	}
	defer introducedSpanFrontier.Release()

	targetSize := targetRestoreSpanSize.Get(&execCfg.Settings.SV)
	maxFiles := maxFileCount.Get(&execCfg.Settings.SV)

	var fsc fileSpanComparator = &exclusiveEndKeyComparator{}
	filter, err := makeSpanCoveringFilter(
		spec.Spans,
		[]jobspb.RestoreProgress_FrontierEntry{},
		introducedSpanFrontier,
		targetSize,
		maxFiles,
	)
	if err != nil {
		return err
	}

	genSpan := func(ctx context.Context, entryCh chan execinfrapb.RestoreSpanEntry) error {
		defer close(entryCh)
		return errors.Wrapf(generateAndSendImportSpans(
			ctx,
			spec.Spans,
			compactChain.chainToCompact,
			compactChain.compactedIterFactory,
			backupLocalityMap,
			filter,
			fsc,
			entryCh,
		), "generate and send import spans")
	}

	filterIntersectingSpans := func(ctx context.Context, entryCh chan execinfrapb.RestoreSpanEntry) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case entry, ok := <-entryCh:
				if !ok {
					return nil
				}
				for _, assignedSpan := range spec.AssignedSpans {
					if assignedSpan.Overlaps(entry.Span) {
						intersectSpanCh <- entry
						break
					}
				}
			}
		}
	}

	entryCh := make(chan execinfrapb.RestoreSpanEntry, 1000)
	tasks := []func(context.Context) error{
		func(ctx context.Context) error {
			return genSpan(ctx, entryCh)
		},
		func(ctx context.Context) error {
			return filterIntersectingSpans(ctx, entryCh)
		},
	}
	return ctxgroup.GoAndWait(ctx, tasks...)
}

func openSSTs(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	entry execinfrapb.RestoreSpanEntry,
	encryptionOptions *kvpb.FileEncryptionOptions,
	endTime hlc.Timestamp,
) (mergedSST, error) {
	var dirs []cloud.ExternalStorage
	storeFiles := make([]storageccl.StoreFile, 0, len(entry.Files))
	for idx := range entry.Files {
		file := entry.Files[idx]
		dir, err := execCfg.DistSQLSrv.ExternalStorage(ctx, file.Dir)
		if err != nil {
			return mergedSST{}, err
		}
		dirs = append(dirs, dir)
		storeFiles = append(storeFiles, storageccl.StoreFile{Store: dir, FilePath: file.Path})
	}
	iterOpts := storage.IterOptions{
		// TODO (kev-cao): Come back and update this to range keys when
		// SSTSinkKeyWriter has been updated to support range keys.
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}
	iter, err := storageccl.ExternalSSTReader(ctx, storeFiles, encryptionOptions, iterOpts)
	if err != nil {
		return mergedSST{}, err
	}
	compactionIter, err := storage.NewBackupCompactionIterator(iter, endTime)
	if err != nil {
		return mergedSST{}, err
	}
	return mergedSST{
		entry: entry,
		iter:  compactionIter,
		cleanup: func() {
			log.VInfof(ctx, 1, "finished with and closing %d files in span %d %v", len(entry.Files), entry.ProgressIdx, entry.Span.String())
			compactionIter.Close()
			for _, dir := range dirs {
				if err := dir.Close(); err != nil {
					log.Warningf(ctx, "close export storage failed: %v", err)
				}
			}
		},
		completeUpTo: endTime,
	}, nil
}

func compactSpanEntry(
	ctx context.Context, sstIter mergedSST, sink *backupsink.SSTSinkKeyWriter,
) error {
	defer sstIter.cleanup()
	entry := sstIter.entry
	prefix, err := backupsink.ElidedPrefix(entry.Span.Key, entry.ElidedPrefix)
	if err != nil {
		return err
	} else if prefix == nil {
		return errors.New("backup compactions does not supported non-elided keys")
	}
	trimmedStart := storage.MVCCKey{Key: bytes.TrimPrefix(entry.Span.Key, prefix)}
	trimmedEnd := storage.MVCCKey{Key: bytes.TrimPrefix(entry.Span.EndKey, prefix)}
	if err := sink.Reset(ctx, entry.Span); err != nil {
		return err
	}
	scratch := make([]byte, 0, len(prefix))
	scratch = append(scratch, prefix...)
	iter := sstIter.iter
	for iter.SeekGE(trimmedStart); ; iter.NextKey() {
		var key storage.MVCCKey
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		key = iter.UnsafeKey()
		if !key.Less(trimmedEnd) {
			break
		}
		value, err := iter.UnsafeValue()
		if err != nil {
			return err
		}
		// The sst sink requires full keys including their prefix, so for every
		// key, we need to prepend the prefix to the key. To avoid unnecessary
		// allocations, we reuse the scratch buffer to build the full key.
		scratch = append(scratch[:len(prefix)], key.Key...)
		key.Key = scratch
		if err := sink.WriteKey(ctx, key, value); err != nil {
			return err
		}
	}
	sink.AssumeNotMidRow()
	return nil
}

func init() {
	rowexec.NewCompactBackupsProcessor = newCompactBackupsProcessor
}
