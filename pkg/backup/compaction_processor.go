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
	"github.com/cockroachdb/cockroach/pkg/util/admission"
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
		p.compactionErr = p.runCompactBackups(ctx)
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

// constructProgressProducerMeta takes the BulkProcessorProgress message and
// converts it into a ProducerMetadata message.
func (p *compactBackupsProcessor) constructProgressProducerMeta(
	prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) *execinfrapb.ProducerMetadata {
	prog.NodeID = p.FlowCtx.NodeID.SQLInstanceID()
	prog.FlowID = p.FlowCtx.ID

	progDetails := backuppb.BackupManifest_Progress{}
	if err := gogotypes.UnmarshalAny(&prog.ProgressDetails, &progDetails); err != nil {
		log.Warningf(p.Ctx(), "failed to unmarshal progress details: %v", err)
	} else {
		p.completedSpans += progDetails.CompletedSpans
	}

	return &execinfrapb.ProducerMetadata{BulkProcessorProgress: &prog}
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

// runCompactBackups is the main entry point for the backup compaction processor.
func (p *compactBackupsProcessor) runCompactBackups(ctx context.Context) error {
	if len(p.spec.AssignedSpans) == 0 {
		return nil
	}
	user := p.spec.User()
	execCfg, ok := p.FlowCtx.Cfg.ExecutorConfig.(*sql.ExecutorConfig)
	if !ok {
		return errors.New("executor config is not of type sql.ExecutorConfig")
	}
	defaultConf, err := cloud.ExternalStorageConfFromURI(p.spec.DefaultURI, user)
	if err != nil {
		return errors.Wrapf(err, "export configuration")
	}
	defaultStore, err := execCfg.DistSQLSrv.ExternalStorage(ctx, defaultConf)
	if err != nil {
		return errors.Wrapf(err, "external storage")
	}

	compactChain, encryption, err := p.compactionChainFromSpec(ctx, execCfg, user)
	if err != nil {
		return err
	}

	backupLocalityMap, err := makeBackupLocalityMap(compactChain.compactedLocalityInfo, p.spec.User())
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

	var fsc fileSpanComparator = &exclusiveEndKeyComparator{}
	filter, err := makeSpanCoveringFilter(
		p.spec.Spans,
		[]jobspb.RestoreProgress_FrontierEntry{},
		introducedSpanFrontier,
		p.spec.TargetSize,
		p.spec.MaxFiles,
	)
	if err != nil {
		return err
	}

	genSpan := func(ctx context.Context, entryCh chan execinfrapb.RestoreSpanEntry) error {
		return errors.Wrapf(generateAndSendImportSpans(
			ctx,
			p.spec.Spans,
			compactChain.chainToCompact,
			compactChain.compactedIterFactory,
			backupLocalityMap,
			filter,
			fsc,
			entryCh,
		), "generate and send import spans")
	}

	entryCh := make(chan execinfrapb.RestoreSpanEntry, 1000)
	tasks := []func(context.Context) error{
		func(ctx context.Context) error {
			defer close(entryCh)
			return genSpan(ctx, entryCh)
		},
		func(ctx context.Context) error {
			return p.processSpanEntries(
				ctx, execCfg, entryCh, encryption, defaultStore,
			)
		},
	}

	return ctxgroup.GoAndWait(ctx, tasks...)
}

// processSpanEntries pulls RestoreSpanEntries from entryCh and if they are
// assigned to the processor, performs compaction on them.
func (p *compactBackupsProcessor) processSpanEntries(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	entryCh chan execinfrapb.RestoreSpanEntry,
	encryption *jobspb.BackupEncryptionOptions,
	store cloud.ExternalStorage,
) error {
	var fileEncryption *kvpb.FileEncryptionOptions
	if encryption != nil {
		fileEncryption = &kvpb.FileEncryptionOptions{Key: encryption.Key}
	}
	sinkConf := backupsink.SSTSinkConf{
		ID:        execCfg.DistSQLSrv.NodeID.SQLInstanceID(),
		Enc:       fileEncryption,
		ProgCh:    p.progCh,
		Settings:  &execCfg.Settings.SV,
		ElideMode: p.spec.ElideMode,
	}
	sink, err := backupsink.MakeSSTSinkKeyWriter(sinkConf, store)
	if err != nil {
		return err
	}
	defer func() {
		if err := sink.Flush(ctx); err != nil {
			log.Warningf(ctx, "failed to flush sink: %v", err)
			logClose(ctx, sink, "SST sink")
		}
	}()
	pacer := newBackupPacer(
		ctx, p.FlowCtx.Cfg.AdmissionPacerFactory, p.FlowCtx.Cfg.Settings,
	)
	// It is safe to close a nil pacer.
	defer pacer.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case entry, ok := <-entryCh:
			if !ok {
				return nil
			}
			if assigned, err := p.isAssignedEntry(entry); err != nil {
				return err
			} else if !assigned {
				continue
			}
			sstIter, err := openSSTs(ctx, execCfg, entry, fileEncryption, p.spec.EndTime)
			if err != nil {
				return errors.Wrap(err, "opening SSTs")
			}

			if err := compactSpanEntry(ctx, sstIter, sink, pacer); err != nil {
				return errors.Wrap(err, "compacting span entry")
			}
		}
	}
}

// isAssignedEntry checks if a RestoreSpanEntry is assigned to be compacted by
// this processor. We expect that if an entry is assigned to a processor, it is
// to be assigned in its entirety. Otherwise, an error is returned.
func (p *compactBackupsProcessor) isAssignedEntry(
	entry execinfrapb.RestoreSpanEntry,
) (bool, error) {
	for _, assignedSpan := range p.spec.AssignedSpans {
		if assignedSpan.Contains(entry.Span) {
			return true, nil
		} else if assignedSpan.Overlaps(entry.Span) {
			// If a RestoreSpanEntry overlaps with an assigned span but is not contained
			// within it, that implies that multiple processors will attempt to compact
			// the restore entry. This is an error as it will result in some conflicting
			// span to SST file mappings in the manifest.
			return false, errors.AssertionFailedf(
				"assigned span %v overlaps with entry span %v but does not contain it",
				assignedSpan, entry.Span,
			)
		}
	}
	return false, nil
}

// compactionChainFromSpec constructs a compactionChain for the spec of a processor.
func (p *compactBackupsProcessor) compactionChainFromSpec(
	ctx context.Context, execCfg *sql.ExecutorConfig, user username.SQLUsername,
) (compactionChain, *jobspb.BackupEncryptionOptions, error) {
	kmsEnv := backupencryption.MakeBackupKMSEnv(
		execCfg.Settings,
		&execCfg.ExternalIODirConfig,
		execCfg.InternalDB,
		user,
	)
	prevManifests, localityInfo, encryption, allIters, err := getBackupChain(
		ctx, execCfg, user, p.spec.Destination, p.spec.Encryption, p.spec.EndTime, &kmsEnv,
	)
	if err != nil {
		return compactionChain{}, nil, err
	}
	compactChain, err := newCompactionChain(
		prevManifests, p.spec.StartTime, p.spec.EndTime, localityInfo, allIters,
	)
	if err != nil {
		return compactionChain{}, nil, err
	}
	return compactChain, encryption, nil
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
	ctx context.Context, sstIter mergedSST, sink *backupsink.SSTSinkKeyWriter, pacer *admission.Pacer,
) error {
	defer sstIter.cleanup()
	entry := sstIter.entry
	if err := assertCommonPrefix(entry.Span, entry.ElidedPrefix); err != nil {
		return err
	}
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
		if err := pacer.Pace(ctx); err != nil {
			return err
		}
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
