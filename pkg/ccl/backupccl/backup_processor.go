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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
)

var backupOutputTypes = []*types.T{}

var (
	useTBI = settings.RegisterBoolSetting(
		"kv.bulk_io_write.experimental_incremental_export_enabled",
		"use experimental time-bound file filter when exporting in BACKUP",
		true,
	)
	priorityAfter = settings.RegisterDurationSetting(
		"bulkio.backup.read_with_priority_after",
		"age of read-as-of time above which a BACKUP should read with priority",
		time.Minute,
		settings.NonNegativeDuration,
	)
	delayPerAttmpt = settings.RegisterDurationSetting(
		"bulkio.backup.read_retry_delay",
		"amount of time since the read-as-of time, per-prior attempt, to wait before making another attempt",
		time.Second*5,
		settings.NonNegativeDuration,
	)
	timeoutPerAttempt = settings.RegisterDurationSetting(
		"bulkio.backup.read_timeout",
		"amount of time after which a read attempt is considered timed out and is canceled. "+
			"Hitting this timeout will cause the backup job to fail.",
		time.Minute*5,
		settings.NonNegativeDuration,
	)
)

const backupProcessorName = "backupDataProcessor"

// TODO(pbardea): It would be nice if we could add some DistSQL processor tests
// we would probably want to have a mock cloudStorage object that we could
// verify with.

// backupDataProcessor represents the work each node in a cluster performs
// during a BACKUP. It is assigned a set of spans to export to a given URI. It
// will create parallel workers (whose parallelism is also specified), which
// will each export a span at a time. After exporting the span, it will stream
// back its progress through the metadata channel provided by DistSQL.
type backupDataProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.BackupDataSpec
	output  execinfra.RowReceiver

	progCh    chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	backupErr error
}

var _ execinfra.Processor = &backupDataProcessor{}
var _ execinfra.RowSource = &backupDataProcessor{}

func newBackupDataProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BackupDataSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	bp := &backupDataProcessor{
		flowCtx: flowCtx,
		spec:    spec,
		output:  output,
		progCh:  make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress),
	}
	if err := bp.Init(bp, post, backupOutputTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			// This processor doesn't have any inputs to drain.
			InputsToDrain: nil,
		}); err != nil {
		return nil, err
	}
	return bp, nil
}

// Start is part of the RowSource interface.
func (bp *backupDataProcessor) Start(ctx context.Context) {
	ctx = bp.StartInternal(ctx, backupProcessorName)
	go func() {
		defer close(bp.progCh)
		bp.backupErr = runBackupProcessor(ctx, bp.flowCtx, &bp.spec, bp.progCh)
	}()
}

// Next is part of the RowSource interface.
func (bp *backupDataProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if bp.State != execinfra.StateRunning {
		return nil, bp.DrainHelper()
	}

	for prog := range bp.progCh {
		// Take a copy so that we can send the progress address to the output
		// processor.
		p := prog
		return nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &p}
	}

	if bp.backupErr != nil {
		bp.MoveToDraining(bp.backupErr)
		return nil, bp.DrainHelper()
	}

	bp.MoveToDraining(nil /* error */)
	return nil, bp.DrainHelper()
}

type spanAndTime struct {
	// spanIdx is a unique identifier of this object.
	spanIdx    int
	span       roachpb.Span
	start, end hlc.Timestamp
	attempts   int
	lastTried  time.Time
}

type returnedSST struct {
	f        BackupManifest_File
	sst      []byte
	revStart hlc.Timestamp
	finished bool
}

func runBackupProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.BackupDataSpec,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	backupProcessorSpan := tracing.SpanFromContext(ctx)
	clusterSettings := flowCtx.Cfg.Settings

	todo := make(chan spanAndTime, len(spec.Spans)+len(spec.IntroducedSpans))
	var spanIdx int
	for _, s := range spec.IntroducedSpans {
		todo <- spanAndTime{spanIdx: spanIdx, span: s, start: hlc.Timestamp{},
			end: spec.BackupStartTime}
		spanIdx++
	}
	for _, s := range spec.Spans {
		todo <- spanAndTime{spanIdx: spanIdx, span: s, start: spec.BackupStartTime,
			end: spec.BackupEndTime}
		spanIdx++
	}

	// TODO(pbardea): Check to see if this benefits from any tuning (e.g. +1, or
	//  *2). See #49798.
	targetFileSize := storageccl.ExportRequestTargetFileSize.Get(&clusterSettings.SV)

	// For all backups, partitioned or not, the main BACKUP manifest is stored at
	// details.URI.
	defaultConf, err := cloud.ExternalStorageConfFromURI(spec.DefaultURI, spec.User())
	if err != nil {
		return err
	}

	storageConfByLocalityKV := make(map[string]*roachpb.ExternalStorage)
	for kv, uri := range spec.URIsByLocalityKV {
		conf, err := cloud.ExternalStorageConfFromURI(uri, spec.User())
		if err != nil {
			return err
		}
		storageConfByLocalityKV[kv] = &conf
	}

	// If this is a tenant backup, we need to write the file from the SQL layer.
	writeSSTsInProcessor := !flowCtx.Cfg.Codec.ForSystemTenant()

	returnedSSTs := make(chan returnedSST, 1)

	grp := ctxgroup.WithContext(ctx)
	// Start a goroutine that will then start a group of goroutines which each
	// pull spans off of todo and send export requests for them, pushing any
	// remainders based ResumeSpans or retries back onto todo, and pushing the
	// responses -- which may contain inline SSTs -- back to
	grp.GoCtx(func(ctx context.Context) error {
		defer close(returnedSSTs)
		numSenders := int(kvserver.ExportRequestsLimit.Get(&clusterSettings.SV)) * 2

		return ctxgroup.GroupWorkers(ctx, numSenders, func(ctx context.Context, _ int) error {

			readTime := spec.BackupEndTime.GoTime()

			// priority becomes true when we're sending re-attempts of reads far enough
			// in the past that we want to run them with priority.
			var priority bool
			timer := timeutil.NewTimer()
			defer timer.Stop()

			ctxDone := ctx.Done()
			for {
				select {
				case <-ctxDone:
					return ctx.Err()
				case span := <-todo:
					// TODO(pbardea): It would be nice if we could avoid producing many small
					//  SSTs. See #44480.
					header := roachpb.Header{Timestamp: span.end}
					req := &roachpb.ExportRequest{
						RequestHeader:                       roachpb.RequestHeaderFromSpan(span.span),
						StorageByLocalityKV:                 storageConfByLocalityKV,
						StartTime:                           span.start,
						EnableTimeBoundIteratorOptimization: useTBI.Get(&clusterSettings.SV),
						MVCCFilter:                          spec.MVCCFilter,
						TargetFileSize:                      targetFileSize,
					}
					if writeSSTsInProcessor {
						req.ReturnSST = true
					} else {
						req.Storage = defaultConf
						req.Encryption = spec.Encryption
					}

					// If we're doing re-attempts but are not yet in the priority regime,
					// check to see if it is time to switch to priority.
					if !priority && span.attempts > 0 {
						// Check if this is starting a new pass and we should delay first.
						// We're okay with delaying this worker until then since we assume any
						// other work it could pull off the queue will likely want to delay to
						// a similar or later time anyway.
						if delay := delayPerAttmpt.Get(&clusterSettings.SV) - timeutil.Since(span.lastTried); delay > 0 {
							timer.Reset(delay)
							log.Infof(ctx, "waiting %s to start attempt %d of remaining spans", delay, span.attempts+1)
							select {
							case <-ctxDone:
								return ctx.Err()
							case <-timer.C:
								timer.Read = true
							}
						}

						priority = timeutil.Since(readTime) > priorityAfter.Get(&clusterSettings.SV)
					}

					if priority {
						// This re-attempt is reading far enough in the past that we just want
						// to abort any transactions it hits.
						header.UserPriority = roachpb.MaxUserPriority
					} else {
						// On the initial attempt to export this span and re-attempts that are
						// done while it is still less than the configured time above the read
						// time, we set WaitPolicy to Error, so that the export will return an
						// error to us instead of instead doing blocking wait if it hits any
						// other txns. This lets us move on to other ranges we have to export,
						// provide an indication of why we're blocked, etc instead and come
						// back to this range later.
						header.WaitPolicy = lock.WaitPolicy_Error
					}

					// If we are asking for the SSTs to be returned, we set the DistSender
					// response target bytes field to a sentinel value.
					// The sentinel value of 1 forces the ExportRequest to paginate after
					// creating a single SST. The max size of this SST can be controlled
					// using the existing cluster settings, `kv.bulk_sst.target_size` and
					// `kv.bulk_sst.max_allowed_overage`.
					// This allows us to cap the size of the ExportRequest response (stored
					// in memory) to the sum of the above cluster settings.
					if req.ReturnSST {
						header.TargetBytes = 1
					}

					log.Infof(ctx, "sending ExportRequest for span %s (attempt %d, priority %s)",
						span.span, span.attempts+1, header.UserPriority.String())
					var rawRes roachpb.Response
					var pErr *roachpb.Error
					var reqSentTime time.Time
					var respReceivedTime time.Time
					exportRequestErr := contextutil.RunWithTimeout(ctx,
						fmt.Sprintf("ExportRequest for span %s", span.span),
						timeoutPerAttempt.Get(&clusterSettings.SV), func(ctx context.Context) error {
							reqSentTime = timeutil.Now()
							backupProcessorSpan.RecordStructured(&BackupExportTraceRequestEvent{
								Span:        span.span.String(),
								Attempt:     int32(span.attempts + 1),
								Priority:    header.UserPriority.String(),
								ReqSentTime: reqSentTime.String(),
							})

							rawRes, pErr = kv.SendWrappedWith(ctx, flowCtx.Cfg.DB.NonTransactionalSender(),
								header, req)
							respReceivedTime = timeutil.Now()
							if pErr != nil {
								return pErr.GoError()
							}
							return nil
						})
					if exportRequestErr != nil {
						if intentErr, ok := pErr.GetDetail().(*roachpb.WriteIntentError); ok {
							span.lastTried = timeutil.Now()
							span.attempts++
							todo <- span
							// TODO(dt): send a progress update to update job progress to note
							// the intents being hit.
							backupProcessorSpan.RecordStructured(&BackupExportTraceResponseEvent{
								RetryableError: fmt.Sprintf("%v", intentErr)})
							continue
						}
						// TimeoutError improves the opaque `context deadline exceeded` error
						// message so use that instead.
						if errors.HasType(exportRequestErr, (*contextutil.TimeoutError)(nil)) {
							return errors.Wrapf(exportRequestErr, "timeout: %s", exportRequestErr.Error())
						}
						return errors.Wrapf(exportRequestErr, "exporting %s", span.span)
					}

					res := rawRes.(*roachpb.ExportResponse)

					// If the reply has a resume span, put the remaining span on
					// todo to be picked up again in the next round.
					if res.ResumeSpan != nil {
						if !res.ResumeSpan.Valid() {
							return errors.Errorf("invalid resume span: %s", res.ResumeSpan)
						}
						resumeSpan := spanAndTime{
							span:      *res.ResumeSpan,
							start:     span.start,
							end:       span.end,
							attempts:  span.attempts,
							lastTried: span.lastTried,
						}
						todo <- resumeSpan
					}

					if backupKnobs, ok := flowCtx.TestingKnobs().BackupRestoreTestingKnobs.(*sql.BackupRestoreTestingKnobs); ok {
						if backupKnobs.RunAfterExportingSpanEntry != nil {
							backupKnobs.RunAfterExportingSpanEntry(ctx)
						}
					}

					duration := respReceivedTime.Sub(reqSentTime)
					exportResponseTraceEvent := &BackupExportTraceResponseEvent{
						Duration:      duration.String(),
						FileSummaries: make([]RowCount, 0),
					}
					var numFiles int
					files := make([]BackupManifest_File, 0)
					for _, file := range res.Files {
						numFiles++
						f := BackupManifest_File{
							Span:        file.Span,
							Path:        file.Path,
							EntryCounts: countRows(file.Exported, spec.PKIDs),
							LocalityKV:  file.LocalityKV,
						}
						exportResponseTraceEvent.FileSummaries = append(exportResponseTraceEvent.FileSummaries, f.EntryCounts)
						if span.start != spec.BackupStartTime {
							f.StartTime = span.start
							f.EndTime = span.end
						}
						// If this file reply has an inline SST, push it to the
						// ch for the writer goroutine to handle. Otherwise, go
						// ahead and record the file for progress reporting.
						if len(file.SST) > 0 {
							exportResponseTraceEvent.HasReturnedSSTs = true
							returnedSSTs <- returnedSST{f: f, sst: file.SST, revStart: res.StartTime, finished: res.ResumeSpan == nil}
						} else {
							files = append(files, f)
						}
					}
					exportResponseTraceEvent.NumFiles = int32(numFiles)
					backupProcessorSpan.RecordStructured(exportResponseTraceEvent)

					// If we have replies for exported files (as oppposed to the
					// ones with inline SSTs we had to forward to the uploader
					// goroutine), we can report them as progress completed.
					if len(files) > 0 {
						progDetails := BackupManifest_Progress{
							RevStartTime: res.StartTime,
							Files:        files,
							Partial:      res.ResumeSpan != nil,
						}
						var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
						details, err := gogotypes.MarshalAny(&progDetails)
						if err != nil {
							return err
						}
						prog.ProgressDetails = *details
						select {
						case <-ctx.Done():
							return ctx.Err()
						case progCh <- prog:
						}
					}
				default:
					// No work left to do, so we can exit. Note that another worker could
					// still be running and may still push new work (a retry) on to todo but
					// that is OK, since that also means it is still running and thus can
					// pick up that work on its next iteration.
					return nil
				}
			}
		})
	})

	// Start another goroutine which will read from returnedSSTs ch and push
	// ssts from it into an sstSink responsible for actually writing their
	// contents to cloud storage.
	grp.GoCtx(func(ctx context.Context) error {
		sinkConf := sstSinkConf{
			id:             flowCtx.NodeID.SQLInstanceID(),
			enc:            spec.Encryption,
			targetFileSize: targetFileSize,
			progCh:         progCh,
		}

		defaultStore, err := flowCtx.Cfg.ExternalStorage(ctx, defaultConf)
		if err != nil {
			return err
		}
		defer defaultStore.Close()
		defaultSink := &sstSink{conf: sinkConf, dest: defaultStore}

		localitySinks := make(map[string]*sstSink)
		defer func() {
			for i := range localitySinks {
				localitySinks[i].dest.Close()
			}
		}()

		for res := range returnedSSTs {
			var sink *sstSink

			if existing, ok := localitySinks[res.f.LocalityKV]; ok {
				sink = existing
			} else if conf, ok := storageConfByLocalityKV[res.f.LocalityKV]; ok {
				es, err := flowCtx.Cfg.ExternalStorage(ctx, *conf)
				if err != nil {
					return err
				}
				// No defer Close here -- we defer a close of all of them above.
				sink = &sstSink{conf: sinkConf, dest: es}
				localitySinks[res.f.LocalityKV] = sink
			} else {
				sink = defaultSink
			}

			if err := sink.write(ctx, res); err != nil {
				return err
			}
		}

		for _, s := range localitySinks {
			if err := s.flush(ctx); err != nil {
				return err
			}
		}
		return defaultSink.flush(ctx)
	})

	return grp.Wait()
}

type sstSinkConf struct {
	progCh         chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	targetFileSize int64
	enc            *roachpb.FileEncryptionOptions
	id             base.SQLInstanceID
}

type sstSink struct {
	dest             cloud.ExternalStorage
	conf             sstSinkConf
	sstWriter        *storage.SSTWriter
	bufferedSST      storage.MemFile
	bufferedFiles    []BackupManifest_File
	bufferedRevStart hlc.Timestamp
	finishedSpan     bool
}

func (s *sstSink) flush(ctx context.Context) error {
	if s.sstWriter == nil {
		return nil
	}
	if err := s.sstWriter.Finish(); err != nil {
		return err
	}

	data := s.bufferedSST.Bytes()
	if s.conf.enc != nil {
		var err error
		data, err = storageccl.EncryptFile(data, s.conf.enc.Key)
		if err != nil {
			return err
		}
	}

	name := fmt.Sprintf("%d.sst", builtins.GenerateUniqueInt(s.conf.id))
	if err := cloud.WriteFile(ctx, s.dest, name, bytes.NewReader(data)); err != nil {
		log.VEventf(ctx, 1, "failed to put file: %+v", err)
		return errors.Wrap(err, "writing SST")
	}

	for i := range s.bufferedFiles {
		s.bufferedFiles[i].Path = name
	}

	progDetails := BackupManifest_Progress{
		RevStartTime: s.bufferedRevStart,
		Files:        s.bufferedFiles,
		Partial:      !s.finishedSpan,
	}
	var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	details, err := gogotypes.MarshalAny(&progDetails)
	if err != nil {
		return err
	}
	prog.ProgressDetails = *details
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.conf.progCh <- prog:
	}

	s.sstWriter = nil
	s.bufferedSST.Reset()
	s.bufferedFiles = nil
	s.bufferedRevStart.Reset()
	s.finishedSpan = false

	return nil
}

func (s *sstSink) write(ctx context.Context, resp returnedSST) error {
	span := resp.f.Span

	// If this span starts before the last buffered span ended, we need to flush
	// since it overlaps but SSTWriter demands writes in-order.
	if len(s.bufferedFiles) > 0 && span.Key.Compare(s.bufferedFiles[len(s.bufferedFiles)-1].Span.EndKey) < 0 {
		if err := s.flush(ctx); err != nil {
			return err
		}
	}

	// Initialize the writer if needed then copy the SST content to the writer.
	if s.sstWriter == nil {
		w := storage.MakeBackupSSTWriter(&s.bufferedSST)
		s.sstWriter = &w
	}
	sst, err := storage.NewMemSSTIterator(resp.sst, false)
	if err != nil {
		return err
	}
	defer sst.Close()
	sst.SeekGE(storage.MVCCKey{Key: keys.MinKey})
	for {
		if valid, err := sst.Valid(); !valid || err != nil {
			if err != nil {
				return err
			}
			break
		}
		k := sst.UnsafeKey()
		if k.Timestamp.IsEmpty() {
			if err := s.sstWriter.PutUnversioned(k.Key, sst.UnsafeValue()); err != nil {
				return err
			}
		} else {
			if err := s.sstWriter.PutMVCC(sst.UnsafeKey(), sst.UnsafeValue()); err != nil {
				return err
			}
		}
		sst.Next()
	}

	// If this span extended the last span added -- that is, picked up where it
	// ended and has the same time-bounds -- then we can simply extend that span
	// and add to its entry counts. Otherwise we need to record it separately.
	if l := len(s.bufferedFiles) - 1; l > 0 && s.bufferedFiles[l].Span.EndKey.Equal(span.Key) &&
		s.bufferedFiles[l].EndTime.EqOrdering(resp.f.EndTime) &&
		s.bufferedFiles[l].StartTime.EqOrdering(resp.f.StartTime) {
		s.bufferedFiles[l].Span.EndKey = span.EndKey
		s.bufferedFiles[l].EntryCounts.add(resp.f.EntryCounts)
	} else {
		s.bufferedFiles = append(s.bufferedFiles, resp.f)
	}
	s.bufferedRevStart.Forward(resp.revStart)
	s.finishedSpan = s.finishedSpan || resp.finished

	// If our accumulated SST is now big enough, flush it.
	if int64(s.bufferedSST.Len()) > s.conf.targetFileSize {
		if err := s.flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func init() {
	rowexec.NewBackupDataProcessor = newBackupDataProcessor
}
