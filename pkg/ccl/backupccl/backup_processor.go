// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"io"
	"sort"
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
	alwaysWriteInProc = settings.RegisterBoolSetting(
		"bulkio.backup.proxy_file_writes.enabled",
		"return files to the backup coordination processes to write to "+
			"external storage instead of writing them directly from the storage layer",
		false,
	)
	smallFileSize = settings.RegisterByteSizeSetting(
		"bulkio.backup.merge_file_size",
		"size under which backup files will be forwarded to another node to be merged with other smaller files "+
			"(and implies files will be buffered in-memory until this size before being written to backup storage)",
		16<<20,
		settings.NonNegativeInt,
	)
	smallFileBuffer = settings.RegisterByteSizeSetting(
		"bulkio.backup.merge_file_buffer_size",
		"size limit used when buffering backup files before merging them",
		16<<20,
		settings.NonNegativeInt,
	)
)

// maxSinkQueueFiles is how many replies we'll queue up before flushing to allow
// some re-ordering, unless we hit smallFileBuffer size first.
const maxSinkQueueFiles = 24

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
	f              BackupManifest_File
	sst            []byte
	revStart       hlc.Timestamp
	completedSpans int32
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
	writeSSTsInProcessor := !flowCtx.Cfg.Codec.ForSystemTenant() || alwaysWriteInProc.Get(&clusterSettings.SV)

	returnedSSTs := make(chan returnedSST, 1)

	grp := ctxgroup.WithContext(ctx)
	// Start a goroutine that will then start a group of goroutines which each
	// pull spans off of `todo` and send export requests. Any resume spans are put
	// back on `todo`. Any returned SSTs are put on a  `returnedSSTs` to be routed
	// to a buffered sink that merges them until they are large enough to flush.
	grp.GoCtx(func(ctx context.Context) error {
		defer close(returnedSSTs)
		// TODO(pbardea): Check to see if this benefits from any tuning (e.g. +1, or
		//  *2). See #49798.
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
					header := roachpb.Header{Timestamp: span.end}
					req := &roachpb.ExportRequest{
						RequestHeader:                       roachpb.RequestHeaderFromSpan(span.span),
						StorageByLocalityKV:                 storageConfByLocalityKV,
						StartTime:                           span.start,
						EnableTimeBoundIteratorOptimization: useTBI.Get(&clusterSettings.SV),
						MVCCFilter:                          spec.MVCCFilter,
						TargetFileSize:                      targetFileSize,
						ReturnSstBelowSize:                  smallFileSize.Get(&clusterSettings.SV),
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
								RetryableError: tracing.RedactAndTruncateError(intentErr)})
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

					var completedSpans int32
					if res.ResumeSpan == nil {
						completedSpans = 1
					}

					duration := respReceivedTime.Sub(reqSentTime)
					exportResponseTraceEvent := &BackupExportTraceResponseEvent{
						Duration:      duration.String(),
						FileSummaries: make([]RowCount, 0),
					}
					var numFiles int
					files := make([]BackupManifest_File, 0)
					for i, file := range res.Files {
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
							ret := returnedSST{f: f, sst: file.SST, revStart: res.StartTime}
							// If multiple files were returned for this span, only one -- the
							// last -- should count as completing the requested span.
							if i == len(files)-1 {
								ret.completedSpans = completedSpans
							}
							select {
							case returnedSSTs <- ret:
							case <-ctxDone:
								return ctx.Err()
							}
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
							RevStartTime:   res.StartTime,
							Files:          files,
							CompletedSpans: completedSpans,
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
			settings:       &flowCtx.Cfg.Settings.SV,
		}

		defaultStore, err := flowCtx.Cfg.ExternalStorage(ctx, defaultConf)
		if err != nil {
			return err
		}

		localitySinks := make(map[string]*sstSink)
		defaultSink := &sstSink{conf: sinkConf, dest: defaultStore}

		defer func() {
			for i := range localitySinks {
				err := localitySinks[i].Close()
				err = errors.CombineErrors(localitySinks[i].dest.Close(), err)
				if err != nil {
					log.Warningf(ctx, "failed to close backup sink(s): %+v", err)
				}
			}
			err := defaultSink.Close()
			err = errors.CombineErrors(defaultStore.Close(), err)
			if err != nil {
				log.Warningf(ctx, "failed to close backup sink(s): %+v", err)
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

			if err := sink.push(ctx, res); err != nil {
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
	settings       *settings.Values
}

type sstSink struct {
	dest cloud.ExternalStorage
	conf sstSinkConf

	queue     []returnedSST
	queueSize int

	sst     storage.SSTWriter
	ctx     context.Context
	cancel  func()
	out     io.WriteCloser
	outName string

	flushedFiles    []BackupManifest_File
	flushedSize     int64
	flushedRevStart hlc.Timestamp
	completedSpans  int32

	stats struct {
		files       int
		flushes     int
		oooFlushes  int
		sizeFlushes int
		spanGrows   int
	}
}

func (s *sstSink) Close() error {
	if log.V(1) && s.ctx != nil {
		log.Infof(s.ctx, "backup sst sink recv'd %d files, wrote %d (%d due to size, %d due to re-ordering), %d recv files extended prior span",
			s.stats.files, s.stats.flushes, s.stats.sizeFlushes, s.stats.oooFlushes, s.stats.spanGrows)
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.out != nil {
		return s.out.Close()
	}
	return nil
}

// push pushes one returned backup file into the sink. Returned files can arrive
// out of order, but must be written to an underlying file in-order or else a
// new underlying file has to be opened. The queue allows buffering up files and
// sorting them before pushing them to the underlying file to try to avoid this.
// When the queue length or sum of the data sizes in it exceeds thresholds the
// queue is sorted and the first half is flushed.
func (s *sstSink) push(ctx context.Context, resp returnedSST) error {
	s.queue = append(s.queue, resp)
	s.queueSize += len(resp.sst)

	if len(s.queue) >= maxSinkQueueFiles || s.queueSize >= int(smallFileBuffer.Get(s.conf.settings)) {
		sort.Slice(s.queue, func(i, j int) bool { return s.queue[i].f.Span.Key.Compare(s.queue[j].f.Span.Key) < 0 })

		// Drain the first half.
		drain := len(s.queue) / 2
		if drain < 1 {
			drain = 1
		}
		for i := range s.queue[:drain] {
			if err := s.write(ctx, s.queue[i]); err != nil {
				return err
			}
			s.queueSize -= len(s.queue[i].sst)
		}

		// Shift down the remainder of the queue and slice off the tail.
		copy(s.queue, s.queue[drain:])
		s.queue = s.queue[:len(s.queue)-drain]
	}
	return nil
}

func (s *sstSink) flush(ctx context.Context) error {
	for i := range s.queue {
		if err := s.write(ctx, s.queue[i]); err != nil {
			return err
		}
	}
	s.queue = nil
	return s.flushFile(ctx)
}

func (s *sstSink) flushFile(ctx context.Context) error {
	if s.out == nil {
		return nil
	}
	s.stats.flushes++

	if err := s.sst.Finish(); err != nil {
		return err
	}
	if err := s.out.Close(); err != nil {
		return errors.Wrap(err, "writing SST")
	}
	s.outName = ""
	s.out = nil

	progDetails := BackupManifest_Progress{
		RevStartTime:   s.flushedRevStart,
		Files:          s.flushedFiles,
		CompletedSpans: s.completedSpans,
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

	s.flushedFiles = nil
	s.flushedSize = 0
	s.flushedRevStart.Reset()
	s.completedSpans = 0

	return nil
}

func (s *sstSink) open(ctx context.Context) error {
	s.outName = storageccl.GenerateUniqueSSTName(s.conf.id)
	if s.ctx == nil {
		s.ctx, s.cancel = context.WithCancel(ctx)
	}
	w, err := s.dest.Writer(s.ctx, s.outName)
	if err != nil {
		return err
	}
	if s.conf.enc != nil {
		var err error
		w, err = storageccl.EncryptingWriter(w, s.conf.enc.Key)
		if err != nil {
			return err
		}
	}
	s.out = w
	s.sst = storage.MakeBackupSSTWriter(s.out)
	return nil
}

func (s *sstSink) write(ctx context.Context, resp returnedSST) error {
	s.stats.files++

	span := resp.f.Span

	// If this span starts before the last buffered span ended, we need to flush
	// since it overlaps but SSTWriter demands writes in-order.
	if len(s.flushedFiles) > 0 && span.Key.Compare(s.flushedFiles[len(s.flushedFiles)-1].Span.EndKey) < 0 {
		s.stats.oooFlushes++
		if err := s.flushFile(ctx); err != nil {
			return err
		}
	}

	// Initialize the writer if needed.
	if s.out == nil {
		if err := s.open(ctx); err != nil {
			return err
		}
	}

	// Copy SST content.
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
			if err := s.sst.PutUnversioned(k.Key, sst.UnsafeValue()); err != nil {
				return err
			}
		} else {
			if err := s.sst.PutMVCC(sst.UnsafeKey(), sst.UnsafeValue()); err != nil {
				return err
			}
		}
		sst.Next()
	}

	// If this span extended the last span added -- that is, picked up where it
	// ended and has the same time-bounds -- then we can simply extend that span
	// and add to its entry counts. Otherwise we need to record it separately.
	if l := len(s.flushedFiles) - 1; l > 0 && s.flushedFiles[l].Span.EndKey.Equal(span.Key) &&
		s.flushedFiles[l].EndTime.EqOrdering(resp.f.EndTime) &&
		s.flushedFiles[l].StartTime.EqOrdering(resp.f.StartTime) {
		s.flushedFiles[l].Span.EndKey = span.EndKey
		s.flushedFiles[l].EntryCounts.add(resp.f.EntryCounts)
		s.stats.spanGrows++
	} else {
		f := resp.f
		f.Path = s.outName
		s.flushedFiles = append(s.flushedFiles, f)
	}
	s.flushedRevStart.Forward(resp.revStart)
	s.completedSpans += resp.completedSpans
	s.flushedSize += int64(len(resp.sst))

	// If our accumulated SST is now big enough, flush it.
	if s.flushedSize > s.conf.targetFileSize {
		s.stats.sizeFlushes++
		if err := s.flushFile(ctx); err != nil {
			return err
		}
	}
	return nil
}

func init() {
	rowexec.NewBackupDataProcessor = newBackupDataProcessor
}
