// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backupsink"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	gogotypes "github.com/gogo/protobuf/types"
)

var backupOutputTypes = []*types.T{}

var (
	priorityAfter = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"bulkio.backup.read_with_priority_after",
		"amount of time since the read-as-of time above which a BACKUP should use priority when retrying reads",
		time.Minute,
		settings.NonNegativeDuration,
		settings.WithPublic)
	delayPerAttempt = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"bulkio.backup.read_retry_delay",
		"amount of time since the read-as-of time, per-prior attempt, to wait before making another attempt",
		time.Second*5,
		settings.NonNegativeDuration,
	)
	timeoutPerAttempt = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"bulkio.backup.read_timeout",
		"amount of time after which a read attempt is considered timed out, which causes the backup to fail",
		time.Minute*5,
		settings.NonNegativeDuration,
		settings.WithPublic)

	preSplitExports = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"bulkio.backup.presplit_request_spans.enabled",
		"split the spans that will be requests before requesting them",
		metamorphic.ConstantWithTestBool("backup-presplit-spans", true),
	)

	sendExportRequestWithVerboseTracing = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"bulkio.backup.export_request_verbose_tracing",
		"send each export request with a verbose tracing span",
		metamorphic.ConstantWithTestBool("export_request_verbose_tracing", false),
		settings.WithName("bulkio.backup.verbose_tracing.enabled"),
	)

	workerCount = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"bulkio.backup.max_workers",
		"maximum number of workers to use for reading and uploading data in a backup",
		5,
		settings.PositiveInt,
	)

	testingDiscardBackupData = envutil.EnvOrDefaultBool("COCKROACH_BACKUP_TESTING_DISCARD_DATA", false)

	fileSSTSinkElasticCPUControlEnabled = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"bulkio.backup.file_sst_sink_elastic_control.enabled",
		"determines whether the file sst sink integrates with elastic CPU control",
		true,
	)
)

const (
	backupProcessorName = "backupDataProcessor"

	// minimumWorkerCount is the minimum number of workers we will try to start.
	// If we can't reserve memory for at least this many workers, the backup
	// processors will fail.
	minimumWorkerCount = 2
)

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

	spec execinfrapb.BackupDataSpec

	// cancelAndWaitForWorker cancels the producer goroutine and waits for it to
	// finish. It can be called multiple times.
	cancelAndWaitForWorker func()
	progCh                 chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	backupErr              error

	// BoundAccount that reserves the memory usage of the backup processor.
	memAcc *mon.BoundAccount

	// Aggregator that aggregates StructuredEvents emitted in the
	// backupDataProcessors' trace recording.
	agg      *tracing.TracingAggregator
	aggTimer timeutil.Timer

	// completedSpans tracks how many spans have been successfully backed up by
	// the backup processor.
	completedSpans int32
}

var (
	_ execinfra.Processor = &backupDataProcessor{}
	_ execinfra.RowSource = &backupDataProcessor{}
)

func newBackupDataProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BackupDataSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	memMonitor := flowCtx.Cfg.BackupMonitor
	if knobs, ok := flowCtx.TestingKnobs().BackupRestoreTestingKnobs.(*sql.BackupRestoreTestingKnobs); ok {
		if knobs.BackupMemMonitor != nil {
			memMonitor = knobs.BackupMemMonitor
		}
	}
	ba := memMonitor.MakeBoundAccount()
	bp := &backupDataProcessor{
		spec:   spec,
		progCh: make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress),
		memAcc: &ba,
	}
	if err := bp.Init(ctx, bp, post, backupOutputTypes, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			// This processor doesn't have any inputs to drain.
			InputsToDrain: nil,
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				bp.close()
				if bp.agg != nil {
					meta := bulk.ConstructTracingAggregatorProducerMeta(ctx,
						bp.FlowCtx.NodeID.SQLInstanceID(), bp.FlowCtx.ID, bp.agg)
					return []execinfrapb.ProducerMetadata{*meta}
				}
				return nil
			},
		}); err != nil {
		return nil, err
	}
	return bp, nil
}

// Start is part of the RowSource interface.
func (bp *backupDataProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", bp.spec.JobID)

	// Construct an Aggregator to aggregate and render AggregatorEvents emitted in
	// bps' trace recording.
	bp.agg = tracing.TracingAggregatorForContext(ctx)
	// If the aggregator is nil, we do not want the timer to fire.
	if bp.agg != nil {
		bp.aggTimer.Reset(15 * time.Second)
	}
	ctx = bp.StartInternal(ctx, backupProcessorName, bp.agg)
	ctx, cancel := context.WithCancel(ctx)

	bp.cancelAndWaitForWorker = func() {
		cancel()
		for range bp.progCh {
		}
	}
	log.Infof(ctx, "starting backup data")
	if err := bp.FlowCtx.Stopper().RunAsyncTaskEx(ctx, stop.TaskOpts{
		TaskName: "backupDataProcessor.runBackupProcessor",
		SpanOpt:  stop.ChildSpan,
	}, func(ctx context.Context) {
		bp.backupErr = runBackupProcessor(ctx, bp.FlowCtx, &bp.spec, bp.progCh, bp.memAcc)
		cancel()
		close(bp.progCh)
	}); err != nil {
		// The closure above hasn't run, so we have to do the cleanup.
		bp.backupErr = err
		cancel()
		close(bp.progCh)
	}
}

func (bp *backupDataProcessor) constructProgressProducerMeta(
	prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) *execinfrapb.ProducerMetadata {
	// Take a copy so that we can send the progress address to the output
	// processor.
	p := prog
	p.NodeID = bp.FlowCtx.NodeID.SQLInstanceID()
	p.FlowID = bp.FlowCtx.ID

	// Annotate the progress with the fraction completed by this backupDataProcessor.
	progDetails := backuppb.BackupManifest_Progress{}
	if err := gogotypes.UnmarshalAny(&prog.ProgressDetails, &progDetails); err != nil {
		log.Warningf(bp.Ctx(), "failed to unmarshal progress details %v", err)
	} else {
		totalSpans := int32(len(bp.spec.Spans) + len(bp.spec.IntroducedSpans))
		bp.completedSpans += progDetails.CompletedSpans
		if totalSpans != 0 {
			if p.CompletedFraction == nil {
				p.CompletedFraction = make(map[int32]float32)
			}
			p.CompletedFraction[bp.ProcessorID] = float32(bp.completedSpans) / float32(totalSpans)
		}
	}

	return &execinfrapb.ProducerMetadata{BulkProcessorProgress: &p}
}

// Next is part of the RowSource interface.
func (bp *backupDataProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if bp.State != execinfra.StateRunning {
		return nil, bp.DrainHelper()
	}

	select {
	case prog, ok := <-bp.progCh:
		if !ok {
			bp.MoveToDraining(bp.backupErr)
			return nil, bp.DrainHelper()
		}
		return nil, bp.constructProgressProducerMeta(prog)
	case <-bp.aggTimer.C:
		bp.aggTimer.Read = true
		bp.aggTimer.Reset(15 * time.Second)
		return nil, bulk.ConstructTracingAggregatorProducerMeta(bp.Ctx(),
			bp.FlowCtx.NodeID.SQLInstanceID(), bp.FlowCtx.ID, bp.agg)
	}
}

func (bp *backupDataProcessor) close() {
	if bp.cancelAndWaitForWorker != nil {
		bp.cancelAndWaitForWorker()
	}
	if bp.InternalClose() {
		bp.aggTimer.Stop()
		bp.memAcc.Close(bp.Ctx())
	}
}

// ConsumerClosed is part of the RowSource interface. We have to override the
// implementation provided by ProcessorBase.
func (bp *backupDataProcessor) ConsumerClosed() {
	bp.close()
}

type spanAndTime struct {
	span         roachpb.Span
	firstKeyTS   hlc.Timestamp
	start, end   hlc.Timestamp
	attempts     int
	lastTried    time.Time
	finishesSpec bool
}

func runBackupProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.BackupDataSpec,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	memAcc *mon.BoundAccount,
) error {
	backupProcessorSpan := tracing.SpanFromContext(ctx)
	clusterSettings := flowCtx.Cfg.Settings

	totalSpans := len(spec.Spans) + len(spec.IntroducedSpans)
	requestSpans := make([]spanAndTime, 0, totalSpans)
	rangeSizedSpans := preSplitExports.Get(&flowCtx.Cfg.Settings.SV)

	splitSpans := func(spans []roachpb.Span, start, end hlc.Timestamp) error {
		for _, fullSpan := range spans {
			remainingSpan := fullSpan

			if rangeSizedSpans {
				const pageSize = 100
				rdi, err := flowCtx.Cfg.ExecutorConfig.(*sql.ExecutorConfig).RangeDescIteratorFactory.NewLazyIterator(ctx, fullSpan, pageSize)
				if err != nil {
					return err
				}
				for ; rdi.Valid(); rdi.Next() {
					rangeDesc := rdi.CurRangeDescriptor()
					rangeSpan := roachpb.Span{Key: rangeDesc.StartKey.AsRawKey(), EndKey: rangeDesc.EndKey.AsRawKey()}
					subspan := remainingSpan.Intersect(rangeSpan)
					if !subspan.Valid() {
						return errors.AssertionFailedf("%s not in %s of %s", rangeSpan, remainingSpan, fullSpan)
					}
					requestSpans = append(requestSpans, spanAndTime{span: subspan, start: start, end: end})
					remainingSpan.Key = subspan.EndKey
				}
				if err := rdi.Error(); err != nil {
					return err
				}
			}

			if remainingSpan.Valid() {
				requestSpans = append(requestSpans, spanAndTime{span: remainingSpan, start: start, end: end})
			}
			requestSpans[len(requestSpans)-1].finishesSpec = true
		}
		return nil
	}

	if err := splitSpans(spec.IntroducedSpans, hlc.Timestamp{}, spec.BackupStartTime); err != nil {
		return err
	}
	if err := splitSpans(spec.Spans, spec.BackupStartTime, spec.BackupEndTime); err != nil {
		return err
	}

	log.Infof(ctx, "backup processor is assigned %d spans covering %d ranges", totalSpans, len(requestSpans))

	destURI := spec.DefaultURI
	var destLocalityKV string

	if len(spec.URIsByLocalityKV) > 0 {
		var localitySinkURI string
		// When matching, more specific KVs in the node locality take precedence
		// over less specific ones so search back to front.
		for i := len(flowCtx.EvalCtx.Locality.Tiers) - 1; i >= 0; i-- {
			tier := flowCtx.EvalCtx.Locality.Tiers[i].String()
			if dest, ok := spec.URIsByLocalityKV[tier]; ok {
				localitySinkURI = dest
				destLocalityKV = tier
				break
			}
		}
		if localitySinkURI != "" {
			log.Infof(ctx, "backing up %d spans to destination specified by locality %s", totalSpans, destLocalityKV)
			destURI = localitySinkURI
		} else {
			nodeLocalities := make([]string, 0, len(flowCtx.EvalCtx.Locality.Tiers))
			for _, i := range flowCtx.EvalCtx.Locality.Tiers {
				nodeLocalities = append(nodeLocalities, i.String())
			}
			backupLocalities := make([]string, 0, len(spec.URIsByLocalityKV))
			for i := range spec.URIsByLocalityKV {
				backupLocalities = append(backupLocalities, i)
			}
			log.Infof(ctx, "backing up %d spans to default locality because backup localities %s have no match in node's localities %s", totalSpans, backupLocalities, nodeLocalities)
		}
	}
	if testingDiscardBackupData {
		destURI = "null:///discard"
	}
	dest, err := cloud.ExternalStorageConfFromURI(destURI, spec.User())
	if err != nil {
		return err
	}

	sinkConf := backupsink.SSTSinkConf{
		ID:        flowCtx.NodeID.SQLInstanceID(),
		Enc:       spec.Encryption,
		ProgCh:    progCh,
		Settings:  &flowCtx.Cfg.Settings.SV,
		ElideMode: spec.ElidePrefix,
	}
	storage, err := flowCtx.Cfg.ExternalStorage(ctx, dest, cloud.WithClientName("backup"))
	if err != nil {
		return err
	}
	defer logClose(ctx, storage, "external storage")

	// Start start a group of goroutines which each pull spans off of `todo` and
	// send export requests. Any spans that encounter lock conflict errors during
	// Export are put back on the todo queue for later processing.
	numSenders, release, err := reserveWorkerMemory(ctx, clusterSettings, memAcc)
	if err != nil {
		return err
	}
	log.Infof(ctx, "starting %d backup export workers", numSenders)
	defer release()

	todo := make(chan []spanAndTime, len(requestSpans))

	const maxChunkSize = 100
	// Aim to make at least 4 chunks per worker, ensuring size is >=1 and <= max.
	chunkSize := (len(requestSpans) / (numSenders * 4)) + 1
	if chunkSize > maxChunkSize {
		chunkSize = maxChunkSize
	}

	chunk := make([]spanAndTime, 0, chunkSize)
	for i := range requestSpans {
		if !rangeSizedSpans {
			todo <- []spanAndTime{requestSpans[i]}
			continue
		}

		chunk = append(chunk, requestSpans[i])
		if len(chunk) > chunkSize {
			todo <- chunk
			chunk = make([]spanAndTime, 0, chunkSize)
		}
	}
	if len(chunk) > 0 {
		todo <- chunk
	}
	return ctxgroup.GroupWorkers(ctx, numSenders, func(ctx context.Context, _ int) error {
		readTime := spec.BackupEndTime.GoTime()

		// Passing a nil pacer is effectively a noop if CPU control is disabled.
		var pacer *admission.Pacer = nil
		if fileSSTSinkElasticCPUControlEnabled.Get(&clusterSettings.SV) {
			tenantID, ok := roachpb.ClientTenantFromContext(ctx)
			if !ok {
				tenantID = roachpb.SystemTenantID
			}
			pacer = flowCtx.Cfg.AdmissionPacerFactory.NewPacer(
				100*time.Millisecond,
				admission.WorkInfo{
					TenantID:        tenantID,
					Priority:        admissionpb.BulkNormalPri,
					CreateTime:      timeutil.Now().UnixNano(),
					BypassAdmission: false,
				},
			)
		}
		// It is safe to close a nil pacer.
		defer pacer.Close()

		sink := backupsink.MakeFileSSTSink(sinkConf, storage, pacer)
		defer func() {
			if err := sink.Flush(ctx); err != nil {
				log.Warningf(ctx, "failed to flush SST sink: %s", err)
			}
			logClose(ctx, sink, "SST sink")
		}()

		// priority becomes true when we're sending re-attempts of reads far enough
		// in the past that we want to run them with priority.
		var priority bool
		var timer timeutil.Timer
		defer timer.Stop()

		ctxDone := ctx.Done()
		for {
			select {
			case <-ctxDone:
				return ctx.Err()
			case spans := <-todo:
				for _, span := range spans {
					resumed := false
					for len(span.span.Key) != 0 {
						req := &kvpb.ExportRequest{
							RequestHeader:          kvpb.RequestHeaderFromSpan(span.span),
							ResumeKeyTS:            span.firstKeyTS,
							StartTime:              span.start,
							MVCCFilter:             spec.MVCCFilter,
							TargetFileSize:         batcheval.ExportRequestTargetFileSize.Get(&clusterSettings.SV),
							SplitMidKey:            true,
							IncludeMVCCValueHeader: spec.IncludeMVCCValueHeader,
						}

						// If we're doing re-attempts but are not yet in the priority regime,
						// check to see if it is time to switch to priority.
						if !priority && span.attempts > 0 {
							// Check if this is starting a new pass and we should delay first.
							// We're okay with delaying this worker until then since we assume any
							// other work it could pull off the queue will likely want to delay to
							// a similar or later time anyway.
							if delay := delayPerAttempt.Get(&clusterSettings.SV) - timeutil.Since(span.lastTried); delay > 0 {
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

						header := kvpb.Header{
							// We set the DistSender response target bytes field to a sentinel
							// value. The sentinel value of 1 forces the ExportRequest to paginate
							// after creating a single SST.
							TargetBytes:                 1,
							Timestamp:                   span.end,
							ReturnElasticCPUResumeSpans: true,
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

						admissionHeader := kvpb.AdmissionHeader{
							// Export requests are currently assigned BulkNormalPri.
							//
							// TODO(dt): Consider linking this to/from the UserPriority field.
							Priority:                 int32(admissionpb.BulkNormalPri),
							CreateTime:               timeutil.Now().UnixNano(),
							Source:                   kvpb.AdmissionHeader_FROM_SQL,
							NoMemoryReservedAtSource: true,
						}
						log.VEventf(ctx, 1, "sending ExportRequest for span %s (attempt %d, priority %s)",
							span.span, span.attempts+1, header.UserPriority.String())
						var rawResp kvpb.Response
						var recording tracingpb.Recording
						var pErr *kvpb.Error
						requestSentAt := timeutil.Now()
						exportRequestErr := timeutil.RunWithTimeout(ctx,
							redact.Sprintf("ExportRequest for span %s", span.span),
							timeoutPerAttempt.Get(&clusterSettings.SV), func(ctx context.Context) error {
								sp := tracing.SpanFromContext(ctx)
								tracer := sp.Tracer()
								if tracer == nil {
									tracer = flowCtx.Cfg.Tracer
								}
								if tracer == nil {
									log.Warning(ctx, "nil tracer in backup processor")
								}
								opts := make([]tracing.SpanOption, 0)
								opts = append(opts, tracing.WithParent(sp))
								if sendExportRequestWithVerboseTracing.Get(&clusterSettings.SV) {
									opts = append(opts, tracing.WithRecording(tracingpb.RecordingVerbose))
								}
								ctx, exportSpan := tracer.StartSpanCtx(ctx, "backup.ExportRequest", opts...)
								rawResp, pErr = kv.SendWrappedWithAdmission(
									ctx, flowCtx.Cfg.DB.KV().NonTransactionalSender(), header, admissionHeader, req)
								recording = exportSpan.FinishAndGetConfiguredRecording()
								if pErr != nil {
									return pErr.GoError()
								}
								return nil
							})
						if exportRequestErr != nil {
							// If we got a write intent error because we requested it rather
							// than blocking, either put the request on the back of the queue
							// to revisit later or, if the request was resuming in the middle
							// of a key, reattempt it immediately.
							if lockErr, ok := pErr.GetDetail().(*kvpb.WriteIntentError); ok && header.WaitPolicy == lock.WaitPolicy_Error {
								// TODO(dt): send a progress update to update job progress to note
								// the intents being hit.
								span.lastTried = timeutil.Now()
								span.attempts++
								log.VEventf(ctx, 1, "retrying ExportRequest for span %s; encountered WriteIntentError: %s", span.span, lockErr.Error())
								// If we're not mid-span we can put this on the the queue to
								// give it time to resolve on its own while we work on other
								// spans; if we've flushed any of this span though we finish it
								// so that we get to a known row end key for our backed up span.
								if !resumed {
									todo <- []spanAndTime{span}
									span = spanAndTime{}
								}
								continue
							}
							// TimeoutError improves the opaque `context deadline exceeded` error
							// message so use that instead.
							if errors.HasType(exportRequestErr, (*timeutil.TimeoutError)(nil)) {
								if recording != nil {
									log.Errorf(ctx, "failed export request for span %s\n trace:\n%s", span.span, recording)
								}
								return errors.Wrap(exportRequestErr, "KV storage layer did not respond to BACKUP within timeout")
							}
							// BatchTimestampBeforeGCError is returned if the ExportRequest
							// attempts to read below the range's GC threshold.
							if batchTimestampBeforeGCError, ok := pErr.GetDetail().(*kvpb.BatchTimestampBeforeGCError); ok {
								// If the range we are exporting is marked to be excluded from
								// backup, it is safe to ignore the error. It is likely that the
								// table has been configured with a low GC TTL, and so the data
								// the backup is targeting has already been gc'ed.
								if batchTimestampBeforeGCError.DataExcludedFromBackup {
									span = spanAndTime{}
									continue
								}
							}

							if recording != nil {
								log.Errorf(ctx, "failed export request %s\n trace:\n%s", span.span, recording)
							}
							return errors.Wrapf(exportRequestErr, "exporting %s", span.span)
						}

						resp := rawResp.(*kvpb.ExportResponse)

						// If the reply has a resume span, we process it immediately.
						var resumeSpan spanAndTime
						if resp.ResumeSpan != nil {
							if !resp.ResumeSpan.Valid() {
								return errors.Errorf("invalid resume span: %s", resp.ResumeSpan)
							}
							resumed = true
							resumeTS := hlc.Timestamp{}
							// Taking resume timestamp from the last file of response since files must
							// always be consecutive even if we currently expect only one.
							if fileCount := len(resp.Files); fileCount > 0 {
								resumeTS = resp.Files[fileCount-1].EndKeyTS
							}
							resumeSpan = span
							resumeSpan.span = *resp.ResumeSpan
							resumeSpan.firstKeyTS = resumeTS
						}

						if backupKnobs, ok := flowCtx.TestingKnobs().BackupRestoreTestingKnobs.(*sql.BackupRestoreTestingKnobs); ok {
							if backupKnobs.RunAfterExportingSpanEntry != nil {
								backupKnobs.RunAfterExportingSpanEntry(ctx, resp)
							}
						}

						var completedSpans int32
						if span.finishesSpec && resp.ResumeSpan == nil {
							completedSpans = 1
						}

						if len(resp.Files) > 1 {
							log.Warning(ctx, "unexpected multi-file response using header.TargetBytes = 1")
						}

						// Even if the ExportRequest did not export any data we want to report
						// the span as completed for accurate progress tracking.
						if len(resp.Files) == 0 {
							sink.WriteWithNoData(backupsink.ExportedSpan{CompletedSpans: completedSpans})
						}
						for i, file := range resp.Files {
							entryCounts := countRows(file.Exported, spec.PKIDs)

							ret := backupsink.ExportedSpan{
								// BackupManifest_File just happens to contain the exact fields
								// to store the metadata we need, but there's no actual File
								// on-disk anywhere yet.
								Metadata: backuppb.BackupManifest_File{
									Span:                    file.Span,
									EntryCounts:             entryCounts,
									LocalityKV:              destLocalityKV,
									ApproximatePhysicalSize: uint64(len(file.SST)),
								},
								DataSST:  file.SST,
								RevStart: resp.StartTime,
							}
							if resp.ResumeSpan != nil {
								ret.ResumeKey = resumeSpan.span.Key
							}
							if span.start != spec.BackupStartTime {
								ret.Metadata.StartTime = span.start
								ret.Metadata.EndTime = span.end
							}
							// If multiple files were returned for this span, only one -- the
							// last -- should count as completing the requested span.
							if i == len(resp.Files)-1 {
								ret.CompletedSpans = completedSpans
							}

							// Cannot set the error to err, which is shared across workers.
							var writeErr error
							resumeSpan.span.Key, writeErr = sink.Write(ctx, ret)
							if writeErr != nil {
								return err
							}
						}
						// Emit the stats for the processed ExportRequest.
						recordExportStats(backupProcessorSpan, resp, requestSentAt)
						span = resumeSpan
					}
				}
			default:
				// No work left to do, so we can exit. Note that another worker could
				// still be running and may still push new work (a retry) on to todo but
				// that is OK, since that also means it is still running and thus can
				// pick up that work on its next iteration.
				return sink.Flush(ctx)
			}
		}
	})
}

// recordExportStats emits a StructuredEvent containing the stats about the
// evaluated ExportRequest.
func recordExportStats(sp *tracing.Span, resp *kvpb.ExportResponse, requestSentAt time.Time) {
	if sp == nil {
		return
	}
	now := timeutil.Now()
	exportStats := backuppb.ExportStats{
		StartTime: hlc.Timestamp{WallTime: requestSentAt.UnixNano()},
		EndTime:   hlc.Timestamp{WallTime: now.UnixNano()},
		Duration:  now.Sub(requestSentAt),
	}
	for _, f := range resp.Files {
		exportStats.NumFiles++
		exportStats.DataSize += int64(len(f.SST))
	}
	sp.RecordStructured(&exportStats)
}

// reserveWorkerMemory returns the number of workers after reserving the appropriate
// amount of memory for each worker.
func reserveWorkerMemory(
	ctx context.Context, settings *cluster.Settings, memAcc *mon.BoundAccount,
) (int, func(), error) {
	maxWorkerCount := int(workerCount.Get(&settings.SV))
	// We assume that each worker needs at least enough memory to hold onto
	// 1 buffer used by the external storage.
	perWorkerMemory := cloud.WriteChunkSize.Get(&settings.SV)
	// TODO(ssd): We could also add the size of the SST we might be holding here.
	// Previously we would reserve a fixed-size buffer, but we left the possibly
	// in-flight SSTs unaccounted for.
	// perWorkerMemory = perWorkerMemory + batcheval.ExportRequestTargetFileSize.Get(&settings.SV)
	if err := memAcc.Grow(ctx, minimumWorkerCount*perWorkerMemory); err != nil {
		return 0, nil, errors.Wrapf(err, "could not reserve memory for minimum number of backup workers (%d * %d)", minimumWorkerCount, perWorkerMemory)
	}

	workerCount := minimumWorkerCount
	for i := 0; i < (maxWorkerCount - minimumWorkerCount); i++ {
		if err := memAcc.Grow(ctx, perWorkerMemory); err != nil {
			log.Warningf(ctx, "backup worker count restricted by memory limit")
			break
		}
		workerCount++
	}
	return workerCount, func() { memAcc.Shrink(ctx, int64(workerCount)*perWorkerMemory) }, nil
}

func logClose(ctx context.Context, c io.Closer, desc string) {
	if err := c.Close(); err != nil {
		log.Warningf(ctx, "failed to close %s: %s", redact.SafeString(desc), err.Error())
	}
}

func init() {
	rowexec.NewBackupDataProcessor = newBackupDataProcessor
}
