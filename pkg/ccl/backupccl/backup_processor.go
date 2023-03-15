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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
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
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

var backupOutputTypes = []*types.T{}

var (
	priorityAfter = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"bulkio.backup.read_with_priority_after",
		"amount of time since the read-as-of time above which a BACKUP should use priority when retrying reads",
		time.Minute,
		settings.NonNegativeDuration,
	).WithPublic()
	delayPerAttmpt = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"bulkio.backup.read_retry_delay",
		"amount of time since the read-as-of time, per-prior attempt, to wait before making another attempt",
		time.Second*5,
		settings.NonNegativeDuration,
	)
	timeoutPerAttempt = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"bulkio.backup.read_timeout",
		"amount of time after which a read attempt is considered timed out, which causes the backup to fail",
		time.Minute*5,
		settings.NonNegativeDuration,
	).WithPublic()
	targetFileSize = settings.RegisterByteSizeSetting(
		settings.TenantWritable,
		"bulkio.backup.file_size",
		"target size for individual data files produced during BACKUP",
		128<<20,
	).WithPublic()

	splitKeysOnTimestamps = settings.RegisterBoolSetting(
		settings.TenantWritable,
		"bulkio.backup.split_keys_on_timestamps",
		"split backup data on timestamps when writing revision history",
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

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.BackupDataSpec

	// cancelAndWaitForWorker cancels the producer goroutine and waits for it to
	// finish. It can be called multiple times.
	cancelAndWaitForWorker func()
	progCh                 chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	backupErr              error

	// BoundAccount that reserves the memory usage of the backup processor.
	memAcc *mon.BoundAccount

	// Aggregator that aggregates StructuredEvents emitted in the
	// backupDataProcessors' trace recording.
	agg *bulk.TracingAggregator
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
		flowCtx: flowCtx,
		spec:    spec,
		progCh:  make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress),
		memAcc:  &ba,
	}
	if err := bp.Init(ctx, bp, post, backupOutputTypes, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			// This processor doesn't have any inputs to drain.
			InputsToDrain: nil,
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				bp.close()
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
	ctx = bp.StartInternal(ctx, backupProcessorName)
	ctx, cancel := context.WithCancel(ctx)

	// Construct an Aggregator to aggregate and render AggregatorEvents emitted in
	// bps' trace recording.
	ctx, bp.agg = bulk.MakeTracingAggregatorWithSpan(ctx,
		fmt.Sprintf("%s-aggregator", backupProcessorName), bp.EvalCtx.Tracer)

	bp.cancelAndWaitForWorker = func() {
		cancel()
		for range bp.progCh {
		}
	}
	log.Infof(ctx, "starting backup data")
	if err := bp.flowCtx.Stopper().RunAsyncTaskEx(ctx, stop.TaskOpts{
		TaskName: "backupDataProcessor.runBackupProcessor",
		SpanOpt:  stop.ChildSpan,
	}, func(ctx context.Context) {
		bp.backupErr = runBackupProcessor(ctx, bp.flowCtx, &bp.spec, bp.progCh, bp.memAcc)
		cancel()
		close(bp.progCh)
	}); err != nil {
		// The closure above hasn't run, so we have to do the cleanup.
		bp.backupErr = err
		cancel()
		close(bp.progCh)
	}
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

func (bp *backupDataProcessor) close() {
	bp.cancelAndWaitForWorker()
	bp.agg.Close()
	if bp.InternalClose() {
		bp.memAcc.Close(bp.Ctx())
	}
}

// ConsumerClosed is part of the RowSource interface. We have to override the
// implementation provided by ProcessorBase.
func (bp *backupDataProcessor) ConsumerClosed() {
	bp.close()
}

type spanAndTime struct {
	// spanIdx is a unique identifier of this object.
	spanIdx    int
	span       roachpb.Span
	firstKeyTS hlc.Timestamp
	start, end hlc.Timestamp
	attempts   int
	lastTried  time.Time
}

type exportedSpan struct {
	metadata       backuppb.BackupManifest_File
	dataSST        []byte
	revStart       hlc.Timestamp
	completedSpans int32
	atKeyBoundary  bool
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
	todo := make(chan spanAndTime, totalSpans)
	var spanIdx int
	for _, s := range spec.IntroducedSpans {
		todo <- spanAndTime{
			spanIdx: spanIdx, span: s, firstKeyTS: hlc.Timestamp{}, start: hlc.Timestamp{},
			end: spec.BackupStartTime,
		}
		spanIdx++
	}
	for _, s := range spec.Spans {
		todo <- spanAndTime{
			spanIdx: spanIdx, span: s, firstKeyTS: hlc.Timestamp{}, start: spec.BackupStartTime,
			end: spec.BackupEndTime,
		}
		spanIdx++
	}

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
	dest, err := cloud.ExternalStorageConfFromURI(destURI, spec.User())
	if err != nil {
		return err
	}

	sinkConf := sstSinkConf{
		id:       flowCtx.NodeID.SQLInstanceID(),
		enc:      spec.Encryption,
		progCh:   progCh,
		settings: &flowCtx.Cfg.Settings.SV,
	}
	storage, err := flowCtx.Cfg.ExternalStorage(ctx, dest)
	if err != nil {
		return err
	}
	defer logClose(ctx, storage, "external storage")

	// Start start a group of goroutines which each pull spans off of `todo` and
	// send export requests. Any spans that encounter write intent errors during
	// Export are put back on the todo queue for later processing.
	numSenders, release, err := reserveWorkerMemory(ctx, clusterSettings, memAcc)
	if err != nil {
		return err
	}
	log.Infof(ctx, "starting %d backup export workers", numSenders)
	defer release()

	return ctxgroup.GroupWorkers(ctx, numSenders, func(ctx context.Context, _ int) error {
		readTime := spec.BackupEndTime.GoTime()
		sink := makeFileSSTSink(sinkConf, storage)
		defer func() {
			if err := sink.flush(ctx); err != nil {
				log.Warningf(ctx, "failed to flush SST sink: %s", err)
			}
			logClose(ctx, sink, "SST sink")
		}()

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
				for len(span.span.Key) != 0 {
					splitMidKey := splitKeysOnTimestamps.Get(&clusterSettings.SV)
					// If we started splitting already, we must continue until we reach the end
					// of split span.
					if !span.firstKeyTS.IsEmpty() {
						splitMidKey = true
					}

					req := &kvpb.ExportRequest{
						RequestHeader:  kvpb.RequestHeaderFromSpan(span.span),
						ResumeKeyTS:    span.firstKeyTS,
						StartTime:      span.start,
						MVCCFilter:     spec.MVCCFilter,
						TargetFileSize: batcheval.ExportRequestTargetFileSize.Get(&clusterSettings.SV),
						SplitMidKey:    splitMidKey,
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
					var pErr *kvpb.Error
					requestSentAt := timeutil.Now()
					exportRequestErr := contextutil.RunWithTimeout(ctx,
						fmt.Sprintf("ExportRequest for span %s", span.span),
						timeoutPerAttempt.Get(&clusterSettings.SV), func(ctx context.Context) error {
							rawResp, pErr = kv.SendWrappedWithAdmission(
								ctx, flowCtx.Cfg.DB.KV().NonTransactionalSender(), header, admissionHeader, req)
							if pErr != nil {
								return pErr.GoError()
							}
							return nil
						})
					if exportRequestErr != nil {
						if intentErr, ok := pErr.GetDetail().(*kvpb.WriteIntentError); ok {
							span.lastTried = timeutil.Now()
							span.attempts++
							todo <- span
							// TODO(dt): send a progress update to update job progress to note
							// the intents being hit.
							log.VEventf(ctx, 1, "retrying ExportRequest for span %s; encountered WriteIntentError: %s", span.span, intentErr.Error())
							span = spanAndTime{}
							continue
						}
						// TimeoutError improves the opaque `context deadline exceeded` error
						// message so use that instead.
						if errors.HasType(exportRequestErr, (*contextutil.TimeoutError)(nil)) {
							return errors.Wrap(exportRequestErr, "export request timeout")
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
						return errors.Wrapf(exportRequestErr, "exporting %s", span.span)
					}

					resp := rawResp.(*kvpb.ExportResponse)

					// If the reply has a resume span, we process it immediately.
					var resumeSpan spanAndTime
					if resp.ResumeSpan != nil {
						if !resp.ResumeSpan.Valid() {
							return errors.Errorf("invalid resume span: %s", resp.ResumeSpan)
						}

						resumeTS := hlc.Timestamp{}
						// Taking resume timestamp from the last file of response since files must
						// always be consecutive even if we currently expect only one.
						if fileCount := len(resp.Files); fileCount > 0 {
							resumeTS = resp.Files[fileCount-1].EndKeyTS
						}
						resumeSpan = spanAndTime{
							span:       *resp.ResumeSpan,
							firstKeyTS: resumeTS,
							start:      span.start,
							end:        span.end,
							attempts:   span.attempts,
							lastTried:  span.lastTried,
						}
					}

					if backupKnobs, ok := flowCtx.TestingKnobs().BackupRestoreTestingKnobs.(*sql.BackupRestoreTestingKnobs); ok {
						if backupKnobs.RunAfterExportingSpanEntry != nil {
							backupKnobs.RunAfterExportingSpanEntry(ctx, resp)
						}
					}

					var completedSpans int32
					if resp.ResumeSpan == nil {
						completedSpans = 1
					}

					if len(resp.Files) > 1 {
						log.Warning(ctx, "unexpected multi-file response using header.TargetBytes = 1")
					}

					for i, file := range resp.Files {
						entryCounts := countRows(file.Exported, spec.PKIDs)

						ret := exportedSpan{
							// BackupManifest_File just happens to contain the exact fields
							// to store the metadata we need, but there's no actual File
							// on-disk anywhere yet.
							metadata: backuppb.BackupManifest_File{
								Span:        file.Span,
								Path:        file.Path,
								EntryCounts: entryCounts,
								LocalityKV:  destLocalityKV,
							},
							dataSST:       file.SST,
							revStart:      resp.StartTime,
							atKeyBoundary: file.EndKeyTS.IsEmpty()}
						if span.start != spec.BackupStartTime {
							ret.metadata.StartTime = span.start
							ret.metadata.EndTime = span.end
						}
						// If multiple files were returned for this span, only one -- the
						// last -- should count as completing the requested span.
						if i == len(resp.Files)-1 {
							ret.completedSpans = completedSpans
						}

						if err := sink.write(ctx, ret); err != nil {
							return err
						}
					}
					// Emit the stats for the processed ExportRequest.
					recordExportStats(backupProcessorSpan, resp, requestSentAt)
					span = resumeSpan
				}
			default:
				// No work left to do, so we can exit. Note that another worker could
				// still be running and may still push new work (a retry) on to todo but
				// that is OK, since that also means it is still running and thus can
				// pick up that work on its next iteration.
				return sink.flush(ctx)
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
	// TODO(pbardea): Check to see if this benefits from any tuning (e.g. +1, or
	//  *2). See #49798.
	maxWorkerCount := int(kvserver.ExportRequestsLimit.Get(&settings.SV)) * 2
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
