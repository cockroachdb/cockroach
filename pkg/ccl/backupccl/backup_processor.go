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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	hlc "github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	priorityAfter = settings.RegisterNonNegativeDurationSetting(
		"bulkio.backup.read_with_priority_after",
		"age of read-as-of time above which a BACKUP should read with priority",
		time.Minute,
	)
	delayPerAttmpt = settings.RegisterNonNegativeDurationSetting(
		"bulkio.backup.read_retry_delay",
		"amount of time since the read-as-of time, per-prior attempt, to wait before making another attempt",
		time.Second*5,
	)
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
	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.BackupDataSpec
	output  execinfra.RowReceiver
}

var _ execinfra.Processor = &backupDataProcessor{}

func (cp *backupDataProcessor) OutputTypes() []*types.T {
	return backupOutputTypes
}

func newBackupDataProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BackupDataSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	cp := &backupDataProcessor{
		flowCtx: flowCtx,
		spec:    spec,
		output:  output,
	}
	return cp, nil
}

func (cp *backupDataProcessor) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "backupDataProcessor")
	defer span.Finish()
	defer cp.output.ProducerDone()

	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)

	var err error
	// We don't have to worry about this go routine leaking because next we loop over progCh
	// which is closed only after the go routine returns.
	go func() {
		defer close(progCh)
		err = runBackupProcessor(ctx, cp.flowCtx, &cp.spec, progCh)
	}()

	for prog := range progCh {
		// Take a copy so that we can send the progress address to the output processor.
		p := prog
		cp.output.Push(nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &p})
	}

	if err != nil {
		cp.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
	}
}

type spanAndTime struct {
	span       roachpb.Span
	start, end hlc.Timestamp
	attempts   int
	lastTried  time.Time
}

func runBackupProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.BackupDataSpec,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	settings := flowCtx.Cfg.Settings

	todo := make(chan spanAndTime, len(spec.Spans)+len(spec.IntroducedSpans))
	for _, s := range spec.IntroducedSpans {
		todo <- spanAndTime{span: s, start: hlc.Timestamp{}, end: spec.BackupStartTime}
	}
	for _, s := range spec.Spans {
		todo <- spanAndTime{span: s, start: spec.BackupStartTime, end: spec.BackupEndTime}
	}

	// TODO(pbardea): Check to see if this benefits from any tuning (e.g. +1, or
	//  *2). See #49798.
	numSenders := int(kvserver.ExportRequestsLimit.Get(&settings.SV)) * 2
	targetFileSize := storageccl.ExportRequestTargetFileSize.Get(&settings.SV)

	// For all backups, partitioned or not, the main BACKUP manifest is stored at
	// details.URI.
	defaultConf, err := cloudimpl.ExternalStorageConfFromURI(spec.DefaultURI, spec.User())
	if err != nil {
		return err
	}
	storageByLocalityKV := make(map[string]*roachpb.ExternalStorage)
	for kv, uri := range spec.URIsByLocalityKV {
		conf, err := cloudimpl.ExternalStorageConfFromURI(uri, spec.User())
		if err != nil {
			return err
		}
		storageByLocalityKV[kv] = &conf
	}

	return ctxgroup.GroupWorkers(ctx, numSenders, func(ctx context.Context, _ int) error {
		readTime := spec.BackupEndTime.GoTime()

		// priority becomes true when we're sending re-attempts of reads far enough
		// in the past that we want to run them with priority.
		var priority bool
		timer := timeutil.NewTimer()
		defer timer.Stop()

		done := ctx.Done()
		for {
			select {
			case <-done:
				return ctx.Err()
			case span := <-todo:
				// TODO(pbardea): It would be nice if we could avoid producing many small
				//  SSTs. See #44480.
				header := roachpb.Header{Timestamp: span.end}
				req := &roachpb.ExportRequest{
					RequestHeader:                       roachpb.RequestHeaderFromSpan(span.span),
					Storage:                             defaultConf,
					StorageByLocalityKV:                 storageByLocalityKV,
					StartTime:                           span.start,
					EnableTimeBoundIteratorOptimization: useTBI.Get(&settings.SV),
					MVCCFilter:                          spec.MVCCFilter,
					Encryption:                          spec.Encryption,
					TargetFileSize:                      targetFileSize,
				}

				// If we're doing re-attempts but are not yet in the priority regime,
				// check to see if it is time to switch to priority.
				if !priority && span.attempts > 0 {
					// Check if this is starting a new pass and we should delay first.
					// We're okay with delaying this worker until then since we assume any
					// other work it could pull off the queue will likely want to delay to
					// a similar or later time anyway.
					if delay := delayPerAttmpt.Get(&settings.SV) - timeutil.Since(span.lastTried); delay > 0 {
						timer.Reset(delay)
						log.Infof(ctx, "waiting %s to start attempt %d of remaining spans", delay, span.attempts+1)
						select {
						case <-done:
							return ctx.Err()
						case <-timer.C:
							timer.Read = true
						}
					}

					priority = timeutil.Since(readTime) > priorityAfter.Get(&settings.SV)
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
				log.Infof(ctx, "sending ExportRequest for span %s (attempt %d, priority %s)",
					span.span, span.attempts+1, header.UserPriority.String())
				rawRes, pErr := kv.SendWrappedWith(ctx, flowCtx.Cfg.DB.NonTransactionalSender(), header, req)
				if pErr != nil {
					if err := pErr.Detail.GetWriteIntent(); err != nil {
						span.lastTried = timeutil.Now()
						span.attempts++
						todo <- span
						// TODO(dt): send a progress update to update job progress to note
						// the intents being hit.
						continue
					}
					return errors.Wrapf(pErr.GoError(), "exporting %s", span.span)
				}
				res := rawRes.(*roachpb.ExportResponse)
				files := make([]BackupManifest_File, 0)
				var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
				progDetails := BackupManifest_Progress{}
				progDetails.RevStartTime = res.StartTime
				for _, file := range res.Files {
					f := BackupManifest_File{
						Span:        file.Span,
						Path:        file.Path,
						Sha512:      file.Sha512,
						EntryCounts: countRows(file.Exported, spec.PKIDs),
						LocalityKV:  file.LocalityKV,
					}
					if span.start != spec.BackupStartTime {
						f.StartTime = span.start
						f.EndTime = span.end
					}
					files = append(files, f)
				}
				progDetails.Files = files
				details, err := gogotypes.MarshalAny(&progDetails)
				if err != nil {
					return err
				}
				prog.ProgressDetails = *details
				progCh <- prog
			default:
				// No work left to do, so we can exit. Note that another worker could
				// still be running and may still push new work (a retry) on to todo but
				// that is OK, since that also means it is still running and thus can
				// pick up that work on its next iteration.
				return nil
			}
		}
	})
}

func init() {
	rowexec.NewBackupDataProcessor = newBackupDataProcessor
}
