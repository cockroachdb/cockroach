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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	hlc "github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
)

var exportOutputTypes = []*types.T{}

// TODO(pbardea): It would be nice if we could add some DistSQL processor tests
// we would probably want to have a mock cloudStorage object that we could
// verify with.

// exportDataProcessor represents the work each node in a cluster performs
// during a BACKUP. It is assigned a set of spans to export to a given URI. It
// will create parallel workers (whose parallelism is also specified), which
// will each export a span at a time. After exporting the span, it will stream
// back its progress through the metadata channel provided by DistSQL.
type exportDataProcessor struct {
	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.ExportDataSpec
	output  execinfra.RowReceiver
}

var _ execinfra.Processor = &exportDataProcessor{}

func (cp *exportDataProcessor) OutputTypes() []*types.T {
	return exportOutputTypes
}

func newExportDataProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ExportDataSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	cp := &exportDataProcessor{
		flowCtx: flowCtx,
		spec:    spec,
		output:  output,
	}
	return cp, nil
}

func (cp *exportDataProcessor) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "exportDataProcessor")
	defer tracing.FinishSpan(span)
	defer cp.output.ProducerDone()

	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)

	var err error
	// We don't have to worry about this go routine leaking because next we loop over progCh
	// which is closed only after the go routine returns.
	go func() {
		defer close(progCh)
		err = runExport(ctx, cp.flowCtx, &cp.spec, progCh)
	}()

	for prog := range progCh {
		// Take a copy so that we can send the progress address to the output processor.
		p := prog
		cp.output.Push(nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &p})
	}

	if err != nil {
		cp.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
		return
	}
}

func runExport(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ExportDataSpec,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	settings := flowCtx.Cfg.Settings

	allSpans := make([]spanAndTime, 0, len(spec.Spans)+len(spec.IntroducedSpans))
	for _, s := range spec.IntroducedSpans {
		allSpans = append(allSpans, spanAndTime{span: s, start: hlc.Timestamp{}, end: spec.BackupStartTime})
	}
	for _, s := range spec.Spans {
		allSpans = append(allSpans, spanAndTime{span: s, start: spec.BackupStartTime, end: spec.BackupEndTime})
	}

	// TODO(pbardea): This was previously multiplied by 10... we may want to
	// either increase the default to 30 or entirely revisit what a good default
	// is for this.
	maxConcurrentExports := kvserver.ExportRequestsLimit.Get(&settings.SV)
	exportsSem := make(chan struct{}, maxConcurrentExports)

	// For all backups, partitioned or not, the main BACKUP manifest is stored at
	// details.URI.
	defaultConf, err := cloud.ExternalStorageConfFromURI(spec.DefaultURI)
	if err != nil {
		return errors.Wrapf(err, "export configuration")
	}
	defaultStore, err := flowCtx.Cfg.ExternalStorage(ctx, defaultConf)
	if err != nil {
		return errors.Wrapf(err, "make storage")
	}
	storageByLocalityKV := make(map[string]*roachpb.ExternalStorage)
	for kv, uri := range spec.URIsByLocalityKV {
		conf, err := cloud.ExternalStorageConfFromURI(uri)
		if err != nil {
			return err
		}
		storageByLocalityKV[kv] = &conf
	}

	g := ctxgroup.WithContext(ctx)

	g.GoCtx(func(ctx context.Context) error {
		for i := range allSpans {
			{
				select {
				case exportsSem <- struct{}{}:
				case <-ctx.Done():
					// Break the for loop to avoid creating more work - the backup
					// has failed because either the context has been canceled or an
					// error has been returned. Either way, Wait() is guaranteed to
					// return an error now.
					return ctx.Err()
				}
			}

			span := allSpans[i]
			// TODO(pbardea): It would be nice if we could avoid producing many small
			//  SSTs.
			g.GoCtx(func(ctx context.Context) error {
				defer func() { <-exportsSem }()
				header := roachpb.Header{Timestamp: span.end}
				req := &roachpb.ExportRequest{
					RequestHeader:                       roachpb.RequestHeaderFromSpan(span.span),
					Storage:                             defaultStore.Conf(),
					StorageByLocalityKV:                 storageByLocalityKV,
					StartTime:                           span.start,
					EnableTimeBoundIteratorOptimization: useTBI.Get(&settings.SV),
					MVCCFilter:                          spec.MVCCFilter,
					Encryption:                          spec.Encryption,
				}
				rawRes, pErr := kv.SendWrappedWith(ctx, flowCtx.Cfg.DB.NonTransactionalSender(), header, req)
				if pErr != nil {
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
				return nil
			})
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return errors.Wrapf(err, "exporting %d ranges", errors.Safe(len(allSpans)))
	}
	return nil
}

func init() {
	rowexec.NewExportDataProcessor = newExportDataProcessor
}
