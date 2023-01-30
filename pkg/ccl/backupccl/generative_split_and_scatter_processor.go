// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/logtags"
)

const generativeSplitAndScatterProcessorName = "generativeSplitAndScatter"

var generativeSplitAndScatterOutputTypes = []*types.T{
	types.Bytes, // Span key for the range router
	types.Bytes, // RestoreDataEntry bytes
}

// generativeSplitAndScatterProcessor is given a backup chain, whose manifests
// are specified in URIs and iteratively generates RestoreSpanEntries to be
// distributed across the cluster. Depending on which node the span ends up on,
// it forwards RestoreSpanEntry as bytes along with the key of the span on a
// row. It expects an output RangeRouter and before it emits each row, it
// updates the entry in the RangeRouter's map with the destination of the
// scatter.
type generativeSplitAndScatterProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.GenerativeSplitAndScatterSpec
	output  execinfra.RowReceiver

	scatterer splitAndScatterer
	// cancelScatterAndWaitForWorker cancels the scatter goroutine and waits for
	// it to finish.
	cancelScatterAndWaitForWorker func()

	doneScatterCh chan entryNode
	// A cache for routing datums, so only 1 is allocated per node.
	routingDatumCache map[roachpb.NodeID]rowenc.EncDatum
	scatterErr        error
}

var _ execinfra.Processor = &generativeSplitAndScatterProcessor{}

func newGenerativeSplitAndScatterProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.GenerativeSplitAndScatterSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {

	db := flowCtx.Cfg.DB
	kr, err := MakeKeyRewriterFromRekeys(flowCtx.Codec(), spec.TableRekeys, spec.TenantRekeys,
		false /* restoreTenantFromStream */)
	if err != nil {
		return nil, err
	}

	scatterer := makeSplitAndScatterer(db.KV(), kr)
	if spec.ValidateOnly {
		nodeID, _ := flowCtx.NodeID.OptionalNodeID()
		scatterer = noopSplitAndScatterer{nodeID}
	}
	ssp := &generativeSplitAndScatterProcessor{
		flowCtx:   flowCtx,
		spec:      spec,
		output:    output,
		scatterer: scatterer,
		// Large enough so that it never blocks.
		doneScatterCh:     make(chan entryNode, spec.NumEntries),
		routingDatumCache: make(map[roachpb.NodeID]rowenc.EncDatum),
	}
	if err := ssp.Init(ctx, ssp, post, generativeSplitAndScatterOutputTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: nil, // there are no inputs to drain
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				ssp.close()
				return nil
			},
		}); err != nil {
		return nil, err
	}
	return ssp, nil
}

// Start is part of the RowSource interface.
func (gssp *generativeSplitAndScatterProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", gssp.spec.JobID)
	ctx = gssp.StartInternal(ctx, generativeSplitAndScatterProcessorName)
	// Note that the loop over doneScatterCh in Next should prevent the goroutine
	// below from leaking when there are no errors. However, if that loop needs to
	// exit early, runSplitAndScatter's context will be canceled.
	scatterCtx, cancel := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	gssp.cancelScatterAndWaitForWorker = func() {
		cancel()
		<-workerDone
	}
	if err := gssp.flowCtx.Stopper().RunAsyncTaskEx(scatterCtx, stop.TaskOpts{
		TaskName: "generativeSplitAndScatter-worker",
		SpanOpt:  stop.ChildSpan,
	}, func(ctx context.Context) {
		gssp.scatterErr = runGenerativeSplitAndScatter(scatterCtx, gssp.flowCtx, &gssp.spec, gssp.scatterer, gssp.doneScatterCh)
		cancel()
		close(gssp.doneScatterCh)
		close(workerDone)
	}); err != nil {
		gssp.scatterErr = err
		cancel()
		close(workerDone)
	}
}

// Next implements the execinfra.RowSource interface.
func (gssp *generativeSplitAndScatterProcessor) Next() (
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if gssp.State != execinfra.StateRunning {
		return nil, gssp.DrainHelper()
	}

	scatteredEntry, ok := <-gssp.doneScatterCh
	if ok {
		entry := scatteredEntry.entry
		entryBytes, err := protoutil.Marshal(&entry)
		if err != nil {
			gssp.MoveToDraining(err)
			return nil, gssp.DrainHelper()
		}

		// The routing datums informs the router which output stream should be used.
		routingDatum, ok := gssp.routingDatumCache[scatteredEntry.node]
		if !ok {
			routingDatum, _ = routingDatumsForSQLInstance(base.SQLInstanceID(scatteredEntry.node))
			gssp.routingDatumCache[scatteredEntry.node] = routingDatum
		}

		row := rowenc.EncDatumRow{
			routingDatum,
			rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(entryBytes))),
		}
		return row, nil
	}

	if gssp.scatterErr != nil {
		gssp.MoveToDraining(gssp.scatterErr)
		return nil, gssp.DrainHelper()
	}

	gssp.MoveToDraining(nil /* error */)
	return nil, gssp.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (gssp *generativeSplitAndScatterProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	gssp.close()
}

// close stops the production workers. This needs to be called if the consumer
// runs into an error and stops consuming scattered entries to make sure we
// don't leak goroutines.
func (gssp *generativeSplitAndScatterProcessor) close() {
	gssp.cancelScatterAndWaitForWorker()
	gssp.InternalClose()
}

func makeBackupMetadata(
	ctx context.Context, flowCtx *execinfra.FlowCtx, spec *execinfrapb.GenerativeSplitAndScatterSpec,
) ([]backuppb.BackupManifest, backupinfo.LayerToBackupManifestFileIterFactory, error) {

	execCfg := flowCtx.Cfg.ExecutorConfig.(*sql.ExecutorConfig)

	kmsEnv := backupencryption.MakeBackupKMSEnv(execCfg.Settings, &execCfg.ExternalIODirConfig,
		execCfg.InternalDB, spec.User())

	backupManifests, _, err := backupinfo.LoadBackupManifestsAtTime(ctx, nil, spec.URIs,
		spec.User(), execCfg.DistSQLSrv.ExternalStorageFromURI, spec.Encryption, &kmsEnv, spec.EndTime)
	if err != nil {
		return nil, nil, err
	}

	layerToBackupManifestFileIterFactory, err := backupinfo.GetBackupManifestIterFactories(ctx, execCfg.DistSQLSrv.ExternalStorage,
		backupManifests, spec.Encryption, &kmsEnv)
	if err != nil {
		return nil, nil, err
	}

	return backupManifests, layerToBackupManifestFileIterFactory, nil
}

type restoreEntryChunk struct {
	entries  []execinfrapb.RestoreSpanEntry
	splitKey roachpb.Key
}

func runGenerativeSplitAndScatter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.GenerativeSplitAndScatterSpec,
	scatterer splitAndScatterer,
	doneScatterCh chan<- entryNode,
) error {
	log.Infof(ctx, "Running generative split and scatter with %d total spans, %d chunk size, %d nodes",
		spec.NumEntries, spec.ChunkSize, spec.NumNodes)
	g := ctxgroup.WithContext(ctx)

	splitWorkers := int(spec.NumNodes)
	restoreSpanEntriesCh := make(chan execinfrapb.RestoreSpanEntry, splitWorkers*int(spec.ChunkSize))
	g.GoCtx(func(ctx context.Context) error {
		defer close(restoreSpanEntriesCh)

		backups, layerToFileIterFactory, err := makeBackupMetadata(ctx,
			flowCtx, spec)
		if err != nil {
			return err
		}

		introducedSpanFrontier, err := createIntroducedSpanFrontier(backups, spec.EndTime)
		if err != nil {
			return err
		}

		backupLocalityMap, err := makeBackupLocalityMap(spec.BackupLocalityInfo, spec.User())
		if err != nil {
			return err
		}

		return generateAndSendImportSpans(
			ctx,
			spec.Spans,
			backups,
			layerToFileIterFactory,
			backupLocalityMap,
			introducedSpanFrontier,
			spec.HighWater,
			spec.TargetSize,
			restoreSpanEntriesCh,
			spec.UseSimpleImportSpans,
		)
	})

	restoreEntryChunksCh := make(chan restoreEntryChunk, splitWorkers)
	g.GoCtx(func(ctx context.Context) error {
		defer close(restoreEntryChunksCh)

		var idx int64
		var chunk restoreEntryChunk
		for entry := range restoreSpanEntriesCh {
			entry.ProgressIdx = idx
			idx++
			if len(chunk.entries) == int(spec.ChunkSize) {
				chunk.splitKey = entry.Span.Key
				restoreEntryChunksCh <- chunk
				chunk = restoreEntryChunk{}
			}
			chunk.entries = append(chunk.entries, entry)
		}

		if len(chunk.entries) > 0 {
			restoreEntryChunksCh <- chunk
		}
		return nil
	})

	importSpanChunksCh := make(chan scatteredChunk, splitWorkers*2)
	g2 := ctxgroup.WithContext(ctx)
	for worker := 0; worker < splitWorkers; worker++ {
		g2.GoCtx(func(ctx context.Context) error {
			// Chunks' leaseholders should be randomly placed throughout the
			// cluster.
			for importSpanChunk := range restoreEntryChunksCh {
				scatterKey := importSpanChunk.entries[0].Span.Key
				if !importSpanChunk.splitKey.Equal(roachpb.Key{}) {
					// Split at the start of the next chunk, to partition off a
					// prefix of the space to scatter.
					if err := scatterer.split(ctx, flowCtx.Codec(), importSpanChunk.splitKey); err != nil {
						return err
					}
				}
				chunkDestination, err := scatterer.scatter(ctx, flowCtx.Codec(), scatterKey)
				if err != nil {
					return err
				}
				if chunkDestination == 0 {
					// If scatter failed to find a node for range ingestion, route the range
					// to the node currently running the split and scatter processor.
					if nodeID, ok := flowCtx.NodeID.OptionalNodeID(); ok {
						chunkDestination = nodeID
						log.Warningf(ctx, "scatter returned node 0. "+
							"Route span starting at %s to current node %v", scatterKey, nodeID)
					} else {
						log.Warningf(ctx, "scatter returned node 0. "+
							"Route span starting at %s to default stream", scatterKey)
					}
				}

				sc := scatteredChunk{
					destination: chunkDestination,
					entries:     importSpanChunk.entries,
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case importSpanChunksCh <- sc:
				}
			}
			return nil
		})
	}

	g.GoCtx(func(ctx context.Context) error {
		defer close(importSpanChunksCh)
		return g2.Wait()
	})

	// TODO(pbardea): This tries to cover for a bad scatter by having 2 * the
	// number of nodes in the cluster. Is it necessary?
	splitScatterWorkers := 2 * splitWorkers
	for worker := 0; worker < splitScatterWorkers; worker++ {
		g.GoCtx(func(ctx context.Context) error {
			for importSpanChunk := range importSpanChunksCh {
				chunkDestination := importSpanChunk.destination
				for i, importEntry := range importSpanChunk.entries {
					nextChunkIdx := i + 1

					log.VInfof(ctx, 2, "processing a span [%s,%s)", importEntry.Span.Key, importEntry.Span.EndKey)
					var splitKey roachpb.Key
					if nextChunkIdx < len(importSpanChunk.entries) {
						// Split at the next entry.
						splitKey = importSpanChunk.entries[nextChunkIdx].Span.Key
						if err := scatterer.split(ctx, flowCtx.Codec(), splitKey); err != nil {
							return err
						}
					}

					scatteredEntry := entryNode{
						entry: importEntry,
						node:  chunkDestination,
					}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case doneScatterCh <- scatteredEntry:
					}
				}
			}
			return nil
		})
	}

	return g.Wait()
}

func init() {
	rowexec.NewGenerativeSplitAndScatterProcessor = newGenerativeSplitAndScatterProcessor
}
