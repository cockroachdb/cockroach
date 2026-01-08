// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

import (
	"context"
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkutil"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var (
	ingestProcessorConcurrencyOverride = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"bulkingest.ingest_processor_concurrency",
		"sets the number of concurrent workers for ingest file processors. "+
			"Defaults to 1. Setting to 0 uses the value based on GOMAXPROCS. "+
			"Values above GOMAXPROCS are capped.",
		1,
		settings.NonNegativeInt,
	)
)

// getIngestProcessorConcurrency returns the number of concurrent workers to use
// for ingest file processing. If the cluster setting is non-zero, it uses the
// minimum of the override and GOMAXPROCS. Otherwise, it defaults to GOMAXPROCS.
func getIngestProcessorConcurrency(flowCtx *execinfra.FlowCtx) int {
	override := int(ingestProcessorConcurrencyOverride.Get(&flowCtx.Cfg.Settings.SV))
	if override > 0 {
		return min(runtime.GOMAXPROCS(0), override)
	}
	return runtime.GOMAXPROCS(0)
}

type ingestFileProcessor struct {
	processorID    int32
	flowCtx        *execinfra.FlowCtx
	spec           execinfrapb.IngestFileSpec
	concurrency    int
	createCloudMux func() *bulkutil.ExternalStorageMux
	memMonitor     *mon.BytesMonitor
}

var _ execinfra.Processor = (*ingestFileProcessor)(nil)

// OutputTypes is part of the execinfra.Processor interface.
func (*ingestFileProcessor) OutputTypes() []*types.T {
	return nil
}

// MustBeStreaming is part of the execinfra.Processor interface.
func (*ingestFileProcessor) MustBeStreaming() bool {
	return false
}

// Resume is part of the execinfra.Processor interface.
func (*ingestFileProcessor) Resume(execinfra.RowReceiver) {
	panic("not implemented")
}

// Close is part of the execinfra.Processor interface.
func (p *ingestFileProcessor) Close(ctx context.Context) {
	// Each worker manages its own cloudMux and memAcc and closes them on completion.
	p.memMonitor.Stop(ctx)
}

// Run is part of the execinfra.Processor interface.
func (p *ingestFileProcessor) Run(ctx context.Context, output execinfra.RowReceiver) {
	var span *tracing.Span
	noSpan := p.flowCtx != nil && p.flowCtx.Cfg != nil &&
		p.flowCtx.Cfg.TestingKnobs.ProcessorNoTracingSpan
	if !noSpan {
		ctx, span = execinfra.ProcessorSpan(ctx, p.flowCtx, "ingestFile", p.processorID)
		defer span.Finish()
	}
	defer output.ProducerDone()
	defer execinfra.SendTraceData(ctx, p.flowCtx, output)
	err := p.runIngest(ctx)
	if err != nil {
		output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
	}
}

// runIngest starts a set of worker goroutines to process SST files concurrently.
// Each SST is read from a buffered channel and passed to processSST.
// The function blocks until all SSTs are processed or an error occurs.
func (p *ingestFileProcessor) runIngest(ctx context.Context) error {
	log.Dev.Infof(ctx, "IngestFile processor started processorID=%d concurrency=%d ssts=%d",
		p.processorID, p.concurrency, len(p.spec.SSTs))

	group := ctxgroup.WithContext(ctx)

	if p.concurrency == 0 {
		return errors.AssertionFailedf("must have at least one worker")
	}
	sstCh := make(chan execinfrapb.BulkMergeSpec_SST, p.concurrency)

	// Launch worker goroutines. Each worker reads SSTs from sstCh and processes them.
	for i := 0; i < p.concurrency; i++ {
		workerIndex := i
		group.GoCtx(func(ctx context.Context) error {
			// Each worker gets its own cloudMux to avoid concurrent map access.
			cloudMux := p.createCloudMux()
			defer func() {
				if err := cloudMux.Close(); err != nil {
					log.Dev.Warningf(ctx, "worker %d: failed to close external storage mux: %v", workerIndex, err)
				}
			}()

			// Each worker gets its own memory account to track buffer memory.
			memAcc := p.memMonitor.MakeBoundAccount()
			defer memAcc.Close(ctx)

			// Allocate a buffer per worker to avoid contention.
			var buffer []byte
			for {
				select {
				case <-ctx.Done():
					// If the context is canceled (e.g., due to an error in another worker), exit.
					return ctx.Err()
				case sst, ok := <-sstCh:
					if !ok {
						// Channel is closed, no more SSTs to process.
						return nil
					}
					var err error
					buffer, err = p.processSST(ctx, sst, workerIndex, cloudMux, &memAcc, buffer)
					if err != nil {
						// On error, return it. ctxgroup will cancel all other goroutines.
						return err
					}
				}
			}
		})
	}

	// Producer goroutine: feeds all SSTs into the channel for workers to consume.
	group.GoCtx(func(ctx context.Context) error {
		defer close(sstCh)
		for _, sst := range p.spec.SSTs {
			select {
			case <-ctx.Done():
				// Exit early if context is canceled.
				return ctx.Err()
			case sstCh <- sst:
				// Send SST to the workers.
			}
		}
		return nil
	})

	// Wait for all goroutines to finish.
	if err := group.Wait(); err != nil {
		return err
	}

	log.Dev.Infof(ctx, "IngestFile processor completed processorID=%d", p.processorID)
	return nil
}

// processSST ingests a single SST file into the database. It returns the buffer
// for reuse by the caller.
func (p *ingestFileProcessor) processSST(
	ctx context.Context,
	sst execinfrapb.BulkMergeSpec_SST,
	workerIndex int,
	cloudMux *bulkutil.ExternalStorageMux,
	memAcc *mon.BoundAccount,
	buffer []byte,
) ([]byte, error) {
	file, err := cloudMux.StoreFile(ctx, sst.URI)
	if err != nil {
		return buffer, err
	}

	db := p.flowCtx.Cfg.DB.KV()
	err = func() error {
		reader, _, err := file.Store.ReadFile(ctx, file.FilePath, cloud.ReadOptions{})
		if err != nil {
			return err
		}
		defer reader.Close(ctx)

		buffer, err = ioctx.ReadAllWithScratch(ctx, reader, buffer)
		if err != nil {
			return err
		}

		// Account for the buffer capacity. ResizeTo handles both growth and shrinkage.
		if err := memAcc.ResizeTo(ctx, int64(cap(buffer))); err != nil {
			return err
		}

		// TODO(158338): flow completed spans/sst back to the coordinator in order
		// to track progress and allow for retries on failure.
		_, _, err = db.AddSSTable(
			ctx,
			sst.StartKey,
			sst.EndKey,
			buffer,
			false,           /* disallowShadowing */
			hlc.Timestamp{}, /* statsEmpty */
			nil,             /* ingestAsWrites */
			false,           /* writeAtBatchTimestamp */
			hlc.Timestamp{}, /* mvccStatsTestOverride */
		)
		return err
	}()
	if err != nil {
		return buffer, errors.Wrapf(
			err, "worker %d: failed to ingest SST (uri: %s, start: %s, end: %s)",
			workerIndex, sst.URI, sst.StartKey, sst.EndKey,
		)
	}
	return buffer, nil
}

func newIngestFileProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.IngestFileSpec,
) (execinfra.Processor, error) {
	storageFactory := flowCtx.Cfg.ExternalStorageFromURI
	user := flowCtx.EvalCtx.SessionData().User()

	memMonitor := execinfra.NewMonitor(ctx, flowCtx.Mon, mon.MakeName("ingest-file-mem"))
	return &ingestFileProcessor{
		spec:        spec,
		processorID: processorID,
		flowCtx:     flowCtx,
		concurrency: getIngestProcessorConcurrency(flowCtx),
		memMonitor:  memMonitor,
		createCloudMux: func() *bulkutil.ExternalStorageMux {
			return bulkutil.NewExternalStorageMux(storageFactory, user)
		},
	}, nil
}

func init() {
	rowexec.NewIngestFileProcessor = newIngestFileProcessor
}
