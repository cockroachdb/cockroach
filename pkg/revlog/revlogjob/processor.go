// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/errors"
)

const processorName = "revlog-producer"

// proc is the per-node revlog producer processor. It opens a
// rangefeed over its assigned spans, feeds events into a Producer,
// and emits ProducerMetadata for each FileAdded / FrontierAdvanced
// signal the producer's TickSink reports. The gateway-side metadata
// callback decodes those signals and drives a TickManager that
// writes manifests.
//
// Lifecycle (mirroring backup_processor.go):
//
//   - Start: open external storage, build the Producer, launch the
//     rangefeed and the dispatcher goroutine. The dispatcher feeds
//     the Producer; the Producer's sink writes BulkProcessorProgress
//     entries to progCh.
//   - Next: blocks on progCh; each entry becomes a ProducerMetadata.
//     When progCh closes (dispatcher returned), MoveToDraining with
//     any captured error.
//   - Close: cancels the dispatcher context (which closes the
//     rangefeed) and waits for it to drain.
type proc struct {
	execinfra.ProcessorBase

	spec execinfrapb.RevlogSpec

	// progCh is filled by the producer's metaSink and drained by
	// Next(). Closed by the dispatcher goroutine on shutdown.
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress

	// cancelAndWait stops the dispatcher and rangefeed and waits for
	// the dispatcher goroutine to exit. Idempotent.
	cancelAndWait func()

	// runErr is the first non-nil error reported by the dispatcher.
	runErr error

	// es and rf are owned by the proc; Close releases them.
	es cloud.ExternalStorage
}

var (
	_ execinfra.Processor = &proc{}
	_ execinfra.RowSource = &proc{}
)

func newRevlogProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.RevlogSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	p := &proc{
		spec:   spec,
		progCh: make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress),
	}
	if err := p.Init(ctx, p, post, nil /* outTypes */, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: nil,
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				p.close()
				return nil
			},
		}); err != nil {
		return nil, err
	}
	return p, nil
}

// Start is part of the RowSource interface.
func (p *proc) Start(ctx context.Context) {
	ctx = p.StartInternal(ctx, processorName)
	ctx, cancel := context.WithCancel(ctx)

	dispatcherDone := make(chan struct{})
	p.cancelAndWait = func() {
		cancel()
		<-dispatcherDone
	}

	if err := p.FlowCtx.Stopper().RunAsyncTaskEx(ctx, stop.TaskOpts{
		TaskName: "revlogjob.proc.run",
		SpanOpt:  stop.ChildSpan,
	}, func(ctx context.Context) {
		defer close(dispatcherDone)
		defer close(p.progCh)
		p.runErr = p.run(ctx)
		cancel()
	}); err != nil {
		// The closure above won't run; do its cleanup here.
		p.runErr = err
		cancel()
		close(p.progCh)
		close(dispatcherDone)
	}
}

// run owns the rangefeed + producer pipeline lifetime. Returns the
// first error from any component.
//
// TODO(revlog): persistent checkpoint + resume. v1 always starts
// fresh from spec.StartHLC; partially-buffered ticks at shutdown are
// dropped.
//
// TODO(revlog): collect errCh from the rangefeed and surface them
// (or retry per WithRetryStrategy).
func (p *proc) run(ctx context.Context) error {
	cfg := p.FlowCtx.Cfg.ExecutorConfig.(*sql.ExecutorConfig)

	dest, err := cloud.ExternalStorageConfFromURI(p.spec.Dest, username.RootUserName())
	if err != nil {
		return errors.Wrap(err, "parsing destination URI")
	}
	es, err := cfg.DistSQLSrv.ExternalStorage(ctx, dest)
	if err != nil {
		return errors.Wrap(err, "opening external storage")
	}
	p.es = es

	sink := newMetaSink(p.progCh)
	tickWidth := time.Duration(p.spec.TickWidthNanos)
	resume := resumeFromSpec(p.spec)
	producer, err := NewProducer(es, p.spec.Spans, p.spec.StartHLC, tickWidth,
		nodeFileIDs{instanceID: unique.ProcessUniqueID(p.FlowCtx.NodeID.SQLInstanceID())},
		sink, resume)
	if err != nil {
		return errors.Wrap(err, "constructing producer")
	}

	eventsCh := make(chan rangefeedEvent, 1024)
	errCh := make(chan error, 1)

	// Open the rangefeed at the lowest persisted resume timestamp
	// across this producer's spans (or StartHLC on first run). The
	// rangefeed's catchup scan will redeliver events between this
	// point and any per-span persisted positions; the producer's
	// per-span frontier (initialized from resume.SpanResumes) and
	// the manager's monotonic frontier dedupe correctly.
	//
	// TODO(dt): open one rangefeed per (start_ts) group of spans
	// to skip the redelivery for spans whose persisted ts is above
	// the slowest's.
	rfStart := rangefeedStart(p.spec, resume)
	rf, err := startRangeFeed(ctx, cfg.RangeFeedFactory, "revlog",
		p.spec.Spans, rfStart, eventsCh, errCh)
	if err != nil {
		return err
	}
	defer rf.Close()

	// Dispatcher goroutine: pumps events to the producer. Errors
	// from rangefeed (errCh) are checked alongside events.
	dispErr := make(chan error, 1)
	go func() { dispErr <- runDispatcher(ctx, producer, eventsCh) }()

	select {
	case err := <-dispErr:
		return err
	case err := <-errCh:
		return errors.Wrap(err, "rangefeed internal error")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Next is part of the RowSource interface.
func (p *proc) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if p.State != execinfra.StateRunning {
		return nil, p.DrainHelper()
	}
	prog, ok := <-p.progCh
	if !ok {
		p.MoveToDraining(p.runErr)
		return nil, p.DrainHelper()
	}
	prog.NodeID = p.FlowCtx.NodeID.SQLInstanceID()
	prog.FlowID = p.FlowCtx.ID
	prog.ProcessorID = p.ProcessorID
	return nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &prog}
}

// ConsumerClosed is part of the RowSource interface.
func (p *proc) ConsumerClosed() {
	p.close()
}

func (p *proc) close() {
	if p.cancelAndWait != nil {
		p.cancelAndWait()
		p.cancelAndWait = nil
	}
	if p.InternalClose() {
		if p.es != nil {
			if err := p.es.Close(); err != nil {
				log.Dev.Warningf(p.Ctx(), "closing external storage: %v", err)
			}
		}
	}
}

// nodeFileIDs is a FileIDSource backed by the cluster's unique-int
// generator. The IDs are non-negative (top bit always zero), roughly
// monotonic per node (timestamp-derived), and disambiguated across
// nodes by the embedded SQL instance ID — same scheme backup uses
// for its data SST names (pkg/backup/backupsink/sink_utils.go).
type nodeFileIDs struct {
	instanceID unique.ProcessUniqueID
}

func (n nodeFileIDs) Next() int64 {
	return unique.GenerateUniqueInt(n.instanceID)
}

func init() {
	rowexec.NewRevlogProcessor = newRevlogProcessor
}
