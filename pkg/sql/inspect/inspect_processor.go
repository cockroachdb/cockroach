// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

var (
	// errInspectFoundInconsistencies is a sentinel error used to mark errors
	// returned when INSPECT jobs find data inconsistencies.
	errInspectFoundInconsistencies = pgerror.New(pgcode.DataException, "INSPECT found inconsistencies")
	// errInspectInternalErrors marks jobs that only encountered internal errors while running.
	errInspectInternalErrors = pgerror.New(pgcode.Internal, "INSPECT encountered internal errors")

	processorConcurrencyOverride = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"sql.inspect.processor_concurrency",
		"sets the number of concurrent processors for INSPECT jobs. "+
			"0 uses the default based on GOMAXPROCS. Values above GOMAXPROCS are capped.",
		0,
		settings.NonNegativeInt,
	)

	fractionUpdateInterval = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"sql.inspect.fraction_update_interval",
		"the amount of time between INSPECT job percent complete progress updates",
		10*time.Second,
		settings.DurationWithMinimum(1*time.Millisecond),
	)

	checkpointInterval = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"sql.inspect.checkpoint_interval",
		"the amount of time between INSPECT job checkpoint updates",
		30*time.Second,
		settings.DurationWithMinimum(1*time.Millisecond),
	)

	admissionControlEnabled = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"sql.inspect.admission_control.enabled",
		"when enabled, INSPECT operations are throttled to minimize impact on foreground query performance; "+
			"when disabled, INSPECT runs faster but may slow down foreground queries",
		true,
	)
)

// getInspectQoS returns the QoS level to use for INSPECT operations based on
// the cluster setting sql.inspect.admission_control.enabled.
func getInspectQoS(sv *settings.Values) sessiondatapb.QoSLevel {
	if admissionControlEnabled.Get(sv) {
		return sessiondatapb.BulkLowQoS
	}
	return sessiondatapb.Normal
}

type inspectCheckFactory func(asOf hlc.Timestamp) inspectCheck

type inspectProcessor struct {
	processorID         int32
	flowCtx             *execinfra.FlowCtx
	spec                execinfrapb.InspectSpec
	cfg                 *execinfra.ServerConfig
	checkFactories      []inspectCheckFactory
	spanSrc             spanSource
	loggerBundle        *inspectLoggerBundle
	concurrency         int
	clock               *hlc.Clock
	spansProcessedCtr   *metric.Counter
	numActiveSpansGauge *metric.Gauge

	mu struct {
		// Guards calls to output.Push because DistSQLReceiver.Push is not
		// concurrency safe and progress metadata can be emitted from multiple
		// worker goroutines.
		syncutil.Mutex
	}
}

var _ execinfra.Processor = (*inspectProcessor)(nil)

// OutputTypes is part of the execinfra.Processor interface.
func (*inspectProcessor) OutputTypes() []*types.T {
	return nil
}

// MustBeStreaming is part of the execinfra.Processor interface.
func (*inspectProcessor) MustBeStreaming() bool {
	return false
}

// Resume is part of the execinfra.Processor interface.
func (*inspectProcessor) Resume(execinfra.RowReceiver) {
	panic("not implemented")
}

// Close is part of the execinfra.Processor interface.
func (*inspectProcessor) Close(context.Context) {}

// Run is part of the execinfra.Processor interface.
func (p *inspectProcessor) Run(ctx context.Context, output execinfra.RowReceiver) {
	var span *tracing.Span
	noSpan := p.flowCtx != nil && p.flowCtx.Cfg != nil &&
		p.flowCtx.Cfg.TestingKnobs.ProcessorNoTracingSpan
	if !noSpan {
		ctx, span = execinfra.ProcessorSpan(ctx, p.flowCtx, "inspect", p.processorID)
		defer span.Finish()
	}
	err := p.runInspect(ctx, output)
	if err != nil {
		output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
	}
	execinfra.SendTraceData(ctx, p.flowCtx, output)
	output.ProducerDone()
}

// runInspect starts a set of worker goroutines to process spans concurrently.
// Each span is read from a buffered channel and passed to processSpan.
// The function blocks until all spans are processed or an error occurs.
func (p *inspectProcessor) runInspect(ctx context.Context, output execinfra.RowReceiver) error {
	log.Dev.Infof(ctx, "INSPECT processor started processorID=%d concurrency=%d spans=%d", p.processorID, p.concurrency, len(p.spec.Spans))

	group := ctxgroup.WithContext(ctx)

	if p.concurrency == 0 {
		return errors.AssertionFailedf("must have at least one worker")
	}
	spanCh := make(chan roachpb.Span, p.concurrency)

	// Launch worker goroutines. Each worker reads spans from spanCh and processes
	// them.
	for i := 0; i < p.concurrency; i++ {
		workerIndex := i
		group.GoCtx(func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					// If the context is canceled (e.g., due to an error in another worker), exit.
					return ctx.Err()
				case span, ok := <-spanCh:
					if !ok {
						// Channel is closed, no more spans to process.
						return nil
					}
					if err := p.processSpan(ctx, span, workerIndex, output); err != nil {
						// On error, return it. ctxgroup will cancel all other goroutines.
						return err
					}
				}
			}
		})
	}

	// Producer goroutine: feeds all spans into the channel for workers to consume.
	group.GoCtx(func(ctx context.Context) error {
		defer close(spanCh)
		for {
			span, ok, err := p.spanSrc.NextSpan(ctx)
			if err != nil {
				return err
			}
			if !ok {
				return nil // done
			}
			select {
			case <-ctx.Done():
				// Exit early if context is canceled.
				return ctx.Err()
			case spanCh <- span:
				// Send span to the workers.
			}
		}
	})

	// Wait for all goroutines to finish.
	if err := group.Wait(); err != nil {
		return err
	}

	// Send final completion message to indicate this processor is finished
	if err := p.sendInspectProgress(ctx, output, 0, true /* finished */); err != nil {
		return err
	}

	log.Dev.Infof(ctx, "INSPECT processor completed processorID=%d issuesFound=%t", p.processorID, p.loggerBundle.hasIssues())
	if p.loggerBundle.hasIssues() {
		errToReturn := errInspectFoundInconsistencies
		if p.loggerBundle.sawOnlyInternalIssues() {
			errToReturn = errInspectInternalErrors
		}
		return errors.WithHintf(
			errToReturn,
			"Run 'SHOW INSPECT ERRORS FOR JOB %d WITH DETAILS' for more information.",
			p.spec.JobID,
		)
	}
	return nil
}

// getProcessorConcurrency returns the number of concurrent workers to use for
// INSPECT processing. If the cluster setting is non-zero, it uses the minimum
// of the override and GOMAXPROCS. Otherwise, it defaults to GOMAXPROCS.
func getProcessorConcurrency(flowCtx *execinfra.FlowCtx) int {
	override := int(processorConcurrencyOverride.Get(&flowCtx.Cfg.Settings.SV))
	if override > 0 {
		return min(runtime.GOMAXPROCS(0), override)
	}
	return runtime.GOMAXPROCS(0)
}

// getInspectLogger returns a logger bundle for the inspect processor.
func getInspectLogger(flowCtx *execinfra.FlowCtx, jobID jobspb.JobID) *inspectLoggerBundle {
	execCfg := flowCtx.Cfg.ExecutorConfig.(*sql.ExecutorConfig)
	metrics := execCfg.JobRegistry.MetricsStruct().Inspect.(*InspectMetrics)

	loggers := []inspectLogger{
		&logSink{},
		&tableSink{
			db:    flowCtx.Cfg.DB,
			jobID: jobID,
			sv:    &flowCtx.Cfg.Settings.SV,
		},
		&metricsLogger{
			issuesFoundCtr: metrics.IssuesFound,
		},
	}

	knobs := execCfg.InspectTestingKnobs
	if knobs != nil && knobs.InspectIssueLogger != nil {
		loggers = append(loggers, knobs.InspectIssueLogger.(inspectLogger))
	}

	return newInspectLoggerBundle(loggers...)
}

// processSpan executes all configured inspect checks against a single span.
// It instantiates a fresh set of checks from the configured factories and uses
// an inspectRunner to drive their execution. After processing the span, it
// sends progress metadata to the coordinator.
func (p *inspectProcessor) processSpan(
	ctx context.Context, span roachpb.Span, workerIndex int, output execinfra.RowReceiver,
) (err error) {
	if p.numActiveSpansGauge != nil {
		p.numActiveSpansGauge.Inc(1)
		defer p.numActiveSpansGauge.Dec(1)
	}

	asOfToUse := p.getTimestampForSpan()

	// Only create checks that apply to this span.
	var checks []inspectCheck
	for _, factory := range p.checkFactories {
		check := factory(asOfToUse)
		applies, err := check.AppliesTo(p.cfg.Codec, span)
		if err != nil {
			return err
		}
		if applies {
			checks = append(checks, check)
		}
	}

	runner := inspectRunner{
		checks: checks,
		logger: p.loggerBundle,
	}
	defer func() {
		if closeErr := runner.Close(ctx); closeErr != nil {
			err = errors.CombineErrors(err, closeErr)
		}
	}()

	// Keep track of completed checks for progress updates.
	initialCheckCount := len(checks)
	lastSeenCheckCount := initialCheckCount

	// Process all checks on the given span.
	for {
		ok, stepErr := runner.Step(ctx, p.cfg, span, workerIndex)
		if stepErr != nil {
			return stepErr
		}

		// Check if any inspections have completed (when the count decreases).
		currentCheckCount := runner.CheckCount()
		if currentCheckCount < lastSeenCheckCount {
			checksCompleted := lastSeenCheckCount - currentCheckCount
			lastSeenCheckCount = currentCheckCount
			if err := p.sendInspectProgress(ctx, output, int64(checksCompleted), false /* finished */); err != nil {
				return err
			}
		}

		if !ok {
			break
		}
	}

	// Report span completion for checkpointing.
	if err := p.sendSpanCompletionProgress(ctx, output, span, false /* finished */); err != nil {
		return err
	}

	return nil
}

// sendInspectProgress marshals and sends inspect processor progress via BulkProcessorProgress.
func (p *inspectProcessor) sendInspectProgress(
	ctx context.Context, output execinfra.RowReceiver, checksCompleted int64, finished bool,
) error {
	progressMsg := &jobspb.InspectProcessorProgress{
		ChecksCompleted: checksCompleted,
		Finished:        finished,
	}

	progressAny, err := pbtypes.MarshalAny(progressMsg)
	if err != nil {
		return errors.Wrapf(err, "unable to marshal inspect processor progress")
	}

	meta := &execinfrapb.ProducerMetadata{
		BulkProcessorProgress: &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
			ProgressDetails: *progressAny,
			NodeID:          p.flowCtx.NodeID.SQLInstanceID(),
			FlowID:          p.flowCtx.ID,
			ProcessorID:     p.processorID,
			Drained:         finished,
		},
	}

	p.pushProgressMeta(output, meta)
	return nil
}

// getTimestampForSpan returns the timestamp to use for processing a span.
// If AsOf is empty, it returns the current timestamp from the processor's clock.
// Otherwise, it returns the specified AsOf timestamp.
func (p *inspectProcessor) getTimestampForSpan() hlc.Timestamp {
	if p.spec.InspectDetails.AsOf.IsEmpty() {
		return p.clock.Now()
	}
	return p.spec.InspectDetails.AsOf
}

// sendSpanCompletionProgress sends progress indicating a span has been completed.
// This is used for checkpointing to track which spans are done.
func (p *inspectProcessor) sendSpanCompletionProgress(
	ctx context.Context, output execinfra.RowReceiver, completedSpan roachpb.Span, finished bool,
) error {
	if p.spansProcessedCtr != nil {
		p.spansProcessedCtr.Inc(1)
	}
	progressMsg := &jobspb.InspectProcessorProgress{
		ChecksCompleted: 0, // No additional checks completed, just marking span done
		Finished:        finished,
	}

	progressAny, err := pbtypes.MarshalAny(progressMsg)
	if err != nil {
		return errors.Wrapf(err, "unable to marshal inspect processor progress")
	}

	meta := &execinfrapb.ProducerMetadata{
		BulkProcessorProgress: &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
			CompletedSpans:  []roachpb.Span{completedSpan}, // Mark this span as completed
			ProgressDetails: *progressAny,
			NodeID:          p.flowCtx.NodeID.SQLInstanceID(),
			FlowID:          p.flowCtx.ID,
			ProcessorID:     p.processorID,
			Drained:         finished,
		},
	}

	p.pushProgressMeta(output, meta)
	return nil
}

// pushProgressMeta serializes metadata pushes so only one goroutine interacts
// with the DistSQL receiver at a time (DistSQLReceiver.Push is not concurrency
// safe).
func (p *inspectProcessor) pushProgressMeta(
	output execinfra.RowReceiver, meta *execinfrapb.ProducerMetadata,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	output.Push(nil, meta)
}

// newInspectProcessor constructs a new inspectProcessor from the given InspectSpec.
// It parses the spec to generate a set of inspectCheck factories, sets up the span source,
// and wires in logging and concurrency controls.
//
// The returned processor is ready for integration into a distributed flow, but will not
// begin execution until Run is called.
func newInspectProcessor(
	ctx context.Context, flowCtx *execinfra.FlowCtx, processorID int32, spec execinfrapb.InspectSpec,
) (execinfra.Processor, error) {
	checkFactories, err := buildInspectCheckFactories(flowCtx, spec)
	if err != nil {
		return nil, err
	}
	var spansProcessedCtr *metric.Counter
	var numActiveSpansGauge *metric.Gauge
	if execCfg, ok := flowCtx.Cfg.ExecutorConfig.(*sql.ExecutorConfig); ok {
		inspectMetrics := execCfg.JobRegistry.MetricsStruct().Inspect.(*InspectMetrics)
		spansProcessedCtr = inspectMetrics.SpansProcessed
		numActiveSpansGauge = inspectMetrics.NumActiveSpans
	}

	return &inspectProcessor{
		spec:                spec,
		processorID:         processorID,
		flowCtx:             flowCtx,
		checkFactories:      checkFactories,
		cfg:                 flowCtx.Cfg,
		spanSrc:             newSliceSpanSource(spec.Spans),
		loggerBundle:        getInspectLogger(flowCtx, spec.JobID),
		concurrency:         getProcessorConcurrency(flowCtx),
		clock:               flowCtx.Cfg.DB.KV().Clock(),
		spansProcessedCtr:   spansProcessedCtr,
		numActiveSpansGauge: numActiveSpansGauge,
	}, nil
}

// buildInspectCheckFactories returns a slice of factory functions that create
// inspectCheck instances at runtime. Each factory corresponds to one check entry
// defined in the job's InspectSpec.
//
// This indirection ensures that each check instance is freshly created per span,
// avoiding shared state across concurrent workers.
func buildInspectCheckFactories(
	flowCtx *execinfra.FlowCtx, spec execinfrapb.InspectSpec,
) ([]inspectCheckFactory, error) {
	checkFactories := make([]inspectCheckFactory, 0, len(spec.InspectDetails.Checks))
	for _, specCheck := range spec.InspectDetails.Checks {
		tableID := specCheck.TableID
		indexID := specCheck.IndexID
		tableVersion := specCheck.TableVersion
		switch specCheck.Type {
		case jobspb.InspectCheckIndexConsistency:
			checkFactories = append(checkFactories, func(asOf hlc.Timestamp) inspectCheck {
				return &indexConsistencyCheck{
					indexConsistencyCheckApplicability: indexConsistencyCheckApplicability{
						tableID: tableID,
					},
					flowCtx:      flowCtx,
					indexID:      indexID,
					tableVersion: tableVersion,
					asOf:         asOf,
				}
			})

		default:
			return nil, errors.AssertionFailedf("unsupported inspect check type: %v", specCheck.Type)
		}
	}
	return checkFactories, nil
}

func init() {
	rowexec.NewInspectProcessor = newInspectProcessor
}
