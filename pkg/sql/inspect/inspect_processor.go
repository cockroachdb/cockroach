// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var (
	processorConcurrencyOverride = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"sql.inspect.processor_concurrency",
		"sets the number of concurrent processors for INSPECT jobs. "+
			"0 uses the default based on GOMAXPROCS. Values above GOMAXPROCS are capped.",
		0,
		settings.NonNegativeInt,
	)
)

type inspectCheckFactory func() inspectCheck

type inspectProcessor struct {
	processorID    int32
	flowCtx        *execinfra.FlowCtx
	spec           execinfrapb.InspectSpec
	cfg            *execinfra.ServerConfig
	checkFactories []inspectCheckFactory
	spanSrc        spanSource
	logger         inspectLogger
	concurrency    int
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
	log.Infof(ctx, "INSPECT processor started processorID=%d concurrency=%d", p.processorID, p.concurrency)

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
					if err := p.processSpan(ctx, span, workerIndex); err != nil {
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
	return group.Wait()
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

// processSpan executes all configured inspect checks against a single span.
// It instantiates a fresh set of checks from the configured factories and uses
// an inspectRunner to drive their execution.
func (p *inspectProcessor) processSpan(
	ctx context.Context, span roachpb.Span, workerIndex int,
) error {
	checks := make([]inspectCheck, len(p.checkFactories))
	for i, factory := range p.checkFactories {
		checks[i] = factory()
	}
	runner := inspectRunner{
		checks: checks,
		logger: p.logger,
	}

	// Process all checks on the given span.
	for {
		ok, err := runner.Step(ctx, p.cfg, span, workerIndex)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
	}
	return nil
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
	checkFactories, err := buildInspectCheckFactories(spec)
	if err != nil {
		return nil, err
	}
	return &inspectProcessor{
		spec:           spec,
		processorID:    processorID,
		flowCtx:        flowCtx,
		checkFactories: checkFactories,
		cfg:            flowCtx.Cfg,
		spanSrc:        newSliceSpanSource(spec.Spans),
		// TODO(148301): log to cockroach.log for now, but later log to system.inspect_errors
		logger:      &logSink{},
		concurrency: getProcessorConcurrency(flowCtx),
	}, nil
}

// buildInspectCheckFactories returns a slice of factory functions that create
// inspectCheck instances at runtime. Each factory corresponds to one check entry
// defined in the job's InspectSpec.
//
// This indirection ensures that each check instance is freshly created per span,
// avoiding shared state across concurrent workers.
func buildInspectCheckFactories(spec execinfrapb.InspectSpec) ([]inspectCheckFactory, error) {
	checkFactories := make([]inspectCheckFactory, 0, len(spec.InspectDetails.Checks))
	for _, specCheck := range spec.InspectDetails.Checks {
		switch specCheck.Type {
		case jobspb.InspectCheckIndexConsistency:
			// TODO(148863): implement the index consistency checker. No-op for now.

		default:
			return nil, errors.AssertionFailedf("unsupported inspect check type: %v", specCheck.Type)
		}
	}
	return checkFactories, nil
}

func init() {
	rowexec.NewInspectProcessor = newInspectProcessor
}
