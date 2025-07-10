// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

// testInspectLogger is a test implementation of inspectLogger that collects issues in memory.
type testInspectLogger struct {
	mu     syncutil.Mutex
	issues []*inspectIssue
}

// logIssue implements the inspectLogger interface.
func (l *testInspectLogger) logIssue(_ context.Context, issue *inspectIssue) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.issues = append(l.issues, issue)
	return nil
}

// getIssues returns the issues that have been emitted to the logger.
func (l *testInspectLogger) getIssues() []*inspectIssue {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]*inspectIssue(nil), l.issues...)
}

// testingSpanSourceMode defines behavior for test only span sources.
// It is used to simulate producer-side edge cases.
type testingSpanSourceMode int

const (
	// Producer emits spans normally and stops.
	spanModeNormal testingSpanSourceMode = iota

	// Producer fails after N spans.
	spanModeFailsAfterN

	// Producer hangs after N spans. It hangs indefinitely unless context is
	// canceled.
	spanModeHangsAfterN
)

// testingSpanSource is an implementation of spanSource that emits spans
// according to the selected mode. It supports injecting hangs or failures.
type testingSpanSource struct {
	mode      testingSpanSourceMode
	count     int
	maxSpans  int
	failAfter int
	hangAfter int
}

var _ spanSource = (*testingSpanSource)(nil)

// NextSpan implements the spanSource interface.
func (s *testingSpanSource) NextSpan(ctx context.Context) (roachpb.Span, bool, error) {
	switch s.mode {
	case spanModeFailsAfterN:
		if s.failAfter == 0 || s.count >= s.failAfter {
			return roachpb.Span{}, false, errors.New("simulated producer failure")
		}
	case spanModeHangsAfterN:
		if s.hangAfter == 0 || s.count >= s.hangAfter {
			for {
				select {
				case <-ctx.Done():
					return roachpb.Span{}, false, ctx.Err()
				default:
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	case spanModeNormal:
		// No special error or hang handling.
	}

	if s.maxSpans > 0 && s.count >= s.maxSpans {
		return roachpb.Span{}, false, nil
	}
	s.count++
	return roachpb.Span{Key: roachpb.Key(fmt.Sprintf("test-span-%d", s.count))}, true, nil
}

// testingCheckMode defines behavior for test-only inspect checks.
// It is used to simulate worker-side edge cases.
type testingCheckMode int

const (
	checkModeNone testingCheckMode = iota

	// Worker fails after N iterations.
	checkModeFailsAfterN

	// Worker hangs until its context is cancelled.
	checkModeBlocksUntilCancel
)

// testingCheckConfig defines the test behavior for a single inspectCheck instance.
// Each check runs independently using its assigned config.
type testingCheckConfig struct {
	mode      testingCheckMode
	stopAfter int
	failAfter int
	issues    []*inspectIssue
}

// testingInspectCheck is a test implementation of inspectCheck.
// Each worker gets a separate copy, and behavior is driven by its assigned config.
type testingInspectCheck struct {
	started     bool
	configs     []testingCheckConfig
	index       int
	iteration   int
	issueCursor int
}

var _ inspectCheck = (*testingInspectCheck)(nil)

// Started implements the inspectCheck interface.
func (t *testingInspectCheck) Started() bool {
	return t.started
}

// Start implements the inspectCheck interface.
func (t *testingInspectCheck) Start(
	ctx context.Context, _ *execinfra.ServerConfig, _ roachpb.Span, workerIndex int,
) error {
	log.Infof(ctx, "Worker index %d given span", workerIndex)
	t.started = true
	t.index = workerIndex
	t.iteration = 0
	t.issueCursor = 0
	return nil
}

// Next implements the inspectCheck interface.
func (t *testingInspectCheck) Next(
	ctx context.Context, _ *execinfra.ServerConfig,
) (*inspectIssue, error) {
	cfg := t.configs[t.index]
	t.iteration++

	switch cfg.mode {
	case checkModeFailsAfterN:
		if cfg.failAfter == 0 || t.iteration >= cfg.failAfter {
			log.Infof(ctx, "Worker %d failing via test check", t.index)
			return nil, errors.New("worker failure triggered by test check")
		}
		time.Sleep(10 * time.Millisecond)

	case checkModeBlocksUntilCancel:
		log.Infof(ctx, "Worker %d blocking until cancelled", t.index)
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}

	case checkModeNone:
		time.Sleep(5 * time.Millisecond)
	}

	if t.issueCursor < len(cfg.issues) {
		issue := cfg.issues[t.issueCursor]
		log.Infof(ctx, "Worker %d emitting issue: %+v", t.index, issue)
		t.issueCursor++
		return issue, nil
	}

	return nil, nil
}

// Done implements the inspectCheck interface.
func (t *testingInspectCheck) Done(context.Context) bool {
	cfg := t.configs[t.index]
	switch cfg.mode {
	case checkModeFailsAfterN, checkModeBlocksUntilCancel:
		// Never returns done. These test modes runs forever and depend on errors,
		// context cancellation to stop.
		return false
	default:
		if cfg.stopAfter > 0 {
			return t.iteration >= cfg.stopAfter
		}
		// Fall back: done when all issues are emitted
		return t.issueCursor >= len(cfg.issues)
	}
}

// Close implements the inspectCheck interface.
func (t *testingInspectCheck) Close(context.Context) error {
	return nil
}

// runProcessorAndWait executes the given inspectProcessor and waits for it to complete.
// It asserts that the processor finishes within a fixed timeout, and that the result
// matches the expected error outcome.
func runProcessorAndWait(t *testing.T, proc *inspectProcessor, expectErr bool) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	processorResultCh := make(chan error, 1)
	go func() {
		processorResultCh <- proc.runInspect(ctx, nil)
	}()

	select {
	case err := <-processorResultCh:
		if expectErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		return
	case <-time.After(10 * time.Second):
		t.Fatal("neither span source nor processor finished in time")
	}
}

// makeProcessor will create an inspect processor for test.
func makeProcessor(
	t *testing.T, checkFactory inspectCheckFactory, src spanSource, concurrency int,
) (*inspectProcessor, *testInspectLogger) {
	t.Helper()
	logger := &testInspectLogger{}
	proc := &inspectProcessor{
		spec:           execinfrapb.InspectSpec{},
		checkFactories: []inspectCheckFactory{checkFactory},
		cfg: &execinfra.ServerConfig{
			Settings: cluster.MakeTestingClusterSettings(),
		},
		spanSrc:     src,
		logger:      logger,
		concurrency: concurrency,
	}
	return proc, logger
}

func TestInspectProcessor_ControlFlow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		desc      string
		configs   []testingCheckConfig
		spanSrc   *testingSpanSource
		expectErr bool
	}{
		{
			desc: "all goroutines exit cleanly",
			configs: []testingCheckConfig{
				{mode: checkModeNone, stopAfter: 1},
				{mode: checkModeNone, stopAfter: 1},
			},
			spanSrc: &testingSpanSource{
				mode:     spanModeNormal,
				maxSpans: 30,
			},
		},
		{
			desc: "one worker fails",
			configs: []testingCheckConfig{
				{mode: checkModeFailsAfterN, failAfter: 2},
			},
			spanSrc: &testingSpanSource{
				mode:     spanModeNormal,
				maxSpans: 100,
			},
			expectErr: true,
		},
		{
			desc: "one worker fails, one worker hangs",
			configs: []testingCheckConfig{
				{mode: checkModeFailsAfterN, failAfter: 2},
				{mode: checkModeBlocksUntilCancel},
			},
			spanSrc: &testingSpanSource{
				mode:     spanModeNormal,
				maxSpans: 10,
			},
			expectErr: true,
		},
		{
			desc: "producer fails",
			configs: []testingCheckConfig{
				{mode: checkModeNone, stopAfter: 2},
				{mode: checkModeNone, stopAfter: 2},
				{mode: checkModeNone, stopAfter: 2},
			},
			spanSrc: &testingSpanSource{
				mode:      spanModeFailsAfterN,
				failAfter: 8,
			},
			expectErr: true,
		},
		{
			desc: "producer hangs after emitting spans, worker triggers cancel",
			configs: []testingCheckConfig{
				{mode: checkModeFailsAfterN, failAfter: 1},
				{mode: checkModeNone, stopAfter: 5},
			},
			spanSrc: &testingSpanSource{
				mode:      spanModeHangsAfterN,
				hangAfter: 5, // emit 5 spans, then block
			},
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			factory := func() inspectCheck {
				return &testingInspectCheck{
					configs: tc.configs,
				}
			}
			proc, _ := makeProcessor(t, factory, tc.spanSrc, len(tc.configs))
			runProcessorAndWait(t, proc, tc.expectErr)
		})
	}
}

func TestInspectProcessor_EmitIssues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	spanSrc := &testingSpanSource{
		mode:     spanModeNormal,
		maxSpans: 1,
	}
	factory := func() inspectCheck {
		return &testingInspectCheck{
			configs: []testingCheckConfig{
				{
					mode: checkModeNone,
					issues: []*inspectIssue{
						{ErrorType: "test_error", PrimaryKey: "pk1"},
						{ErrorType: "test_error", PrimaryKey: "pk2"},
					},
				},
			},
		}
	}
	proc, logger := makeProcessor(t, factory, spanSrc, 1)

	runProcessorAndWait(t, proc, false)

	require.Len(t, logger.getIssues(), 2)
}
