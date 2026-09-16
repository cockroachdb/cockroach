// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	gojson "encoding/json"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl" // multi-tenant tests
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"    // locality-related table mutations
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // registers cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testServerRegion = "us-east-1"

// getWhereClause extracts the predicate from a randomly generated SQL statement.
func getWhereClause(query string) (string, bool) {
	var p parser.Parser
	stmts, err := p.Parse(query)
	if err != nil {
		return "", false
	}
	if len(stmts) != 1 {
		return "", false
	}
	selectStmt, ok := stmts[0].AST.(*tree.Select).Select.(*tree.SelectClause)
	if !ok {
		return "", false
	}
	if selectStmt.Where == nil || len(selectStmt.From.Tables) == 0 {
		return "", false
	}
	// Replace all table references with "seed" because we're not using the FROM clause so we can't reference aliases.
	replaced, err := tree.SimpleVisit(selectStmt.Where.Expr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if ci, ok := expr.(*tree.ColumnItem); ok {
			newCI := *ci
			newCI.TableName = &tree.UnresolvedObjectName{NumParts: 1, Parts: [3]string{``, ``, `seed`}}
			expr = &newCI
		}
		if un, ok := expr.(*tree.UnresolvedName); ok && un.NumParts > 1 {
			un.Parts[un.NumParts-1] = `seed`
		}
		return true, expr, nil
	})
	return replaced.String(), err == nil
}

func toJSON(t *testing.T, x any) string {
	t.Helper()
	// Our json library formats differently than the stdlib, and json.MakeJSON()
	// chokes on some inputs, so marshal it twice.
	bs, err := gojson.Marshal(x)
	require.NoError(t, err)
	j, err := json.ParseJSON(string(bs))
	require.NoError(t, err)
	return j.String()
}

// requireTerminalErrorSoon polls for the test feed for an error and asserts
// that the error matches the provided regex. This can either be a terminal
// error or an error encountered while parsing messages and doing testfeed
// things.
func requireTerminalErrorSoon(
	ctx context.Context, t *testing.T, f cdctest.TestFeed, errRegex *regexp.Regexp,
) {
	err := timeutil.RunWithTimeout(ctx, "requireTerminalErrorSoon", 30*time.Second, func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				m, err := f.Next()
				if err != nil {
					assert.Regexp(t, errRegex, err)
					return nil
				}
				log.Infof(ctx, "waiting for error; skipping test feed message: %s", m.String())
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

type testTelemetryLogger struct {
	telemetryLogger
	id                      int32
	afterIncEmittedCounters func(numMessages int, numBytes int)
}

var _ telemetryLogger = (*testTelemetryLogger)(nil)

func (t *testTelemetryLogger) incEmittedCounters(numMessages int, numBytes int) {
	t.telemetryLogger.incEmittedCounters(numMessages, numBytes)
	t.afterIncEmittedCounters(numMessages, numBytes)
}

func (t *testTelemetryLogger) maybeFlushLogs() {
	if t.id == 1 {
		t.telemetryLogger.maybeFlushLogs()
	}
}

func (t *testTelemetryLogger) close() {
	if t.id == 1 {
		t.telemetryLogger.close()
	}
}

func startMonitorWithBudget(budget int64) *mon.BytesMonitor {
	mm := mon.NewMonitor(mon.Options{
		Name:      mon.MakeName("test-mm"),
		Limit:     budget,
		Increment: 128, /* small allocation increment */
		Settings:  cluster.MakeTestingClusterSettings(),
	})
	mm.Start(context.Background(), nil, mon.NewStandaloneBudget(budget))
	return mm
}

type testSink struct{}

// getConcreteType implements the Sink interfaces.
func (s testSink) getConcreteType() sinkType {
	return sinkTypeNull
}

type memoryHoggingSink struct {
	testSink
	allEmitted chan struct{}
	mu         struct {
		syncutil.Mutex
		expectedRows int
		seenRows     map[string]struct{}
		numFlushes   int
		alloc        kvevent.Alloc
	}
}

var _ Sink = (*memoryHoggingSink)(nil)

func (s *memoryHoggingSink) expectRows(n int) chan struct{} {
	if n <= 0 {
		panic("n<=0")
	}
	s.allEmitted = make(chan struct{})
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.expectedRows = n
	s.mu.numFlushes = 0
	s.mu.seenRows = make(map[string]struct{})
	s.mu.alloc.Release(context.Background()) // Release leftover alloc
	return s.allEmitted
}

func (s *memoryHoggingSink) Dial() error {
	return nil
}

func (s *memoryHoggingSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
	headers rowHeaders,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.seenRows[string(key)] = struct{}{}
	s.mu.alloc.Merge(&alloc)
	if s.mu.expectedRows == len(s.mu.seenRows) && s.allEmitted != nil {
		close(s.allEmitted)
		s.allEmitted = nil
	}
	return nil
}

func (s *memoryHoggingSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	panic("should not be called")
}

func (s *memoryHoggingSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.numFlushes++
	s.mu.alloc.Release(ctx)
	return nil
}

func (s *memoryHoggingSink) numFlushes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.numFlushes
}
func (s *memoryHoggingSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.alloc.Release(context.Background())
	return nil
}

type countEmittedRowsSink struct {
	memoryHoggingSink
	numRows int64 // Accessed atomically; not using atomic.Int64 to make backports possible.
}

func (s *countEmittedRowsSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
	_headers rowHeaders,
) error {
	alloc.Release(ctx)
	atomic.AddInt64(&s.numRows, 1)
	return nil
}

var _ Sink = (*countEmittedRowsSink)(nil)

type changefeedLogSpy struct {
	syncutil.Mutex
	logs []string
}

// Intercept implements log.Interceptor.
func (s *changefeedLogSpy) Intercept(entry []byte) {
	s.Lock()
	defer s.Unlock()
	var j map[string]any
	if err := gojson.Unmarshal(entry, &j); err != nil {
		panic(err)
	}
	if !strings.Contains(j["file"].(string), "ccl/changefeedccl/") {
		return
	}

	s.logs = append(s.logs, j["message"].(string))
}

var _ log.Interceptor = (*changefeedLogSpy)(nil)

func assertReasonableMVCCTimestamp(t *testing.T, ts string) {
	epochNanos := parseTimeToHLC(t, ts).WallTime
	now := timeutil.Now()
	require.GreaterOrEqual(t, epochNanos, now.Add(-1*time.Hour).UnixNano())
}

func assertEqualTSNSHLCWalltime(t *testing.T, tsns int64, tshlc string) {
	tsHLCWallTimeNano := parseTimeToHLC(t, tshlc).WallTime
	require.EqualValues(t, tsns, tsHLCWallTimeNano)
}
