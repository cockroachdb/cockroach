// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func (t *tokenCounter) testingGetLimit() tokensPerWorkClass {
	t.mu.Lock()
	defer t.mu.Unlock()

	return tokensPerWorkClass{
		regular: t.mu.counters[admissionpb.RegularWorkClass].limit,
		elastic: t.mu.counters[admissionpb.ElasticWorkClass].limit,
	}
}

func TestTokenAdjustment(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t, "token_counter"), func(t *testing.T, path string) {
		provider := NewStreamTokenCounterProvider(
			cluster.MakeTestingClusterSettings(),
			hlc.NewClockForTesting(nil),
		)
		var (
			ctx                      = context.Background()
			evalCounter, sendCounter *tokenCounter
			adjustments              []adjustment
		)

		ft := func(v int64) string {
			return printTrimmedTokens(kvflowcontrol.Tokens(v))
		}

		metricsString := func(t TokenType, buf *strings.Builder) {
			counterMetrics := provider.tokenMetrics.CounterMetrics[t]
			streamMetrics := provider.tokenMetrics.StreamMetrics[t]
			for _, wc := range []admissionpb.WorkClass{
				admissionpb.RegularWorkClass,
				admissionpb.ElasticWorkClass,
			} {
				fmt.Fprintf(buf, "  %-7v\n", wc)
				fmt.Fprintf(buf, "    %-66v: %v\n", streamMetrics.Count[wc].GetName(), streamMetrics.Count[wc].Value())
				fmt.Fprintf(buf, "    %-66v: %v\n", streamMetrics.BlockedCount[wc].GetName(), streamMetrics.BlockedCount[wc].Value())
				fmt.Fprintf(buf, "    %-66v: %v\n", streamMetrics.TokensAvailable[wc].GetName(), ft(streamMetrics.TokensAvailable[wc].Value()))
				fmt.Fprintf(buf, "    %-66v: %v\n", counterMetrics.Deducted[wc].GetName(), ft(counterMetrics.Deducted[wc].Count()))
				fmt.Fprintf(buf, "    %-66v: %v\n", counterMetrics.Disconnected[wc].GetName(), ft(counterMetrics.Disconnected[wc].Count()))
				fmt.Fprintf(buf, "    %-66v: %v\n", counterMetrics.Returned[wc].GetName(), ft(counterMetrics.Returned[wc].Count()))
				fmt.Fprintf(buf, "    %-66v: %v\n", counterMetrics.Unaccounted[wc].GetName(), ft(counterMetrics.Unaccounted[wc].Count()))
			}
			if t == SendToken {
				sendQueueMetrics := counterMetrics.SendQueue[0]
				fmt.Fprintf(buf, "  send queue token metrics\n")
				fmt.Fprintf(buf, "    %-66v: %v\n", sendQueueMetrics.ForceFlushDeducted.GetName(), ft(sendQueueMetrics.ForceFlushDeducted.Count()))
				for _, wc := range []admissionpb.WorkClass{
					admissionpb.RegularWorkClass,
					admissionpb.ElasticWorkClass,
				} {
					fmt.Fprintf(buf, "    %-66v: %v\n", sendQueueMetrics.PreventionDeducted[wc].GetName(), ft(sendQueueMetrics.PreventionDeducted[wc].Count()))
				}
			}
		}

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				var stream int
				d.ScanArgs(t, "stream", &stream)
				evalCounter = provider.Eval(kvflowcontrol.Stream{StoreID: roachpb.StoreID(stream)})
				sendCounter = provider.Send(kvflowcontrol.Stream{StoreID: roachpb.StoreID(stream)})
				adjustments = nil
				return ""

			case "adjust":
				typ := "eval"
				d.MaybeScanArgs(t, "type", &typ)
				var counter *tokenCounter
				switch typ {
				case "eval":
					counter = evalCounter
				case "send":
					counter = sendCounter
				default:
					t.Fatalf("unknown type: %s", typ)
				}
				require.NotNilf(t, counter, "uninitialized token counter (did you use 'init'?)")

				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					require.GreaterOrEqual(t, len(parts), 2, "expected form '"+
						"class={regular,elastic} "+
						"delta={+,-}<size> "+
						"[flag={normal,force_flush,prevention,disconnect}]",
					)

					var delta kvflowcontrol.Tokens
					var wc admissionpb.WorkClass
					var flag TokenAdjustFlag

					// Parse class={regular,elastic}.
					parts[0] = strings.TrimSpace(parts[0])
					require.True(t, strings.HasPrefix(parts[0], "class="))
					parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "class=")
					switch parts[0] {
					case "regular":
						wc = admissionpb.RegularWorkClass
					case "elastic":
						wc = admissionpb.ElasticWorkClass
					}

					// Parse delta={+,-}<size>
					parts[1] = strings.TrimSpace(parts[1])
					require.True(t, strings.HasPrefix(parts[1], "delta="))
					parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "delta=")
					require.True(t, strings.HasPrefix(parts[1], "+") || strings.HasPrefix(parts[1], "-"))
					isPositive := strings.Contains(parts[1], "+")
					parts[1] = strings.TrimPrefix(parts[1], "+")
					parts[1] = strings.TrimPrefix(parts[1], "-")
					bytes, err := humanize.ParseBytes(parts[1])
					require.NoError(t, err)
					delta = kvflowcontrol.Tokens(int64(bytes))
					if !isPositive {
						delta = -delta
					}

					// Parse [flag={normal,force_flush,prevention,disconnect}]
					if len(parts) >= 3 {
						parts[2] = strings.TrimSpace(parts[2])
						if strings.HasPrefix(parts[2], "flag=") {
							parts[2] = strings.TrimPrefix(parts[2], "flag=")
							switch parts[2] {
							case "normal":
								flag = AdjNormal
							case "force_flush":
								flag = AdjForceFlush
							case "prevention":
								flag = AdjPreventSendQueue
							case "disconnect":
								flag = AdjDisconnect
							default:
								t.Fatalf("unknown flag: %s", parts[2])
							}
						}
					}

					counter.adjust(ctx, wc, delta, flag)
					adjustments = append(adjustments, adjustment{
						wc:    wc,
						delta: delta,
						post: tokensPerWorkClass{
							regular: counter.tokens(admissionpb.RegularWorkClass),
							elastic: counter.tokens(admissionpb.ElasticWorkClass),
						},
						flag: flag,
					})
				}
				var b strings.Builder
				printStats := func(kind string, stats deltaStats) {
					fmt.Fprintf(&b, "%s: deducted: %s, returned: %s, force-flush: %s, prevent-send-q: %s\n",
						kind, stats.tokensDeducted, stats.tokensReturned, stats.tokensDeductedForceFlush,
						stats.tokensDeductedPreventSendQueue)
				}
				regularStats, elasticStats := counter.GetAndResetStats(timeutil.Now())
				printStats("regular", regularStats)
				printStats("elastic", elasticStats)
				return b.String()

			case "history":
				typ := "eval"
				d.MaybeScanArgs(t, "type", &typ)

				var counter *tokenCounter
				switch typ {
				case "eval":
					counter = evalCounter
				case "send":
					counter = sendCounter
				default:
					t.Fatalf("unknown type: %s", typ)
				}

				limit := counter.testingGetLimit()

				var buf strings.Builder
				buf.WriteString("                                     regular |  elastic\n")
				buf.WriteString(fmt.Sprintf("                                    %8s | %8s\n",
					printTrimmedTokens(limit.regular),
					printTrimmedTokens(limit.elastic),
				))
				buf.WriteString("=======================================================\n")
				for _, h := range adjustments {
					buf.WriteString(fmt.Sprintf("%s\n", h))
				}
				return buf.String()

			case "metrics":
				typ := "eval"
				d.MaybeScanArgs(t, "type", &typ)

				provider.UpdateMetricGauges()
				var buf strings.Builder
				switch typ {
				case "eval":
					metricsString(EvalToken, &buf)
				case "send":
					metricsString(SendToken, &buf)
				case "all":
					buf.WriteString("eval\n")
					metricsString(EvalToken, &buf)
					buf.WriteString("send\n")
					metricsString(SendToken, &buf)
				default:
					t.Fatalf("unknown type: %s", typ)
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

type adjustment struct {
	wc    admissionpb.WorkClass
	delta kvflowcontrol.Tokens
	post  tokensPerWorkClass
	flag  TokenAdjustFlag
}

func printTrimmedTokens(t kvflowcontrol.Tokens) string {
	return strings.ReplaceAll(t.String(), " ", "")
}

func (h adjustment) String() string {
	comment := ""
	if h.post.regular <= 0 {
		comment = "regular"
	}
	if h.post.elastic <= 0 {
		if len(comment) == 0 {
			comment = "elastic"
		} else {
			comment = "regular and elastic"
		}
	}
	if len(comment) != 0 {
		comment = fmt.Sprintf(" (%s blocked)", comment)
	}
	return fmt.Sprintf("%8s %7s %-18s %8s | %8s%s",
		printTrimmedTokens(h.delta),
		h.wc,
		h.flag,
		printTrimmedTokens(h.post.regular),
		printTrimmedTokens(h.post.elastic),
		comment,
	)
}

func TestTokenCounter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	limits := tokensPerWorkClass{
		regular: 50,
		elastic: 50,
	}
	settings := cluster.MakeTestingClusterSettings()
	kvflowcontrol.ElasticTokensPerStream.Override(ctx, &settings.SV, int64(limits.elastic))
	kvflowcontrol.RegularTokensPerStream.Override(ctx, &settings.SV, int64(limits.regular))
	counter := newTokenCounter(
		settings,
		hlc.NewClockForTesting(nil),
		newTokenCounterMetrics(EvalToken),
		kvflowcontrol.Stream{},
		EvalToken,
	)

	assertStateReset := func(t *testing.T) {
		available, handle := counter.TokensAvailable(admissionpb.ElasticWorkClass)
		require.True(t, available)
		require.Equal(t, tokenWaitHandle{}, handle)
		require.Equal(t, limits.regular, counter.tokens(admissionpb.RegularWorkClass))
		require.Equal(t, limits.elastic, counter.tokens(admissionpb.ElasticWorkClass))
	}

	t.Run("tokens_available", func(t *testing.T) {
		// Initially, tokens should be available for both regular and elastic work
		// classes.
		available, handle := counter.TokensAvailable(admissionpb.RegularWorkClass)
		require.True(t, available)
		require.Equal(t, tokenWaitHandle{}, handle)

		available, handle = counter.TokensAvailable(admissionpb.ElasticWorkClass)
		require.True(t, available)
		require.Equal(t, tokenWaitHandle{}, handle)
		assertStateReset(t)
	})

	t.Run("deduct_partial", func(t *testing.T) {
		// Try deducting more tokens than available. Only the available tokens
		// should be deducted.
		t.Logf("tokens before %v", counter.tokens(admissionpb.RegularWorkClass))
		granted := counter.TryDeduct(ctx, admissionpb.RegularWorkClass, limits.regular+50, AdjNormal)
		require.Equal(t, limits.regular, granted)
		t.Logf("tokens after %v", counter.tokens(admissionpb.RegularWorkClass))

		// Now there should be no tokens available for regular work class.
		available, handle := counter.TokensAvailable(admissionpb.RegularWorkClass)
		require.False(t, available)
		require.NotEqual(t, tokenWaitHandle{}, handle)
		counter.Return(ctx, admissionpb.RegularWorkClass, limits.regular, AdjNormal)
		assertStateReset(t)
	})

	t.Run("tokens_unavailable", func(t *testing.T) {
		// Deduct tokens without checking availability, going into debt.
		counter.Deduct(ctx, admissionpb.ElasticWorkClass, limits.elastic+50, AdjNormal)
		// Tokens should now be in debt, meaning future deductions will also go
		// into further debt, or on TryDeduct, deduct no tokens at all.
		granted := counter.TryDeduct(ctx, admissionpb.ElasticWorkClass, 50, AdjNormal)
		require.Equal(t, kvflowcontrol.Tokens(0), granted)
		// Return tokens to bring the counter out of debt.
		counter.Return(ctx, admissionpb.ElasticWorkClass, limits.elastic+50, AdjNormal)
		assertStateReset(t)
	})

	t.Run("wait_no_tokens", func(t *testing.T) {
		// Use up all the tokens trying to deduct the maximum+1
		// (tokensPerWorkClass) tokens. There should be exactly tokensPerWorkClass
		// tokens granted.
		granted := counter.TryDeduct(ctx, admissionpb.RegularWorkClass, limits.regular+1, AdjNormal)
		require.Equal(t, limits.regular, granted)
		// There should be no tokens available for regular work and a handle
		// returned.
		available, handle := counter.TokensAvailable(admissionpb.RegularWorkClass)
		require.False(t, available)
		require.NotEqual(t, tokenWaitHandle{}, handle)
		counter.Return(ctx, admissionpb.RegularWorkClass, limits.regular, AdjNormal)
		// Wait on the handle to be unblocked and expect that there are tokens
		// available when the wait channel is signaled.
		<-handle.waitChannel()
		haveTokens := handle.confirmHaveTokensAndUnblockNextWaiter()
		require.True(t, haveTokens)
		// Wait on the handle to be unblocked again, this time try deducting such
		// that there are no tokens available after.
		counter.Deduct(ctx, admissionpb.RegularWorkClass, limits.regular, AdjNormal)
		<-handle.waitChannel()
		haveTokens = handle.confirmHaveTokensAndUnblockNextWaiter()
		require.False(t, haveTokens)
		// Return the tokens deducted from the first wait above.
		counter.Return(ctx, admissionpb.RegularWorkClass, limits.regular, AdjNormal)
		assertStateReset(t)
	})

	t.Run("wait_multi_goroutine", func(t *testing.T) {
		// Create a group of goroutines which will race on deducting tokens, each
		// requires exactly the limit, so only one will succeed at a time.
		numGoroutines := 5
		tokensRequested := limits.regular
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				remaining := limits.regular

				for {
					granted := counter.TryDeduct(ctx, admissionpb.RegularWorkClass, remaining, AdjNormal)
					remaining = remaining - granted
					if remaining == 0 {
						break
					}
					// If not enough tokens are granted, wait for tokens to become
					// available.
					available, handle := counter.TokensAvailable(admissionpb.RegularWorkClass)
					if !available {
						<-handle.waitChannel()
						// This may or may not have raced with another goroutine, there's
						// no guarantee we have tokens here. If we don't have tokens here,
						// the next call to TryDeduct will fail (unless someone returns
						// tokens between here and that call), which is harmless. This test
						// is using TokensAvailable and the returned handle to avoid
						// busy-waiting.
						handle.confirmHaveTokensAndUnblockNextWaiter()
					}
				}

				// Ensure all requested tokens are granted eventually and return the
				// tokens back to the counter.
				require.Equal(t, kvflowcontrol.Tokens(0), remaining)
				counter.Return(ctx, admissionpb.RegularWorkClass, tokensRequested, AdjNormal)
			}()
		}
		wg.Wait()
		assertStateReset(t)
	})
}

func (t *tokenCounter) testingHandle() tokenWaitHandle {
	return tokenWaitHandle{wc: admissionpb.RegularWorkClass, b: t}
}

type namedTokenCounter struct {
	*tokenCounter
	parent *evalTestState
	stream string
}

type evalTestState struct {
	settings *cluster.Settings
	mu       struct {
		syncutil.Mutex
		counters map[string]*namedTokenCounter
		evals    map[string]*testEval
	}
}

type testEval struct {
	state            WaitEndState
	handles          []tokenWaitingHandleInfo
	quorum           int
	cancel           context.CancelFunc
	configRefreshCh  chan struct{}
	replicaRefreshCh chan struct{}
}

func newTestState() *evalTestState {
	ts := &evalTestState{
		settings: cluster.MakeTestingClusterSettings(),
	}
	// We will only use at most one token per stream, as we only require positive
	// / non-positive token counts.
	kvflowcontrol.RegularTokensPerStream.Override(context.Background(), &ts.settings.SV, 1)
	ts.mu.counters = make(map[string]*namedTokenCounter)
	ts.mu.evals = make(map[string]*testEval)
	return ts
}

func (ts *evalTestState) getOrCreateTC(stream string) *namedTokenCounter {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	tc, exists := ts.mu.counters[stream]
	if !exists {
		tc = &namedTokenCounter{
			parent: ts,
			tokenCounter: newTokenCounter(
				ts.settings,
				hlc.NewClockForTesting(nil),
				newTokenCounterMetrics(EvalToken),
				kvflowcontrol.Stream{},
				EvalToken,
			),
			stream: stream,
		}
		// Ensure the token counter starts with no tokens initially.
		tc.adjust(context.Background(),
			admissionpb.RegularWorkClass,
			-kvflowcontrol.Tokens(kvflowcontrol.RegularTokensPerStream.Get(&ts.settings.SV)),
			AdjNormal,
		)
		ts.mu.counters[stream] = tc
	}
	return tc
}

func (ts *evalTestState) startWaitForEval(
	name string, handles []tokenWaitingHandleInfo, quorum int,
) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	configRefreshCh := make(chan struct{})
	replicaRefreshCh := make(chan struct{})
	ts.mu.evals[name] = &testEval{
		state:            -1,
		handles:          handles,
		quorum:           quorum,
		cancel:           cancel,
		configRefreshCh:  configRefreshCh,
		replicaRefreshCh: replicaRefreshCh,
	}

	go func() {
		state, _ := WaitForEval(
			ctx, configRefreshCh, replicaRefreshCh, handles, quorum, false, nil)
		ts.mu.Lock()
		defer ts.mu.Unlock()

		ts.mu.evals[name].state = state
	}()
}

func (ts *evalTestState) setCounterTokens(stream string, positive bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	tc, exists := ts.mu.counters[stream]
	if !exists {
		panic(fmt.Sprintf("no token counter found for stream: %s", stream))
	}

	wasPositive := tc.tokens(admissionpb.RegularWorkClass) > 0
	if !wasPositive && positive {
		tc.tokenCounter.adjust(context.Background(), admissionpb.RegularWorkClass, +1, AdjNormal)
	} else if wasPositive && !positive {
		tc.tokenCounter.adjust(context.Background(), admissionpb.RegularWorkClass, -1, AdjNormal)
	}
}

func (ts *evalTestState) tokenCountsString() string {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	var streams []string
	for stream := range ts.mu.counters {
		streams = append(streams, stream)
	}
	sort.Strings(streams)

	var b strings.Builder
	for _, stream := range streams {
		tc := ts.mu.counters[stream]
		posString := "non-positive"
		if tc.tokens(admissionpb.RegularWorkClass) > 0 {
			posString = "positive"
		}
		b.WriteString(fmt.Sprintf("%s: %s\n", stream, posString))
	}
	return strings.TrimSpace(b.String())
}

func (ts *evalTestState) evalStatesString() string {
	// Introduce a sleep here to ensure that any evaluation which complete update
	// state before we lock the mutex.
	time.Sleep(100 * time.Millisecond)
	ts.mu.Lock()
	defer ts.mu.Unlock()

	var states []string
	for name, op := range ts.mu.evals {
		switch op.state {
		case -1:
			states = append(states, fmt.Sprintf("%s: waiting", name))
		default:
			states = append(states, fmt.Sprintf("%s: %s", name, op.state))
		}
	}
	sort.Strings(states)
	return strings.Join(states, "\n")
}

// TestWaitForEval is a datadriven test that exercises the WaitForEval function.
//
//   - wait_for_eval <name> <quorum> [handle: <stream> required=<true|false>]
//     name: the name of the WaitForEval operation.
//     quorum: the number of handles that must be unblocked for the operation to
//     succeed.
//     stream: the stream name.
//     required: whether the handle is required for the operation to succeed.
//
//   - set_tokens <stream> <positive>
//     stream: the stream name.
//     positive: whether the stream should have positive tokens.
//
//   - check_state
//     Prints the current state of all WaitForEval operations.
//
//   - cancel <name>
//     name: the name of the WaitForEval operation to cancel.
//
//   - refresh <name>
//     name: the name of the WaitForEval operation to refresh.
func TestWaitForEval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := newTestState()
	datadriven.RunTest(t, "testdata/wait_for_eval", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "wait_for_eval":
			var name string
			var quorum int
			var handles []tokenWaitingHandleInfo

			d.ScanArgs(t, "name", &name)
			d.ScanArgs(t, "quorum", &quorum)
			for _, line := range strings.Split(d.Input, "\n") {
				require.True(t, strings.HasPrefix(line, "handle:"))
				line = strings.TrimPrefix(line, "handle:")
				line = strings.TrimSpace(line)
				parts := strings.Split(line, " ")
				require.Len(t, parts, 2)

				parts[0] = strings.TrimSpace(parts[0])
				require.True(t, strings.HasPrefix(parts[0], "stream="))
				parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "stream=")
				stream := parts[0]

				parts[1] = strings.TrimSpace(parts[1])
				require.True(t, strings.HasPrefix(parts[1], "required="))
				parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "required=")
				required := parts[1] == "true"

				handleInfo := tokenWaitingHandleInfo{
					handle:       ts.getOrCreateTC(stream).testingHandle(),
					requiredWait: required,
					partOfQuorum: true,
				}
				handles = append(handles, handleInfo)
			}

			ts.startWaitForEval(name, handles, quorum)
			return ts.evalStatesString()

		case "set_tokens":
			for _, arg := range d.CmdArgs {
				require.Equal(t, 1, len(arg.Vals))
				ts.setCounterTokens(arg.Key, arg.Vals[0] == "positive")
			}
			return ts.tokenCountsString()

		case "check_state":
			return ts.evalStatesString()

		case "cancel":
			var name string
			d.ScanArgs(t, "name", &name)
			func() {
				ts.mu.Lock()
				defer ts.mu.Unlock()
				if op, exists := ts.mu.evals[name]; exists {
					op.cancel()
				} else {
					panic(fmt.Sprintf("no WaitForEval operation with name: %s", name))
				}
			}()
			return ts.evalStatesString()

		case "refresh":
			var name string
			d.ScanArgs(t, "name", &name)
			var kind string
			d.ScanArgs(t, "kind", &kind)
			func() {
				ts.mu.Lock()
				defer ts.mu.Unlock()
				if op, exists := ts.mu.evals[name]; exists {
					switch kind {
					case "config":
						op.configRefreshCh <- struct{}{}
					case "replica":
						op.replicaRefreshCh <- struct{}{}
					default:
						panic(fmt.Sprintf("unknown channel kind %s", kind))
					}
				} else {
					panic(fmt.Sprintf("no WaitForEval operation with name: %s", name))
				}
			}()
			return ts.evalStatesString()

		default:
			panic(fmt.Sprintf("unknown command: %s", d.Cmd))
		}
	})
}

func TestTokenCounterReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	provider := NewStreamTokenCounterProvider(st, hlc.NewClockForTesting(nil))
	stream := kvflowcontrol.Stream{StoreID: 1}
	evalCounter := provider.Eval(stream)
	sendCounter := provider.Send(stream)
	evalCounter.Deduct(ctx, admissionpb.RegularWorkClass, 1<<20, AdjNormal)
	sendCounter.Deduct(ctx, admissionpb.RegularWorkClass, 1<<20, AdjNormal)
	for wc := admissionpb.WorkClass(0); wc < admissionpb.NumWorkClasses; wc++ {
		require.Greater(t, evalCounter.limit(wc), evalCounter.tokens(wc))
		require.Greater(t, sendCounter.limit(wc), sendCounter.tokens(wc))
	}
	prevEpoch := kvflowcontrol.TokenCounterResetEpoch.Get(&st.SV)
	kvflowcontrol.TokenCounterResetEpoch.Override(ctx, &st.SV, prevEpoch+1)
	for wc := admissionpb.WorkClass(0); wc < admissionpb.NumWorkClasses; wc++ {
		require.Equal(t, evalCounter.limit(wc), evalCounter.tokens(wc))
		require.Equal(t, sendCounter.limit(wc), sendCounter.tokens(wc))
	}
}
