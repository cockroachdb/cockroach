// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
)

func TestBlockedStreamLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := log.ScopeWithoutShowLogs(t)
	// Causes every call to update the gauges to log.
	prevVModule := log.GetVModule()
	_ = log.SetVModule("store_stream=2")
	defer func() { _ = log.SetVModule(prevVModule) }()
	defer s.Close(t)

	ctx := context.Background()
	testStartTs := timeutil.Now()

	makeStream := func(id uint64) kvflowcontrol.Stream {
		return kvflowcontrol.Stream{
			TenantID: roachpb.MustMakeTenantID(id),
			StoreID:  roachpb.StoreID(id),
		}
	}

	st := cluster.MakeTestingClusterSettings()
	const numTokens = 1 << 20 /* 1 MiB */
	kvflowcontrol.ElasticTokensPerStream.Override(ctx, &st.SV, numTokens)
	kvflowcontrol.RegularTokensPerStream.Override(ctx, &st.SV, numTokens)
	p := NewStreamTokenCounterProvider(st, hlc.NewClockForTesting(nil))

	numBlocked := 0
	createStreamAndExhaustTokens := func(id uint64, checkMetric bool) {
		p.Eval(makeStream(id)).Deduct(ctx, admissionpb.RegularWorkClass, kvflowcontrol.Tokens(numTokens), AdjNormal)
		if checkMetric {
			p.UpdateMetricGauges()
			require.Equal(t, int64(numBlocked+1), p.tokenMetrics.StreamMetrics[EvalToken].BlockedCount[elastic].Value())
			require.Equal(t, int64(numBlocked+1), p.tokenMetrics.StreamMetrics[EvalToken].BlockedCount[regular].Value())
		}
		numBlocked++
	}
	// 1 stream that is blocked.
	id := uint64(1)
	createStreamAndExhaustTokens(id, true)
	// Total 24 streams are blocked.
	for id++; id < 25; id++ {
		createStreamAndExhaustTokens(id, false)
	}
	// 25th stream will also be blocked. The detailed stats will only cover an
	// arbitrary subset of 20 streams.
	log.Infof(ctx, "creating stream id %d", id)
	createStreamAndExhaustTokens(id, true)

	// Total 104 streams are blocked.
	for id++; id < 105; id++ {
		createStreamAndExhaustTokens(id, false)
	}
	// 105th stream will also be blocked. The blocked stream names will only
	// list 100 streams.
	log.Infof(ctx, "creating stream id %d", id)
	createStreamAndExhaustTokens(id, true)

	log.FlushFiles()
	entries, err := log.FetchEntriesFromFiles(testStartTs.UnixNano(),
		math.MaxInt64, 2000,
		regexp.MustCompile(`store_stream\.go|store_stream_test\.go`),
		log.WithMarkedSensitiveData)
	require.NoError(t, err)

	blockedStreamRegexp, err := regexp.Compile(
		"eval stream .* was blocked: durations: regular .* elastic .* tokens delta: regular .* elastic .*")
	require.NoError(t, err)
	blockedStreamSkippedRegexp, err := regexp.Compile(
		"skipped logging some streams that were blocked")
	require.NoError(t, err)

	const blockedCountElasticRegexp = "%d blocked eval elastic replication stream.*"
	const blockedCountRegularRegexp = "%d blocked eval regular replication stream.*"
	blocked1ElasticRegexp, err := regexp.Compile(fmt.Sprintf(blockedCountElasticRegexp, 1))
	require.NoError(t, err)
	blocked1RegularRegexp, err := regexp.Compile(fmt.Sprintf(blockedCountRegularRegexp, 1))
	require.NoError(t, err)
	blocked25ElasticRegexp, err := regexp.Compile(fmt.Sprintf(blockedCountElasticRegexp, 25))
	require.NoError(t, err)
	blocked25RegularRegexp, err := regexp.Compile(fmt.Sprintf(blockedCountRegularRegexp, 25))
	require.NoError(t, err)
	blocked105ElasticRegexp, err := regexp.Compile(
		"105 blocked eval elastic replication stream.* omitted some due to overflow")
	require.NoError(t, err)
	blocked105RegularRegexp, err := regexp.Compile(
		"105 blocked eval regular replication stream.* omitted some due to overflow")
	require.NoError(t, err)

	const creatingRegexp = "creating stream id %d"
	creating25Regexp, err := regexp.Compile(fmt.Sprintf(creatingRegexp, 25))
	require.NoError(t, err)
	creating105Regexp, err := regexp.Compile(fmt.Sprintf(creatingRegexp, 105))
	require.NoError(t, err)

	blockedStreamCount := 0
	foundBlockedElastic := false
	foundBlockedRegular := false
	foundBlockedStreamSkipped := false
	// First section of the log where 1 stream blocked. Entries are in reverse
	// chronological order.
	index := len(entries) - 1
	for ; index >= 0; index-- {
		entry := entries[index]
		if creating25Regexp.MatchString(entry.Message) {
			break
		}
		if blockedStreamRegexp.MatchString(entry.Message) {
			blockedStreamCount++
		}
		if blocked1ElasticRegexp.MatchString(entry.Message) {
			foundBlockedElastic = true
		}
		if blocked1RegularRegexp.MatchString(entry.Message) {
			foundBlockedRegular = true
		}
		if blockedStreamSkippedRegexp.MatchString(entry.Message) {
			foundBlockedStreamSkipped = true
		}
	}
	require.Equal(t, 1, blockedStreamCount)
	require.True(t, foundBlockedElastic)
	require.True(t, foundBlockedRegular)
	require.False(t, foundBlockedStreamSkipped)

	blockedStreamCount = 0
	foundBlockedElastic = false
	foundBlockedRegular = false
	// Second section of the log where 25 streams are blocked and 20 are logged
	// (streamStatsCountCap).
	for ; index >= 0; index-- {
		entry := entries[index]
		if creating105Regexp.MatchString(entry.Message) {
			break
		}
		if blockedStreamRegexp.MatchString(entry.Message) {
			blockedStreamCount++
		}
		if blocked25ElasticRegexp.MatchString(entry.Message) {
			foundBlockedElastic = true
		}
		if blocked25RegularRegexp.MatchString(entry.Message) {
			foundBlockedRegular = true
		}
		if blockedStreamSkippedRegexp.MatchString(entry.Message) {
			foundBlockedStreamSkipped = true
		}
	}
	require.Equal(t, 20, blockedStreamCount)
	require.True(t, foundBlockedElastic)
	require.True(t, foundBlockedRegular)
	require.True(t, foundBlockedStreamSkipped)

	blockedStreamCount = 0
	foundBlockedElastic = false
	foundBlockedRegular = false
	// Third section of the log where 105 streams blocked.
	for ; index >= 0; index-- {
		entry := entries[index]
		if blockedStreamRegexp.MatchString(entry.Message) {
			blockedStreamCount++
		}
		if blocked105ElasticRegexp.MatchString(entry.Message) {
			foundBlockedElastic = true
		}
		if blocked105RegularRegexp.MatchString(entry.Message) {
			foundBlockedRegular = true
		}
		if blockedStreamSkippedRegexp.MatchString(entry.Message) {
			foundBlockedStreamSkipped = true
		}
	}
	require.Equal(t, 20, blockedStreamCount)
	require.True(t, foundBlockedElastic, "unable to find %v", blocked105ElasticRegexp)
	require.True(t, foundBlockedRegular, "unable to find %v", blocked105RegularRegexp)
	require.True(t, foundBlockedStreamSkipped, "unable to find %v", blockedStreamSkippedRegexp)
}

func TestStreamTokenCounterProviderInspect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stream := func(i uint64) kvflowcontrol.Stream {
		return kvflowcontrol.Stream{
			TenantID: roachpb.MustMakeTenantID(1),
			StoreID:  roachpb.StoreID(i),
		}
	}

	marshaller := jsonpb.Marshaler{
		Indent:       "  ",
		EmitDefaults: true,
		OrigName:     true,
	}

	p := NewStreamTokenCounterProvider(
		cluster.MakeTestingClusterSettings(), hlc.NewClockForTesting(nil))

	var buf strings.Builder
	defer func() { echotest.Require(t, buf.String(), datapathutils.TestDataPath(t, "stream_inspect")) }()

	record := func(header string) {
		if buf.Len() > 0 {
			buf.WriteString("\n\n")
		}
		buf.WriteString(fmt.Sprintf("# %s\n", header))
		for _, state := range p.Inspect(ctx) {
			marshaled, err := marshaller.MarshalToString(&state)
			require.NoError(t, err)
			buf.WriteString(marshaled)
			buf.WriteString("\n")
		}
	}

	record("No streams.")

	p.Eval(stream(1)).Deduct(ctx, admissionpb.RegularWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	record("Single stream with 1 MiB of eval regular tokens deducted.")

	p.Send(stream(1)).Deduct(ctx, admissionpb.ElasticWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	record("Single stream with 1 MiB of elastic+regular (send+eval) tokens deducted.")

	p.Send(stream(1)).Return(ctx, admissionpb.ElasticWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	p.Eval(stream(2)).Deduct(ctx, admissionpb.RegularWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	p.Eval(stream(3)).Deduct(ctx, admissionpb.RegularWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	record("Three streams, with 1 MiB of regular tokens deducted each.")

	p.Send(stream(1)).Deduct(ctx, admissionpb.ElasticWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	p.Send(stream(2)).Deduct(ctx, admissionpb.ElasticWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	p.Send(stream(3)).Deduct(ctx, admissionpb.ElasticWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	record("Three streams, with 1 MiB of elastic send tokens deducted from each.")

	p.Eval(stream(1)).Return(ctx, admissionpb.RegularWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	p.Eval(stream(2)).Return(ctx, admissionpb.RegularWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	p.Eval(stream(3)).Return(ctx, admissionpb.RegularWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	p.Send(stream(1)).Return(ctx, admissionpb.ElasticWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	p.Send(stream(2)).Return(ctx, admissionpb.ElasticWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	p.Send(stream(3)).Return(ctx, admissionpb.ElasticWorkClass, 1<<20 /* 1 MiB */, AdjNormal)
	record("Three streams, all tokens returned.")
}

// testingTokenGrantNotification is a testing implementation of
// TokenGrantNotification.
type testingTokenGrantNotification struct {
	tc                  *tokenCounter
	name                string
	handle              SendTokenWatcherHandle
	deduction, deducted kvflowcontrol.Tokens
	cancelled           bool
}

var _ TokenGrantNotification = &testingTokenGrantNotification{}

func (n *testingTokenGrantNotification) String() string {
	var state string
	if n.cancelled {
		state = "cancelled"
	} else if n.deducted == n.deduction {
		state = "notified"
	} else {
		state = "waiting"
	}
	return fmt.Sprintf("name=%s handle_id=%d deducted=%d/%d state=%s",
		n.name, n.handle.id, n.deducted, n.deduction, state)
}

func (n *testingTokenGrantNotification) Notify(ctx context.Context) {
	n.tc.Deduct(ctx, admissionpb.ElasticWorkClass, n.deduction, AdjNormal)
	n.deducted += n.deduction
}

func testingParseLineInts(t *testing.T, line string, keys ...string) []int {
	var err error
	ret := make([]int, len(keys))
	parts := strings.Fields(line)
	for i, key := range keys {
		parts[i] = strings.TrimSpace(parts[i])
		require.True(t, strings.HasPrefix(parts[i], key+"="))
		parts[i] = strings.TrimPrefix(parts[i], key+"=")
		ret[i], err = strconv.Atoi(parts[i])
		require.NoError(t, err)
	}
	return ret
}

// TestSendTokenWatcher is a datadriven test that exercises the SendTokenWatcher.
//
//   - register: register notification(s) with the given deduction(s). The
//     deduction will applied when notified.
//     name=<string> store_id=<store_id> deduct=<tokens>
//     ...
//
//   - adjust: adjust the elastic tokens for the given stream.
//     store_id=<store_id> tokens=<tokens>
//     ...
//
//   - cancel: cancel the notification with the given handle_id.
//     name=<string>
func TestSendTokenWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := timeutil.NewManualTime(timeutil.UnixEpoch)
	notifications := make(map[string]*testingTokenGrantNotification)
	streamOrderng := make(map[kvflowcontrol.Stream][]string)
	zeroedCounters := make(map[kvflowcontrol.Stream]struct{})
	watcher := NewSendTokenWatcher(stopper, clock)
	provider := NewStreamTokenCounterProvider(
		cluster.MakeTestingClusterSettings(), hlc.NewClockForTesting(nil))

	makeStream := func(storeID roachpb.StoreID) kvflowcontrol.Stream {
		return kvflowcontrol.Stream{
			StoreID:  storeID,
			TenantID: roachpb.SystemTenantID,
		}
	}

	makeStateString := func() string {
		// Introduce a sleep to ensure that any channel signals are processed
		// before proceeding. This ensures the test remains deterministic.
		time.Sleep(10 * time.Millisecond)
		var buf strings.Builder
		var streams []kvflowcontrol.Stream
		watcher.watchers.Range(func(k kvflowcontrol.Stream, v *sendStreamTokenWatcher) bool {
			streams = append(streams, k)
			return true
		})
		// Sort for deterministic output.
		slices.SortFunc(streams, func(a, b kvflowcontrol.Stream) int {
			return cmp.Or(
				cmp.Compare(a.TenantID.ToUint64(), b.TenantID.ToUint64()),
				cmp.Compare(a.StoreID, b.StoreID),
			)
		})
		for _, stream := range streams {
			tc := provider.Send(stream)
			sendWatcher := watcher.watcher(tc)
			sendWatcher.mu.Lock()
			defer sendWatcher.mu.Unlock()

			buf.WriteString(fmt.Sprintf("stream=%v tokens=%d len=%d running=%v\n",
				stream, tc.tokens(sendTokenWatcherWC), len(sendWatcher.mu.queueItems),
				sendWatcher.mu.started))
			for _, name := range streamOrderng[stream] {
				notification := notifications[name]
				buf.WriteString(fmt.Sprintf("  %v\n", notification))
			}
		}
		return buf.String()
	}

	datadriven.RunTest(t, "testdata/send_token_watcher",
		func(t *testing.T, d *datadriven.TestData) string {

			switch d.Cmd {
			case "register":
				for _, line := range strings.Split(d.Input, "\n") {
					// Parse the name.
					parts := strings.Fields(line)
					parts[0] = strings.TrimSpace(parts[0])
					require.True(t, strings.HasPrefix(parts[0], "name="))
					parts[0] = strings.TrimPrefix(parts[0], "name=")
					name := parts[0]

					// Parse the remaining int fields.
					fields := testingParseLineInts(t, strings.Join(parts[1:], " "), "store_id", "deduct")
					storeID, deduction := roachpb.StoreID(fields[0]), kvflowcontrol.Tokens(fields[1])

					stream := makeStream(storeID)
					tc := provider.Send(stream)
					if _, ok := zeroedCounters[stream]; !ok {
						// Zero the token counter if its the first time we're seeing it. We
						// want all streams to start with 0 elastic tokens.
						tc.adjust(ctx, admissionpb.ElasticWorkClass,
							-kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Default()),
							AdjNormal)
						zeroedCounters[stream] = struct{}{}
					}

					notification := &testingTokenGrantNotification{
						name:      name,
						tc:        tc,
						deduction: deduction,
					}
					notification.handle = watcher.NotifyWhenAvailable(ctx, tc, notification)
					notifications[name] = notification
					streamOrderng[stream] = append(streamOrderng[stream], name)
				}
				return makeStateString()

			case "adjust":
				for _, line := range strings.Split(d.Input, "\n") {
					fields := testingParseLineInts(t, line, "store_id", "tokens")
					provider.Send(makeStream(roachpb.StoreID(fields[0]))).adjust(
						ctx,
						sendTokenWatcherWC,
						kvflowcontrol.Tokens(fields[1]),
						AdjNormal)
					// Introduce a sleep after each token adjustment to ensure that any
					// token channel signals are processed before proceeding. This
					// ensures the test remains deterministic.
					time.Sleep(20 * time.Millisecond)
				}
				return makeStateString()

			case "cancel":
				var name string
				d.ScanArgs(t, "name", &name)
				handle, ok := notifications[name]
				require.True(t, ok)
				watcher.CancelHandle(ctx, handle.handle)
				notifications[name].cancelled = true
				return makeStateString()

			case "tick":
				var seconds int
				d.ScanArgs(t, "seconds", &seconds)
				clock.Advance(time.Duration(seconds) * time.Second)
				// Sleep to ensure that the tick is processed before proceeding.
				time.Sleep(20 * time.Millisecond)
				return fmt.Sprintf("tick=%v\n%s", clock.Now().Unix(), makeStateString())

			default:
				panic(fmt.Sprintf("unknown command: %s", d.Cmd))
			}
		})
}
