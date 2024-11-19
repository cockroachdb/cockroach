// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestReplicatedWriteAdmission is a data-driven test for admission control of
// replicated writes. We provide the following syntax:
//
//   - "init"
//     Initializes the store work queues and test granters with 0B of
//     {regular,elastic} tokens.
//
//   - "admit" tenant=t<int> pri=<string> create-time=<duration> \
//     size=<bytes> range=r<int> log-position=<int>/<int> origin=n<int> \
//     [ingested=<bool>]
//     Admit a replicated write request from the given tenant, of the given
//     priority/size/create-time, writing to the given log position for the
//     specified raft group. Also specified is the node where this request
//     originated and whether it was ingested (i.e. as sstables).
//
//   - "granter" [class={regular,elastic}] adjust-tokens={-,+}<bytes>
//     Adjust the available {regular,elastic} tokens. If no class is specified,
//     the adjustment applies across both work classes.
//
//   - "grant" [class={regular,elastic}]
//     Grant waiting requests until tokens are depleted or there are no more
//     requests waiting. If no class is specified, we grant admission across
//     both classes.
//
//   - "tenant-weights" [t<int>=<int>]...
//     Set weights for each tenant.
//
//   - "print"
//     Pretty-print work queue internal state (waiting requests, consumed tokens
//     per-tenant, physical admission statistics, tenant weights, etc).
func TestReplicatedWriteAdmission(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := datapathutils.TestDataPath(t, "replicated_write_admission")
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		var (
			storeWorkQueue *StoreWorkQueue
			buf            builderWithMu
			tg             [admissionpb.NumWorkClasses]*testReplicatedWriteGranter
		)
		defer func() {
			if storeWorkQueue != nil {
				storeWorkQueue.close()
			}
		}()

		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				tg = [admissionpb.NumWorkClasses]*testReplicatedWriteGranter{
					admissionpb.RegularWorkClass: newTestReplicatedWriteGranter(t, admissionpb.RegularWorkClass, &buf),
					admissionpb.ElasticWorkClass: newTestReplicatedWriteGranter(t, admissionpb.ElasticWorkClass, &buf),
				}
				registry := metric.NewRegistry()
				regMetrics := makeWorkQueueMetrics("regular", registry)
				elasticMetrics := makeWorkQueueMetrics("elastic", registry)
				workQueueMetrics := [admissionpb.NumWorkClasses]*WorkQueueMetrics{regMetrics, elasticMetrics}
				opts := makeWorkQueueOptions(KVWork)
				opts.usesTokens = true
				opts.timeSource = timeutil.NewManualTime(tzero)
				opts.disableEpochClosingGoroutine = true
				knobs := &TestingKnobs{
					AdmittedReplicatedWorkInterceptor: func(
						tenantID roachpb.TenantID,
						pri admissionpb.WorkPriority,
						rwi ReplicatedWorkInfo,
						originalTokens int64,
						createTime int64,
					) {
						ingested := ""
						if rwi.Ingested {
							ingested = " ingested"
						}
						buf.printf("admitted [tenant=t%d pri=%s create-time=%s size=%s range=r%s origin=n%s log-position=%s%s]",
							tenantID.ToUint64(), pri, timeutil.FromUnixNanos(createTime).Sub(tzero),
							printTrimmedBytes(originalTokens), rwi.RangeID, rwi.Origin, rwi.LogPosition, ingested)
					},
				}
				var mockCoordMu syncutil.Mutex
				storeWorkQueue = makeStoreWorkQueue(
					log.MakeTestingAmbientContext(tracing.NewTracer()),
					roachpb.StoreID(1),
					[admissionpb.NumWorkClasses]granterWithStoreReplicatedWorkAdmitted{
						tg[admissionpb.RegularWorkClass],
						tg[admissionpb.ElasticWorkClass],
					},
					st, workQueueMetrics, opts, knobs, &noopOnLogEntryAdmitted{}, metric.NewCounter(metric.Metadata{}), &mockCoordMu,
				).(*StoreWorkQueue)
				tg[admissionpb.RegularWorkClass].r = storeWorkQueue.getRequesters()[admissionpb.RegularWorkClass]
				tg[admissionpb.ElasticWorkClass].r = storeWorkQueue.getRequesters()[admissionpb.ElasticWorkClass]
				return printTestGranterTokens(tg)

			case "admit":
				require.NotNilf(t, storeWorkQueue, "uninitialized store work queue (did you use 'init'?)")
				var arg string

				// Parse tenant=t<int>.
				d.ScanArgs(t, "tenant", &arg)
				ti, err := strconv.Atoi(strings.TrimPrefix(arg, "t"))
				require.NoError(t, err)
				tenantID := roachpb.MustMakeTenantID(uint64(ti))

				// Parse pri=<string>.
				d.ScanArgs(t, "pri", &arg)
				pri, found := admissionpb.TestingReverseWorkPriorityDict[arg]
				require.True(t, found)

				// Parse size=<bytes>.
				d.ScanArgs(t, "size", &arg)
				bytes, err := humanizeutil.ParseBytes(arg)
				require.NoError(t, err)

				// Parse range=r<int>.
				d.ScanArgs(t, "range", &arg)
				ri, err := strconv.Atoi(strings.TrimPrefix(arg, "r"))
				require.NoError(t, err)
				rangeID := roachpb.RangeID(ri)

				// Parse origin=n<int>.
				d.ScanArgs(t, "origin", &arg)
				ni, err := strconv.Atoi(strings.TrimPrefix(arg, "n"))
				require.NoError(t, err)
				nodeID := roachpb.NodeID(ni)

				// Parse log-position=<int>/<int>.
				logPosition := parseLogPosition(t, d)

				// Parse create-time=<duration>.
				d.ScanArgs(t, "create-time", &arg)
				dur, err := time.ParseDuration(arg)
				require.NoError(t, err)
				createTime := tzero.Add(dur)

				// Parse ingested=<bool>.
				var ingested bool
				if d.HasArg("ingested") {
					d.ScanArgs(t, "ingested", &arg)
					ingested, err = strconv.ParseBool(arg)
					require.NoError(t, err)
				}

				info := StoreWriteWorkInfo{
					WorkInfo: WorkInfo{
						TenantID:       tenantID,
						Priority:       pri,
						CreateTime:     createTime.UnixNano(),
						RequestedCount: bytes,
						ReplicatedWorkInfo: ReplicatedWorkInfo{
							Enabled:     true,
							RangeID:     rangeID,
							Origin:      nodeID,
							LogPosition: logPosition,
							Ingested:    ingested,
						},
					},
				}

				handle, err := storeWorkQueue.Admit(ctx, info)
				require.NoError(t, err)
				require.False(t, handle.UseAdmittedWorkDone())
				return buf.stringAndReset()

			case "granter":
				// Parse adjust-tokens={+,-}<bytes>.
				var arg string
				d.ScanArgs(t, "adjust-tokens", &arg)
				isPositive := strings.Contains(arg, "+")
				arg = strings.TrimPrefix(arg, "+")
				arg = strings.TrimPrefix(arg, "-")
				delta, err := humanizeutil.ParseBytes(arg)
				require.NoError(t, err)
				if !isPositive {
					delta = -delta
				}

				var wcs []admissionpb.WorkClass
				if d.HasArg("class") {
					wcs = append(wcs, parseWorkClass(t, d))
				} else {
					wcs = append(wcs,
						admissionpb.RegularWorkClass,
						admissionpb.ElasticWorkClass)
				}
				for _, wc := range wcs {
					tg[wc].tokens += delta
				}
				return printTestGranterTokens(tg)

			case "tenant-weights":
				weightMap := make(map[uint64]uint32)
				for _, cmdArg := range d.CmdArgs {
					tenantID, err := strconv.Atoi(strings.TrimPrefix(cmdArg.Key, "t"))
					require.NoError(t, err)
					weight, err := strconv.Atoi(cmdArg.Vals[0])
					require.NoError(t, err)
					weightMap[uint64(tenantID)] = uint32(weight)
				}
				storeWorkQueue.SetTenantWeights(weightMap)
				return ""

			case "grant":
				var wcs []admissionpb.WorkClass
				if d.HasArg("class") {
					wcs = append(wcs, parseWorkClass(t, d))
				} else {
					wcs = append(wcs,
						admissionpb.RegularWorkClass,
						admissionpb.ElasticWorkClass)
				}
				for _, wc := range wcs {
					tg[wc].grant()
				}
				return buf.stringAndReset()

			case "print":
				storeWorkQueue.mu.Lock()
				defer storeWorkQueue.mu.Unlock()
				return fmt.Sprintf("physical-stats: work-count=%d written-bytes=%s ingested-bytes=%s\n[regular work queue]: %s\n[elastic work queue]: %s\n",
					storeWorkQueue.mu.stats.workCount,
					printTrimmedBytes(int64(storeWorkQueue.mu.stats.writeAccountedBytes)),
					printTrimmedBytes(int64(storeWorkQueue.mu.stats.ingestedAccountedBytes)),
					printWorkQueue(&storeWorkQueue.q[admissionpb.RegularWorkClass]),
					printWorkQueue(&storeWorkQueue.q[admissionpb.ElasticWorkClass]),
				)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

func parseLogPosition(t *testing.T, d *datadriven.TestData) LogPosition {
	// Parse log-position=<int>/<int>.
	var arg string
	d.ScanArgs(t, "log-position", &arg)
	inner := strings.Split(arg, "/")
	require.Len(t, inner, 2)
	term, err := strconv.Atoi(inner[0])
	require.NoError(t, err)
	index, err := strconv.Atoi(inner[1])
	require.NoError(t, err)
	return LogPosition{
		Term:  uint64(term),
		Index: uint64(index),
	}
}

func parseWorkClass(t *testing.T, d *datadriven.TestData) admissionpb.WorkClass {
	// Parse class={regular,elastic}.
	var arg string
	d.ScanArgs(t, "class", &arg)
	var wc admissionpb.WorkClass
	switch arg {
	case "regular":
		wc = admissionpb.RegularWorkClass
	case "elastic":
		wc = admissionpb.ElasticWorkClass
	default:
		t.Fatalf("unexpected class: %s", arg)
	}
	return wc
}

func printTrimmedBytes(bytes int64) string {
	return strings.ReplaceAll(string(humanizeutil.IBytes(bytes)), " ", "")
}

func printTestGranterTokens(tg [admissionpb.NumWorkClasses]*testReplicatedWriteGranter) string {
	var buf strings.Builder
	for i, wc := range []admissionpb.WorkClass{
		admissionpb.RegularWorkClass,
		admissionpb.ElasticWorkClass,
	} {
		if i != 0 {
			buf.WriteString("\n")
		}
		buf.WriteString(fmt.Sprintf("[%s] %s tokens available", wc, printTrimmedBytes(tg[wc].tokens)))
	}
	return buf.String()
}

func printWorkQueue(q *WorkQueue) string {
	var buf strings.Builder
	q.mu.Lock()
	defer q.mu.Unlock()
	buf.WriteString(fmt.Sprintf("len(tenant-heap)=%d", len(q.mu.tenantHeap)))
	if len(q.mu.tenantHeap) > 0 {
		buf.WriteString(fmt.Sprintf(" top-tenant=t%d", q.mu.tenantHeap[0].id))
	}
	var ids []uint64
	for id := range q.mu.tenants {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		tenant := q.mu.tenants[id]
		buf.WriteString(fmt.Sprintf("\n tenant=t%d weight=%d fifo-threshold=%s used=%s",
			tenant.id,
			tenant.weight,
			admissionpb.WorkPriority(tenant.fifoPriorityThreshold),
			printTrimmedBytes(int64(tenant.used)),
		))
		if len(tenant.waitingWorkHeap) > 0 {
			buf.WriteString("\n")

			for i := range tenant.waitingWorkHeap {
				w := tenant.waitingWorkHeap[i]
				if i != 0 {
					buf.WriteString("\n")
				}

				ingested := ""
				if w.replicated.Ingested {
					ingested = " ingested "
				}

				buf.WriteString(fmt.Sprintf("  [%d: pri=%s create-time=%s size=%s range=r%d origin=n%d log-position=%s%s]", i,
					w.priority,
					timeutil.FromUnixNanos(w.createTime).Sub(tzero),
					printTrimmedBytes(w.requestedCount),
					w.replicated.RangeID,
					w.replicated.Origin,
					w.replicated.LogPosition,
					ingested,
				))
			}
		}
	}
	return buf.String()
}

// tzero represents the t=0, the earliest possible time. All other
// create-time=<duration> is relative to this time.
var tzero = timeutil.Unix(0, 0)

type testReplicatedWriteGranter struct {
	t   *testing.T
	wc  admissionpb.WorkClass
	buf *builderWithMu
	r   requester

	tokens int64
}

var _ granterWithStoreReplicatedWorkAdmitted = &testReplicatedWriteGranter{}

func newTestReplicatedWriteGranter(
	t *testing.T, wc admissionpb.WorkClass, buf *builderWithMu,
) *testReplicatedWriteGranter {
	return &testReplicatedWriteGranter{
		t:   t,
		wc:  wc,
		buf: buf,
	}
}
func (tg *testReplicatedWriteGranter) grantKind() grantKind {
	return token
}

func (tg *testReplicatedWriteGranter) tryGet(count int64) bool {
	if count > tg.tokens {
		tg.buf.printf("[%s] try-get=%s available=%s => insufficient tokens",
			tg.wc, printTrimmedBytes(count), printTrimmedBytes(tg.tokens))
		return false
	}

	tg.buf.printf("[%s] try-get=%s available=%s => sufficient tokens",
		tg.wc, printTrimmedBytes(count), printTrimmedBytes(tg.tokens))
	tg.tokens -= count
	return true
}

func (tg *testReplicatedWriteGranter) returnGrant(count int64) {
	tg.t.Fatalf("unimplemented")
}

func (tg *testReplicatedWriteGranter) tookWithoutPermission(count int64) {
	tg.t.Fatalf("unimplemented")
}

func (tg *testReplicatedWriteGranter) continueGrantChain(grantChainID grantChainID) {
	tg.t.Fatalf("unimplemented")
}

func (tg *testReplicatedWriteGranter) grant() {
	for {
		if tg.tokens <= 0 {
			return // nothing left to do
		}
		if !tg.r.hasWaitingRequests() {
			return // nothing left to do
		}
		_ = tg.r.granted(0 /* unused */)
	}
}

func (tg *testReplicatedWriteGranter) storeWriteDone(
	originalTokens int64, doneInfo StoreWorkDoneInfo,
) (additionalTokens int64) {
	tg.tokens -= originalTokens
	return 0
}

func (tg *testReplicatedWriteGranter) storeReplicatedWorkAdmittedLocked(
	originalTokens int64, admittedInfo storeReplicatedWorkAdmittedInfo,
) (additionalTokens int64) {
	tg.tokens -= originalTokens
	return 0
}
