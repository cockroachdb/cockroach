// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

type builderWithMu struct {
	mu  syncutil.Mutex
	buf strings.Builder
}

func (b *builderWithMu) printf(format string, a ...interface{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.buf.Len() > 0 {
		fmt.Fprintf(&b.buf, "\n")
	}
	fmt.Fprintf(&b.buf, format, a...)
}

func (b *builderWithMu) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *builderWithMu) stringAndReset() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	str := b.buf.String()
	b.buf.Reset()
	return str
}

type testGranter struct {
	gk   grantKind
	name string
	buf  *builderWithMu
	r    requester

	// Configurable knobs for tests.
	returnValueFromTryGet bool
	additionalTokens      int64
}

var _ granterWithStoreReplicatedWorkAdmitted = &testGranter{}

func (tg *testGranter) grantKind() grantKind {
	return tg.gk
}

func (tg *testGranter) tryGet(count int64) bool {
	tg.buf.printf("tryGet%s: returning %t", tg.name, tg.returnValueFromTryGet)
	return tg.returnValueFromTryGet
}

func (tg *testGranter) returnGrant(count int64) {
	tg.buf.printf("returnGrant%s %d", tg.name, count)
}

func (tg *testGranter) tookWithoutPermission(count int64) {
	tg.buf.printf("tookWithoutPermission%s %d", tg.name, count)
}

func (tg *testGranter) continueGrantChain(grantChainID grantChainID) {
	tg.buf.printf("continueGrantChain%s %d", tg.name, grantChainID)
}

func (tg *testGranter) storeWriteDone(
	originalTokens int64, doneInfo StoreWorkDoneInfo,
) (additionalTokens int64) {
	tg.buf.printf("storeWriteDone%s: originalTokens %d, doneBytes(write %d,ingested %d) returning %d",
		tg.name, originalTokens, doneInfo.WriteBytes, doneInfo.IngestedBytes, tg.additionalTokens)
	return tg.additionalTokens
}

func (tg *testGranter) storeReplicatedWorkAdmittedLocked(
	originalTokens int64, admittedInfo storeReplicatedWorkAdmittedInfo,
) (additionalTokens int64) {
	tg.buf.printf("storeReplicatedWorkAdmittedLocked%s: originalTokens %d, admittedBytes(write %d,ingested %d) returning %d",
		tg.name, originalTokens, admittedInfo.WriteBytes, admittedInfo.IngestedBytes, tg.additionalTokens)
	return tg.additionalTokens
}

type testWork struct {
	tenantID roachpb.TenantID
	cancel   context.CancelFunc
	admitted bool
	// For StoreWorkQueue testing.
	handle StoreWorkHandle
}

type workMap struct {
	mu      syncutil.Mutex
	workMap map[int]*testWork
}

func (m *workMap) resetMap() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workMap = make(map[int]*testWork)
}

func (m *workMap) set(id int, w *testWork) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workMap[id] = w
}

func (m *workMap) delete(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.workMap, id)
}

// handle can be empty when not testing StoreWorkQueue.
func (m *workMap) setAdmitted(id int, handle StoreWorkHandle) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workMap[id].admitted = true
	m.workMap[id].handle = handle
}

func (m *workMap) get(id int) (work testWork, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.workMap[id]
	if ok {
		work = *w
	}
	return work, ok
}

/*
TestWorkQueueBasic is a datadriven test with the following commands:
init
admit id=<int> tenant=<int> priority=<int> create-time-millis=<int> bypass=<bool>
set-try-get-return-value v=<bool>
granted chain-id=<int>
cancel-work id=<int>
work-done id=<int>
advance-time millis=<int>
print
*/
func TestWorkQueueBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var q *WorkQueue
	closeFn := func() {
		if q != nil {
			q.close()
		}
	}
	defer closeFn()
	var tg *testGranter
	var wrkMap workMap
	var buf builderWithMu
	// 100ms after epoch.
	initialTime := timeutil.FromUnixMicros(int64(100) * int64(time.Millisecond/time.Microsecond))
	var timeSource *timeutil.ManualTime
	var st *cluster.Settings
	registry := metric.NewRegistry()
	metrics := makeWorkQueueMetrics("", registry)
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "work_queue"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				closeFn()
				tg = &testGranter{gk: slot, buf: &buf}
				opts := makeWorkQueueOptions(KVWork)
				timeSource = timeutil.NewManualTime(initialTime)
				opts.timeSource = timeSource
				opts.disableEpochClosingGoroutine = true
				opts.disableGCTenantsAndResetUsed = true
				st = cluster.MakeTestingClusterSettings()
				q = makeWorkQueue(log.MakeTestingAmbientContext(tracing.NewTracer()),
					KVWork, tg, st, metrics, opts).(*WorkQueue)
				tg.r = q
				wrkMap.resetMap()
				return ""

			case "admit":
				var id int
				d.ScanArgs(t, "id", &id)
				if _, ok := wrkMap.get(id); ok {
					panic(fmt.Sprintf("id %d is already used", id))
				}
				tenant := scanTenantID(t, d)
				var priority, createTime int
				d.ScanArgs(t, "priority", &priority)
				d.ScanArgs(t, "create-time-millis", &createTime)
				var bypass bool
				d.ScanArgs(t, "bypass", &bypass)
				ctx, cancel := context.WithCancel(context.Background())
				wrkMap.set(id, &testWork{tenantID: tenant, cancel: cancel})
				workInfo := WorkInfo{
					TenantID:        tenant,
					Priority:        admissionpb.WorkPriority(priority),
					CreateTime:      int64(createTime) * int64(time.Millisecond),
					BypassAdmission: bypass,
				}
				go func(ctx context.Context, info WorkInfo, id int) {
					enabled, err := q.Admit(ctx, info)
					require.True(t, enabled)
					if err != nil {
						buf.printf("id %d: admit failed", id)
						wrkMap.delete(id)
					} else {
						buf.printf("id %d: admit succeeded", id)
						wrkMap.setAdmitted(id, StoreWorkHandle{})
					}
				}(ctx, workInfo, id)
				// Need deterministic output, and this is racing with the goroutine
				// which is trying to get admitted. Retry to let it get scheduled.
				maybeRetryWithWait(t, d.Expected, d.Rewrite, buf.String)
				return buf.stringAndReset()

			case "set-try-get-return-value":
				var v bool
				d.ScanArgs(t, "v", &v)
				tg.returnValueFromTryGet = v
				return ""

			case "granted":
				var chainID int
				d.ScanArgs(t, "chain-id", &chainID)
				rv := tg.r.granted(grantChainID(chainID))
				if rv > 0 {
					// Need deterministic output, and this is racing with the goroutine that was
					// admitted. Retry a few times.
					maybeRetryWithWait(t, d.Expected, d.Rewrite, buf.String)
				}
				tg.buf.printf("granted%s: returned %d", tg.name, rv)
				return buf.stringAndReset()

			case "cancel-work":
				var id int
				d.ScanArgs(t, "id", &id)
				work, ok := wrkMap.get(id)
				if !ok {
					return fmt.Sprintf("unknown id: %d", id)
				}
				if work.admitted {
					return fmt.Sprintf("work already admitted id: %d", id)
				}
				work.cancel()
				// Need deterministic output, and this is racing with the goroutine
				// whose work is canceled. Retry to let it get scheduled.
				maybeRetryWithWait(t, d.Expected, d.Rewrite, buf.String)
				return buf.stringAndReset()

			case "work-done":
				var id int
				d.ScanArgs(t, "id", &id)
				work, ok := wrkMap.get(id)
				if !ok {
					return fmt.Sprintf("unknown id: %d\n", id)
				}
				if !work.admitted {
					return fmt.Sprintf("id not admitted: %d\n", id)
				}
				cpuTime := int64(1)
				if d.HasArg("cpu-time") {
					d.ScanArgs(t, "cpu-time", &cpuTime)
				}
				q.AdmittedWorkDone(work.tenantID, time.Duration(cpuTime))
				wrkMap.delete(id)
				return buf.stringAndReset()

			case "set-tenant-weights":
				var weights string
				d.ScanArgs(t, "weights", &weights)
				fields := strings.FieldsFunc(weights, func(r rune) bool {
					return r == ':' || r == ',' || unicode.IsSpace(r)
				})
				if len(fields)%2 != 0 {
					return "tenant and weight are not paired"
				}
				weightMap := make(map[uint64]uint32)
				for i := 0; i < len(fields); i += 2 {
					tenantID, err := strconv.Atoi(fields[i])
					require.NoError(t, err)
					weight, err := strconv.Atoi(fields[i+1])
					require.NoError(t, err)
					weightMap[uint64(tenantID)] = uint32(weight)
				}
				q.SetTenantWeights(weightMap)
				return q.String()

			case "print":
				// Need deterministic output, and this is racing with the goroutine
				// whose work is canceled. Retry to let it get scheduled.
				maybeRetryWithWait(t, d.Expected, d.Rewrite, func() string {
					return q.String()
				})
				return q.String()

			case "advance-time":
				var millis int
				d.ScanArgs(t, "millis", &millis)
				timeSource.Advance(time.Duration(millis) * time.Millisecond)
				EpochLIFOEnabled.Override(context.Background(), &st.SV, true)
				q.tryCloseEpoch(timeSource.Now())
				return q.String()

			case "gc-tenants-and-reset-used":
				q.gcTenantsAndResetUsed()
				return q.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func scanTenantID(t *testing.T, d *datadriven.TestData) roachpb.TenantID {
	var id int
	d.ScanArgs(t, "tenant", &id)
	return roachpb.MustMakeTenantID(uint64(id))
}

func maybeRetryWithWait(t *testing.T, expected string, rewrite bool, f func() string) {
	t.Helper()
	if rewrite {
		time.Sleep(100 * time.Millisecond)
		return
	}
	var str string
	// Retry for at most 2s (20 iterations with 100ms delay).
	for i := 0; i < 20; i++ {
		str = f()
		if strings.TrimSpace(str) == strings.TrimSpace(expected) {
			if i > 0 {
				t.Logf("retried test case %d times", i)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestWorkQueueTokenResetRace induces racing between tenantInfo.used
// decrements and tenantInfo.used resets that used to fail until we eliminated
// the code that decrements tenantInfo.used for tokens. It would also trigger
// a used-after-free bug where the tenantInfo being used in Admit had been
// returned to the sync.Pool because the used value was reset.
func TestWorkQueueTokenResetRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var buf builderWithMu
	tg := &testGranter{gk: slot, buf: &buf}
	st := cluster.MakeTestingClusterSettings()
	registry := metric.NewRegistry()
	metrics := makeWorkQueueMetrics("", registry)
	q := makeWorkQueue(log.MakeTestingAmbientContext(tracing.NewTracer()), SQLKVResponseWork, tg,
		st, metrics, makeWorkQueueOptions(SQLKVResponseWork)).(*WorkQueue)
	tg.r = q
	createTime := int64(0)
	stopCh := make(chan struct{})
	errCount, totalCount := 0, 0
	var mu syncutil.Mutex
	go func() {
		ticker := time.NewTicker(time.Microsecond * 100)
		done := false
		var work *testWork
		tenantID := uint64(1)
		for !done {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithCancel(context.Background())
				work2 := &testWork{tenantID: roachpb.MustMakeTenantID(tenantID), cancel: cancel}
				tenantID++
				go func(ctx context.Context, w *testWork, createTime int64) {
					enabled, err := q.Admit(ctx, WorkInfo{
						TenantID:   w.tenantID,
						CreateTime: createTime,
					})
					buf.printf("admit")
					require.Equal(t, true, enabled)
					mu.Lock()
					defer mu.Unlock()
					totalCount++
					if err != nil {
						errCount++
					}
				}(ctx, work2, createTime)
				createTime++
				if work != nil {
					rv := tg.r.granted(1)
					if rv > 0 {
						// Need deterministic output, and this is racing with the goroutine that was
						// admitted. Add a retry to let it get scheduled.
						maybeRetryWithWait(t, "admit", false /* rewrite */, buf.String)
					}
					work.cancel()
					buf.stringAndReset()
				}
				work = work2
			case <-stopCh:
				done = true
			}
			if work != nil {
				work.cancel()
				rv := tg.r.granted(1)
				if rv > 0 {
					// Need deterministic output, and this is racing with the goroutine that was
					// admitted. Add a retry to let it get scheduled.
					maybeRetryWithWait(t, "admit", false /* rewrite */, buf.String)
				}
				buf.stringAndReset()
			}
		}
	}()
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				// This hot loop with GC calls is able to trigger the previously buggy
				// code by squeezing in multiple times between the token grant and
				// cancellation.
				q.gcTenantsAndResetUsed()
			}
		}
	}()
	time.Sleep(time.Second)
	close(stopCh)
	q.close()
	mu.Lock()
	t.Logf("total: %d, err: %d", totalCount, errCount)
	mu.Unlock()
}

func TestPriorityStates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var ps priorityStates
	curThreshold := int(admissionpb.LowPri)
	printFunc := func() string {
		var b strings.Builder
		fmt.Fprintf(&b, "lowest-priority: %d", ps.lowestPriorityWithRequests)
		for _, state := range ps.ps {
			fmt.Fprintf(&b, " (pri: %d, delay-millis: %d, admitted: %d)",
				state.priority, state.maxQueueDelay/time.Millisecond, state.admittedCount)
		}
		return b.String()
	}
	datadriven.RunTest(t, "testdata/priority_states",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				ps = priorityStates{
					lowestPriorityWithRequests: admissionpb.OneAboveHighPri,
				}
				return ""

			case "request-received":
				var priority int
				d.ScanArgs(t, "priority", &priority)
				ps.requestAtPriority(admissionpb.WorkPriority(priority))
				return printFunc()

			case "update":
				var priority, delayMillis int
				d.ScanArgs(t, "priority", &priority)
				d.ScanArgs(t, "delay-millis", &delayMillis)
				canceled := false
				if d.HasArg("canceled") {
					d.ScanArgs(t, "canceled", &canceled)
				}
				ps.updateDelayLocked(admissionpb.WorkPriority(priority), time.Duration(delayMillis)*time.Millisecond,
					canceled)
				return printFunc()

			case "get-threshold":
				curThreshold = ps.getFIFOPriorityThresholdAndReset(
					curThreshold, int64(epochLength), maxQueueDelayToSwitchToLifo)
				return fmt.Sprintf("threshold: %d", curThreshold)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func tryScanWorkClass(t *testing.T, d *datadriven.TestData) admissionpb.WorkClass {
	wc := admissionpb.RegularWorkClass
	if d.HasArg("elastic") {
		var b bool
		d.ScanArgs(t, "elastic", &b)
		if b {
			wc = admissionpb.ElasticWorkClass
		}
	}
	return wc
}

/*
TestStoreWorkQueueBasic is a datadriven test with the following commands:
init
admit id=<int> tenant=<int> priority=<int> create-time-millis=<int> bypass=<bool>
set-try-get-return-value v=<bool> [elastic=<bool>]
granted [elastic=<bool>]
cancel-work id=<int>
work-done id=<int> [write-bytes=<int>] [ingested-bytes=<int>] [additional-tokens=<int>]
print
*/
func TestStoreWorkQueueBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var q *StoreWorkQueue
	closeFn := func() {
		if q != nil {
			q.close()
		}
	}
	defer closeFn()
	var tg [admissionpb.NumWorkClasses]*testGranter
	var wrkMap workMap
	var buf builderWithMu
	var st *cluster.Settings
	printQueue := func() string {
		q.mu.Lock()
		defer q.mu.Unlock()
		return fmt.Sprintf("regular workqueue: %s\nelastic workqueue: %s\nstats:%+v\nestimates:%+v",
			q.q[admissionpb.RegularWorkClass].String(), q.q[admissionpb.ElasticWorkClass].String(), q.mu.stats,
			q.mu.estimates)
	}

	registry := metric.NewRegistry()
	regMetrics := makeWorkQueueMetrics("regular", registry)
	elasticMetrics := makeWorkQueueMetrics("elastic", registry)
	workQueueMetrics := [admissionpb.NumWorkClasses]*WorkQueueMetrics{regMetrics, elasticMetrics}
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "store_work_queue"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				closeFn()
				tg[admissionpb.RegularWorkClass] = &testGranter{gk: token, name: " regular", buf: &buf}
				tg[admissionpb.ElasticWorkClass] = &testGranter{gk: token, name: " elastic", buf: &buf}
				opts := makeWorkQueueOptions(KVWork)
				opts.usesTokens = true
				opts.timeSource = timeutil.NewManualTime(timeutil.FromUnixMicros(0))
				opts.disableEpochClosingGoroutine = true
				opts.disableGCTenantsAndResetUsed = true
				st = cluster.MakeTestingClusterSettings()
				var mockCoordMu syncutil.Mutex
				q = makeStoreWorkQueue(log.MakeTestingAmbientContext(tracing.NewTracer()), roachpb.StoreID(1),
					[admissionpb.NumWorkClasses]granterWithStoreReplicatedWorkAdmitted{
						tg[admissionpb.RegularWorkClass],
						tg[admissionpb.ElasticWorkClass],
					},
					st, workQueueMetrics, opts, nil /* testing knobs */, &noopOnLogEntryAdmitted{}, metric.NewCounter(metric.Metadata{}), &mockCoordMu).(*StoreWorkQueue)
				tg[admissionpb.RegularWorkClass].r = q.getRequesters()[admissionpb.RegularWorkClass]
				tg[admissionpb.ElasticWorkClass].r = q.getRequesters()[admissionpb.ElasticWorkClass]
				wrkMap.resetMap()
				return ""

			case "admit":
				var id int
				d.ScanArgs(t, "id", &id)
				if _, ok := wrkMap.get(id); ok {
					panic(fmt.Sprintf("id %d is already used", id))
				}
				tenant := scanTenantID(t, d)
				var priority, createTime int
				d.ScanArgs(t, "priority", &priority)
				d.ScanArgs(t, "create-time-millis", &createTime)
				var bypass bool
				d.ScanArgs(t, "bypass", &bypass)
				ctx, cancel := context.WithCancel(context.Background())
				wrkMap.set(id, &testWork{tenantID: tenant, cancel: cancel})
				workInfo := StoreWriteWorkInfo{
					WorkInfo: WorkInfo{
						TenantID:        tenant,
						Priority:        admissionpb.WorkPriority(priority),
						CreateTime:      int64(createTime) * int64(time.Millisecond),
						BypassAdmission: bypass,
					},
				}
				go func(ctx context.Context, info StoreWriteWorkInfo, id int) {
					handle, err := q.Admit(ctx, workInfo)
					if err != nil {
						buf.printf("id %d: admit failed", id)
						wrkMap.delete(id)
					} else {
						buf.printf("id %d: admit succeeded with handle %+v", id, handle)
						wrkMap.setAdmitted(id, handle)
					}
				}(ctx, workInfo, id)
				// Need deterministic output, and this is racing with the goroutine
				// which is trying to get admitted. Retry to let it get scheduled.
				maybeRetryWithWait(t, d.Expected, d.Rewrite, buf.String)
				return buf.stringAndReset()

			case "set-try-get-return-value":
				var v bool
				d.ScanArgs(t, "v", &v)
				wc := tryScanWorkClass(t, d)
				tg[wc].returnValueFromTryGet = v
				return ""

			case "set-store-request-estimates":
				var estimates storeRequestEstimates
				var writeTokens int
				d.ScanArgs(t, "write-tokens", &writeTokens)
				estimates.writeTokens = int64(writeTokens)
				q.setStoreRequestEstimates(estimates)
				return printQueue()

			case "granted":
				wc := tryScanWorkClass(t, d)
				rv := tg[wc].r.granted(0)
				if rv > 0 {
					// Need deterministic output, and this is racing with the goroutine that was
					// admitted. Retry a few times.
					maybeRetryWithWait(t, d.Expected, d.Rewrite, buf.String)
				}
				buf.printf("granted%s: returned %d", tg[wc].name, rv)
				return buf.stringAndReset()

			case "cancel-work":
				var id int
				d.ScanArgs(t, "id", &id)
				work, ok := wrkMap.get(id)
				if !ok {
					return fmt.Sprintf("unknown id: %d", id)
				}
				if work.admitted {
					return fmt.Sprintf("work already admitted id: %d", id)
				}
				work.cancel()
				// Need deterministic output, and this is racing with the goroutine
				// whose work is canceled. Retry to let it get scheduled.
				maybeRetryWithWait(t, d.Expected, d.Rewrite, buf.String)
				return buf.stringAndReset()

			case "work-done":
				var id int
				d.ScanArgs(t, "id", &id)
				var writeBytes, ingestedBytes int
				if d.HasArg("write-bytes") {
					d.ScanArgs(t, "write-bytes", &writeBytes)
				}
				if d.HasArg("ingested-bytes") {
					d.ScanArgs(t, "ingested-bytes", &ingestedBytes)
				}
				var additionalTokens int
				if d.HasArg("additional-tokens") {
					d.ScanArgs(t, "additional-tokens", &additionalTokens)
				}
				work, ok := wrkMap.get(id)
				if !ok {
					return fmt.Sprintf("unknown id: %d\n", id)
				}
				if !work.admitted {
					return fmt.Sprintf("id not admitted: %d\n", id)
				}
				tg[work.handle.workClass].additionalTokens = int64(additionalTokens)
				require.NoError(t, q.AdmittedWorkDone(work.handle,
					StoreWorkDoneInfo{
						WriteBytes:    int64(writeBytes),
						IngestedBytes: int64(ingestedBytes),
					}))
				wrkMap.delete(id)
				return buf.stringAndReset()

			case "bypassed-work-done":
				var workCount, writeBytes, ingestedBytes int
				d.ScanArgs(t, "work-count", &workCount)
				d.ScanArgs(t, "write-bytes", &writeBytes)
				d.ScanArgs(t, "ingested-bytes", &ingestedBytes)
				q.BypassedWorkDone(int64(workCount), StoreWorkDoneInfo{
					WriteBytes:    int64(writeBytes),
					IngestedBytes: int64(ingestedBytes),
				})
				return buf.stringAndReset()

			case "stats-to-ignore":
				var ingestedBytes, ingestedIntoL0Bytes, writeBytes int
				d.ScanArgs(t, "ingested-bytes", &ingestedBytes)
				d.ScanArgs(t, "ingested-into-L0-bytes", &ingestedIntoL0Bytes)
				d.ScanArgs(t, "write-bytes", &writeBytes)
				q.StatsToIgnore(pebble.IngestOperationStats{
					Bytes:                     uint64(ingestedBytes),
					ApproxIngestedIntoL0Bytes: uint64(ingestedIntoL0Bytes),
				}, uint64(writeBytes))
				return printQueue()

			case "print":
				// Need deterministic output, and this is racing with the goroutine
				// whose work is canceled. Retry to let it get scheduled.
				maybeRetryWithWait(t, d.Expected, d.Rewrite, func() string {
					return printQueue()
				})
				return printQueue()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

// TODO(sumeer):
// - Test metrics
// - Test race between grant and cancellation
// - Add microbenchmark with high concurrency and procs for full admission
//   system
