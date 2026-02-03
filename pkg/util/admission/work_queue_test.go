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
	name string
	buf  *builderWithMu
	r    requester

	// Configurable knobs for tests.
	additionalTokens int64
	printBurstQual   bool

	mu struct {
		syncutil.Mutex
		returnValueFromTryGet bool
	}
}

var _ granterWithStoreReplicatedWorkAdmitted = &testGranter{}

func (tg *testGranter) tryGet(burstQual burstQualification, count int64) bool {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	if tg.printBurstQual {
		tg.buf.printf("tryGet%s: input %s, returning %t", tg.name, burstQual.String(), tg.mu.returnValueFromTryGet)
	} else {
		tg.buf.printf("tryGet%s: returning %t", tg.name, tg.mu.returnValueFromTryGet)
	}
	return tg.mu.returnValueFromTryGet
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
	cancel   context.CancelFunc
	admitted bool
	// For plain WorkQueue testing.
	resp AdmitResponse
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

// resp can be empty when not testing plain WorkQueue (e.g.
// TestSnapshotQueue).
// handle can be empty when not testing StoreWorkQueue.
func (m *workMap) setAdmitted(id int, resp AdmitResponse, handle StoreWorkHandle) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workMap[id].admitted = true
	m.workMap[id].resp = resp
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
				tg = &testGranter{buf: &buf}
				st = cluster.MakeTestingClusterSettings()
				workKind := KVWork
				if d.HasArg("sql-kv") {
					workKind = SQLKVResponseWork
				} else if d.HasArg("sql-sql") {
					workKind = SQLSQLResponseWork
				}
				opts := makeWorkQueueOptions(workKind)
				timeSource = timeutil.NewManualTime(initialTime)
				opts.timeSource = timeSource
				opts.disableEpochClosingGoroutine = true
				opts.disableGCTenantsAndResetUsed = true
				q = makeWorkQueue(log.MakeTestingAmbientContext(tracing.NewTracer()),
					workKind, tg, st, metrics, opts).(*WorkQueue)
				if d.HasArg("override-all-to-bypass") {
					q.SetOverrideAllToBypassAdmission(true)
				} else {
					q.SetOverrideAllToBypassAdmission(false)
				}
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
				wrkMap.set(id, &testWork{cancel: cancel})
				workInfo := WorkInfo{
					TenantID:        tenant,
					Priority:        admissionpb.WorkPriority(priority),
					CreateTime:      int64(createTime) * int64(time.Millisecond),
					BypassAdmission: bypass,
				}
				go func(ctx context.Context, info WorkInfo, id int) {
					require.Equal(t, int64(0), info.RequestedCount)
					resp, err := q.Admit(ctx, info)
					if err != nil {
						buf.printf("id %d: admit failed", id)
						wrkMap.delete(id)
					} else {
						require.True(t, resp.Enabled)
						// Since slots, one concurrent slot always requested.
						require.Equal(t, int64(1), resp.requestedCount)
						buf.printf("id %d: admit succeeded", id)
						wrkMap.setAdmitted(id, resp, StoreWorkHandle{})
					}
				}(ctx, workInfo, id)
				// Need deterministic output, and this is racing with the goroutine
				// which is trying to get admitted. Retry to let it get scheduled.
				maybeRetryWithWait(t, d.Expected, d.Rewrite, buf.String)
				return buf.stringAndReset()

			case "set-try-get-return-value":
				var v bool
				d.ScanArgs(t, "v", &v)
				tg.mu.Lock()
				tg.mu.returnValueFromTryGet = v
				tg.mu.Unlock()
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
				q.AdmittedWorkDone(work.resp, time.Duration(cpuTime))
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
				q.gcTenantsResetUsedAndUpdateEstimators()
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

// TestCPUTimeTokenWorkQueue is a datadriven test of WorkQueue with
// mode set to usesCPUTimeTokens. See the comments at
// testdata/cpu_time_token_work_queue for details on what is tested
// here. See TestCPUTimeTokenEstimation for testing of CPU time token
// estimation.
func TestCPUTimeTokenWorkQueue(t *testing.T) {
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
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "cpu_time_token_work_queue"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				closeFn()
				tg = &testGranter{buf: &buf, printBurstQual: true}
				st = cluster.MakeTestingClusterSettings()
				workKind := KVWork
				opts := makeWorkQueueOptions(workKind)
				timeSource = timeutil.NewManualTime(initialTime)
				opts.timeSource = timeSource
				opts.disableEpochClosingGoroutine = true
				opts.disableGCTenantsAndResetUsed = true
				opts.mode = usesCPUTimeTokens
				q = makeWorkQueue(log.MakeTestingAmbientContext(tracing.NewTracer()),
					workKind, tg, st, metrics, opts).(*WorkQueue)
				q.knobs.DisableCPUTimeTokenEstimation = true
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
				var bypass bool
				if d.HasArg("bypass") {
					bypass = true
				}
				ctx, cancel := context.WithCancel(context.Background())
				var requestedCount int64
				d.ScanArgs(t, "requested-count", &requestedCount)
				wrkMap.set(id, &testWork{cancel: cancel})
				workInfo := WorkInfo{
					TenantID:        tenant,
					Priority:        admissionpb.WorkPriority(0),
					CreateTime:      int64(1) * int64(time.Millisecond),
					BypassAdmission: bypass,
					RequestedCount:  requestedCount,
				}
				go func(ctx context.Context, info WorkInfo, id int) {
					resp, err := q.Admit(ctx, info)
					if err != nil {
						buf.printf("id %d: admit failed", id)
						wrkMap.delete(id)
					} else {
						require.True(t, resp.Enabled)
						buf.printf("id %d: admit succeeded", id)
						wrkMap.setAdmitted(id, resp, StoreWorkHandle{})
					}
				}(ctx, workInfo, id)
				// Need deterministic output, and this is racing with the goroutine
				// which is trying to get admitted. Retry to let it get scheduled.
				maybeRetryWithWait(t, d.Expected, d.Rewrite, buf.String)
				return buf.stringAndReset()

			case "set-try-get-return-value":
				var v bool
				d.ScanArgs(t, "v", &v)
				tg.mu.Lock()
				tg.mu.returnValueFromTryGet = v
				tg.mu.Unlock()
				return ""

			case "granted":
				rv := tg.r.granted(grantChainID(1 /* chain ID not used */))
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
				q.AdmittedWorkDone(work.resp, time.Duration(cpuTime))
				wrkMap.delete(id)
				return buf.stringAndReset()

			case "print":
				// Need deterministic output, and this is racing with the goroutine
				// whose work is canceled. Retry to let it get scheduled.
				maybeRetryWithWait(t, d.Expected, d.Rewrite, q.String)
				return q.String()

			case "refill-burst-buckets":
				var toAdd, capacity int64
				d.ScanArgs(t, "to-add", &toAdd)
				d.ScanArgs(t, "capacity", &capacity)
				q.refillBurstBuckets(toAdd, capacity)
				return ""

			case "gc-tenants-and-reset-used":
				q.gcTenantsResetUsedAndUpdateEstimators()
				return ""

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

// TestCPUTimeTokenEstimation tests estimation of the number of
// CPU time tokens used by some work that calls Admit before it executes.
// This is a test of the estimation logic *only*. See
// TestCPUTimeTokenWorkQueue for a test of the CPU time work queue
// itself.
func TestCPUTimeTokenEstimation(t *testing.T) {
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
	var buf builderWithMu
	// 100ms after epoch.
	initialTime := timeutil.FromUnixMicros(int64(100) * int64(time.Millisecond/time.Microsecond))
	var timeSource *timeutil.ManualTime
	var st *cluster.Settings
	registry := metric.NewRegistry()
	metrics := makeWorkQueueMetrics("", registry)
	closeFn()
	tg = &testGranter{buf: &buf}
	// We want all calls to tryGet to return true. Thus all calls
	// to Admit allow admission immediately.
	tg.mu.returnValueFromTryGet = true
	opts := makeWorkQueueOptions(KVWork)
	opts.mode = usesCPUTimeTokens
	timeSource = timeutil.NewManualTime(initialTime)
	opts.timeSource = timeSource
	opts.disableEpochClosingGoroutine = true
	opts.disableGCTenantsAndResetUsed = true
	st = cluster.MakeTestingClusterSettings()
	q = makeWorkQueue(log.MakeTestingAmbientContext(tracing.NewTracer()),
		KVWork, tg, st, metrics, opts).(*WorkQueue)
	tg.r = q
	ctx := context.Background()

	checkEstimation := func(resp AdmitResponse, expected time.Duration) {
		require.InDelta(
			t, expected.Nanoseconds(), resp.requestedCount, float64(time.Millisecond.Nanoseconds()),
			"expected %v, got %v (tolerance 1ms)", time.Duration(expected.Nanoseconds()), time.Duration(resp.requestedCount))
	}

	// At init time, the estimators haven't seen any requests yet. They are
	// hard-coded to return a single nanosecond token as an estimate in this
	// case.
	info1 := WorkInfo{
		TenantID: roachpb.MustMakeTenantID(1),
	}
	resp1, err := q.Admit(ctx, info1)
	require.NoError(t, err)
	checkEstimation(resp1, time.Nanosecond)

	// A bunch of work finishes. 3*100=300 pieces of work have
	// finished in total. Every 3*10=30 pieces of work that finish,
	// a call to update happens, and the exponentially-smoothed estimates
	// are updated. This simulates 30 pieces of work finishing every 1s,
	// for 10s total (see the ticker in initWorkQueue). The rest of the
	// cases are structured in a similar fashion, and so we will not
	// repeat this explanation.
	//
	// In this case, all work is for tenant 1. The mean is 100ms. So,
	// after a number of calls to update updating the exponentially-
	// smoothed estimates, we expect 100ms for future estimates (see
	// next call to Admit down below).
	for i := 1; i <= 100; i++ {
		q.AdmittedWorkDone(resp1, 100*time.Millisecond)
		q.AdmittedWorkDone(resp1, 50*time.Millisecond)
		q.AdmittedWorkDone(resp1, 150*time.Millisecond)

		if i%10 == 0 {
			q.gcTenantsResetUsedAndUpdateEstimators()
		}
	}

	resp1, err = q.Admit(ctx, info1)
	require.NoError(t, err)
	checkEstimation(resp1, 100*time.Millisecond)

	// Tenant 2 is a new tenant. So the global estimator should be
	// used. The global estimator has seen the same workload as
	// tenant 1's estimator, so it should make the same estimate:
	// 100ms.
	info2 := WorkInfo{
		TenantID: roachpb.MustMakeTenantID(2),
	}
	resp2, err := q.Admit(ctx, info2)
	require.NoError(t, err)
	checkEstimation(resp2, 100*time.Millisecond)

	// A bunch of work finishes for tenant 1 and tenant 2. The mean for
	// the tenant 1 work is 200ms. The mean for the tenant 2 work is
	// 300ms.
	for i := 1; i <= 100; i++ {
		q.AdmittedWorkDone(resp1, 200*time.Millisecond)
		q.AdmittedWorkDone(resp1, 150*time.Millisecond)
		q.AdmittedWorkDone(resp1, 250*time.Millisecond)

		q.AdmittedWorkDone(resp2, 300*time.Millisecond)
		q.AdmittedWorkDone(resp2, 250*time.Millisecond)
		q.AdmittedWorkDone(resp2, 350*time.Millisecond)

		if i%10 == 0 {
			q.gcTenantsResetUsedAndUpdateEstimators()
		}
	}

	resp1, err = q.Admit(ctx, info1)
	require.NoError(t, err)
	checkEstimation(resp1, 200*time.Millisecond)

	resp2, err = q.Admit(ctx, info2)
	require.NoError(t, err)
	checkEstimation(resp2, 300*time.Millisecond)

	// Tenant 3 is a new tenant. So the global estimator should be
	// used. Lately, the estimator has seen equal number of 200ms mean
	// requests and 300ms mean requests. (200 + 300) / 2 = 500 / 2 =
	// 250. So 250ms is the expected estimate for tenant 3 work.
	info3 := WorkInfo{
		TenantID: roachpb.MustMakeTenantID(3),
	}
	resp3, err := q.Admit(ctx, info3)
	require.NoError(t, err)
	checkEstimation(resp3, 250*time.Millisecond)

	// This is a test of GC. If a call to update happens without any
	// work happening during that interval, the tenant's estimator should be
	// GCed. The first call to gcTenantsResetUsedAndUpdateEstimators resets
	// the interval over which activity is checked. The second call GCes the
	// per-tenant estimators. So tenant 1 & tenant 2 should use the global
	// estimator, just like tenant 3.
	q.gcTenantsResetUsedAndUpdateEstimators()
	q.gcTenantsResetUsedAndUpdateEstimators()
	resp1, err = q.Admit(ctx, info1)
	require.NoError(t, err)
	checkEstimation(resp1, 250*time.Millisecond)
	resp2, err = q.Admit(ctx, info2)
	require.NoError(t, err)
	checkEstimation(resp2, 250*time.Millisecond)
	resp3, err = q.Admit(ctx, info3)
	require.NoError(t, err)
	checkEstimation(resp3, 250*time.Millisecond)
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
	tg := &testGranter{buf: &buf}
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
				work2 := &testWork{cancel: cancel}
				tenantID++
				go func(ctx context.Context, tenantID uint64, createTime int64) {
					resp, err := q.Admit(ctx, WorkInfo{
						TenantID:   roachpb.MustMakeTenantID(tenantID),
						CreateTime: createTime,
					})
					buf.printf("admit")
					mu.Lock()
					defer mu.Unlock()
					totalCount++
					if err != nil {
						errCount++
					} else {
						require.Equal(t, true, resp.Enabled)
					}
				}(ctx, tenantID, createTime)
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
				q.gcTenantsResetUsedAndUpdateEstimators()
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
				tg[admissionpb.RegularWorkClass] = &testGranter{name: " regular", buf: &buf}
				tg[admissionpb.ElasticWorkClass] = &testGranter{name: " elastic", buf: &buf}
				opts := makeWorkQueueOptions(KVWork)
				opts.mode = usesTokens
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
				wrkMap.set(id, &testWork{cancel: cancel})
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
						wrkMap.setAdmitted(id, AdmitResponse{}, handle)
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

				tg[wc].mu.Lock()
				tg[wc].mu.returnValueFromTryGet = v
				tg[wc].mu.Unlock()
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
