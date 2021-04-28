// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
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

func (b *builderWithMu) stringAndReset() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	str := b.buf.String()
	b.buf.Reset()
	return str
}

type testGranter struct {
	buf                   *builderWithMu
	r                     requester
	returnValueFromTryGet bool
}

func (tg *testGranter) grantKind() grantKind {
	return slot
}
func (tg *testGranter) tryGet() bool {
	tg.buf.printf("tryGet: returning %t", tg.returnValueFromTryGet)
	return tg.returnValueFromTryGet
}
func (tg *testGranter) returnGrant() {
	tg.buf.printf("returnGrant")
}
func (tg *testGranter) tookWithoutPermission() {
	tg.buf.printf("tookWithoutPermission")
}
func (tg *testGranter) continueGrantChain() {
	tg.buf.printf("continueGrantChain")
}
func (tg *testGranter) grant() {
	rv := tg.r.granted()
	if rv {
		// Need deterministic output, and this is racing with the goroutine that
		// was admitted. Sleep to let it get scheduled. We could do something more
		// sophisticated like monitoring goroutine states like in
		// concurrency_manager_test.go.
		time.Sleep(50 * time.Millisecond)
	}
	tg.buf.printf("granted: returned %t", rv)
}

type testWork struct {
	tenantID roachpb.TenantID
	cancel   context.CancelFunc
	admitted bool
}

type workMap struct {
	mu      syncutil.Mutex
	workMap map[int]*testWork
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

func (m *workMap) setAdmitted(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workMap[id].admitted = true
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
TestAdmissionQueueBasic is a datadriven test with the following commands:
init
admit id=<int> tenant=<int> priority=<int> create-time=<int> bypass=<bool>
set-try-get-return-value v=<bool>
granted
cancel-work id=<int>
work-done id=<int>
print
*/
func TestAdmissionQueueBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var q *AdmissionQueue
	var tg *testGranter
	var workMap workMap
	var buf builderWithMu
	datadriven.RunTest(t, "testdata/admission_queue",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				tg = &testGranter{buf: &buf}
				q = MakeAdmissionQueue(KVWork, tg, false, true).(*AdmissionQueue)
				tg.r = q
				workMap.workMap = make(map[int]*testWork)
				return ""

			case "admit":
				var id int
				d.ScanArgs(t, "id", &id)
				if _, ok := workMap.get(id); ok {
					panic(fmt.Sprintf("id %d is already used", id))
				}
				tenant := scanTenantID(t, d)
				var priority, createTime int
				d.ScanArgs(t, "priority", &priority)
				d.ScanArgs(t, "create-time", &createTime)
				var bypass bool
				d.ScanArgs(t, "bypass", &bypass)
				ctx, cancel := context.WithCancel(context.Background())
				workMap.set(id, &testWork{tenantID: tenant, cancel: cancel})
				workInfo := WorkInfo{
					TenantID:        tenant,
					Priority:        WorkPriority(priority),
					CreateTime:      int64(createTime),
					BypassAdmission: bypass,
				}
				go func(ctx context.Context, info WorkInfo, id int) {
					err := q.Admit(ctx, info)
					if err != nil {
						buf.printf("id %d: admit failed", id)
						workMap.delete(id)
					} else {
						buf.printf("id %d: admit succeeded", id)
						workMap.setAdmitted(id)
					}
				}(ctx, workInfo, id)
				// Need deterministic output, and this is racing with the goroutine
				// which is trying to get admitted. Sleep to let it get scheduled.
				time.Sleep(50 * time.Millisecond)
				return buf.stringAndReset()

			case "set-try-get-return-value":
				var v bool
				d.ScanArgs(t, "v", &v)
				tg.returnValueFromTryGet = v
				return ""

			case "granted":
				tg.grant()
				return buf.stringAndReset()

			case "cancel-work":
				var id int
				d.ScanArgs(t, "id", &id)
				work, ok := workMap.get(id)
				if !ok {
					return fmt.Sprintf("unknown id: %d", id)
				}
				if work.admitted {
					return fmt.Sprintf("work already admitted id: %d", id)
				}
				work.cancel()
				// Need deterministic output, and this is racing with the goroutine
				// whose work is cancelled. Sleep to let it get scheduled.
				time.Sleep(50 * time.Millisecond)
				return buf.stringAndReset()

			case "work-done":
				var id int
				d.ScanArgs(t, "id", &id)
				work, ok := workMap.get(id)
				if !ok {
					return fmt.Sprintf("unknown id: %d\n", id)
				}
				if !work.admitted {
					return fmt.Sprintf("id not admitted: %d\n", id)
				}
				q.AdmittedWorkDone(work.tenantID)
				workMap.delete(id)
				return buf.stringAndReset()

			case "print":
				return q.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	if q != nil {
		q.closeForTesting()
	}
}

func scanTenantID(t *testing.T, d *datadriven.TestData) roachpb.TenantID {
	var id int
	d.ScanArgs(t, "tenant", &id)
	return roachpb.MakeTenantID(uint64(id))
}

// TODO(sumeer):
// - Test metrics
// - Test race between grant and cancellation
// - Test AdmissionQueue for tokens
