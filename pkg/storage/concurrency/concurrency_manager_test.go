// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

// TestLockTableBasic verifies that sequences of requests interacting with a
// concurrency manager perform properly.
//
// The input files use the following DSL:
//
// txn             name=<txn-name> ts=<int>[,<int>] epoch=<int>
// single-request  name=<req-name> type=<proto-name> [fields=<name=value>[,<name2=value2>]...]
// batch-request   name=<batch-name> txn=<txn-name>|none ts=<int>[,<int>] reqs=<req-name>... [priority] [consistency]
// sequence        req=<batch-name>
// finish          req=<batch-name>
//
// handle-write-intent-error req=<batch-name> txn=<txn-name> key=<key>
// handle-txn-push-error     req=<batch-name> txn=<txn-name> key=<key>  TODO(nvanbenschoten): implement this
//
// on-lock-acquired  txn=<txn-name> key=<key>
// on-lock-updated   txn=<txn-name> key=<start>[,<end>]
// on-txn-updated    txn=<txn-name>
//
// on-desc-updated   TODO(nvanbenschoten): implement this
// on-lease-updated  TODO(nvanbenschoten): implement this
// on-split          TODO(nvanbenschoten): implement this
// on-merge          TODO(nvanbenschoten): implement this
//
// debug-latch-manager
// debug-lock-table
// reset
//
func TestConcurrencyManagerBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata/concurrency_manager", func(t *testing.T, path string) {
		c := newCluster()
		m := NewManager(c, c.rangeDesc)
		c.m = m
		mon := newMonitor()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "txn":
				var txnName string
				d.ScanArgs(t, "name", &txnName)
				ts := scanTimestamp(t, d)

				var epoch int
				d.ScanArgs(t, "epoch", &epoch)

				txn, ok := c.txnsByName[txnName]
				var id uuid.UUID
				if ok {
					id = txn.ID
				} else {
					id = c.createTxnRecord()
				}
				txn = &roachpb.Transaction{
					TxnMeta: enginepb.TxnMeta{
						ID:             id,
						Epoch:          enginepb.TxnEpoch(epoch),
						WriteTimestamp: ts,
						MinTimestamp:   ts,
					},
					ReadTimestamp: ts,
				}
				txn.UpdateObservedTimestamp(c.NodeDescriptor().NodeID, ts)
				c.txnsByName[txnName] = txn
				return ""

			case "single-request":
				var reqName string
				d.ScanArgs(t, "name", &reqName)
				if _, ok := c.singleRequestsByName[reqName]; ok {
					d.Fatalf(t, "duplicate single request: %s", reqName)
				}

				req := scanSingleRequest(t, d)
				c.singleRequestsByName[reqName] = req
				return ""

			case "batch-request":
				var batchName string
				d.ScanArgs(t, "name", &batchName)
				if _, ok := c.batchRequestsByName[batchName]; ok {
					d.Fatalf(t, "duplicate batch request: %s", batchName)
				}

				var txnName string
				d.ScanArgs(t, "txn", &txnName)
				txn, ok := c.txnsByName[txnName]
				if !ok && txnName != "none" {
					d.Fatalf(t, "unknown txn %s", txnName)
				}

				ts := scanTimestamp(t, d)
				if txn != nil && txn.ReadTimestamp != ts {
					d.Fatalf(t, "txn read timestamp != timestamp")
				}

				reqNames := scanStringSlice(t, d, "reqs")
				reqs := make([]roachpb.Request, len(reqNames))
				for i, reqName := range reqNames {
					req, ok := c.singleRequestsByName[reqName]
					if !ok {
						d.Fatalf(t, "unknown request %s", reqName)
					}
					reqs[i] = req
				}
				reqUnions := make([]roachpb.RequestUnion, len(reqs))
				for i, req := range reqs {
					reqUnions[i].MustSetInner(req)
				}
				spans := c.collectSpans(t, txn, ts, reqs)

				readConsistency := roachpb.CONSISTENT
				if d.HasArg("inconsistent") {
					readConsistency = roachpb.INCONSISTENT
				}

				c.batchRequestsByName[batchName] = Request{
					Txn:       txn,
					Timestamp: ts,
					// TODO(nvanbenschoten): test Priority
					ReadConsistency: readConsistency,
					Requests:        reqUnions,
					Spans:           spans,
				}
				return ""

			case "sequence":
				var batchName string
				d.ScanArgs(t, "req", &batchName)
				batch, ok := c.batchRequestsByName[batchName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", batchName)
				}

				c.mu.Lock()
				prev := c.guardsByReqName[batchName]
				delete(c.guardsByReqName, batchName)
				c.mu.Unlock()

				opName := fmt.Sprintf("sequence %s", batchName)
				mon.launchMonitored(opName, func(ctx context.Context) {
					guard, resp, err := m.SequenceReq(ctx, prev, batch)
					if err != nil {
						log.Eventf(ctx, "sequencing complete, returned error: %v", err)
					} else if resp != nil {
						log.Eventf(ctx, "sequencing complete, returned response: %v", resp)
					} else if guard != nil {
						log.Event(ctx, "sequencing complete, returned guard")
						c.mu.Lock()
						c.guardsByReqName[batchName] = guard
						c.mu.Unlock()
					} else {
						log.Event(ctx, "sequencing complete, returned no guard")
					}
				})
				return mon.waitAndCollect(t)

			case "finish":
				var batchName string
				d.ScanArgs(t, "req", &batchName)
				guard, ok := c.guardsByReqName[batchName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", batchName)
				}

				opName := fmt.Sprintf("finish %s", batchName)
				mon.launchMonitored(opName, func(ctx context.Context) {
					log.Event(ctx, "finishing request")
					m.FinishReq(guard)
					c.mu.Lock()
					delete(c.guardsByReqName, batchName)
					c.mu.Unlock()
				})
				return mon.waitAndCollect(t)

			case "handle-write-intent-error":
				var batchName string
				d.ScanArgs(t, "req", &batchName)
				guard, ok := c.guardsByReqName[batchName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", batchName)
				}

				var txnName string
				d.ScanArgs(t, "txn", &txnName)
				txn, ok := c.txnsByName[txnName]
				if !ok {
					d.Fatalf(t, "unknown txn %s", txnName)
				}

				var key string
				d.ScanArgs(t, "key", &key)

				opName := fmt.Sprintf("handle write intent error %s", batchName)
				mon.launchMonitored(opName, func(ctx context.Context) {
					err := &roachpb.WriteIntentError{Intents: []roachpb.Intent{{
						Span: roachpb.Span{Key: roachpb.Key(key)},
						Txn:  txn.TxnMeta,
					}}}
					log.Eventf(ctx, "handling %v", err)
					guard = m.HandleWriterIntentError(ctx, guard, err)
				})
				return mon.waitAndCollect(t)

			case "on-lock-acquired":
				var txnName string
				d.ScanArgs(t, "txn", &txnName)
				txn, ok := c.txnsByName[txnName]
				if !ok {
					d.Fatalf(t, "unknown txn %s", txnName)
				}

				var key string
				d.ScanArgs(t, "key", &key)

				mon.launchMonitored("acquire lock", func(ctx context.Context) {
					log.Eventf(ctx, "%s @ %s", txnName, key)
					m.OnLockAcquired(ctx, &roachpb.Intent{
						Span: roachpb.Span{Key: roachpb.Key(key)},
						Txn:  txn.TxnMeta,
					})
				})
				return mon.waitAndCollect(t)

			case "on-txn-updated":
				var txnName string
				d.ScanArgs(t, "txn", &txnName)
				txn, ok := c.txnsByName[txnName]
				if !ok {
					d.Fatalf(t, "unknown txn %s", txnName)
				}

				mon.launchMonitored("update txn", func(ctx context.Context) {
					log.Eventf(ctx, "committing %s", txnName)
					if err := c.commitTxnRecord(txn.ID); err != nil {
						d.Fatalf(t, err.Error())
					}
				})
				return mon.waitAndCollect(t)

			case "debug-latch-manager":
				global, local := m.(*managerImpl).lm.Info()
				output := []string{
					fmt.Sprintf("write count: %d", global.WriteCount+local.WriteCount),
					fmt.Sprintf(" read count: %d", global.ReadCount+local.ReadCount),
				}
				return strings.Join(output, "\n")

			case "debug-lock-table":
				return c.lockTable().String()

			case "reset":
				if n := mon.numMonitored(); n > 0 {
					d.Fatalf(t, "%d requests still in flight", n)
				}
				mon.resetSeqNums()
				if err := c.reset(); err != nil {
					d.Fatalf(t, "could not reset cluster: %v", err)
				}
				return ""

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

// cluster encapsulates the state of a running cluster and a set of requests.
// It serves as the test harness in TestConcurrencyManagerBasic - maintaining
// transaction and request declarations, recording the state of in-flight
// requests as they flow through the concurrency manager, and mocking out the
// interfaces that the concurrency manager interacts with.
type cluster struct {
	nodeDesc  *roachpb.NodeDescriptor
	rangeDesc *roachpb.RangeDescriptor
	m         Manager

	// Definitions.
	txnCounter           uint128.Uint128
	txnsByName           map[string]*roachpb.Transaction
	singleRequestsByName map[string]roachpb.Request
	batchRequestsByName  map[string]Request

	// Request state. Cleared on reset.
	mu              syncutil.Mutex
	guardsByReqName map[string]*Guard
	txnRecord       map[uuid.UUID]chan struct{} // closed on commit/abort
}

func newCluster() *cluster {
	return &cluster{
		nodeDesc:  &roachpb.NodeDescriptor{NodeID: 1},
		rangeDesc: &roachpb.RangeDescriptor{RangeID: 1},

		txnsByName:           make(map[string]*roachpb.Transaction),
		singleRequestsByName: make(map[string]roachpb.Request),
		batchRequestsByName:  make(map[string]Request),
		guardsByReqName:      make(map[string]*Guard),
		txnRecord:            make(map[uuid.UUID]chan struct{}),
	}
}

// cluster implements the Store interface.
func (c *cluster) NodeDescriptor() *roachpb.NodeDescriptor { return c.nodeDesc }
func (c *cluster) DB() *client.DB                          { return nil }
func (c *cluster) Clock() *hlc.Clock                       { return nil }
func (c *cluster) Stopper() *stop.Stopper                  { return nil }
func (c *cluster) IntentResolver() IntentResolver          { return c }
func (c *cluster) GetTxnWaitKnobs() txnwait.TestingKnobs   { return txnwait.TestingKnobs{} }
func (c *cluster) GetTxnWaitMetrics() *txnwait.Metrics     { return nil }
func (c *cluster) GetSlowLatchGauge() *metric.Gauge        { return nil }

// PushTransaction implements the IntentResolver interface.
func (c *cluster) PushTransaction(
	ctx context.Context, pushee *enginepb.TxnMeta, h roachpb.Header, pushType roachpb.PushTxnType,
) (roachpb.Transaction, *Error) {
	log.Eventf(ctx, "pushing txn %s", pushee.ID)
	pusheeRecord, err := c.getTxnRecord(pushee.ID)
	if err != nil {
		return roachpb.Transaction{}, roachpb.NewError(err)
	}
	// Wait for the transaction to commit.
	<-pusheeRecord
	return roachpb.Transaction{TxnMeta: *pushee, Status: roachpb.COMMITTED}, nil
}

// ResolveIntent implements the IntentResolver interface.
func (c *cluster) ResolveIntent(
	ctx context.Context, intent roachpb.Intent, _ intentresolver.ResolveOptions,
) *Error {
	log.Eventf(ctx, "resolving intent %s for txn %s", intent.Key, intent.Txn.ID)
	c.m.OnLockUpdated(ctx, &intent)
	return nil
}

// TODO(nvanbenschoten): remove the following methods once all their uses are
// possible through the external interface of the concurrency Manager.
func (c *cluster) latchManager() *latchManagerImpl { return c.m.(*managerImpl).lm.(*latchManagerImpl) }
func (c *cluster) lockTable() *lockTableImpl       { return c.m.(*managerImpl).lt.(*lockTableImpl) }

func (c *cluster) createTxnRecord() uuid.UUID {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := nextUUID(&c.txnCounter)
	c.txnRecord[id] = make(chan struct{})
	return id
}

func (c *cluster) getTxnRecord(id uuid.UUID) (chan struct{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok := c.txnRecord[id]
	if !ok {
		return nil, errors.Errorf("unknown txn %v: %v", id, c.txnRecord)
	}
	return ch, nil
}

func (c *cluster) commitTxnRecord(id uuid.UUID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok := c.txnRecord[id]
	if !ok {
		return errors.Errorf("unknown txn %v: %v", id, c.txnRecord)
	}
	close(ch)
	// Keep the record in the map so others can observe it.
	return nil
}

// reset clears all request state in the cluster. This avoids portions of tests
// leaking into one another and also serves as an assertion that a sequence of
// commands has completed without leaking any requests.
func (c *cluster) reset() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Reset all transactions to pending.
	for id := range c.txnRecord {
		c.txnRecord[id] = make(chan struct{})
	}
	// There should be no remaining concurrency guards.
	for name := range c.guardsByReqName {
		return errors.Errorf("unfinished guard for request: %s", name)
	}
	// There should be no outstanding latches.
	lm := c.latchManager()
	global, local := lm.Info()
	if global.ReadCount > 0 || global.WriteCount > 0 ||
		local.ReadCount > 0 || local.WriteCount > 0 {
		return errors.Errorf("outstanding latches")
	}
	// Clear the lock table.
	c.lockTable().Clear()
	return nil
}

// collectSpans collects the declared spans for a set of requests.
// Its logic mirrors that in Replica.collectSpans.
func (c *cluster) collectSpans(
	t *testing.T, txn *roachpb.Transaction, ts hlc.Timestamp, reqs []roachpb.Request,
) *spanset.SpanSet {
	spans := &spanset.SpanSet{}
	h := roachpb.Header{Txn: txn, Timestamp: ts}
	for _, req := range reqs {
		if cmd, ok := batcheval.LookupCommand(req.Method()); ok {
			cmd.DeclareKeys(c.rangeDesc, h, req, spans)
		} else {
			t.Fatalf("unrecognized command %s", req.Method())
		}
	}

	// Commands may create a large number of duplicate spans. De-duplicate
	// them to reduce the number of spans we pass to the spanlatch manager.
	spans.SortAndDedup()
	if err := spans.Validate(); err != nil {
		t.Fatal(err)
	}
	return spans

}

// monitor tracks a set of running goroutines as they execute and captures
// tracing recordings from them. It is capable of watching its set of goroutines
// until they all mutually stall.
//
// It is NOT safe to use multiple monitors concurrently.
type monitor struct {
	seq int
	gs  map[*monitoredGoroutine]struct{}
	buf []byte // avoids allocations
}

type monitoredGoroutine struct {
	opSeq     int
	opName    string
	collect   func() tracing.Recording
	cancel    func()
	prevLines int
	finished  int32
}

func newMonitor() *monitor {
	return &monitor{
		gs: make(map[*monitoredGoroutine]struct{}),
	}
}

func (m *monitor) launchMonitored(opName string, fn func(context.Context)) {
	m.seq++
	ctx := context.Background()
	ctx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, opName)
	g := &monitoredGoroutine{
		opSeq:   m.seq,
		opName:  opName,
		collect: collect,
		cancel:  cancel,
	}
	m.gs[g] = struct{}{}
	go func() {
		fn(ctx)
		atomic.StoreInt32(&g.finished, 1)
	}()
}

func (m *monitor) numMonitored() int {
	return len(m.gs)
}

func (m *monitor) resetSeqNums() {
	m.seq = 0
}

func (m *monitor) waitAndCollect(t *testing.T) string {
	m.waitForMonitoredGoroutinesToStall(t)
	return m.collectRecordings()
}

func (m *monitor) collectRecordings() string {
	// Collect trace recordings from each goroutine.
	type logRecord struct {
		g        *monitoredGoroutine
		time     time.Time
		fieldIdx int
		value    string
	}
	var logs []logRecord
	for g := range m.gs {
		prev := g.prevLines
		rec := g.collect()
		for _, span := range rec {
			for _, log := range span.Logs {
				for i, field := range log.Fields {
					if prev > 0 {
						prev--
						continue
					}
					logs = append(logs, logRecord{
						g:        g,
						time:     log.Time,
						fieldIdx: i,
						value:    field.Value,
					})
					g.prevLines++
				}
			}
		}
		if atomic.LoadInt32(&g.finished) == 1 {
			g.cancel()
			delete(m.gs, g)
		}
	}

	// Sort logs by (time, fieldIdx).
	//
	// TODO(nvanbenschoten): this won't be enough to make the trace output
	// deterministic when there is no enforced ordering between some of the
	// events in two different goroutines. We may need to simply sort by
	// goroutine sequence and avoid any notion of real-time ordering.
	sort.Slice(logs, func(i int, j int) bool {
		if logs[i].time.Equal(logs[j].time) {
			return logs[i].fieldIdx < logs[j].fieldIdx
		}
		return logs[i].time.Before(logs[j].time)
	})

	var buf strings.Builder
	for i, log := range logs {
		if i > 0 {
			buf.WriteByte('\n')
		}
		fmt.Fprintf(&buf, "[%d] %s: %s", log.g.opSeq, log.g.opName, log.value)
	}
	return buf.String()
}

// waitForMonitoredGoroutinesToStall waits for all goroutines that were launched
// by the monitor's launchMonitored method to stall due to cross-goroutine
// synchronization dependencies. For instance, it waits for all goroutines to
// stall while receiving from channels. When the method returns, the caller has
// exclusive access to any memory that it shares only with monitored goroutines
// until it performs an action that may unblock any of the goroutines.
func (m *monitor) waitForMonitoredGoroutinesToStall(t *testing.T) {
	// Iterate until we see two iterations in a row that both observe all
	// monitored goroutines to be stalled and also both observe the exact
	// same goroutine state. Waiting for two iterations to be identical
	// isn't required for correctness because the goroutine dump itself
	// should provide a consistent snapshot of all goroutine states and
	// statuses (runtime.Stack(all=true) stops the world), but it still
	// seems like a generally good idea.
	var prevStatus []goroutine
	filter := funcName((*monitor).launchMonitored)
	testutils.SucceedsSoon(t, func() error {
		status := goroutineStatus(t, filter, &m.buf)
		if len(status) == 0 {
			// No monitored goroutines.
			return nil
		}

		lastStatus := prevStatus
		prevStatus = status
		if lastStatus == nil {
			// First iteration.
			return errors.Errorf("previous status unset")
		}

		// Check whether all monitored goroutines are stalled. If not, retry.
		for _, g := range status {
			stalled, ok := goroutineStalledStates[g.status]
			if !ok {
				// NB: this will help us avoid rotting on Go runtime changes.
				t.Fatalf("unknown goroutine state: %s", g.status)
			}
			if !stalled {
				return errors.Errorf("goroutine %d is not stalled; status %s\n\n%s",
					g.id, g.status, g.frame)
			}
		}

		// Make sure the goroutines haven't changed since the last iteration.
		// This ensures that the goroutines stay stable for some amount of time.
		// NB: status and lastStatus are sorted by goroutine id.
		if !reflect.DeepEqual(status, lastStatus) {
			return errors.Errorf("goroutines rapidly changing")
		}
		return nil
	})
}

func funcName(f interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

// goroutineStalledStates maps all goroutine states as reported by runtime.Stack
// to a boolean representing whether that state indicates a stalled goroutine. A
// stalled goroutine is one that is waiting on a change on another goroutine in
// order for it to make forward progress itself. If all goroutines enter stalled
// states simultaneously, a process would encounter a deadlock.
var goroutineStalledStates = map[string]bool{
	// See gStatusStrings in runtime/traceback.go and associated comments about
	// the corresponding G statuses in runtime/runtime2.go.
	"idle":      false,
	"runnable":  false,
	"running":   false,
	"syscall":   false,
	"waiting":   true,
	"dead":      false,
	"copystack": false,
	"???":       false, // catch-all in runtime.goroutineheader

	// runtime.goroutineheader may override these G statuses with a waitReason.
	// See waitReasonStrings in runtime/runtime2.go.
	"GC assist marking":       false,
	"IO wait":                 false,
	"chan receive (nil chan)": true,
	"chan send (nil chan)":    true,
	"dumping heap":            false,
	"garbage collection":      false,
	"garbage collection scan": false,
	"panicwait":               false,
	"select":                  true,
	"select (no cases)":       true,
	"GC assist wait":          false,
	"GC sweep wait":           false,
	"GC scavenge wait":        false,
	"chan receive":            true,
	"chan send":               true,
	"finalizer wait":          false,
	"force gc (idle)":         false,
	// Perhaps surprisingly, we mark "semacquire" as a non-stalled state. This
	// is because it is possible to see goroutines briefly enter this state when
	// performing fine-grained memory synchronization, occasionally in the Go
	// runtime itself. No request-level synchronization points use mutexes to
	// wait for state transitions by other requests, so it is safe to ignore
	// this state and wait for it to exit.
	"semacquire":             false,
	"sleep":                  false,
	"sync.Cond.Wait":         true,
	"timer goroutine (idle)": false,
	"trace reader (blocked)": false,
	"wait for GC cycle":      false,
	"GC worker (idle)":       false,
}

type goroutine struct {
	id     int
	status string
	frame  []byte
}

var goroutineStackStatusRE = regexp.MustCompile(`goroutine (\d+) \[(.+)\]:`)

// goroutineStatus returns a stack trace for each goroutine whose stack frame
// matches the provided filter. It uses the provided buffer to avoid repeat
// allocations.
func goroutineStatus(t *testing.T, filter string, buf *[]byte) []goroutine {
	// We don't know how big the buffer needs to be to collect all the
	// goroutines. Start with 1 MB and try a few times, doubling each time.
	// NB: This is inspired by runtime/pprof/pprof.go:writeGoroutineStacks.
	if len(*buf) == 0 {
		*buf = make([]byte, 1<<20)
	}
	var truncBuf []byte
	for i := 0; ; i++ {
		n := runtime.Stack(*buf, true /* all */)
		if n < len(*buf) {
			truncBuf = (*buf)[:n]
			break
		}
		*buf = make([]byte, 2*len(*buf))
	}

	// Split up each stack frame.
	frames := bytes.Split(truncBuf, []byte("\n\n"))

	// Filter down to only those being monitored by the test harness.
	var filtered [][]byte
	for _, f := range frames {
		if bytes.Contains(f, []byte(filter)) {
			filtered = append(filtered, f)
		}
	}

	// Parse matching goroutine statuses.
	status := make([]goroutine, len(filtered))
	for i, f := range filtered {
		match := goroutineStackStatusRE.FindSubmatch(f)
		if len(match) != 3 {
			t.Fatalf("could not find goroutine header in:\n%s", f)
		}
		gid, err := strconv.Atoi(string(match[1]))
		if err != nil {
			t.Fatal(err)
		}
		status[i] = goroutine{
			id:     gid,
			status: string(match[2]),
			frame:  f,
		}
	}
	sort.Slice(status, func(i int, j int) bool {
		return status[i].id < status[j].id
	})
	return status
}
