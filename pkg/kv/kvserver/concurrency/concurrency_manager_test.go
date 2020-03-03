// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/concurrency"
	"github.com/cockroachdb/cockroach/pkg/storage/concurrency/lock"
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
	"github.com/maruel/panicparse/stack"
	"github.com/petermattis/goid"
)

// TestConcurrencyManagerBasic verifies that sequences of requests interacting
// with a concurrency manager perform properly.
//
// The input files use the following DSL:
//
// new-txn      name=<txn-name> ts=<int>[,<int>] epoch=<int>
// new-request  name=<req-name> txn=<txn-name>|none ts=<int>[,<int>] [priority] [consistency]
//   <proto-name> [<field-name>=<field-value>...]
// sequence     req=<req-name>
// finish       req=<req-name>
//
// handle-write-intent-error  req=<req-name> txn=<txn-name> key=<key>
// handle-txn-push-error      req=<req-name> txn=<txn-name> key=<key>  TODO(nvanbenschoten): implement this
//
// on-lock-acquired  txn=<txn-name> key=<key>
// on-txn-updated    txn=<txn-name> status=[committed|aborted|pending] [ts<int>[,<int>]]
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
		m := concurrency.NewManager(c, c.rangeDesc)
		c.m = m
		mon := newMonitor()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "new-txn":
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
					id = c.newTxnID()
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
				c.registerTxn(txnName, txn)
				return ""

			case "new-request":
				var reqName string
				d.ScanArgs(t, "name", &reqName)
				if _, ok := c.requestsByName[reqName]; ok {
					d.Fatalf(t, "duplicate request: %s", reqName)
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

				readConsistency := roachpb.CONSISTENT
				if d.HasArg("inconsistent") {
					readConsistency = roachpb.INCONSISTENT
				}

				// Each roachpb.Request is provided on an indented line.
				var reqs []roachpb.Request
				singleReqLines := strings.Split(d.Input, "\n")
				for _, line := range singleReqLines {
					req := scanSingleRequest(t, d, line)
					reqs = append(reqs, req)
				}
				reqUnions := make([]roachpb.RequestUnion, len(reqs))
				for i, req := range reqs {
					reqUnions[i].MustSetInner(req)
				}
				spans := c.collectSpans(t, txn, ts, reqs)

				c.requestsByName[reqName] = concurrency.Request{
					Txn:       txn,
					Timestamp: ts,
					// TODO(nvanbenschoten): test Priority
					ReadConsistency: readConsistency,
					Requests:        reqUnions,
					Spans:           spans,
				}
				return ""

			case "sequence":
				var reqName string
				d.ScanArgs(t, "req", &reqName)
				req, ok := c.requestsByName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}

				c.mu.Lock()
				prev := c.guardsByReqName[reqName]
				delete(c.guardsByReqName, reqName)
				c.mu.Unlock()

				opName := fmt.Sprintf("sequence %s", reqName)
				mon.runAsync(opName, func(ctx context.Context) {
					guard, resp, err := m.SequenceReq(ctx, prev, req)
					if err != nil {
						log.Eventf(ctx, "sequencing complete, returned error: %v", err)
					} else if resp != nil {
						log.Eventf(ctx, "sequencing complete, returned response: %v", resp)
					} else if guard != nil {
						log.Event(ctx, "sequencing complete, returned guard")
						c.mu.Lock()
						c.guardsByReqName[reqName] = guard
						c.mu.Unlock()
					} else {
						log.Event(ctx, "sequencing complete, returned no guard")
					}
				})
				return mon.waitAndCollect(t)

			case "finish":
				var reqName string
				d.ScanArgs(t, "req", &reqName)
				guard, ok := c.guardsByReqName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}

				opName := fmt.Sprintf("finish %s", reqName)
				mon.runSync(opName, func(ctx context.Context) {
					log.Event(ctx, "finishing request")
					m.FinishReq(guard)
					c.mu.Lock()
					delete(c.guardsByReqName, reqName)
					c.mu.Unlock()
				})
				return mon.waitAndCollect(t)

			case "handle-write-intent-error":
				var reqName string
				d.ScanArgs(t, "req", &reqName)
				guard, ok := c.guardsByReqName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}

				var txnName string
				d.ScanArgs(t, "txn", &txnName)
				txn, ok := c.txnsByName[txnName]
				if !ok {
					d.Fatalf(t, "unknown txn %s", txnName)
				}

				var key string
				d.ScanArgs(t, "key", &key)

				opName := fmt.Sprintf("handle write intent error %s", reqName)
				mon.runSync(opName, func(ctx context.Context) {
					err := &roachpb.WriteIntentError{Intents: []roachpb.Intent{
						roachpb.MakeIntent(&txn.TxnMeta, roachpb.Key(key)),
					}}
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

				mon.runSync("acquire lock", func(ctx context.Context) {
					log.Eventf(ctx, "%s @ %s", txnName, key)
					span := roachpb.Span{Key: roachpb.Key(key)}
					up := roachpb.MakeLockUpdateWithDur(txn, span, lock.Unreplicated)
					m.OnLockAcquired(ctx, &up)
				})
				return mon.waitAndCollect(t)

			case "on-txn-updated":
				var txnName string
				d.ScanArgs(t, "txn", &txnName)
				txn, ok := c.txnsByName[txnName]
				if !ok {
					d.Fatalf(t, "unknown txn %s", txnName)
				}

				var statusStr, verb string
				var status roachpb.TransactionStatus
				d.ScanArgs(t, "status", &statusStr)
				switch statusStr {
				case "committed":
					verb = "committing"
					status = roachpb.COMMITTED
				case "aborted":
					verb = "aborting"
					status = roachpb.ABORTED
				case "pending":
					verb = "increasing timestamp of"
					status = roachpb.PENDING
				default:
					d.Fatalf(t, "unknown txn statusStr: %s", status)
				}

				var ts hlc.Timestamp
				if d.HasArg("ts") {
					ts = scanTimestamp(t, d)
				}

				mon.runSync("update txn", func(ctx context.Context) {
					log.Eventf(ctx, "%s %s", verb, txnName)
					if err := c.updateTxnRecord(txn.ID, status, ts); err != nil {
						d.Fatalf(t, err.Error())
					}
				})
				return mon.waitAndCollect(t)

			case "debug-latch-manager":
				global, local := m.LatchMetrics()
				output := []string{
					fmt.Sprintf("write count: %d", global.WriteCount+local.WriteCount),
					fmt.Sprintf(" read count: %d", global.ReadCount+local.ReadCount),
				}
				return strings.Join(output, "\n")

			case "debug-lock-table":
				return m.LockTableDebug()

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
	m         concurrency.Manager

	// Definitions.
	txnCounter     uint128.Uint128
	txnsByName     map[string]*roachpb.Transaction
	requestsByName map[string]concurrency.Request

	// Request state. Cleared on reset.
	mu              syncutil.Mutex
	guardsByReqName map[string]*concurrency.Guard
	txnRecords      map[uuid.UUID]*txnRecord
}

type txnRecord struct {
	mu               syncutil.Mutex
	cond             sync.Cond
	txn              *roachpb.Transaction // immutable, modify fields below
	updatedStatus    roachpb.TransactionStatus
	updatedTimestamp hlc.Timestamp
}

func newCluster() *cluster {
	return &cluster{
		nodeDesc:  &roachpb.NodeDescriptor{NodeID: 1},
		rangeDesc: &roachpb.RangeDescriptor{RangeID: 1},

		txnsByName:      make(map[string]*roachpb.Transaction),
		requestsByName:  make(map[string]concurrency.Request),
		guardsByReqName: make(map[string]*concurrency.Guard),
		txnRecords:      make(map[uuid.UUID]*txnRecord),
	}
}

// cluster implements the Store interface. Many of these methods are only used
// by the txnWaitQueue, whose functionality is fully mocked out in this test by
// cluster's implementation of concurrency.IntentResolver.
func (c *cluster) NodeDescriptor() *roachpb.NodeDescriptor    { return c.nodeDesc }
func (c *cluster) DB() *client.DB                             { return nil }
func (c *cluster) Clock() *hlc.Clock                          { return nil }
func (c *cluster) Stopper() *stop.Stopper                     { return nil }
func (c *cluster) IntentResolver() concurrency.IntentResolver { return c }
func (c *cluster) GetTxnWaitKnobs() txnwait.TestingKnobs      { return txnwait.TestingKnobs{} }
func (c *cluster) GetTxnWaitMetrics() *txnwait.Metrics        { return txnwait.NewMetrics(time.Minute) }
func (c *cluster) GetSlowLatchGauge() *metric.Gauge           { return nil }

// PushTransaction implements the concurrency.IntentResolver interface.
func (c *cluster) PushTransaction(
	ctx context.Context, pushee *enginepb.TxnMeta, h roachpb.Header, pushType roachpb.PushTxnType,
) (roachpb.Transaction, *roachpb.Error) {
	log.Eventf(ctx, "pushing txn %s", pushee.ID)
	pusheeRecord, err := c.getTxnRecord(pushee.ID)
	if err != nil {
		return roachpb.Transaction{}, roachpb.NewError(err)
	}
	// Wait for the transaction to be pushed.
	pusheeRecord.mu.Lock()
	defer pusheeRecord.mu.Unlock()
	for {
		pusheeTxn := pusheeRecord.asTxnLocked()
		var pushed bool
		switch pushType {
		case roachpb.PUSH_TIMESTAMP:
			pushed = h.Timestamp.Less(pusheeTxn.WriteTimestamp) || pusheeTxn.Status.IsFinalized()
		case roachpb.PUSH_ABORT:
			pushed = pusheeTxn.Status.IsFinalized()
		default:
			return roachpb.Transaction{}, roachpb.NewErrorf("unexpected push type: %s", pushType)
		}
		if pushed {
			return pusheeTxn, nil
		}
		pusheeRecord.cond.Wait()
	}
}

// ResolveIntent implements the concurrency.IntentResolver interface.
func (c *cluster) ResolveIntent(
	ctx context.Context, intent roachpb.LockUpdate, _ intentresolver.ResolveOptions,
) *roachpb.Error {
	log.Eventf(ctx, "resolving intent %s for txn %s with %s status", intent.Key, intent.Txn.ID, intent.Status)
	c.m.OnLockUpdated(ctx, &intent)
	return nil
}

func (c *cluster) newTxnID() uuid.UUID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return nextUUID(&c.txnCounter)
}

func (c *cluster) registerTxn(name string, txn *roachpb.Transaction) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txnsByName[name] = txn
	r := &txnRecord{txn: txn}
	r.cond.L = &r.mu
	c.txnRecords[txn.ID] = r
}

func (c *cluster) getTxnRecord(id uuid.UUID) (*txnRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.txnRecords[id]
	if !ok {
		return nil, errors.Errorf("unknown txn %v: %v", id, c.txnRecords)
	}
	return r, nil
}

func (c *cluster) updateTxnRecord(
	id uuid.UUID, status roachpb.TransactionStatus, ts hlc.Timestamp,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.txnRecords[id]
	if !ok {
		return errors.Errorf("unknown txn %v: %v", id, c.txnRecords)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updatedStatus = status
	r.updatedTimestamp = ts
	r.cond.Broadcast()
	return nil
}

func (r *txnRecord) asTxnLocked() roachpb.Transaction {
	txn := *r.txn
	if r.updatedStatus > txn.Status {
		txn.Status = r.updatedStatus
	}
	txn.WriteTimestamp.Forward(r.updatedTimestamp)
	return txn
}

// reset clears all request state in the cluster. This avoids portions of tests
// leaking into one another and also serves as an assertion that a sequence of
// commands has completed without leaking any requests.
func (c *cluster) reset() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Reset all transactions to their original state.
	for id := range c.txnRecords {
		r := c.txnRecords[id]
		r.mu.Lock()
		r.updatedStatus = roachpb.PENDING
		r.updatedTimestamp = hlc.Timestamp{}
		r.mu.Unlock()
	}
	// There should be no remaining concurrency guards.
	for name := range c.guardsByReqName {
		return errors.Errorf("unfinished guard for request: %s", name)
	}
	// There should be no outstanding latches.
	global, local := c.m.LatchMetrics()
	if global.ReadCount > 0 || global.WriteCount > 0 ||
		local.ReadCount > 0 || local.WriteCount > 0 {
		return errors.Errorf("outstanding latches")
	}
	// Clear the lock table by marking the range as merged.
	c.m.OnMerge()
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
	opSeq    int
	opName   string
	gID      int64
	finished int32

	ctx        context.Context
	collect    func() tracing.Recording
	cancel     func()
	prevEvents int
}

func newMonitor() *monitor {
	return &monitor{
		gs: make(map[*monitoredGoroutine]struct{}),
	}
}

func (m *monitor) runSync(opName string, fn func(context.Context)) {
	ctx, collect, cancel := tracing.ContextWithRecordingSpan(context.Background(), opName)
	g := &monitoredGoroutine{
		opSeq:   0, // synchronous
		opName:  opName,
		ctx:     ctx,
		collect: collect,
		cancel:  cancel,
	}
	m.gs[g] = struct{}{}
	fn(ctx)
	atomic.StoreInt32(&g.finished, 1)
}

func (m *monitor) runAsync(opName string, fn func(context.Context)) {
	m.seq++
	ctx, collect, cancel := tracing.ContextWithRecordingSpan(context.Background(), opName)
	g := &monitoredGoroutine{
		opSeq:   m.seq,
		opName:  opName,
		ctx:     ctx,
		collect: collect,
		cancel:  cancel,
	}
	m.gs[g] = struct{}{}
	go func() {
		atomic.StoreInt64(&g.gID, goid.Get())
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
	m.waitForAsyncGoroutinesToStall(t)
	return m.collectRecordings()
}

func (m *monitor) collectRecordings() string {
	// Collect trace recordings from each goroutine.
	type logRecord struct {
		g     *monitoredGoroutine
		value string
	}
	var logs []logRecord
	for g := range m.gs {
		prev := g.prevEvents
		rec := g.collect()
		for _, span := range rec {
			for _, log := range span.Logs {
				for _, field := range log.Fields {
					if prev > 0 {
						prev--
						continue
					}
					logs = append(logs, logRecord{
						g: g, value: field.Value,
					})
					g.prevEvents++
				}
			}
		}
		if atomic.LoadInt32(&g.finished) == 1 {
			g.cancel()
			delete(m.gs, g)
		}
	}

	// Sort logs by g.opSeq. This will sort synchronous goroutines before
	// asynchronous goroutines. Use a stable sort to break ties within
	// goroutines and keep logs in event order.
	sort.SliceStable(logs, func(i int, j int) bool {
		return logs[i].g.opSeq < logs[j].g.opSeq
	})

	var buf strings.Builder
	for i, log := range logs {
		if i > 0 {
			buf.WriteByte('\n')
		}
		seq := "-"
		if log.g.opSeq != 0 {
			seq = strconv.Itoa(log.g.opSeq)
		}
		fmt.Fprintf(&buf, "[%s] %s: %s", seq, log.g.opName, log.value)
	}
	return buf.String()
}

func (m *monitor) hasNewEvents(g *monitoredGoroutine) bool {
	events := 0
	rec := g.collect()
	for _, span := range rec {
		for _, log := range span.Logs {
			for range log.Fields {
				events++
			}
		}
	}
	return events > g.prevEvents
}

// waitForAsyncGoroutinesToStall waits for all goroutines that were launched by
// the monitor's runAsync method to stall due to cross-goroutine synchronization
// dependencies. For instance, it waits for all goroutines to stall while
// receiving from channels. When the method returns, the caller has exclusive
// access to any memory that it shares only with monitored goroutines until it
// performs an action that may unblock any of the goroutines.
func (m *monitor) waitForAsyncGoroutinesToStall(t *testing.T) {
	// Iterate until we see two iterations in a row that both observe all
	// monitored goroutines to be stalled and also both observe the exact
	// same goroutine state. Waiting for two iterations to be identical
	// isn't required for correctness because the goroutine dump itself
	// should provide a consistent snapshot of all goroutine states and
	// statuses (runtime.Stack(all=true) stops the world), but it still
	// seems like a generally good idea.
	var status []*stack.Goroutine
	filter := funcName((*monitor).runAsync)
	testutils.SucceedsSoon(t, func() error {
		prevStatus := status
		status = goroutineStatus(t, filter, &m.buf)
		if len(status) == 0 {
			// No monitored goroutines.
			return nil
		}

		if prevStatus == nil {
			// First iteration.
			return errors.Errorf("previous status unset")
		}

		// Check whether all monitored goroutines are stalled. If not, retry.
		for _, stat := range status {
			stalled, ok := goroutineStalledStates[stat.State]
			if !ok {
				// NB: this will help us avoid rotting on Go runtime changes.
				t.Fatalf("unknown goroutine state: %s", stat.State)
			}
			if !stalled {
				return errors.Errorf("goroutine %d is not stalled; status %s", stat.ID, stat.State)
			}
		}

		// Make sure the goroutines haven't changed since the last iteration.
		// This ensures that the goroutines stay stable for some amount of time.
		// NB: status and lastStatus are sorted by goroutine id.
		if !reflect.DeepEqual(status, prevStatus) {
			return errors.Errorf("goroutines rapidly changing")
		}
		return nil
	})

	// Add a trace event indicating where each stalled goroutine is waiting.
	byID := make(map[int64]*monitoredGoroutine, len(m.gs))
	for g := range m.gs {
		byID[atomic.LoadInt64(&g.gID)] = g
	}
	for _, stat := range status {
		g, ok := byID[int64(stat.ID)]
		if !ok {
			// If we're not tracking this goroutine, just ignore it. This can
			// happen when goroutines from earlier subtests haven't finished
			// yet.
			continue
		}
		// If the goroutine hasn't produced any new events, don't create a new
		// "blocked on" trace event. It likely hasn't moved since the last one.
		if !m.hasNewEvents(g) {
			continue
		}
		stalledCall := firstNonStdlib(stat.Stack.Calls)
		log.Eventf(g.ctx, "blocked on %s in %s", stat.State, stalledCall.Func.PkgDotName())
	}
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

// goroutineStatus returns a stack trace for each goroutine whose stack frame
// matches the provided filter. It uses the provided buffer to avoid repeat
// allocations.
func goroutineStatus(t *testing.T, filter string, buf *[]byte) []*stack.Goroutine {
	// We don't know how big the buffer needs to be to collect all the
	// goroutines. Start with 64 KB and try a few times, doubling each time.
	// NB: This is inspired by runtime/pprof/pprof.go:writeGoroutineStacks.
	if len(*buf) == 0 {
		*buf = make([]byte, 1<<16)
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

	// guesspaths=true is required for Call objects to have IsStdlib filled in.
	guesspaths := true
	ctx, err := stack.ParseDump(bytes.NewBuffer(truncBuf), ioutil.Discard, guesspaths)
	if err != nil {
		t.Fatalf("could not parse goroutine dump: %v", err)
		return nil
	}

	matching := ctx.Goroutines[:0]
	for _, g := range ctx.Goroutines {
		for _, call := range g.Stack.Calls {
			if strings.Contains(call.Func.Raw, filter) {
				matching = append(matching, g)
				break
			}
		}
	}
	return matching
}

func firstNonStdlib(calls []stack.Call) stack.Call {
	for _, call := range calls {
		if !call.IsStdlib {
			return call
		}
	}
	panic("unexpected")
}
