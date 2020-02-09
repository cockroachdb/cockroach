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
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
)

/*
Test needs to handle caller constraints wrt latches being held. The datadriven test uses the
following format:

txn txn=<name> ts=<int>[,<int>] epoch=<int>
----

 Creates a TxnMeta.

request r=<name> txn=<name>|none ts=<int>[,<int>] spans=r|w@<start>[,<end>]+...
----

 Creates a Request.

scan r=<name>
----
<error string>|start-waiting: <bool>

 Calls lockTable.scanAndEnqueue. If the request has an existing guard, uses it. If a guard is
 returned, stores it for later use.

acquire r=<name> k=<key> durability=r|u
----
<error string>

 Acquires lock for the request, using the existing guard for that request.

release txn=<name> span=<start>[,<end>]
----
<error string>

 Releases locks for the named transaction.

update txn=<name> ts=<int>[,<int>] epoch=<int> span=<start>[,<end>]
----
<error string>

 Updates locks for the named transaction.

add-discovered r=<name> k=<key> txn=<name>
----
<error string>

 Adds a discovered lock that is disovered by the named request.

done r=<name>
----
<error string>

 Calls lockTable.done() for the named request. The request and guard are discarded after this.

guard-state r=<name>
----
new|old: state=<state> [txn=<name> ts=<ts>]

  Calls requestGuard.newState() in a non-blocking manner, followed by currentState().

guard-start-waiting r=<name>
----
<bool>

 Calls requestGuard.startWaiting().

print
----
<state of lock table>

 Calls lockTable.String()
*/

func scanTimestamp(t *testing.T, d *datadriven.TestData) hlc.Timestamp {
	var ts hlc.Timestamp
	var tsS string
	d.ScanArgs(t, "ts", &tsS)
	parts := strings.Split(tsS, ",")

	// Find the wall time part.
	tsW, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		d.Fatalf(t, "%v", err)
	}
	ts.WallTime = tsW

	// Find the logical part, if there is one.
	var tsL int64
	if len(parts) > 1 {
		tsL, err = strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			d.Fatalf(t, "%v", err)
		}
	}
	ts.Logical = int32(tsL)
	return ts
}

func nextUUID(counter *uint128.Uint128) uuid.UUID {
	*counter = counter.Add(1)
	return uuid.FromUint128(*counter)
}

func getSpan(t *testing.T, d *datadriven.TestData, str string) roachpb.Span {
	parts := strings.Split(str, ",")
	span := roachpb.Span{Key: roachpb.Key(parts[0])}
	if len(parts) > 2 {
		d.Fatalf(t, "incorrect span format: %s", str)
	} else if len(parts) == 2 {
		span.EndKey = roachpb.Key(parts[1])
	}
	return span
}

func scanSpans(t *testing.T, d *datadriven.TestData, ts hlc.Timestamp) *spanset.SpanSet {
	spans := &spanset.SpanSet{}
	var spansStr string
	d.ScanArgs(t, "spans", &spansStr)
	parts := strings.Split(spansStr, "+")
	for _, p := range parts {
		if len(p) < 2 || p[1] != '@' {
			d.Fatalf(t, "incorrect span with access format: %s", p)
		}
		c := p[0]
		p = p[2:]
		var sa spanset.SpanAccess
		switch c {
		case 'r':
			sa = spanset.SpanReadOnly
		case 'w':
			sa = spanset.SpanReadWrite
		default:
			d.Fatalf(t, "incorrect span access: %c", c)
		}
		spans.AddMVCC(sa, getSpan(t, d, p), ts)
	}
	return spans
}

type testRequest struct {
	tM *enginepb.TxnMeta
	s  *spanset.SpanSet
	t  hlc.Timestamp
}

var _ Request = &testRequest{}

func (r *testRequest) txnMeta() *enginepb.TxnMeta { return r.tM }
func (r *testRequest) spans() *spanset.SpanSet    { return r.s }
func (r *testRequest) ts() hlc.Timestamp          { return r.t }

func TestLockTableBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	lt := newLockTable(1000)
	txnsByName := make(map[string]*enginepb.TxnMeta)
	txnCounter := uint128.FromInts(0, 0)
	requestsByName := make(map[string]*testRequest)
	guardsByReqName := make(map[string]requestGuard)
	datadriven.RunTest(t, "testdata/lock_table", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "txn":
			var txnName string
			// UUIDs for transactions are numbered from 1 by this test code and
			// lockTableImpl.String() knows about UUIDs and not transaction names.
			// Assigning txnNames of the form txn1, txn2, ... keeps the two in sync,
			// which makes test cases easier to understand.
			d.ScanArgs(t, "txn", &txnName)
			ts := scanTimestamp(t, d)
			var epoch int
			d.ScanArgs(t, "epoch", &epoch)
			txnMeta, ok := txnsByName[txnName]
			var id uuid.UUID
			if ok {
				id = txnMeta.ID
			} else {
				id = nextUUID(&txnCounter)
			}
			txnsByName[txnName] = &enginepb.TxnMeta{
				ID:             id,
				Epoch:          enginepb.TxnEpoch(epoch),
				WriteTimestamp: ts,
			}
			return ""

		case "request":
			// Seqnums for requests are numbered from 1 by lockTableImpl and
			// lockTableImpl.String() does not know about request names. Assigning
			// request names of the form req1, req2, ... keeps the two in sync,
			// which makes test cases easier to understand.
			var reqName string
			d.ScanArgs(t, "r", &reqName)
			if _, ok := requestsByName[reqName]; ok {
				d.Fatalf(t, "duplicate request: %s", reqName)
			}
			var txnName string
			d.ScanArgs(t, "txn", &txnName)
			txnMeta, ok := txnsByName[txnName]
			if !ok && txnName != "none" {
				d.Fatalf(t, "unknown txn %s", txnName)
			}
			ts := scanTimestamp(t, d)
			spans := scanSpans(t, d, ts)
			req := &testRequest{
				tM: txnMeta,
				s:  spans,
				t:  ts,
			}
			requestsByName[reqName] = req
			return ""

		case "scan":
			var reqName string
			d.ScanArgs(t, "r", &reqName)
			req := requestsByName[reqName]
			if req == nil {
				d.Fatalf(t, "unknown request: %s", reqName)
			}
			g := guardsByReqName[reqName]
			g, err := lt.scanAndEnqueue(req, g)
			guardsByReqName[reqName] = g
			if err != nil {
				return err.Error()
			}
			return fmt.Sprintf("start-waiting: %t", g.startWaiting())

		case "acquire":
			var reqName string
			d.ScanArgs(t, "r", &reqName)
			g := guardsByReqName[reqName]
			if g == nil {
				d.Fatalf(t, "unknown guard: %s", reqName)
			}
			var key string
			d.ScanArgs(t, "k", &key)
			var s string
			d.ScanArgs(t, "durability", &s)
			if len(s) != 1 || (s[0] != 'r' && s[0] != 'u') {
				d.Fatalf(t, "incorrect durability: %s", s)
			}
			durability := lock.Unreplicated
			if s[0] == 'r' {
				durability = lock.Replicated
			}
			if err := lt.acquireLock(roachpb.Key(key), lock.Exclusive, durability, g); err != nil {
				return err.Error()
			}
			return lt.(*lockTableImpl).String()

		case "release":
			var txnName string
			d.ScanArgs(t, "txn", &txnName)
			txnMeta, ok := txnsByName[txnName]
			if !ok {
				d.Fatalf(t, "unknown txn %s", txnName)
			}
			var s string
			d.ScanArgs(t, "span", &s)
			span := getSpan(t, d, s)
			// TODO(sbhola): also test ABORTED.
			intent := &roachpb.Intent{Span: span, Txn: *txnMeta, Status: roachpb.COMMITTED}
			if err := lt.updateLocks(intent); err != nil {
				return err.Error()
			}
			return lt.(*lockTableImpl).String()

		case "update":
			var txnName string
			d.ScanArgs(t, "txn", &txnName)
			txnMeta, ok := txnsByName[txnName]
			if !ok {
				d.Fatalf(t, "unknown txn %s", txnName)
			}
			ts := scanTimestamp(t, d)
			var epoch int
			d.ScanArgs(t, "epoch", &epoch)
			txnMeta = &enginepb.TxnMeta{ID: txnMeta.ID}
			txnMeta.Epoch = enginepb.TxnEpoch(epoch)
			txnMeta.WriteTimestamp = ts
			txnsByName[txnName] = txnMeta
			var s string
			d.ScanArgs(t, "span", &s)
			span := getSpan(t, d, s)
			// TODO(sbhola): also test STAGING.
			intent := &roachpb.Intent{Span: span, Txn: *txnMeta, Status: roachpb.PENDING}
			if err := lt.updateLocks(intent); err != nil {
				return err.Error()
			}
			return lt.(*lockTableImpl).String()

		case "add-discovered":
			var reqName string
			d.ScanArgs(t, "r", &reqName)
			g := guardsByReqName[reqName]
			if g == nil {
				d.Fatalf(t, "unknown guard: %s", reqName)
			}
			var key string
			d.ScanArgs(t, "k", &key)
			var txnName string
			d.ScanArgs(t, "txn", &txnName)
			txnMeta, ok := txnsByName[txnName]
			if !ok {
				d.Fatalf(t, "unknown txn %s", txnName)
			}
			if err := lt.addDiscoveredLock(roachpb.Key(key), txnMeta, txnMeta.WriteTimestamp, g); err != nil {
				return err.Error()
			}
			return lt.(*lockTableImpl).String()

		case "done":
			var reqName string
			d.ScanArgs(t, "r", &reqName)
			g := guardsByReqName[reqName]
			if g == nil {
				d.Fatalf(t, "unknown guard: %s", reqName)
			}
			err := lt.done(g)
			delete(guardsByReqName, reqName)
			delete(requestsByName, reqName)
			if err != nil {
				return err.Error()
			}
			return lt.(*lockTableImpl).String()

		case "guard-start-waiting":
			var reqName string
			d.ScanArgs(t, "r", &reqName)
			g := guardsByReqName[reqName]
			if g == nil {
				d.Fatalf(t, "unknown guard: %s", reqName)
			}
			return fmt.Sprintf("%t", g.startWaiting())

		case "guard-state":
			var reqName string
			d.ScanArgs(t, "r", &reqName)
			g := guardsByReqName[reqName]
			if g == nil {
				d.Fatalf(t, "unknown guard: %s", reqName)
			}
			var str string
			select {
			case <-g.newState():
				str = "new: "
			default:
				str = "old: "
			}
			state, err := g.currentState()
			if err != nil {
				return str + err.Error()
			}
			var typeStr string
			switch state.stateKind {
			case waitForDistinguished:
				typeStr = "waitForDistinguished"
			case waitFor:
				typeStr = "waitFor"
			case waitElsewhere:
				typeStr = "waitElsewhere"
			case waitSelf:
				return str + "state=waitSelf"
			case doneWaiting:
				return str + "state=doneWaiting"
			}
			id := state.txn.ID
			var txnS string
			for k, v := range txnsByName {
				if v.ID.Equal(id) {
					txnS = k
					break
				}
			}
			if txnS == "" {
				txnS = fmt.Sprintf("unknown txn with ID: %v", state.txn.ID)
			}
			tsS := fmt.Sprintf("%d", state.ts.WallTime)
			if state.ts.Logical != 0 {
				tsS += fmt.Sprintf(",%d", state.ts.Logical)
			}
			return fmt.Sprintf("%sstate=%s txn=%s ts=%s", str, typeStr, txnS, tsS)

		case "print":
			return lt.(*lockTableImpl).String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

type workItem struct {
	// Contains one of request or intents.

	// Request.
	request        *testRequest
	locksToAcquire []roachpb.Key

	// updateLocks
	intents []*roachpb.Intent
}

func (w *workItem) getRequestTxnID() uuid.UUID {
	if w.request != nil && w.request.tM != nil {
		return w.request.tM.ID
	}
	return uuid.UUID{}
}

func doWork(ctx context.Context, item *workItem, e *workloadExecutor) error {
	defer func() {
		e.doneWork <- item
	}()
	if item.request != nil {
		var lg *spanlatch.Guard
		var g requestGuard
		var err error
		for {
			// Since we can't do a select involving latch acquisition and context
			// cancellation, the code makes sure to release latches when returning
			// early due to error. Otherwise other requests will get stuck and
			// group.Wait() will not return until the test times out.
			lg, err = e.lm.Acquire(context.TODO(), item.request.s)
			if err != nil {
				return err
			}
			g, err = e.lt.scanAndEnqueue(item.request, g)
			if err != nil {
				e.lm.Release(lg)
				return err
			}
			if !g.startWaiting() {
				break
			}
			e.lm.Release(lg)
			var lastID uuid.UUID
		L:
			for {
				select {
				case <-g.newState():
				case <-ctx.Done():
					return ctx.Err()
				}
				state, err := g.currentState()
				if err != nil {
					_ = e.lt.done(g)
					return err
				}
				switch state.stateKind {
				case doneWaiting:
					if !lastID.Equal(uuid.UUID{}) && item.request.tM != nil {
						_, err = e.waitingFor(item.request.tM.ID, lastID, uuid.UUID{})
						if err != nil {
							_ = e.lt.done(g)
							return err
						}
					}
					break L
				case waitSelf:
					if item.request.tM == nil {
						_ = e.lt.done(g)
						return errors.Errorf("non-transactional request cannot waitSelf")
					}
				case waitForDistinguished, waitFor, waitElsewhere:
					if item.request.tM != nil {
						var aborted bool
						aborted, err = e.waitingFor(item.request.tM.ID, lastID, state.txn.ID)
						if !aborted {
							lastID = state.txn.ID
						}
						if aborted {
							_ = e.lt.done(g)
							return err
						}
					}
				default:
					return errors.Errorf("unexpected state: %v", state.stateKind)
				}
			}
		}

		// acquire locks.
		for _, k := range item.locksToAcquire {
			err = e.acquireLock(k, g, item.request.tM.ID)
			if err != nil {
				break
			}
		}
		err = firstError(err, e.lt.done(g))
		e.lm.Release(lg)
		return err
	}
	for _, intent := range item.intents {
		if err := e.lt.updateLocks(intent); err != nil {
			return err
		}
	}
	return nil
}

// Contains either a request or the ID of the transactions whose locks should
// be released.
type workloadItem struct {
	// Request to be executed, iff request != nil
	request *testRequest
	// locks to be acquired by the request.
	locksToAcquire []roachpb.Key

	// Non-empty when transaction should release locks.
	finish uuid.UUID
}

// state of a transaction maintained by workloadExecutor, for deadlock
// detection, and deciding when transaction can be finished (when a
// workloadItem has instructed that it be finished and all its ongoing
// requests have finished). Requests can be aborted due to deadlock. For
// workload execution convenience this does not abort the whole transaction,
// but it does mean that the locks acquired by the transaction can be a subset
// of what it was instructed to acquire. The locks acquired are tracked in
// acquiredLocks.
type transactionState struct {
	txn *enginepb.TxnMeta

	// A map of each transaction it depends on and a refcount (the refcount is >
	// 0).
	dependsOn       map[uuid.UUID]int
	ongoingRequests map[*workItem]struct{}
	acquiredLocks   []roachpb.Key
	finish          bool
}

func makeWorkItemFinishTxn(tstate *transactionState) workItem {
	wItem := workItem{}
	for i := range tstate.acquiredLocks {
		intent := &roachpb.Intent{
			Span:   roachpb.Span{Key: tstate.acquiredLocks[i]},
			Txn:    *tstate.txn,
			Status: roachpb.COMMITTED,
		}
		wItem.intents = append(wItem.intents, intent)
	}
	return wItem
}

func makeWorkItemForRequest(wi workloadItem) workItem {
	wItem := workItem{
		request:        wi.request,
		locksToAcquire: wi.locksToAcquire,
	}
	return wItem
}

type workloadExecutor struct {
	lm spanlatch.Manager
	lt lockTable

	// Protects the following fields in transactionState: acquiredLocks and
	// dependsOn, and the transactions map.
	mu                syncutil.Mutex
	items             []workloadItem
	transactions      map[uuid.UUID]*transactionState
	doneWork          chan *workItem
	concurrency       int
	numAborted        int
	numConcViolations int
}

func newWorkLoadExecutor(items []workloadItem, concurrency int) *workloadExecutor {
	return &workloadExecutor{
		lm:           spanlatch.Manager{},
		lt:           newLockTable(1000),
		items:        items,
		transactions: make(map[uuid.UUID]*transactionState),
		doneWork:     make(chan *workItem),
		concurrency:  concurrency,
	}
}

func (e *workloadExecutor) acquireLock(k roachpb.Key, g requestGuard, txnID uuid.UUID) error {
	err := e.lt.acquireLock(k, lock.Exclusive, lock.Unreplicated, g)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	tstate, ok := e.transactions[txnID]
	if !ok {
		return errors.Errorf("testbug: lock acquiring request with txnID %v has no transaction", txnID)
	}
	tstate.acquiredLocks = append(tstate.acquiredLocks, k)
	return nil
}

// Returns true if cycle was found. err != nil => true
func (e *workloadExecutor) findCycle(node uuid.UUID, cycleNode uuid.UUID) (bool, error) {
	if node == cycleNode {
		return true, nil
	}
	tstate, ok := e.transactions[node]
	if !ok {
		return true, errors.Errorf("edge to txn that is not in map")
	}
	for k := range tstate.dependsOn {
		found, err := e.findCycle(k, cycleNode)
		if err != nil || found {
			return true, err
		}
	}
	return false, nil
}

// Returns true if request should abort. err != nil => true
func (e *workloadExecutor) waitingFor(
	waiter uuid.UUID, lastID uuid.UUID, currID uuid.UUID,
) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	tstate, ok := e.transactions[waiter]
	if !ok {
		return true, errors.Errorf("testbug: request calling waitingFor with txnID %v has no transaction", waiter)
	}
	if !lastID.Equal(uuid.UUID{}) {
		refcount := tstate.dependsOn[lastID]
		refcount--
		if refcount > 0 {
			tstate.dependsOn[lastID] = refcount
		} else if refcount == 0 {
			delete(tstate.dependsOn, lastID)
		} else {
			return true, errors.Errorf("testbug: txn %v has a negative refcount %d for dependency on %v", waiter, refcount, lastID)
		}
	}
	if !currID.Equal(uuid.UUID{}) {
		refcount := tstate.dependsOn[currID]
		// Cycle detection to detect if this new edge has introduced any
		// cycle. We know there cannot be a cycle preceding this edge,
		// so any cycle must involve waiter. We do a trivial recursive
		// DFS (does not avoid exploring the same node multiple times).
		if refcount == 0 {
			found, err := e.findCycle(currID, waiter)
			if found {
				return found, err
			}
		}
		refcount++
		tstate.dependsOn[currID] = refcount
	}
	return false, nil
}

// Returns true if it started a goroutine.
func (e *workloadExecutor) tryFinishTxn(
	ctx context.Context, group *errgroup.Group, txnID uuid.UUID, tstate *transactionState,
) bool {
	if !tstate.finish || len(tstate.ongoingRequests) > 0 {
		return false
	}
	if len(tstate.acquiredLocks) > 0 {
		work := makeWorkItemFinishTxn(tstate)
		group.Go(func() error { return doWork(ctx, &work, e) })
		return true
	}
	return false
}

// Execution can be either in strict or non-strict mode. In strict mode
// the executor expects to be able to limit concurrency to the configured
// value L by waiting for the L ongoing requests to finish. It sets a
// deadline for this, and returns with an error if the deadline expires
// (this is just a slightly cleaner error than the test exceeding the
// deadline since it prints out the lock table contents).
//
// When executing in non-strict mode, the concurrency bound is not necessarily
// respected since requests may be waiting for locks to be released. The
// executor waits for a tiny interval when concurrency is >= L and if
// no request completes it starts another request. Just for our curiosity
// these "concurrency violations" are tracked in a counter.
func (e *workloadExecutor) execute(strict bool) error {
	numOutstanding := 0
	i := 0
	group, ctx := errgroup.WithContext(context.TODO())
	timer := time.NewTimer(time.Second)
	timer.Stop()
	var err error
L:
	for i < len(e.items) || numOutstanding > 0 {
		if numOutstanding >= e.concurrency || (i == len(e.items) && numOutstanding > 0) {
			strictIter := strict || i == len(e.items)
			if strictIter {
				timer.Reset(30 * time.Second)
			} else {
				timer.Reset(time.Millisecond)
			}
			var w *workItem
			select {
			case w = <-e.doneWork:
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
				if strictIter {
					err = errors.Errorf("timer expired with lock table: %v", e.lt)
					break L
				} else {
					e.numConcViolations++
				}
			}
			if w != nil {
				numOutstanding--
				txnID := w.getRequestTxnID()
				if !txnID.Equal(uuid.UUID{}) {
					tstate, ok := e.transactions[txnID]
					if !ok {
						err = errors.Errorf("testbug: finished request with txnID %v has no transaction", txnID)
						break
					}
					delete(tstate.ongoingRequests, w)
					if e.tryFinishTxn(ctx, group, txnID, tstate) {
						numOutstanding++
						continue
					}
				}
			}
		}
		if i == len(e.items) {
			continue
		}

		wi := e.items[i]
		i++
		if wi.request != nil {
			work := makeWorkItemForRequest(wi)
			if wi.request.tM != nil {
				txnID := wi.request.tM.ID
				_, ok := e.transactions[txnID]
				if !ok {
					// New transaction
					tstate := &transactionState{
						txn:             wi.request.tM,
						dependsOn:       make(map[uuid.UUID]int),
						ongoingRequests: make(map[*workItem]struct{}),
					}
					e.mu.Lock()
					e.transactions[txnID] = tstate
					e.mu.Unlock()
				}
				e.transactions[txnID].ongoingRequests[&work] = struct{}{}
			}
			group.Go(func() error { return doWork(ctx, &work, e) })
			numOutstanding++
			continue
		}
		tstate, ok := e.transactions[wi.finish]
		if !ok {
			err = errors.Errorf("testbug: txn to finish not found: %v", wi.finish)
			break
		}
		tstate.finish = true
		if e.tryFinishTxn(ctx, group, wi.finish, tstate) {
			numOutstanding++
		}
	}
	err2 := group.Wait()
	if err2 != nil {
		err = err2
	}
	fmt.Printf("items: %d, aborted: %d, concurrency violations: %d, lock table: %v\n",
		len(e.items), e.numAborted, e.numConcViolations, e.lt)
	return err
}

// Randomized test with each transaction having a single request that does not
// acquire locks. Note that this ensures there will be no deadlocks. And the
// test executor can run in strict concurrency mode (see comment in execute()).
func TestLockTableConcurrentSingleRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()

	txnCounter := uint128.FromInts(0, 0)
	var timestamps []hlc.Timestamp
	for i := 0; i < 10; i++ {
		timestamps = append(timestamps, hlc.Timestamp{WallTime: int64(i + 1)})
	}
	var keys []roachpb.Key
	for i := 0; i < 10; i++ {
		keys = append(keys, roachpb.Key(string('a'+i)))
	}
	rng := rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano())))

	const numKeys = 2
	var items []workloadItem
	var startedTxnIDs []uuid.UUID // inefficient queue, but ok for a test.
	const maxStartedTxns = 10
	const numRequests = 5000
	for i := 0; i < numRequests; i++ {
		ts := timestamps[rng.Intn(len(timestamps))]
		keysPerm := rng.Perm(len(keys))
		spans := &spanset.SpanSet{}
		onlyReads := true
		for i := 0; i < numKeys; i++ {
			span := roachpb.Span{Key: keys[keysPerm[i]]}
			acc := spanset.SpanAccess(rng.Intn(int(spanset.NumSpanAccess)))
			spans.AddMVCC(acc, span, ts)
			if acc != spanset.SpanReadOnly {
				onlyReads = false
			}
		}
		var txnMeta *enginepb.TxnMeta
		if !onlyReads || rng.Intn(2) == 0 {
			txnMeta = &enginepb.TxnMeta{
				ID:             nextUUID(&txnCounter),
				WriteTimestamp: ts,
			}
		}
		request := &testRequest{
			tM: txnMeta,
			s:  spans,
			t:  ts,
		}
		items = append(items, workloadItem{request: request})
		if txnMeta != nil {
			startedTxnIDs = append(startedTxnIDs, txnMeta.ID)
		}
		randomMaxStartedTxns := rng.Intn(maxStartedTxns)
		for len(startedTxnIDs) > randomMaxStartedTxns {
			items = append(items, workloadItem{finish: startedTxnIDs[0]})
			startedTxnIDs = startedTxnIDs[1:]
		}
	}
	for len(startedTxnIDs) > 0 {
		items = append(items, workloadItem{finish: startedTxnIDs[0]})
		startedTxnIDs = startedTxnIDs[1:]
	}
	concurrency := []int{2, 8, 16, 32}
	for _, c := range concurrency {
		t.Run(fmt.Sprintf("concurrency %d", c), func(t *testing.T) {
			exec := newWorkLoadExecutor(items, c)
			if err := exec.execute(true); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// General randomized test.
func TestLockTableConcurrentRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(sbhola): different test cases with different settings of the
	// randomization parameters.
	txnCounter := uint128.FromInts(0, 0)
	var timestamps []hlc.Timestamp
	for i := 0; i < 2; i++ {
		timestamps = append(timestamps, hlc.Timestamp{WallTime: int64(i + 1)})
	}
	var keys []roachpb.Key
	for i := 0; i < 10; i++ {
		keys = append(keys, roachpb.Key(string('a'+i)))
	}
	rng := rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano())))
	const numActiveTxns = 8
	var activeTxns [numActiveTxns]*enginepb.TxnMeta
	var items []workloadItem
	const numRequests = 5000
	for i := 0; i < numRequests; i++ {
		var txnMeta *enginepb.TxnMeta
		var ts hlc.Timestamp
		if rng.Intn(10) != 0 {
			txnIndex := rng.Intn(len(activeTxns))
			newTxn := rng.Intn(4) != 0 || activeTxns[txnIndex] == nil
			if newTxn {
				ts = timestamps[rng.Intn(len(timestamps))]
				txnMeta = &enginepb.TxnMeta{
					ID:             nextUUID(&txnCounter),
					WriteTimestamp: ts,
				}
				oldTxn := activeTxns[txnIndex]
				if oldTxn != nil {
					items = append(items, workloadItem{finish: oldTxn.ID})
				}
				activeTxns[txnIndex] = txnMeta
			} else {
				txnMeta = activeTxns[txnIndex]
				ts = txnMeta.WriteTimestamp
			}
		} else {
			ts = timestamps[rng.Intn(len(timestamps))]
		}
		keysPerm := rng.Perm(len(keys))
		spans := &spanset.SpanSet{}
		onlyReads := txnMeta == nil
		numKeys := rng.Intn(len(keys)-1) + 1
		request := &testRequest{
			tM: txnMeta,
			s:  spans,
			t:  ts,
		}
		wi := workloadItem{request: request}
		for i := 0; i < numKeys; i++ {
			span := roachpb.Span{Key: keys[keysPerm[i]]}
			acc := spanset.SpanReadOnly
			if !onlyReads {
				acc = spanset.SpanAccess(rng.Intn(int(spanset.NumSpanAccess)))
				if acc == spanset.SpanReadWrite && rng.Intn(2) == 0 {
					wi.locksToAcquire = append(wi.locksToAcquire, span.Key)
				}
			}
			spans.AddMVCC(acc, span, ts)
		}
		items = append(items, wi)
	}
	for i := range activeTxns {
		if txnMeta := activeTxns[i]; txnMeta != nil {
			items = append(items, workloadItem{finish: txnMeta.ID})
		}
	}
	concurrency := []int{2, 8, 16, 32}
	for _, c := range concurrency {
		t.Run(fmt.Sprintf("concurrency %d", c), func(t *testing.T) {
			exec := newWorkLoadExecutor(items, c)
			if err := exec.execute(false); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TODO(sbhola):
// - More datadriven and randomized test cases:
//   - both local and global keys
//   - repeated lock acquisition at same seqnums, different seqnums, epoch changes
//   - updates with ignored seqs
//   - error paths
// - Test with concurrency in lockTable calls.
//   - test for race in gc'ing lock that has since become non-empty or new
//     non-empty one has been inserted.
// - Benchmark.
