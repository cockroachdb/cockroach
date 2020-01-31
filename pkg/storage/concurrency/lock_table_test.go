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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
)

/*
Test needs to handle caller constraints wrt latches being held. The datadriven test uses the
following format:

txn txn=<name> ts=<int>[,<int>] epoch=<int>
----

 Creates a TxnMeta.

request r=<name> txn=<name> ts=<int>[,<int>] spans=r|w@<start>[,<end>]+...
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
			if !ok {
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

// TODO(sbhola):
// - More datadriven test cases:
//   - both local and global keys
//   - repeated lock acquisition at same seqnums, different seqnums, epoch changes
//   - updates with ignored seqs
//   - error paths
// - Test with concurrency in lockTable calls.
//   - general concurrency testing
//   - test for race in gc'ing lock that has since become non-empty or new
//     non-empty one has been inserted.
// - Benchmark.
// - Randomized test.
