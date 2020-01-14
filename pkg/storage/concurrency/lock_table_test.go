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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"strconv"
	"strings"
	"testing"
)

/*
Test needs to handle caller constraints wrt latches being held. The datadriven test uses the
following format:

txn txn=<name> ts=<int>[,<int>] epoch=<int>
----

Creates a TxnMeta.

request r=<name> txn=<name> ts=<int>[,<int>] spans=r|w@<start>[,<end>]+...
----

 For creating a request.

scan r=<name>
----
<error string>
 If the request has an existing guard, use it. If a guard is returned, store it for later use.

acquire r=<name> k=<key> durability=r|u
----
<error string>
 Use the existing guard for the request name.

release txn=<name> span=<start>[,<end>]
----
<error string>

update txn=<name> ts=<int>[,<int>] epoch=<int> span=<start>[,<end>]
----
<error string>

add-discovered r=<name> access=r|w k=<key> txn=<name>
----
<error string>

done r=<name>
----
<error string>

guard-new-state r=<name>
----

guard-start-waiting r=<name>
----
<bool>

guard-state r=<name>
-----
state=<state> [txn=<name> ts=<ts>]
*/

func getTimestamp(t *testing.T, d *datadriven.TestData) hlc.Timestamp {
	var ts hlc.Timestamp
	var tsS string
	d.ScanArgs(t,"ts", &tsS)
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

func getUUID(counter *uint128.Uint128) uuid.UUID {
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

func getSpans(t *testing.T, d *datadriven.TestData, ts hlc.Timestamp) *spanset.SpanSet {
	spans := &spanset.SpanSet{}
	var spansStr string
	d.ScanArgs(t, "spans", &spansStr)
	parts := strings.Split(spansStr, "+")
	for _, p := range parts {
		if len(p) < 2 || p[1] != '@' {
			d.Fatalf(t, "incorrect span with access format: %s", p)
		}
		c := p[0]
		p := p[2:]
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

func getSpanAndScope(t *testing.T, d *datadriven.TestData) (roachpb.Span, spanset.SpanScope){
	var s string
	d.ScanArgs(t, "span", &s)
	span := getSpan(t, d, s)
	ss := spanset.SpanGlobal
	if keys.IsLocal(span.Key) {
		ss = spanset.SpanLocal
	}
	return span, ss
}

type testRequest struct {
	tM *enginepb.TxnMeta
	s *spanset.SpanSet
	t hlc.Timestamp
}
var _ Request = &testRequest{}
func (r *testRequest) txnMeta() *enginepb.TxnMeta { return r.tM }
func (r *testRequest) spans() *spanset.SpanSet { return r.s }
func (r *testRequest) ts() hlc.Timestamp { return r.t }

func TestLockTableBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	lt := newLockTable(1000)
	txnsByName := make(map[string]*enginepb.TxnMeta)
	txnCounter := uint128.FromInts(0, 1)
	requestsByName := make(map[string]*testRequest)
	guardsByReqName := make(map[string]requestGuard)
	datadriven.RunTest(t, "testdata/lock_table", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "txn":
			var txnName string
			d.ScanArgs(t, "txn", &txnName)
			ts := getTimestamp(t, d)
			var epoch int
			d.ScanArgs(t, "epoch", &epoch)
			if txnMeta, ok := txnsByName[txnName]; ok {
				txnMeta = &enginepb.TxnMeta{ID: txnMeta.ID}
				txnMeta.Epoch = enginepb.TxnEpoch(epoch)
				txnMeta.WriteTimestamp = ts
				txnsByName[txnName] = txnMeta
			} else {
				txnMeta = &enginepb.TxnMeta{
					ID:             getUUID(&txnCounter),
					Epoch:          enginepb.TxnEpoch(epoch),
					WriteTimestamp: ts,
				}
				txnsByName[txnName] = txnMeta
			}
			return ""

		case "request":
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
			ts := getTimestamp(t, d)
			spans := getSpans(t, d, ts)
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
			return ""

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
		  durability := Unreplicated
		  if s[0] == 'r' {
				durability = Replicated
			}
			if err := lt.acquireLock(roachpb.Key(key), Exclusive, durability, g); err != nil {
				return err.Error()
			}
			return ""

		case "release":
			var txnName string
			d.ScanArgs(t, "txn", &txnName)
			txnMeta, ok := txnsByName[txnName]
			if !ok {
				d.Fatalf(t, "unknown txn %s", txnName)
			}
			span, ss := getSpanAndScope(t, d)
			if err := lt.releaseLocks(&txnMeta.ID, span, ss); err != nil {
				return err.Error()
			}
			return ""

		case "update":
			var txnName string
			d.ScanArgs(t, "txn", &txnName)
			txnMeta, ok := txnsByName[txnName]
			if !ok {
				d.Fatalf(t, "unknown txn %s", txnName)
			}
			ts := getTimestamp(t, d)
			var epoch int
			d.ScanArgs(t, "epoch", &epoch)
			txnMeta = &enginepb.TxnMeta{ID: txnMeta.ID}
			txnMeta.Epoch = enginepb.TxnEpoch(epoch)
			txnMeta.WriteTimestamp = ts
			txnsByName[txnName] = txnMeta
			span, ss := getSpanAndScope(t, d)
			if err := lt.updateLocks(txnMeta, ts, span, ss); err != nil {
				return err.Error()
			}
			return ""

		case "add-discovered":
			var reqName string
			d.ScanArgs(t, "r", &reqName)
			g := guardsByReqName[reqName]
			if g == nil {
				d.Fatalf(t, "unknown guard: %s", reqName)
			}
			var s string
			d.ScanArgs(t, "access", &s)
			if len(s) != 1 || (s[0] != 'r' && s[0] != 'w') {
				d.Fatalf(t, "incorrect access: %s", s)
			}
			sa := spanset.SpanReadOnly
			if s[0] == 'w' {
				sa = spanset.SpanReadWrite
			}
			var key string
			d.ScanArgs(t, "k", &key)
			var txnName string
			d.ScanArgs(t, "txn", &txnName)
			txnMeta, ok := txnsByName[txnName]
			if !ok {
				d.Fatalf(t, "unknown txn %s", txnName)
			}
			if err := lt.addDiscoveredLock(roachpb.Key(key), txnMeta, txnMeta.WriteTimestamp, g, sa); err != nil {
				return err.Error()
			}
			return ""

		case "done":
			var reqName string
			d.ScanArgs(t, "r", &reqName)
			g := guardsByReqName[reqName]
			if g == nil {
				d.Fatalf(t, "unknown guard: %s", reqName)
			}
			var str string
			if err := lt.done(g); err != nil {
				str = err.Error()
			}
			delete(guardsByReqName, reqName)
			delete(requestsByName, reqName)
			return str

		case "guard-new-state":
			var reqName string
			d.ScanArgs(t, "r", &reqName)
			g := guardsByReqName[reqName]
			if g == nil {
				d.Fatalf(t, "unknown guard: %s", reqName)
			}
			select {
			case <-g.newState():
				return ""
			default:
				return "no new state"
			}

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
			state := g.currentState()
			if state.stateType == doneWaiting {
				return "state=doneWaiting"
			}
			var typeStr string
			switch state.stateType {
			case waitForDistinguished: typeStr = "waitForDistinguished"
			case waitFor: typeStr = "waitFor"
			case waitElsewhere: typeStr = "waitElsewhere"
			}
			id := state.txn.ID
			var txn string
			for k, v := range txnsByName {
				if v.ID.Equal(id) {
					txn = k
					break
				}
			}
			if txn == "" {
				txn = fmt.Sprintf("unknown txn with ID: %v", state.txn.ID)
			}
			tsS := fmt.Sprintf("%d", state.ts.WallTime)
			if state.ts.Logical != 0 {
				tsS += fmt.Sprintf(",%d", state.ts.Logical)
			}
			return fmt.Sprintf("state=%s txn=%s ts=%s", typeStr, txn, tsS)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

// TODO(sbhola):
// - Benchmark.
// - Randomized test.
