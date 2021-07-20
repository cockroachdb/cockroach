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
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
)

/*
Test needs to handle caller constraints wrt latches being held. The datadriven
test uses the following format:

new-lock-table maxlocks=<int>
----

  Creates a lockTable. The lockTable is initially enabled.

new-txn txn=<name> ts=<int>[,<int>] epoch=<int> [seq=<int>]
----

 Creates a TxnMeta.

new-request r=<name> txn=<name>|none ts=<int>[,<int>] spans=r|w@<start>[,<end>]+...
----

 Creates a Request.

scan r=<name>
----
start-waiting: <bool>

 Calls lockTable.ScanAndEnqueue. If the request has an existing guard, uses it.
 If a guard is returned, stores it for later use.

scan-opt r=<name>
----
start-waiting: <bool>

 Calls lockTable.ScanOptimistic. The request must not have an existing guard.
 If a guard is returned, stores it for later use.

acquire r=<name> k=<key> durability=r|u
----
<error string>

 Acquires lock for the request, using the existing guard for that request.

release txn=<name> span=<start>[,<end>]
----
<error string>

 Releases locks for the named transaction.

update txn=<name> ts=<int>[,<int>] epoch=<int> span=<start>[,<end>] [ignored-seqs=<int>[-<int>][,<int>[-<int>]]]
----
<error string>

 Updates locks for the named transaction.

txn-finalized txn=<name> status=committed|aborted
----

 Informs the lock table that the named transaction is finalized.

add-discovered r=<name> k=<key> txn=<name> [lease-seq=<seq>] [consult-finalized-txn-cache=<bool>]
----
<error string>

 Adds a discovered lock that is discovered by the named request.

check-opt-no-conflicts r=<name> spans=r|w@<start>[,<end>]+...
----
no-conflicts: <bool>

 Checks whether the request, which previously called ScanOptimistic, has no lock conflicts.

dequeue r=<name>
----
<error string>

 Calls lockTable.Dequeue for the named request. The request and guard are
 discarded after this.

guard-state r=<name>
----
new|old: state=<state> [txn=<name> ts=<ts>]

  Calls lockTableGuard.NewStateChan in a non-blocking manner, followed by
  CurState.

should-wait r=<name>
----
<bool>

 Calls lockTableGuard.ShouldWait.

resolve-before-scanning r=<name>
----
<intents to resolve>

enable [lease-seq=<seq>]
----

 Calls lockTable.Enable.

clear [disable]
----
<state of lock table>

 Calls lockTable.Clear. Optionally disables the lockTable.

print
----
<state of lock table>

 Calls lockTable.String.
*/

func TestLockTableBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, "testdata/lock_table", func(t *testing.T, path string) {
		var lt lockTable
		var txnsByName map[string]*enginepb.TxnMeta
		var txnCounter uint128.Uint128
		var requestsByName map[string]Request
		var guardsByReqName map[string]lockTableGuard
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "new-lock-table":
				var maxLocks int
				d.ScanArgs(t, "maxlocks", &maxLocks)
				ltImpl := newLockTable(int64(maxLocks))
				ltImpl.enabled = true
				ltImpl.enabledSeq = 1
				ltImpl.minLocks = 0
				lt = ltImpl
				txnsByName = make(map[string]*enginepb.TxnMeta)
				txnCounter = uint128.FromInts(0, 0)
				requestsByName = make(map[string]Request)
				guardsByReqName = make(map[string]lockTableGuard)
				return ""

			case "new-txn":
				// UUIDs for transactions are numbered from 1 by this test code and
				// lockTableImpl.String() knows about UUIDs and not transaction names.
				// Assigning txnNames of the form txn1, txn2, ... keeps the two in sync,
				// which makes test cases easier to understand.
				var txnName string
				d.ScanArgs(t, "txn", &txnName)
				ts := scanTimestamp(t, d)
				var epoch int
				d.ScanArgs(t, "epoch", &epoch)
				var seq int
				if d.HasArg("seq") {
					d.ScanArgs(t, "seq", &seq)
				}
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
					Sequence:       enginepb.TxnSeq(seq),
					WriteTimestamp: ts,
				}
				return ""

			case "txn-finalized":
				var txnName string
				d.ScanArgs(t, "txn", &txnName)
				txnMeta, ok := txnsByName[txnName]
				if !ok {
					return fmt.Sprintf("txn %s not found", txnName)
				}
				txn := &roachpb.Transaction{
					TxnMeta: *txnMeta,
				}
				var statusStr string
				d.ScanArgs(t, "status", &statusStr)
				switch statusStr {
				case "committed":
					txn.Status = roachpb.COMMITTED
				case "aborted":
					txn.Status = roachpb.ABORTED
				default:
					return fmt.Sprintf("unknown txn status %s", statusStr)
				}
				lt.TransactionIsFinalized(txn)
				return ""

			case "new-request":
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
				req := Request{
					Timestamp:  ts,
					LatchSpans: spans,
					LockSpans:  spans,
				}
				if txnMeta != nil {
					// Update the transaction's timestamp, if necessary. The transaction
					// may have needed to move its timestamp for any number of reasons.
					txnMeta.WriteTimestamp = ts
					req.Txn = &roachpb.Transaction{
						TxnMeta:       *txnMeta,
						ReadTimestamp: ts,
					}
				}
				requestsByName[reqName] = req
				return ""

			case "scan":
				var reqName string
				d.ScanArgs(t, "r", &reqName)
				req, ok := requestsByName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}
				g := guardsByReqName[reqName]
				g = lt.ScanAndEnqueue(req, g)
				guardsByReqName[reqName] = g
				return fmt.Sprintf("start-waiting: %t", g.ShouldWait())

			case "scan-opt":
				var reqName string
				d.ScanArgs(t, "r", &reqName)
				req, ok := requestsByName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}
				_, ok = guardsByReqName[reqName]
				if ok {
					d.Fatalf(t, "request has an existing guard: %s", reqName)
				}
				g := lt.ScanOptimistic(req)
				guardsByReqName[reqName] = g
				return fmt.Sprintf("start-waiting: %t", g.ShouldWait())

			case "acquire":
				var reqName string
				d.ScanArgs(t, "r", &reqName)
				req, ok := requestsByName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
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
				if err := lt.AcquireLock(&req.Txn.TxnMeta, roachpb.Key(key), lock.Exclusive, durability); err != nil {
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
				intent := &roachpb.LockUpdate{Span: span, Txn: *txnMeta, Status: roachpb.COMMITTED}
				if err := lt.UpdateLocks(intent); err != nil {
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
				txnMeta = &enginepb.TxnMeta{ID: txnMeta.ID, Sequence: txnMeta.Sequence}
				txnMeta.Epoch = enginepb.TxnEpoch(epoch)
				txnMeta.WriteTimestamp = ts
				txnsByName[txnName] = txnMeta
				var s string
				d.ScanArgs(t, "span", &s)
				span := getSpan(t, d, s)
				var ignored []enginepb.IgnoredSeqNumRange
				if d.HasArg("ignored-seqs") {
					var seqsStr string
					d.ScanArgs(t, "ignored-seqs", &seqsStr)
					parts := strings.Split(seqsStr, ",")
					for _, p := range parts {
						pair := strings.Split(p, "-")
						if len(pair) != 1 && len(pair) != 2 {
							d.Fatalf(t, "error parsing %s", parts)
						}
						startNum, err := strconv.ParseInt(pair[0], 10, 32)
						if err != nil {
							d.Fatalf(t, "error parsing ignored seqnums: %s", err)
						}
						ignoredRange := enginepb.IgnoredSeqNumRange{
							Start: enginepb.TxnSeq(startNum), End: enginepb.TxnSeq(startNum)}
						if len(pair) == 2 {
							endNum, err := strconv.ParseInt(pair[1], 10, 32)
							if err != nil {
								d.Fatalf(t, "error parsing ignored seqnums: %s", err)
							}
							ignoredRange.End = enginepb.TxnSeq(endNum)
						}
						ignored = append(ignored, ignoredRange)
					}
				}
				// TODO(sbhola): also test STAGING.
				intent := &roachpb.LockUpdate{
					Span: span, Txn: *txnMeta, Status: roachpb.PENDING, IgnoredSeqNums: ignored}
				if err := lt.UpdateLocks(intent); err != nil {
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
				intent := roachpb.MakeIntent(txnMeta, roachpb.Key(key))
				seq := int(1)
				if d.HasArg("lease-seq") {
					d.ScanArgs(t, "lease-seq", &seq)
				}
				consultFinalizedTxnCache := false
				if d.HasArg("consult-finalized-txn-cache") {
					d.ScanArgs(t, "consult-finalized-txn-cache", &consultFinalizedTxnCache)
				}
				leaseSeq := roachpb.LeaseSequence(seq)
				if _, err := lt.AddDiscoveredLock(
					&intent, leaseSeq, consultFinalizedTxnCache, g); err != nil {
					return err.Error()
				}
				return lt.(*lockTableImpl).String()

			case "check-opt-no-conflicts":
				var reqName string
				d.ScanArgs(t, "r", &reqName)
				req, ok := requestsByName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}
				g := guardsByReqName[reqName]
				if g == nil {
					d.Fatalf(t, "unknown guard: %s", reqName)
				}
				spans := scanSpans(t, d, req.Timestamp)
				return fmt.Sprintf("no-conflicts: %t", g.CheckOptimisticNoConflicts(spans))

			case "dequeue":
				var reqName string
				d.ScanArgs(t, "r", &reqName)
				g := guardsByReqName[reqName]
				if g == nil {
					d.Fatalf(t, "unknown guard: %s", reqName)
				}
				lt.Dequeue(g)
				delete(guardsByReqName, reqName)
				delete(requestsByName, reqName)
				return lt.(*lockTableImpl).String()

			case "should-wait":
				var reqName string
				d.ScanArgs(t, "r", &reqName)
				g := guardsByReqName[reqName]
				if g == nil {
					d.Fatalf(t, "unknown guard: %s", reqName)
				}
				return fmt.Sprintf("%t", g.ShouldWait())

			case "guard-state":
				var reqName string
				d.ScanArgs(t, "r", &reqName)
				g := guardsByReqName[reqName]
				if g == nil {
					d.Fatalf(t, "unknown guard: %s", reqName)
				}
				var str string
				stateTransition := false
				select {
				case <-g.NewStateChan():
					str = "new: "
					stateTransition = true
				default:
					str = "old: "
				}
				state := g.CurState()
				var typeStr string
				switch state.kind {
				case waitForDistinguished:
					typeStr = "waitForDistinguished"
				case waitFor:
					typeStr = "waitFor"
				case waitElsewhere:
					typeStr = "waitElsewhere"
				case waitSelf:
					return str + "state=waitSelf"
				case doneWaiting:
					var toResolveStr string
					if stateTransition {
						toResolveStr = intentsToResolveToStr(g.ResolveBeforeScanning(), true)
					}
					return str + "state=doneWaiting" + toResolveStr
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
				return fmt.Sprintf("%sstate=%s txn=%s key=%s held=%t guard-access=%s",
					str, typeStr, txnS, state.key, state.held, state.guardAccess)

			case "resolve-before-scanning":
				var reqName string
				d.ScanArgs(t, "r", &reqName)
				g := guardsByReqName[reqName]
				if g == nil {
					d.Fatalf(t, "unknown guard: %s", reqName)
				}
				return intentsToResolveToStr(g.ResolveBeforeScanning(), false)

			case "enable":
				seq := int(1)
				if d.HasArg("lease-seq") {
					d.ScanArgs(t, "lease-seq", &seq)
				}
				lt.Enable(roachpb.LeaseSequence(seq))
				return ""

			case "clear":
				lt.Clear(d.HasArg("disable"))
				return lt.(*lockTableImpl).String()

			case "print":
				return lt.(*lockTableImpl).String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

func nextUUID(counter *uint128.Uint128) uuid.UUID {
	*counter = counter.Add(1)
	return uuid.FromUint128(*counter)
}

func scanTimestamp(t *testing.T, d *datadriven.TestData) hlc.Timestamp {
	var tsS string
	d.ScanArgs(t, "ts", &tsS)
	ts, err := hlc.ParseTimestamp(tsS)
	if err != nil {
		d.Fatalf(t, "%v", err)
	}
	return ts
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

func intentsToResolveToStr(toResolve []roachpb.LockUpdate, startOnNewLine bool) string {
	if len(toResolve) == 0 {
		return ""
	}
	var buf strings.Builder
	if startOnNewLine {
		fmt.Fprintf(&buf, "\n")
	}
	fmt.Fprintf(&buf, "Intents to resolve:")
	for i := range toResolve {
		fmt.Fprintf(&buf, "\n key=%s txn=%s status=%s", toResolve[i].Key,
			toResolve[i].Txn.ID.Short(), toResolve[i].Status)
	}
	return buf.String()
}

func TestLockTableMaxLocks(t *testing.T) {
	lt := newLockTable(5)
	lt.minLocks = 0
	lt.enabled = true
	var keys []roachpb.Key
	var guards []lockTableGuard
	var reqs []Request
	// 10 requests, each with 10 discovered locks. Only 1 will be considered
	// notRemovable per request.
	for i := 0; i < 10; i++ {
		spans := &spanset.SpanSet{}
		for j := 0; j < 20; j++ {
			k := roachpb.Key(fmt.Sprintf("%08d", i*20+j))
			keys = append(keys, k)
			spans.AddMVCC(spanset.SpanReadWrite, roachpb.Span{Key: k}, hlc.Timestamp{WallTime: 1})
		}
		req := Request{
			Timestamp:  hlc.Timestamp{WallTime: 1},
			LatchSpans: spans,
			LockSpans:  spans,
		}
		reqs = append(reqs, req)
		ltg := lt.ScanAndEnqueue(req, nil)
		require.Nil(t, ltg.ResolveBeforeScanning())
		require.False(t, ltg.ShouldWait())
		guards = append(guards, ltg)
	}
	for i := range guards {
		for j := 0; j < 10; j++ {
			k := i*20 + j
			added, err := lt.AddDiscoveredLock(
				&roachpb.Intent{Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[k]}},
				0, false, guards[i])
			require.True(t, added)
			require.NoError(t, err)
		}
	}
	// Only the notRemovable locks survive after addition.
	require.Equal(t, int64(10), lt.lockCountForTesting())
	// Two guards are dequeued.
	lt.Dequeue(guards[0])
	lt.Dequeue(guards[1])
	require.Equal(t, int64(10), lt.lockCountForTesting())
	// Two guards do ScanAndEnqueue.
	for i := 2; i < 4; i++ {
		guards[i] = lt.ScanAndEnqueue(reqs[i], guards[i])
		require.True(t, guards[i].ShouldWait())
	}
	require.Equal(t, int64(10), lt.lockCountForTesting())
	// Add another discovered lock, to trigger tryClearLocks.
	added, err := lt.AddDiscoveredLock(
		&roachpb.Intent{Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+10]}},
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// The 6 notRemovable locks remain.
	require.Equal(t, int64(6), lt.lockCountForTesting())
	require.Equal(t, int64(101), int64(lt.locks[spanset.SpanGlobal].lockIDSeqNum))
	// Add another discovered lock, to trigger tryClearLocks.
	added, err = lt.AddDiscoveredLock(
		&roachpb.Intent{Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+11]}},
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// Still the 6 notRemovable locks remain.
	require.Equal(t, int64(6), lt.lockCountForTesting())
	require.Equal(t, int64(102), int64(lt.locks[spanset.SpanGlobal].lockIDSeqNum))
	// Two more guards are dequeued, so we are down to 4 notRemovable locks.
	lt.Dequeue(guards[4])
	lt.Dequeue(guards[5])
	// Bump up the enforcement interval manually.
	lt.locks[spanset.SpanGlobal].lockAddMaxLocksCheckInterval = 2
	// Add another discovered lock.
	added, err = lt.AddDiscoveredLock(
		&roachpb.Intent{Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+12]}},
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// This notRemovable=false lock is also added, since enforcement not done.
	require.Equal(t, int64(7), lt.lockCountForTesting())
	// Add another discovered lock, to trigger tryClearLocks.
	added, err = lt.AddDiscoveredLock(
		&roachpb.Intent{Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+13]}},
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// Now enforcement is done, so only 4 remain.
	require.Equal(t, int64(4), lt.lockCountForTesting())
	// Bump down the enforcement interval manually, and bump up minLocks
	lt.locks[spanset.SpanGlobal].lockAddMaxLocksCheckInterval = 1
	lt.minLocks = 2
	// Three more guards dequeued.
	lt.Dequeue(guards[6])
	lt.Dequeue(guards[7])
	lt.Dequeue(guards[8])
	// Add another discovered lock.
	added, err = lt.AddDiscoveredLock(
		&roachpb.Intent{Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+14]}},
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	require.Equal(t, int64(5), lt.lockCountForTesting())
	// Add another discovered lock, to trigger tryClearLocks, and push us over 5
	// locks.
	added, err = lt.AddDiscoveredLock(
		&roachpb.Intent{Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+15]}},
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// Enforcement keeps the 1 notRemovable lock, and another, since minLocks=2.
	require.Equal(t, int64(2), lt.lockCountForTesting())
	// Restore minLocks to 0.
	lt.minLocks = 0
	// Add locks to push us over 5 locks.
	for i := 16; i < 20; i++ {
		added, err = lt.AddDiscoveredLock(
			&roachpb.Intent{Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+i]}},
			0, false, guards[9])
		require.True(t, added)
		require.NoError(t, err)
	}
	// Only the 1 notRemovable lock remains.
	require.Equal(t, int64(1), lt.lockCountForTesting())
}

// TestLockTableMaxLocksWithMultipleNotRemovableRefs tests the notRemovable
// ref counting.
func TestLockTableMaxLocksWithMultipleNotRemovableRefs(t *testing.T) {
	lt := newLockTable(2)
	lt.minLocks = 0
	lt.enabled = true
	var keys []roachpb.Key
	var guards []lockTableGuard
	// 10 requests. Every pair of requests have the same span.
	for i := 0; i < 10; i++ {
		spans := &spanset.SpanSet{}
		key := roachpb.Key(fmt.Sprintf("%08d", i/2))
		if i%2 == 0 {
			keys = append(keys, key)
		}
		spans.AddMVCC(spanset.SpanReadWrite, roachpb.Span{Key: key}, hlc.Timestamp{WallTime: 1})
		req := Request{
			Timestamp:  hlc.Timestamp{WallTime: 1},
			LatchSpans: spans,
			LockSpans:  spans,
		}
		ltg := lt.ScanAndEnqueue(req, nil)
		require.Nil(t, ltg.ResolveBeforeScanning())
		require.False(t, ltg.ShouldWait())
		guards = append(guards, ltg)
	}
	// The first 6 requests discover 3 locks total.
	for i := 0; i < 6; i++ {
		added, err := lt.AddDiscoveredLock(
			&roachpb.Intent{Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[i/2]}},
			0, false, guards[i])
		require.True(t, added)
		require.NoError(t, err)
	}
	// All the 3 locks are there.
	require.Equal(t, int64(3), lt.lockCountForTesting())
	// Remove one of the notRemovable refs from each lock.
	for i := 0; i < 6; i++ {
		if i%2 == 0 {
			lt.Dequeue(guards[i])
		}
	}
	// Add another lock using request 6.
	added, err := lt.AddDiscoveredLock(
		&roachpb.Intent{Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[6/2]}},
		0, false, guards[6])
	require.True(t, added)
	require.NoError(t, err)
	// There are 4 locks.
	require.Equal(t, int64(4), lt.lockCountForTesting())
	// Remove the remaining notRemovable refs.
	for i := 0; i < 6; i++ {
		if i%2 == 1 {
			lt.Dequeue(guards[i])
		}
	}
	lt.Dequeue(guards[6])
	// There are still 4 locks since tryClearLocks has not happened since the
	// ref counts went to 0.
	require.Equal(t, int64(4), lt.lockCountForTesting())
	// Add another lock using request 8.
	added, err = lt.AddDiscoveredLock(
		&roachpb.Intent{Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[8/2]}},
		0, false, guards[8])
	require.True(t, added)
	require.NoError(t, err)
	// There is only 1 lock.
	require.Equal(t, int64(1), lt.lockCountForTesting())
}

type workItem struct {
	// Contains one of request or intents.

	// Request.
	request        *Request
	locksToAcquire []roachpb.Key

	// Update locks.
	intents []roachpb.LockUpdate
}

func (w *workItem) getRequestTxnID() uuid.UUID {
	if w.request != nil && w.request.Txn != nil {
		return w.request.Txn.ID
	}
	return uuid.UUID{}
}

func doWork(ctx context.Context, item *workItem, e *workloadExecutor) error {
	defer func() {
		e.doneWork <- item
	}()
	if item.request != nil {
		var lg *spanlatch.Guard
		var g lockTableGuard
		var err error
		for {
			// Since we can't do a select involving latch acquisition and context
			// cancellation, the code makes sure to release latches when returning
			// early due to error. Otherwise other requests will get stuck and
			// group.Wait() will not return until the test times out.
			lg, err = e.lm.Acquire(context.Background(), item.request.LatchSpans)
			if err != nil {
				return err
			}
			g = e.lt.ScanAndEnqueue(*item.request, g)
			if !g.ShouldWait() {
				break
			}
			e.lm.Release(lg)
			var lastID uuid.UUID
		L:
			for {
				select {
				case <-g.NewStateChan():
				case <-ctx.Done():
					return ctx.Err()
				}
				state := g.CurState()
				switch state.kind {
				case doneWaiting:
					if !lastID.Equal(uuid.UUID{}) && item.request.Txn != nil {
						_, err = e.waitingFor(item.request.Txn.ID, lastID, uuid.UUID{})
						if err != nil {
							e.lt.Dequeue(g)
							return err
						}
					}
					break L
				case waitSelf:
					if item.request.Txn == nil {
						e.lt.Dequeue(g)
						return errors.Errorf("non-transactional request cannot waitSelf")
					}
				case waitForDistinguished, waitFor, waitElsewhere:
					if item.request.Txn != nil {
						var aborted bool
						aborted, err = e.waitingFor(item.request.Txn.ID, lastID, state.txn.ID)
						if !aborted {
							lastID = state.txn.ID
						}
						if aborted {
							e.lt.Dequeue(g)
							return err
						}
					}
				default:
					return errors.Errorf("unexpected state: %v", state.kind)
				}
			}
		}

		// acquire locks.
		for _, k := range item.locksToAcquire {
			err = e.acquireLock(&item.request.Txn.TxnMeta, k)
			if err != nil {
				break
			}
		}
		e.lt.Dequeue(g)
		e.lm.Release(lg)
		return err
	}
	for i := range item.intents {
		if err := e.lt.UpdateLocks(&item.intents[i]); err != nil {
			return err
		}
	}
	return nil
}

// Contains either a request or the ID of the transactions whose locks should
// be released.
type workloadItem struct {
	// Request to be executed, iff request != nil
	request *Request
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
		wItem.intents = append(wItem.intents, roachpb.LockUpdate{
			Span:   roachpb.Span{Key: tstate.acquiredLocks[i]},
			Txn:    *tstate.txn,
			Status: roachpb.COMMITTED,
		})
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
	const maxLocks = 100000
	lt := newLockTable(maxLocks)
	lt.enabled = true
	return &workloadExecutor{
		lm:           spanlatch.Manager{},
		lt:           lt,
		items:        items,
		transactions: make(map[uuid.UUID]*transactionState),
		doneWork:     make(chan *workItem),
		concurrency:  concurrency,
	}
}

func (e *workloadExecutor) acquireLock(txn *enginepb.TxnMeta, k roachpb.Key) error {
	err := e.lt.AcquireLock(txn, k, lock.Exclusive, lock.Unreplicated)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	tstate, ok := e.transactions[txn.ID]
	if !ok {
		return errors.Errorf("testbug: lock acquiring request with txnID %v has no transaction", txn.ID)
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
// these "concurrency violations" are tracked in a counter. The amount of
// concurrency in this non-strict mode is bounded by maxNonStrictConcurrency.
func (e *workloadExecutor) execute(strict bool, maxNonStrictConcurrency int) error {
	numOutstanding := 0
	i := 0
	group, ctx := errgroup.WithContext(context.Background())
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
				} else if numOutstanding > maxNonStrictConcurrency {
					continue
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
			if wi.request.Txn != nil {
				txnID := wi.request.Txn.ID
				_, ok := e.transactions[txnID]
				if !ok {
					// New transaction
					tstate := &transactionState{
						txn:             &wi.request.Txn.TxnMeta,
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
	defer log.Scope(t).Close(t)

	txnCounter := uint128.FromInts(0, 0)
	var timestamps []hlc.Timestamp
	for i := 0; i < 10; i++ {
		timestamps = append(timestamps, hlc.Timestamp{WallTime: int64(i + 1)})
	}
	var keys []roachpb.Key
	for i := 0; i < 10; i++ {
		keys = append(keys, roachpb.Key(string(rune('a'+i))))
	}
	rng := rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano())))

	const numKeys = 2
	var items []workloadItem
	var startedTxnIDs []uuid.UUID // inefficient queue, but ok for a test.
	const maxStartedTxns = 10
	const numRequests = 10000
	for i := 0; i < numRequests; i++ {
		ts := timestamps[rng.Intn(len(timestamps))]
		keysPerm := rng.Perm(len(keys))
		spans := &spanset.SpanSet{}
		for i := 0; i < numKeys; i++ {
			span := roachpb.Span{Key: keys[keysPerm[i]]}
			acc := spanset.SpanAccess(rng.Intn(int(spanset.NumSpanAccess)))
			spans.AddMVCC(acc, span, ts)
		}
		var txn *roachpb.Transaction
		if rng.Intn(2) == 0 {
			txn = &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					ID:             nextUUID(&txnCounter),
					WriteTimestamp: ts,
				},
				ReadTimestamp: ts,
			}
		}
		request := &Request{
			Txn:        txn,
			Timestamp:  ts,
			LatchSpans: spans,
			LockSpans:  spans,
		}
		items = append(items, workloadItem{request: request})
		if txn != nil {
			startedTxnIDs = append(startedTxnIDs, txn.ID)
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
			if err := exec.execute(true, 0); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// General randomized test.
func TestLockTableConcurrentRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(sbhola): different test cases with different settings of the
	// randomization parameters.
	txnCounter := uint128.FromInts(0, 0)
	var timestamps []hlc.Timestamp
	for i := 0; i < 2; i++ {
		timestamps = append(timestamps, hlc.Timestamp{WallTime: int64(i + 1)})
	}
	var keys []roachpb.Key
	for i := 0; i < 10; i++ {
		keys = append(keys, roachpb.Key(string(rune('a'+i))))
	}
	rng := rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano())))
	const numActiveTxns = 8
	var activeTxns [numActiveTxns]*enginepb.TxnMeta
	var items []workloadItem
	const numRequests = 1000
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
		onlyReads := txnMeta == nil && rng.Intn(2) != 0
		numKeys := rng.Intn(len(keys)-1) + 1
		request := &Request{
			Timestamp:  ts,
			LatchSpans: spans,
			LockSpans:  spans,
		}
		if txnMeta != nil {
			request.Txn = &roachpb.Transaction{
				TxnMeta:       *txnMeta,
				ReadTimestamp: ts,
			}
		}
		wi := workloadItem{request: request}
		for i := 0; i < numKeys; i++ {
			span := roachpb.Span{Key: keys[keysPerm[i]]}
			acc := spanset.SpanReadOnly
			dupRead := false
			if !onlyReads {
				acc = spanset.SpanAccess(rng.Intn(int(spanset.NumSpanAccess)))
				if acc == spanset.SpanReadWrite && txnMeta != nil && rng.Intn(2) == 0 {
					// Acquire lock.
					wi.locksToAcquire = append(wi.locksToAcquire, span.Key)
				}
				if acc == spanset.SpanReadWrite && rng.Intn(2) == 0 {
					// Also include the key as read.
					dupRead = true
				}
			}
			spans.AddMVCC(acc, span, ts)
			if dupRead {
				spans.AddMVCC(spanset.SpanReadOnly, span, ts)
			}
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
			if err := exec.execute(false, 200); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type benchWorkItem struct {
	Request
	locksToAcquire []roachpb.Key
}

type benchEnv struct {
	lm *spanlatch.Manager
	lt lockTable
	// Stats. As expected, the contended benchmarks have most requests
	// encountering a wait, and the number of scan calls is twice the
	// number of requests.
	numRequestsWaited *uint64
	numScanCalls      *uint64
}

// Does the work for one request. Both acquires and releases the locks.
// It drops the latches after acquiring locks and reacquires latches
// before releasing the locks, which will induce contention in the
// lockTable since the requests that were waiting for latches will do
// a call to ScanAndEnqueue before the locks are released.
func doBenchWork(item *benchWorkItem, env benchEnv, doneCh chan<- error) {
	var lg *spanlatch.Guard
	var g lockTableGuard
	var err error
	firstIter := true
	for {
		if lg, err = env.lm.Acquire(context.Background(), item.LatchSpans); err != nil {
			doneCh <- err
			return
		}
		g = env.lt.ScanAndEnqueue(item.Request, g)
		atomic.AddUint64(env.numScanCalls, 1)
		if !g.ShouldWait() {
			break
		}
		if firstIter {
			atomic.AddUint64(env.numRequestsWaited, 1)
			firstIter = false
		}
		env.lm.Release(lg)
		for {
			<-g.NewStateChan()
			state := g.CurState()
			if state.kind == doneWaiting {
				break
			}
		}
	}
	for _, k := range item.locksToAcquire {
		if err = env.lt.AcquireLock(&item.Txn.TxnMeta, k, lock.Exclusive, lock.Unreplicated); err != nil {
			doneCh <- err
			return
		}
	}
	env.lt.Dequeue(g)
	env.lm.Release(lg)
	if len(item.locksToAcquire) == 0 {
		doneCh <- nil
		return
	}
	// Release locks.
	if lg, err = env.lm.Acquire(context.Background(), item.LatchSpans); err != nil {
		doneCh <- err
		return
	}
	for _, k := range item.locksToAcquire {
		intent := roachpb.LockUpdate{
			Span:   roachpb.Span{Key: k},
			Txn:    item.Request.Txn.TxnMeta,
			Status: roachpb.COMMITTED,
		}
		if err = env.lt.UpdateLocks(&intent); err != nil {
			doneCh <- err
			return
		}
	}
	env.lm.Release(lg)
	doneCh <- nil
}

// Creates requests for a group of contending requests. The group is
// identified by index. The group has numOutstanding requests at a time, so
// that is the number of requests created. numKeys is the number of keys
// accessed by this group, of which numReadKeys are only read -- the remaining
// keys will be locked.
func createRequests(index int, numOutstanding int, numKeys int, numReadKeys int) []benchWorkItem {
	ts := hlc.Timestamp{WallTime: 10}
	spans := &spanset.SpanSet{}
	wi := benchWorkItem{
		Request: Request{
			Timestamp:  ts,
			LatchSpans: spans,
			LockSpans:  spans,
		},
	}
	for i := 0; i < numKeys; i++ {
		key := roachpb.Key(fmt.Sprintf("k%d.%d", index, i))
		if i <= numReadKeys {
			spans.AddMVCC(spanset.SpanReadOnly, roachpb.Span{Key: key}, ts)
		} else {
			spans.AddMVCC(spanset.SpanReadWrite, roachpb.Span{Key: key}, ts)
			wi.locksToAcquire = append(wi.locksToAcquire, key)
		}
	}
	var result []benchWorkItem
	txnCounter := uint128.FromInts(0, 0)
	for i := 0; i < numOutstanding; i++ {
		wiCopy := wi
		wiCopy.Request.Txn = &roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				ID:             nextUUID(&txnCounter),
				WriteTimestamp: ts,
			},
			ReadTimestamp: ts,
		}
		result = append(result, wiCopy)
	}
	return result
}

// Interface that is also implemented by *testing.PB.
type iterations interface {
	Next() bool
}

type simpleIters struct {
	n int
}

var _ iterations = (*simpleIters)(nil)

func (i *simpleIters) Next() bool {
	i.n--
	return i.n >= 0
}

// Runs the requests for a group of contending requests. The number of
// concurrent requests is equal to the length of items.
func runRequests(b *testing.B, pb iterations, items []benchWorkItem, env benchEnv) {
	requestDoneCh := make(chan error, len(items))
	i := 0
	outstanding := 0
	for pb.Next() {
		if outstanding == len(items) {
			if err := <-requestDoneCh; err != nil {
				b.Fatal(err)
			}
			outstanding--
		}
		go doBenchWork(&items[i], env, requestDoneCh)
		outstanding++
		i = (i + 1) % len(items)
	}
	// Drain
	for outstanding > 0 {
		if err := <-requestDoneCh; err != nil {
			b.Fatal(err)
		}
		outstanding--
	}
}

// Benchmarks that varies the number of request groups (requests within a
// group are contending), the number of outstanding requests per group, and
// the number of read keys for the requests in each group (when the number of
// read keys is equal to the total keys, there is no contention within the
// group). The number of groups is either 1 or GOMAXPROCS, in order to use
// RunParallel() -- it doesn't seem possible to get parallelism between these
// two values when using B.RunParallel() since B.SetParallelism() accepts an
// integer multiplier to GOMAXPROCS.
func BenchmarkLockTable(b *testing.B) {
	maxGroups := runtime.GOMAXPROCS(0)
	const numKeys = 5
	for _, numGroups := range []int{1, maxGroups} {
		for _, outstandingPerGroup := range []int{1, 2, 4, 8, 16} {
			for numReadKeys := 0; numReadKeys <= numKeys; numReadKeys++ {
				b.Run(
					fmt.Sprintf("groups=%d,outstanding=%d,read=%d/", numGroups, outstandingPerGroup,
						numReadKeys),
					func(b *testing.B) {
						var numRequestsWaited uint64
						var numScanCalls uint64
						const maxLocks = 100000
						lt := newLockTable(maxLocks)
						lt.enabled = true
						env := benchEnv{
							lm:                &spanlatch.Manager{},
							lt:                lt,
							numRequestsWaited: &numRequestsWaited,
							numScanCalls:      &numScanCalls,
						}
						var requestsPerGroup [][]benchWorkItem
						for i := 0; i < numGroups; i++ {
							requestsPerGroup = append(requestsPerGroup,
								createRequests(i, outstandingPerGroup, numKeys, numReadKeys))
						}
						b.ResetTimer()
						if numGroups > 1 {
							var groupNum int32 = -1
							b.RunParallel(func(pb *testing.PB) {
								index := atomic.AddInt32(&groupNum, 1)
								runRequests(b, pb, requestsPerGroup[index], env)
							})
						} else {
							iters := &simpleIters{b.N}
							runRequests(b, iters, requestsPerGroup[0], env)
						}
						if log.V(1) {
							log.Infof(context.Background(), "num requests that waited: %d, num scan calls: %d\n",
								atomic.LoadUint64(&numRequestsWaited), atomic.LoadUint64(&numScanCalls))
						}
					})
			}
		}
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

func TestLockStateSafeFormat(t *testing.T) {
	l := &lockState{
		id:     1,
		key:    []byte("KEY"),
		endKey: []byte("END"),
	}
	l.holder.locked = true
	l.holder.holder[lock.Replicated] = lockHolderInfo{
		txn:  &enginepb.TxnMeta{ID: uuid.NamespaceDNS},
		ts:   hlc.Timestamp{WallTime: 123, Logical: 7},
		seqs: []enginepb.TxnSeq{1},
	}
	require.EqualValues(t,
		" lock: ‹\"KEY\"›\n  holder: txn: 6ba7b810-9dad-11d1-80b4-00c04fd430c8, ts: 0.000000123,7, info: repl epoch: 0, seqs: [1]\n",
		redact.Sprint(l))
	require.EqualValues(t,
		" lock: ‹×›\n  holder: txn: 6ba7b810-9dad-11d1-80b4-00c04fd430c8, ts: 0.000000123,7, info: repl epoch: 0, seqs: [1]\n",
		redact.Sprint(l).Redact())
}
