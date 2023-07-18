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
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/poison"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
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
	"gopkg.in/yaml.v2"
)

/*
Test needs to handle caller constraints wrt latches being held. The datadriven
test uses the following format:

new-lock-table maxlocks=<int>
----

  Creates a lockTable. The lockTable is initially enabled.

time-tick [m=<int>] [s=<int>] [ms=<int>] [ns=<int>]
----

  Forces the manual clock to tick forward m minutes, s seconds, ms milliseconds, and ns nanoseconds.

new-txn txn=<name> ts=<int>[,<int>] epoch=<int> [seq=<int>]
----

 Creates a TxnMeta.

new-request r=<name> txn=<name>|none ts=<int>[,<int>] spans=none|shared|update|exclusive|intent@<start>[,<end>]+... [skip-locked] [max-lock-wait-queue-length=<int>]
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

acquire r=<name> k=<key> durability=r|u [ignored-seqs=<int>[-<int>][,<int>[-<int>]]]
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

pushed-txn-updated txn=<name> status=committed|aborted|pending [ts=<ts>]
----

 Informs the lock table that the named transaction is finalized.

add-discovered r=<name> k=<key> txn=<name> [lease-seq=<seq>] [consult-txn-status-cache=<bool>]
----
<error string>

 Adds a discovered lock that is discovered by the named request.

check-opt-no-conflicts r=<name> spans=none|shared|update|exclusive|intent@<start>[,<end>]+...
----
no-conflicts: <bool>

 Checks whether the request, which previously called ScanOptimistic, has no lock conflicts.

is-key-locked-by-conflicting-txn r=<name> k=<key> strength=<strength>
----
locked: <bool>

 Checks whether the provided key is locked by a conflicting transaction.

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

query span=<start>[,<end> | /Max] [max-locks=<int>] [max-bytes=<int>] [uncontended]
----

 Queries the lockTable over a given span (or over the entire LT if no span
 provided), returning lock state info up to a maximum number of locks or bytes
 if provided.  By default only returns contended locks (those with waiters),
 unless the uncontended option is given.


metrics
----
<metrics for lock table>

 Calls lockTable.String.
*/

func TestLockTableBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t, "lock_table"), func(t *testing.T, path string) {
		var lt lockTable
		var txnsByName map[string]*enginepb.TxnMeta
		var txnCounter uint128.Uint128
		var requestsByName map[string]Request
		var guardsByReqName map[string]lockTableGuard
		manualClock := timeutil.NewManualTime(timeutil.Unix(0, 123))
		clock := hlc.NewClockForTesting(manualClock)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "new-lock-table":
				var maxLocks int
				d.ScanArgs(t, "maxlocks", &maxLocks)
				ltImpl := newLockTable(
					int64(maxLocks), roachpb.RangeID(3), clock, cluster.MakeTestingClusterSettings(),
				)
				ltImpl.enabled = true
				ltImpl.enabledSeq = 1
				ltImpl.minLocks = 0
				lt = ltImpl
				txnsByName = make(map[string]*enginepb.TxnMeta)
				txnCounter = uint128.FromInts(0, 0)
				requestsByName = make(map[string]Request)
				guardsByReqName = make(map[string]lockTableGuard)
				return ""

			case "time-tick":
				var timeDelta time.Duration
				var delta int
				if d.HasArg("m") {
					d.ScanArgs(t, "m", &delta)
					timeDelta += time.Duration(delta) * time.Minute
				}
				if d.HasArg("s") {
					d.ScanArgs(t, "s", &delta)
					timeDelta += time.Duration(delta) * time.Second
				}
				if d.HasArg("ms") {
					d.ScanArgs(t, "ms", &delta)
					timeDelta += time.Duration(delta) * time.Millisecond
				}
				if d.HasArg("ns") {
					d.ScanArgs(t, "ns", &delta)
					timeDelta += time.Duration(delta)
				}
				manualClock.Advance(timeDelta)
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

			case "pushed-txn-updated":
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
				case "pending":
					txn.Status = roachpb.PENDING
				default:
					return fmt.Sprintf("unknown txn status %s", statusStr)
				}
				if d.HasArg("ts") {
					ts := scanTimestamp(t, d)
					txn.WriteTimestamp.Forward(ts)
				}
				lt.PushedTransactionUpdated(txn)
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
				waitPolicy := lock.WaitPolicy_Block
				if d.HasArg("skip-locked") {
					waitPolicy = lock.WaitPolicy_SkipLocked
				}
				var maxLockWaitQueueLength int
				if d.HasArg("max-lock-wait-queue-length") {
					d.ScanArgs(t, "max-lock-wait-queue-length", &maxLockWaitQueueLength)
				}
				latchSpans, lockSpans := scanSpans(t, d, ts)
				req := Request{
					Timestamp:              ts,
					WaitPolicy:             waitPolicy,
					MaxLockWaitQueueLength: maxLockWaitQueueLength,
					LatchSpans:             latchSpans,
					LockSpans:              lockSpans,
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
				acq := roachpb.MakeLockAcquisition(req.Txn, roachpb.Key(key), durability)
				var ignored []enginepb.IgnoredSeqNumRange
				if d.HasArg("ignored-seqs") {
					ignored = scanIgnoredSeqNumbers(t, d)
				}
				acq.IgnoredSeqNums = ignored
				if err := lt.AcquireLock(&acq); err != nil {
					return err.Error()
				}
				return lt.String()

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
				return lt.String()

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
					ignored = scanIgnoredSeqNumbers(t, d)
				}
				// TODO(sbhola): also test STAGING.
				intent := &roachpb.LockUpdate{
					Span: span, Txn: *txnMeta, Status: roachpb.PENDING, IgnoredSeqNums: ignored}
				if err := lt.UpdateLocks(intent); err != nil {
					return err.Error()
				}
				return lt.String()

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
				consultTxnStatusCache := false
				if d.HasArg("consult-txn-status-cache") {
					d.ScanArgs(t, "consult-txn-status-cache", &consultTxnStatusCache)
				}
				leaseSeq := roachpb.LeaseSequence(seq)
				if _, err := lt.AddDiscoveredLock(
					&intent, leaseSeq, consultTxnStatusCache, g); err != nil {
					return err.Error()
				}
				return lt.String()

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
				_, lockSpans := scanSpans(t, d, req.Timestamp)
				return fmt.Sprintf("no-conflicts: %t", g.CheckOptimisticNoConflicts(lockSpans))

			case "is-key-locked-by-conflicting-txn":
				var reqName string
				d.ScanArgs(t, "r", &reqName)
				g := guardsByReqName[reqName]
				if g == nil {
					d.Fatalf(t, "unknown guard: %s", reqName)
				}
				var key string
				d.ScanArgs(t, "k", &key)
				strength := ScanLockStrength(t, d)
				if ok, txn := g.IsKeyLockedByConflictingTxn(roachpb.Key(key), strength); ok {
					holder := "<nil>"
					if txn != nil {
						holder = txn.ID.String()
					}
					return fmt.Sprintf("locked: true, holder: %s", holder)
				}
				return "locked: false"

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
				return lt.String()

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
				case waitQueueMaxLengthExceeded:
					typeStr = "waitQueueMaxLengthExceeded"
				case doneWaiting:
					var toResolveStr string
					if stateTransition {
						toResolveStr = intentsToResolveToStr(g.ResolveBeforeScanning(), true)
					}
					return str + "state=doneWaiting" + toResolveStr
				default:
					d.Fatalf(t, "unexpected state: %v", state.kind)
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
				return fmt.Sprintf("%sstate=%s txn=%s key=%s held=%t guard-strength=%s",
					str, typeStr, txnS, state.key, state.held, state.guardStrength)

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
				return lt.String()

			case "print":
				return lt.String()

			case "query":
				span := keys.EverythingSpan
				var maxLocks int
				var targetBytes int
				if d.HasArg("span") {
					var spanStr string
					d.ScanArgs(t, "span", &spanStr)
					span = getSpan(t, d, spanStr)
				}
				if d.HasArg("max-locks") {
					d.ScanArgs(t, "max-locks", &maxLocks)
				}
				if d.HasArg("max-bytes") {
					d.ScanArgs(t, "max-bytes", &targetBytes)
				}
				scanOpts := QueryLockTableOptions{
					MaxLocks:           int64(maxLocks),
					TargetBytes:        int64(targetBytes),
					IncludeUncontended: d.HasArg("uncontended"),
				}
				lockInfos, resumeState := lt.QueryLockTableState(span, scanOpts)
				var lockInfoBytes int64
				for _, lockInfo := range lockInfos {
					lockInfoBytes += int64(lockInfo.Size())
				}

				var buf strings.Builder
				fmt.Fprintf(&buf, "num locks: %d, bytes returned: %d, resume reason: %s, resume span: %s",
					len(lockInfos), lockInfoBytes, resumeState.ResumeReason, resumeState.ResumeSpan)
				if len(lockInfos) > 0 {
					fmt.Fprintf(&buf, "\n locks:")
				}
				for _, lockInfo := range lockInfos {
					lockInfoStrLines := strings.Split(redact.Sprintf("%+v", lockInfo).StripMarkers(), "\n")
					for _, line := range lockInfoStrLines {
						fmt.Fprintf(&buf, "\n  %s", line)
					}
				}
				return buf.String()

			case "metrics":
				metrics := lt.Metrics()
				b, err := yaml.Marshal(&metrics)
				if err != nil {
					d.Fatalf(t, "marshaling metrics: %v", err)
				}
				return string(b)

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
		if parts[1] == "/Max" {
			span.EndKey = roachpb.KeyMax
		} else {
			span.EndKey = roachpb.Key(parts[1])
		}
	}
	return span
}

func scanSpans(
	t *testing.T, d *datadriven.TestData, ts hlc.Timestamp,
) (*spanset.SpanSet, *lockspanset.LockSpanSet) {
	latchSpans := &spanset.SpanSet{}
	lockSpans := &lockspanset.LockSpanSet{}
	var spansStr string
	d.ScanArgs(t, "spans", &spansStr)
	lockSpanStrs := strings.Split(spansStr, "+")
	for _, lockSpanStr := range lockSpanStrs {
		parts := strings.Split(lockSpanStr, "@")
		if len(parts) != 2 {
			d.Fatalf(t, "incorrect span with strength format: %s", parts)
		}
		strS := parts[0]
		spanStr := parts[1]
		str := getStrength(t, d, strS)
		// Compute latch span access based on the supplied strength.
		var sa spanset.SpanAccess
		switch str {
		case lock.None:
			sa = spanset.SpanReadOnly
		case lock.Intent:
			sa = spanset.SpanReadWrite
		default:
			d.Fatalf(t, "unsupported lock strength: %s", str)
		}
		latchSpans.AddMVCC(sa, getSpan(t, d, spanStr), ts)
		lockSpans.Add(str, getSpan(t, d, spanStr))
	}
	return latchSpans, lockSpans
}

func ScanLockStrength(t *testing.T, d *datadriven.TestData) lock.Strength {
	var strS string
	d.ScanArgs(t, "strength", &strS)
	return getStrength(t, d, strS)
}

func getStrength(t *testing.T, d *datadriven.TestData, strS string) lock.Strength {
	switch strS {
	case "none":
		return lock.None
	case "shared":
		return lock.Shared
	case "update":
		return lock.Update
	case "exclusive":
		return lock.Exclusive
	case "intent":
		return lock.Intent
	default:
		d.Fatalf(t, "unknown lock strength: %s", strS)
		return 0
	}
}

func scanIgnoredSeqNumbers(t *testing.T, d *datadriven.TestData) []enginepb.IgnoredSeqNumRange {
	var ignored []enginepb.IgnoredSeqNumRange
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
	return ignored
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
	lt := newLockTable(
		5, roachpb.RangeID(3), hlc.NewClockForTesting(nil), cluster.MakeTestingClusterSettings(),
	)
	lt.minLocks = 0
	lt.enabled = true
	var keys []roachpb.Key
	var guards []lockTableGuard
	var reqs []Request
	// 10 requests, each with 10 discovered locks. Only 1 will be considered
	// notRemovable per request.
	for i := 0; i < 10; i++ {
		latchSpans := &spanset.SpanSet{}
		lockSpans := &lockspanset.LockSpanSet{}
		for j := 0; j < 20; j++ {
			k := roachpb.Key(fmt.Sprintf("%08d", i*20+j))
			keys = append(keys, k)
			latchSpans.AddMVCC(spanset.SpanReadWrite, roachpb.Span{Key: k}, hlc.Timestamp{WallTime: 1})
			lockSpans.Add(lock.Intent, roachpb.Span{Key: k})
		}
		req := Request{
			Timestamp:  hlc.Timestamp{WallTime: 1},
			LatchSpans: latchSpans,
			LockSpans:  lockSpans,
		}
		reqs = append(reqs, req)
		ltg := lt.ScanAndEnqueue(req, nil)
		require.Nil(t, ltg.ResolveBeforeScanning())
		require.False(t, ltg.ShouldWait())
		guards = append(guards, ltg)
	}
	txnMeta := enginepb.TxnMeta{
		ID:             uuid.MakeV4(),
		WriteTimestamp: hlc.Timestamp{WallTime: 10},
	}
	for i := range guards {
		for j := 0; j < 10; j++ {
			k := i*20 + j
			added, err := lt.AddDiscoveredLock(
				&roachpb.Intent{
					Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[k]}, Txn: txnMeta,
				},
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
		&roachpb.Intent{
			Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+10]}, Txn: txnMeta,
		},
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// The 6 notRemovable locks remain.
	require.Equal(t, int64(6), lt.lockCountForTesting())
	require.Equal(t, int64(101), int64(lt.locks.lockIDSeqNum))
	// Add another discovered lock, to trigger tryClearLocks.
	added, err = lt.AddDiscoveredLock(
		&roachpb.Intent{
			Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+11]}, Txn: txnMeta,
		},
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// Still the 6 notRemovable locks remain.
	require.Equal(t, int64(6), lt.lockCountForTesting())
	require.Equal(t, int64(102), int64(lt.locks.lockIDSeqNum))
	// Two more guards are dequeued, so we are down to 4 notRemovable locks.
	lt.Dequeue(guards[4])
	lt.Dequeue(guards[5])
	// Bump up the enforcement interval manually.
	lt.locks.lockAddMaxLocksCheckInterval = 2
	// Add another discovered lock.
	added, err = lt.AddDiscoveredLock(
		&roachpb.Intent{
			Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+12]}, Txn: txnMeta,
		},
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// This notRemovable=false lock is also added, since enforcement not done.
	require.Equal(t, int64(7), lt.lockCountForTesting())
	// Add another discovered lock, to trigger tryClearLocks.
	added, err = lt.AddDiscoveredLock(
		&roachpb.Intent{
			Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+13]}, Txn: txnMeta,
		},
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// Now enforcement is done, so only 4 remain.
	require.Equal(t, int64(4), lt.lockCountForTesting())
	// Bump down the enforcement interval manually, and bump up minLocks
	lt.locks.lockAddMaxLocksCheckInterval = 1
	lt.minLocks = 2
	// Three more guards dequeued.
	lt.Dequeue(guards[6])
	lt.Dequeue(guards[7])
	lt.Dequeue(guards[8])
	// Add another discovered lock.
	added, err = lt.AddDiscoveredLock(
		&roachpb.Intent{
			Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+14]}, Txn: txnMeta,
		},
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	require.Equal(t, int64(5), lt.lockCountForTesting())
	// Add another discovered lock, to trigger tryClearLocks, and push us over 5
	// locks.
	added, err = lt.AddDiscoveredLock(
		&roachpb.Intent{
			Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+15]}, Txn: txnMeta,
		},
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
			&roachpb.Intent{
				Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[9*20+i]}, Txn: txnMeta,
			},
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
	lt := newLockTable(
		2, roachpb.RangeID(3), hlc.NewClockForTesting(nil), cluster.MakeTestingClusterSettings(),
	)
	lt.minLocks = 0
	lt.enabled = true
	var keys []roachpb.Key
	var guards []lockTableGuard
	// 10 requests. Every pair of requests have the same span.
	for i := 0; i < 10; i++ {
		latchSpans := &spanset.SpanSet{}
		lockSpans := &lockspanset.LockSpanSet{}
		key := roachpb.Key(fmt.Sprintf("%08d", i/2))
		if i%2 == 0 {
			keys = append(keys, key)
		}
		latchSpans.AddMVCC(spanset.SpanReadWrite, roachpb.Span{Key: key}, hlc.Timestamp{WallTime: 1})
		lockSpans.Add(lock.Intent, roachpb.Span{Key: key})
		req := Request{
			Timestamp:  hlc.Timestamp{WallTime: 1},
			LatchSpans: latchSpans,
			LockSpans:  lockSpans,
		}
		ltg := lt.ScanAndEnqueue(req, nil)
		require.Nil(t, ltg.ResolveBeforeScanning())
		require.False(t, ltg.ShouldWait())
		guards = append(guards, ltg)
	}
	txnMeta := enginepb.TxnMeta{
		ID:             uuid.MakeV4(),
		WriteTimestamp: hlc.Timestamp{WallTime: 10},
	}
	// The first 6 requests discover 3 locks total.
	for i := 0; i < 6; i++ {
		added, err := lt.AddDiscoveredLock(
			&roachpb.Intent{
				Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[i/2]}, Txn: txnMeta,
			},
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
		&roachpb.Intent{
			Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[6/2]}, Txn: txnMeta,
		},
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
		&roachpb.Intent{
			Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys[8/2]}, Txn: txnMeta,
		},
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
			lg, err = e.lm.Acquire(context.Background(), item.request.LatchSpans, poison.Policy_Error)
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
			err = e.acquireLock(item.request.Txn, k)
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
	lt := newLockTable(
		maxLocks, roachpb.RangeID(3), hlc.NewClockForTesting(nil), cluster.MakeTestingClusterSettings(),
	)
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

func (e *workloadExecutor) acquireLock(txn *roachpb.Transaction, k roachpb.Key) error {
	acq := roachpb.MakeLockAcquisition(txn, k, lock.Unreplicated)
	err := e.lt.AcquireLock(&acq)
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
		latchSpans := &spanset.SpanSet{}
		lockSpans := &lockspanset.LockSpanSet{}
		for i := 0; i < numKeys; i++ {
			span := roachpb.Span{Key: keys[keysPerm[i]]}
			acc := spanset.SpanAccess(rng.Intn(int(spanset.NumSpanAccess)))
			str := lock.None
			if acc == spanset.SpanReadWrite {
				str = lock.Intent
			}
			latchSpans.AddMVCC(acc, span, ts)
			lockSpans.Add(str, span)
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
			LatchSpans: latchSpans,
			LockSpans:  lockSpans,
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
		latchSpans := &spanset.SpanSet{}
		lockSpans := &lockspanset.LockSpanSet{}
		onlyReads := txnMeta == nil && rng.Intn(2) != 0
		numKeys := rng.Intn(len(keys)-1) + 1
		request := &Request{
			Timestamp:  ts,
			LatchSpans: latchSpans,
			LockSpans:  lockSpans,
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
			str := lock.None
			dupRead := false
			if !onlyReads {
				acc = spanset.SpanAccess(rng.Intn(int(spanset.NumSpanAccess)))
				if acc == spanset.SpanReadWrite && txnMeta != nil && rng.Intn(2) == 0 {
					// Acquire lock.
					wi.locksToAcquire = append(wi.locksToAcquire, span.Key)
					str = lock.Intent
				}
				if acc == spanset.SpanReadWrite && rng.Intn(2) == 0 {
					// Also include the key as read.
					dupRead = true
					str = lock.Intent
				}
			}
			latchSpans.AddMVCC(acc, span, ts)
			lockSpans.Add(str, span)
			if dupRead {
				latchSpans.AddMVCC(spanset.SpanReadOnly, span, ts)
				lockSpans.Add(lock.None, span)
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
		if lg, err = env.lm.Acquire(context.Background(), item.LatchSpans, poison.Policy_Error); err != nil {
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
		acq := roachpb.MakeLockAcquisition(item.Txn, k, lock.Unreplicated)
		if err = env.lt.AcquireLock(&acq); err != nil {
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
	if lg, err = env.lm.Acquire(context.Background(), item.LatchSpans, poison.Policy_Error); err != nil {
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
	latchSpans := &spanset.SpanSet{}
	lockSpans := &lockspanset.LockSpanSet{}
	wi := benchWorkItem{
		Request: Request{
			Timestamp:  ts,
			LatchSpans: latchSpans,
			LockSpans:  lockSpans,
		},
	}
	for i := 0; i < numKeys; i++ {
		key := roachpb.Key(fmt.Sprintf("k%d.%d", index, i))
		span := roachpb.Span{Key: key}
		if i <= numReadKeys {
			latchSpans.AddMVCC(spanset.SpanReadOnly, span, ts)
			lockSpans.Add(lock.None, span)
		} else {
			latchSpans.AddMVCC(spanset.SpanReadWrite, span, ts)
			lockSpans.Add(lock.Intent, span)
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
						lt := newLockTable(
							maxLocks,
							roachpb.RangeID(3),
							hlc.NewClockForTesting(nil),
							cluster.MakeTestingClusterSettings(),
						)
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

// BenchmarkLockTableMetrics populates variable sized lock-tables and ensures
// that grabbing metrics from them is reasonably fast.
func BenchmarkLockTableMetrics(b *testing.B) {
	for _, locks := range []int{0, 1 << 0, 1 << 4, 1 << 8, 1 << 12} {
		b.Run(fmt.Sprintf("locks=%d", locks), func(b *testing.B) {
			const maxLocks = 100000
			lt := newLockTable(
				maxLocks,
				roachpb.RangeID(3),
				hlc.NewClockForTesting(nil),
				cluster.MakeTestingClusterSettings(),
			)
			lt.enabled = true

			txn := &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{ID: uuid.MakeV4()},
			}
			for i := 0; i < locks; i++ {
				k := roachpb.Key(fmt.Sprintf("%03d", i))
				acq := roachpb.MakeLockAcquisition(txn, k, lock.Unreplicated)
				err := lt.AcquireLock(&acq)
				if err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = lt.Metrics()
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

func TestLockStateSafeFormat(t *testing.T) {
	l := &lockState{
		id:     1,
		key:    []byte("KEY"),
		endKey: []byte("END"),
	}
	l.holder.txn = &enginepb.TxnMeta{ID: uuid.NamespaceDNS}
	// TODO(arul): add something about replicated locks here too.
	l.holder.unreplicatedInfo = unreplicatedLockHolderInfo{
		ts:   hlc.Timestamp{WallTime: 123, Logical: 7},
		seqs: []enginepb.TxnSeq{1},
	}
	require.EqualValues(t,
		" lock: ‹\"KEY\"›\n  holder: txn: 6ba7b810-9dad-11d1-80b4-00c04fd430c8 epoch: 0, ts: 0.000000123,7, info: unrepl seqs: [1]\n",
		redact.Sprint(l))
	require.EqualValues(t,
		" lock: ‹×›\n  holder: txn: 6ba7b810-9dad-11d1-80b4-00c04fd430c8 epoch: 0, ts: 0.000000123,7, info: unrepl seqs: [1]\n",
		redact.Sprint(l).Redact())
}

// TestElideWaitingStateUpdatesConsidersAllFields ensures all fields in the
// waitingState struct have been considered for inclusion/non-inclusion in the
// logic of canElideWaitingStateUpdate. The test doesn't check if the
// inclusion/non-inclusion claims are actually reflected in the logic; however,
// it does serve as a glorified nudge to consider new fields added to
// waitingState for inclusion/non-inclusion in canElideWaitingStateUpdate's
// logic.
func TestCanElideWaitingStateUpdateConsidersAllFields(t *testing.T) {
	type inclusionStatus bool
	const (
		includeWhenDeciding      inclusionStatus = true
		doNotIncludeWhenDeciding inclusionStatus = false
	)
	fieldMap := map[string]inclusionStatus{
		"kind":          includeWhenDeciding,
		"txn":           includeWhenDeciding,
		"key":           includeWhenDeciding,
		"held":          includeWhenDeciding,
		"queuedWriters": doNotIncludeWhenDeciding,
		"queuedReaders": doNotIncludeWhenDeciding,
		"guardStrength": doNotIncludeWhenDeciding,
	}
	ws := waitingState{}
	typ := reflect.ValueOf(ws).Type()
	for i := 0; i < typ.NumField(); i++ {
		fieldName := typ.Field(i).Name
		_, ok := fieldMap[fieldName]
		if !ok {
			t.Fatalf("%s field not considered", fieldName)
		}
		typ.Field(i)
	}
}
