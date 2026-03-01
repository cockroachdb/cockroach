// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package concurrency

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/poison"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/dd"
	"github.com/cockroachdb/cockroach/pkg/util"
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

new-txn txn=<name> ts=<int>[,<int>] epoch=<int> [seq=<int>] [iso=<level>]
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

acquire r=<name> k=<key> durability=r|u [ignored-seqs=<int>[-<int>][,<int>[-<int>]] strength=<strength>
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

add-discovered r=<name> k=<key> txn=<name> [lease-seq=<seq>] [consult-txn-status-cache=<bool>] [strength=<strength>]
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
		ctx := context.Background()
		var lt lockTable
		var st *cluster.Settings
		var txnsByName map[string]*enginepb.TxnMeta
		var uncertaintyLimitsByTxn map[string]hlc.Timestamp
		var txnCounter uint128.Uint128
		var requestsByName map[string]Request
		var guardsByReqName map[string]lockTableGuard
		manualClock := timeutil.NewManualTime(timeutil.Unix(0, 123))
		clock := hlc.NewClockForTesting(manualClock)
		// Keep virtualized intent resolution off, unless tests enable it
		// explicitly. When it becomes on by default, there will be some test churn
		// in these data-driven tests.
		st = cluster.MakeTestingClusterSettings()
		VirtualIntentResolution.Override(context.Background(), &st.SV, false)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "new-lock-table":
				maxLocks := dd.ScanArg[int64](t, d, "maxlocks")
				m := TestingMakeLockTableMetricsCfg()
				ltImpl := newLockTable(
					maxLocks, roachpb.RangeID(3), clock, st,
					m.LocksShedDueToMemoryLimit, m.NumLockShedDueToMemoryLimitEvents,
				)
				ltImpl.enabled = true
				ltImpl.enabledSeq = 1
				ltImpl.lockTableLimitsMu.minKeysLocked = 0
				lt = maybeWrapInVerifyingLockTable(ltImpl)
				txnsByName = make(map[string]*enginepb.TxnMeta)
				uncertaintyLimitsByTxn = make(map[string]hlc.Timestamp)
				txnCounter = uint128.FromInts(0, 0)
				requestsByName = make(map[string]Request)
				guardsByReqName = make(map[string]lockTableGuard)
				return ""

			case "time-tick":
				timeDelta := time.Duration(dd.ScanArgOr(t, d, "m", 0))*time.Minute +
					time.Duration(dd.ScanArgOr(t, d, "s", 0))*time.Second +
					time.Duration(dd.ScanArgOr(t, d, "ms", 0))*time.Millisecond +
					time.Duration(dd.ScanArgOr(t, d, "ns", 0))*time.Nanosecond
				manualClock.Advance(timeDelta)
				return ""

			case "new-txn":
				// UUIDs for transactions are numbered from 1 by this test code and
				// lockTableImpl.String() knows about UUIDs and not transaction names.
				// Assigning txnNames of the form txn1, txn2, ... keeps the two in sync,
				// which makes test cases easier to understand.
				txnName := dd.ScanArg[string](t, d, "txn")
				ts := scanTimestamp(t, d)
				epoch := dd.ScanArg[enginepb.TxnEpoch](t, d, "epoch")
				seq := dd.ScanArgOr[enginepb.TxnSeq](t, d, "seq", 0)
				iso := ScanIsoLevel(t, d)
				txnMeta, ok := txnsByName[txnName]
				var id uuid.UUID
				if ok {
					id = txnMeta.ID
				} else {
					id = nextUUID(&txnCounter)
				}
				txnsByName[txnName] = &enginepb.TxnMeta{
					ID:             id,
					Epoch:          epoch,
					Sequence:       seq,
					WriteTimestamp: ts,
					IsoLevel:       iso,
				}
				if d.HasArg("uncertainty-limit") {
					uncertaintyLimitsByTxn[txnName] = scanTimestampWithName(t, d, "uncertainty-limit")
				}
				return ""

			case "pushed-txn-updated":
				txnName := dd.ScanArg[string](t, d, "txn")
				txnMeta, ok := txnsByName[txnName]
				if !ok {
					return fmt.Sprintf("txn %s not found", txnName)
				}
				txn := &roachpb.Transaction{
					TxnMeta: *txnMeta,
				}
				switch statusStr := dd.ScanArg[string](t, d, "status"); statusStr {
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
				var clockObs roachpb.ObservedTimestamp
				if txn.Status == roachpb.PENDING {
					// Pending transactions require a non-empty clock observation.
					clockObs = roachpb.ObservedTimestamp{
						NodeID:    1,
						Timestamp: clock.NowAsClockTimestamp(),
					}
				}
				lt.PushedTransactionUpdated(txn, clockObs)
				return ""

			case "new-request":
				// Seqnums for requests are numbered from 1 by lockTableImpl and
				// lockTableImpl.String() does not know about request names. Assigning
				// request names of the form req1, req2, ... keeps the two in sync,
				// which makes test cases easier to understand.
				reqName := dd.ScanArg[string](t, d, "r")
				if _, ok := requestsByName[reqName]; ok {
					d.Fatalf(t, "duplicate request: %s", reqName)
				}
				txnName := dd.ScanArg[string](t, d, "txn")
				txnMeta, ok := txnsByName[txnName]
				if !ok && txnName != "none" {
					d.Fatalf(t, "unknown txn %s", txnName)
				}
				ts := scanTimestamp(t, d)
				waitPolicy := lock.WaitPolicy_Block
				if d.HasArg("skip-locked") {
					waitPolicy = lock.WaitPolicy_SkipLocked
				}
				updateRetainedTxn := !d.HasArg("no-update-retained-txn")
				maxLockWaitQueueLength := dd.ScanArgOr(t, d, "max-lock-wait-queue-length", 0)
				latchSpans, lockSpans := scanSpans(t, d, ts)
				ba := &kvpb.BatchRequest{}
				ba.Timestamp = ts
				ba.WaitPolicy = waitPolicy
				req := Request{
					Timestamp:              ba.Timestamp,
					WaitPolicy:             ba.WaitPolicy,
					MaxLockWaitQueueLength: maxLockWaitQueueLength,
					LatchSpans:             latchSpans,
					LockSpans:              lockSpans,
					Batch:                  ba,
				}
				if txnMeta != nil {
					// Update the transaction's timestamp, if necessary. The transaction
					// may have needed to move its timestamp for any number of reasons.
					if updateRetainedTxn {
						txnMeta.WriteTimestamp = ts
					}
					ba.Txn = &roachpb.Transaction{
						TxnMeta:       *txnMeta,
						ReadTimestamp: ts,
					}
					if !updateRetainedTxn {
						ba.Txn.WriteTimestamp = ts
					}
					// Apply uncertainty limit if one was specified on the transaction.
					if ul, ok := uncertaintyLimitsByTxn[txnName]; ok {
						ba.Txn.GlobalUncertaintyLimit = ul
					}
					// Add observed timestamp if specified. Format: "nodeID@ts"
					if d.HasArg("observed-ts") {
						obsStr := dd.ScanArg[string](t, d, "observed-ts")
						parts := strings.Split(obsStr, "@")
						nodeID, err := strconv.Atoi(parts[0])
						if err != nil {
							d.Fatalf(t, "invalid node ID in observed-ts: %s", obsStr)
						}
						obsTs, err := hlc.ParseTimestamp(parts[1])
						if err != nil {
							d.Fatalf(t, "invalid timestamp in observed-ts: %s", obsStr)
						}
						ba.Txn.UpdateObservedTimestamp(
							roachpb.NodeID(nodeID),
							obsTs.UnsafeToClockTimestamp(),
						)
					}

					req.Txn = ba.Txn
				}
				requestsByName[reqName] = req
				return ""

			case "scan":
				reqName := dd.ScanArg[string](t, d, "r")
				req, ok := requestsByName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}
				g := guardsByReqName[reqName]
				var err *Error
				g, err = lt.ScanAndEnqueue(ctx, req, g)
				if err != nil {
					return err.String()
				}
				guardsByReqName[reqName] = g
				return fmt.Sprintf("start-waiting: %t", g.ShouldWait())

			case "scan-opt":
				reqName := dd.ScanArg[string](t, d, "r")
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
				reqName := dd.ScanArg[string](t, d, "r")
				req, ok := requestsByName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}
				key := dd.ScanArg[string](t, d, "k")
				s := dd.ScanArg[string](t, d, "durability")
				if len(s) != 1 || (s[0] != 'r' && s[0] != 'u') {
					d.Fatalf(t, "incorrect durability: %s", s)
				}
				durability := lock.Unreplicated
				if s[0] == 'r' {
					durability = lock.Replicated
				}
				strength := ScanLockStrength(t, d)
				acq := roachpb.MakeLockAcquisition(
					req.Txn.TxnMeta, roachpb.Key(key), durability, strength, req.Txn.IgnoredSeqNums,
				)
				acq.IgnoredSeqNums = ScanIgnoredSeqNumbers(t, d)
				if err := lt.AcquireLock(ctx, &acq); err != nil {
					return err.Error()
				}
				return lt.String()

			case "release":
				txnName := dd.ScanArg[string](t, d, "txn")
				txnMeta, ok := txnsByName[txnName]
				if !ok {
					d.Fatalf(t, "unknown txn %s", txnName)
				}
				span := getSpan(t, d, dd.ScanArg[string](t, d, "span"))
				// TODO(sbhola): also test ABORTED.
				intent := &roachpb.LockUpdate{Span: span, Txn: *txnMeta, Status: roachpb.COMMITTED}
				if err := lt.UpdateLocks(ctx, intent); err != nil {
					return err.Error()
				}
				return lt.String()

			case "update":
				txnName := dd.ScanArg[string](t, d, "txn")
				txnMeta, ok := txnsByName[txnName]
				if !ok {
					d.Fatalf(t, "unknown txn %s", txnName)
				}
				ts := scanTimestamp(t, d)
				epoch := dd.ScanArg[enginepb.TxnEpoch](t, d, "epoch")
				txnMeta = &enginepb.TxnMeta{
					ID: txnMeta.ID, Sequence: txnMeta.Sequence, Epoch: epoch, WriteTimestamp: ts,
				}
				txnsByName[txnName] = txnMeta
				span := getSpan(t, d, dd.ScanArg[string](t, d, "span"))
				ignored := ScanIgnoredSeqNumbers(t, d)
				// TODO(sbhola): also test STAGING.
				intent := &roachpb.LockUpdate{
					Span: span, Txn: *txnMeta, Status: roachpb.PENDING, IgnoredSeqNums: ignored}
				if err := lt.UpdateLocks(ctx, intent); err != nil {
					return err.Error()
				}
				return lt.String()

			case "update-txn-not-observed":
				txnName := dd.ScanArg[string](t, d, "txn")
				txnMeta, ok := txnsByName[txnName]
				if !ok {
					d.Fatalf(t, "unknown txn %s", txnName)
				}
				ts := scanTimestamp(t, d)
				epoch := dd.ScanArg[enginepb.TxnEpoch](t, d, "epoch")
				txnsByName[txnName] = &enginepb.TxnMeta{
					ID: txnMeta.ID, Sequence: txnMeta.Sequence,
					Epoch: epoch, WriteTimestamp: ts,
				}
				return ""

			case "add-discovered":
				reqName := dd.ScanArg[string](t, d, "r")
				g := guardsByReqName[reqName]
				if g == nil {
					d.Fatalf(t, "unknown guard: %s", reqName)
				}
				key := dd.ScanArg[string](t, d, "k")
				txnName := dd.ScanArg[string](t, d, "txn")
				txnMeta, ok := txnsByName[txnName]
				if !ok {
					d.Fatalf(t, "unknown txn %s", txnName)
				}
				foundLock := roachpb.MakeLock(txnMeta, roachpb.Key(key), lock.Intent)
				leaseSeq := dd.ScanArgOr[roachpb.LeaseSequence](t, d, "lease-seq", 1)
				consultTxnStatusCache := dd.ScanArgOr(t, d, "consult-txn-status-cache", false)
				str := lock.Intent // default replicated locks to write intents
				if d.HasArg("strength") {
					str = ScanLockStrength(t, d)
				}
				foundLock.Strength = str

				if _, err := lt.AddDiscoveredLock(
					ctx, &foundLock, leaseSeq, consultTxnStatusCache, g); err != nil {
					return err.Error()
				}
				return lt.String()

			case "check-opt-no-conflicts":
				reqName := dd.ScanArg[string](t, d, "r")
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
				reqName := dd.ScanArg[string](t, d, "r")
				g := guardsByReqName[reqName]
				if g == nil {
					d.Fatalf(t, "unknown guard: %s", reqName)
				}
				key := dd.ScanArg[string](t, d, "k")
				strength := ScanLockStrength(t, d)
				ok, txn, err := g.IsKeyLockedByConflictingTxn(ctx, roachpb.Key(key), strength)
				if err != nil {
					return err.Error()
				}
				if ok {
					holder := "<nil>"
					if txn != nil {
						holder = txn.ID.String()
					}
					return fmt.Sprintf("locked: true, holder: %s", holder)
				}
				return "locked: false"

			case "dequeue":
				reqName := dd.ScanArg[string](t, d, "r")
				g := guardsByReqName[reqName]
				if g == nil {
					d.Fatalf(t, "unknown guard: %s", reqName)
				}
				lt.Dequeue(ctx, g)
				delete(guardsByReqName, reqName)
				delete(requestsByName, reqName)
				return lt.String()

			case "should-wait":
				reqName := dd.ScanArg[string](t, d, "r")
				g := guardsByReqName[reqName]
				if g == nil {
					d.Fatalf(t, "unknown guard: %s", reqName)
				}
				return fmt.Sprintf("%t", g.ShouldWait())

			case "guard-state":
				reqName := dd.ScanArg[string](t, d, "r")
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
				state, err := g.CurState()
				if err != nil {
					return err.Error()
				}
				var typeStr string
				switch state.kind {
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
						toResolveStr = intentsToResolveToStr(g, true)
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
				reqName := dd.ScanArg[string](t, d, "r")
				g := guardsByReqName[reqName]
				if g == nil {
					d.Fatalf(t, "unknown guard: %s", reqName)
				}
				return intentsToResolveToStr(g, false)

			case "enable":
				lt.Enable(dd.ScanArgOr[roachpb.LeaseSequence](t, d, "lease-seq", 1))
				return ""

			case "clear":
				lt.Clear(d.HasArg("disable"))
				return lt.String()
			case "clear-ge":
				endKeyStr := dd.ScanArg[string](t, d, "key")
				locks := lt.ClearGE(roachpb.Key(endKeyStr))
				var buf strings.Builder
				fmt.Fprintf(&buf, "num returned for re-acquisition: %d", len(locks))
				for _, l := range locks {
					fmt.Fprintf(&buf, "\n span: %s, txn: %s epo: %d, dur: %s, str: %s",
						l.Span, l.Txn.ID, l.Txn.Epoch, l.Durability, l.Strength)
				}
				return buf.String()
			case "print":
				return lt.String()

			case "query":
				span := keys.EverythingSpan
				if str, ok := dd.ScanArgOpt[string](t, d, "span"); ok {
					span = getSpan(t, d, str)
				}
				scanOpts := QueryLockTableOptions{
					MaxLocks:           dd.ScanArgOr[int64](t, d, "max-locks", 0),
					TargetBytes:        dd.ScanArgOr[int64](t, d, "max-bytes", 0),
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

			case "set-virtual-intent-resolution":
				ok := dd.ScanArg[bool](t, d, "ok")
				VirtualIntentResolution.Override(context.Background(), &st.SV, ok)
				return ""

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
	ts, err := hlc.ParseTimestamp(dd.ScanArg[string](t, d, "ts"))
	if err != nil {
		d.Fatalf(t, "%v", err)
	}
	return ts
}

func scanTimestampWithName(t *testing.T, d *datadriven.TestData, name string) hlc.Timestamp {
	ts, err := hlc.ParseTimestamp(dd.ScanArg[string](t, d, name))
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
	spansStr := dd.ScanArg[string](t, d, "spans")
	lockSpanStrs := strings.Split(spansStr, "+")
	for _, lockSpanStr := range lockSpanStrs {
		parts := strings.Split(lockSpanStr, "@")
		if len(parts) != 2 {
			d.Fatalf(t, "incorrect span with strength format: %s", parts)
		}
		strS := parts[0]
		spanStr := parts[1]
		str := GetStrength(t, d, strS)
		// Compute latch span access based on the supplied strength.
		sa, latchTs := latchAccessForLockStrength(str, ts)
		latchSpans.AddMVCC(sa, getSpan(t, d, spanStr), latchTs)
		lockSpans.Add(str, getSpan(t, d, spanStr))
	}
	return latchSpans, lockSpans
}

// latchAccessForLockStrength returns the latch access and timestamp to use for
// a given lock strength and request timestamp. It duplicates some of the logic
// in DefaultDeclareIsolatedKeys to avoid the package dependency.
func latchAccessForLockStrength(
	str lock.Strength, ts hlc.Timestamp,
) (spanset.SpanAccess, hlc.Timestamp) {
	switch str {
	case lock.None:
		return spanset.SpanReadOnly, ts
	case lock.Shared:
		// Unlike non-locking reads, shared-locking reads are isolated at all
		// timestamps (not just the request's timestamp); so we acquire a read
		// latch at max timestamp. See
		// https://github.com/cockroachdb/cockroach/issues/102264.
		//
		// We don't need to duplicate the special case for replicated shared
		// locks here (see ReplicatedSharedLocksTransactionLatchingKey) because
		// there is no risk in these tests of two shared lock acquisitions from
		// the same transaction clobbering each other's state.
		return spanset.SpanReadOnly, hlc.MaxTimestamp
	case lock.Exclusive:
		return spanset.SpanReadWrite, ts
	case lock.Intent:
		return spanset.SpanReadWrite, ts
	default:
		panic(fmt.Sprintf("unsupported lock strength: %s", str))
	}
}

func ScanIsoLevel(t *testing.T, d *datadriven.TestData) isolation.Level {
	isoS, ok := dd.ScanArgOpt[string](t, d, "iso")
	if !ok {
		return isolation.Serializable
	}
	switch isoS {
	case "serializable":
		return isolation.Serializable
	case "snapshot":
		return isolation.Snapshot
	case "read-committed":
		return isolation.ReadCommitted
	default:
		d.Fatalf(t, "unknown isolation level: %s", isoS)
		return 0
	}
}

func ScanLockStrength(t *testing.T, d *datadriven.TestData) lock.Strength {
	return GetStrength(t, d, dd.ScanArg[string](t, d, "strength"))
}

func GetStrength(t *testing.T, d *datadriven.TestData, strS string) lock.Strength {
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

func ScanIgnoredSeqNumbers(t *testing.T, d *datadriven.TestData) []enginepb.IgnoredSeqNumRange {
	seqsStr, ok := dd.ScanArgOpt[string](t, d, "ignored-seqs")
	if !ok {
		return nil
	}
	var ignored []enginepb.IgnoredSeqNumRange
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

		ignored = enginepb.TxnSeqListAppend(ignored, ignoredRange)
	}
	return ignored
}

func intentsToResolveToStr(g lockTableGuard, startOnNewLine bool) string {
	// Depending on whether intent resolution is virtual or not, the intents to
	// resolve are returned from either of the following functions.
	toResolve := append(g.IntentsToResolveVirtually(), g.ResolveBeforeScanning()...)
	if len(toResolve) == 0 {
		return ""
	}
	var buf strings.Builder
	if startOnNewLine {
		fmt.Fprintf(&buf, "\n")
	}
	fmt.Fprintf(&buf, "Intents to resolve:")
	for i := range toResolve {
		txn := toResolve[i].Txn
		fmt.Fprintf(&buf, "\n key=%s txn=%s epoch=%d ts=%s status=%s", toResolve[i].Key,
			txn.ID, txn.Epoch, txn.WriteTimestamp, toResolve[i].Status)
	}
	return buf.String()
}

func newLock(txn *enginepb.TxnMeta, key roachpb.Key, str lock.Strength) *roachpb.Lock {
	l := roachpb.MakeLock(txn, key, str)
	return &l
}

func TestLockTableMaxLocks(t *testing.T) {
	ctx := context.Background()
	m := TestingMakeLockTableMetricsCfg()
	lt := newLockTable(
		5, roachpb.RangeID(3), hlc.NewClockForTesting(nil), cluster.MakeTestingClusterSettings(),
		m.LocksShedDueToMemoryLimit, m.NumLockShedDueToMemoryLimitEvents,
	)
	lt.lockTableLimitsMu.minKeysLocked = 0
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
		ba := &kvpb.BatchRequest{}
		ba.Timestamp = hlc.Timestamp{WallTime: 1}
		req := Request{
			Timestamp:  ba.Timestamp,
			LatchSpans: latchSpans,
			LockSpans:  lockSpans,
			Batch:      ba,
		}
		reqs = append(reqs, req)
		ltg, err := lt.ScanAndEnqueue(ctx, req, nil)
		require.Nil(t, err)
		require.Nil(t, ltg.ResolveBeforeScanning())
		require.False(t, ltg.ShouldWait())
		guards = append(guards, ltg)
	}
	txnMeta := enginepb.TxnMeta{
		ID:             uuid.MakeV4(),
		WriteTimestamp: hlc.Timestamp{WallTime: 10},
	}
	// Sanity check counters at the start before we start adding locks.
	require.Equal(t, int64(0), m.NumLockShedDueToMemoryLimitEvents.Count())
	require.Equal(t, int64(0), m.LocksShedDueToMemoryLimit.Count())
	for i := range guards {
		for j := 0; j < 10; j++ {
			k := i*20 + j
			added, err := lt.AddDiscoveredLock(
				ctx, newLock(&txnMeta, keys[k], lock.Intent),
				0, false, guards[i])
			require.True(t, added)
			require.NoError(t, err)
		}
	}
	// Only the notRemovable locks survive after addition.
	require.Equal(t, int64(10), lt.lockCountForTesting())
	// The other 90 locks should be shed.
	require.Equal(t, int64(90), m.LocksShedDueToMemoryLimit.Count())
	require.Equal(t, int64(66), m.NumLockShedDueToMemoryLimitEvents.Count())
	// Two guards are dequeued. This marks 2 notRemovable locks as removable.
	// We're at 8 notRemovable locks now.
	lt.Dequeue(ctx, guards[0])
	lt.Dequeue(ctx, guards[1])
	require.Equal(t, int64(10), lt.lockCountForTesting())
	// Two guards do ScanAndEnqueue. This marks 2 notRemovable locks as
	// removable. We're at 6 notRemovable locks now.
	for i := 2; i < 4; i++ {
		var err *Error
		guards[i], err = lt.ScanAndEnqueue(ctx, reqs[i], guards[i])
		require.Nil(t, err)
		require.True(t, guards[i].ShouldWait())
	}
	require.Equal(t, int64(10), lt.lockCountForTesting())
	// Add another discovered lock, to trigger tryClearLocks.
	added, err := lt.AddDiscoveredLock(
		ctx, newLock(&txnMeta, keys[9*20+10], lock.Intent),
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// The 6 notRemovable locks remain.
	require.Equal(t, int64(6), lt.lockCountForTesting())
	// NB: 4 locks that became removable are cleared + the lock that was added by
	// AddDiscoveredLock, taking the total number of locks removed to 5.
	require.Equal(t, int64(95), m.LocksShedDueToMemoryLimit.Count())
	require.Equal(t, int64(67), m.NumLockShedDueToMemoryLimitEvents.Count())
	require.Equal(t, int64(101), int64(lt.locks.lockIDSeqNum))
	// Add another discovered lock, to trigger tryClearLocks.
	added, err = lt.AddDiscoveredLock(
		ctx, newLock(&txnMeta, keys[9*20+11], lock.Intent),
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// Still the 6 notRemovable locks remain.
	require.Equal(t, int64(6), lt.lockCountForTesting())
	// NB: We cleared the lock added by AddDiscoveredLock above.
	require.Equal(t, int64(96), m.LocksShedDueToMemoryLimit.Count())
	require.Equal(t, int64(68), m.NumLockShedDueToMemoryLimitEvents.Count())
	// Two more guards are dequeued, so we are down to 4 notRemovable locks.
	lt.Dequeue(ctx, guards[4])
	lt.Dequeue(ctx, guards[5])
	// Bump up the enforcement interval manually.
	lt.lockTableLimitsMu.lockAddMaxLocksCheckInterval = 2
	// Add another discovered lock.
	added, err = lt.AddDiscoveredLock(
		ctx, newLock(&txnMeta, keys[9*20+12], lock.Intent),
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// This notRemovable=false lock is also added, since enforcement not done.
	require.Equal(t, int64(7), lt.lockCountForTesting())
	// Metrics shouldn't change as enforcement wasn't done.
	require.Equal(t, int64(96), m.LocksShedDueToMemoryLimit.Count())
	require.Equal(t, int64(68), m.NumLockShedDueToMemoryLimitEvents.Count())
	// Add another discovered lock, to trigger tryClearLocks.
	added, err = lt.AddDiscoveredLock(
		ctx, newLock(&txnMeta, keys[9*20+13], lock.Intent),
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// Now enforcement is done, so only 4 remain.
	require.Equal(t, int64(4), lt.lockCountForTesting())
	// We made 2 locks removable above + added to locks by calling
	// AddDiscoveredLocks, resulting in 4 locks being cleared in total.
	require.Equal(t, int64(100), m.LocksShedDueToMemoryLimit.Count())
	require.Equal(t, int64(69), m.NumLockShedDueToMemoryLimitEvents.Count())
	// Bump down the enforcement interval manually, and bump up minKeysLocked.
	lt.lockTableLimitsMu.lockAddMaxLocksCheckInterval = 1
	lt.lockTableLimitsMu.minKeysLocked = 2
	// Three more guards dequeued. Now only 1 lock is notRemovable.
	lt.Dequeue(ctx, guards[6])
	lt.Dequeue(ctx, guards[7])
	lt.Dequeue(ctx, guards[8])
	// Add another discovered lock.
	added, err = lt.AddDiscoveredLock(
		ctx, newLock(&txnMeta, keys[9*20+14], lock.Intent),
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	require.Equal(t, int64(5), lt.lockCountForTesting())
	// NB: We're allowed 5 locks. We won't trigger tryClearLocks and the metrics
	// should reflect this.
	require.Equal(t, int64(100), m.LocksShedDueToMemoryLimit.Count())
	require.Equal(t, int64(69), m.NumLockShedDueToMemoryLimitEvents.Count())
	// Add another discovered lock, to trigger tryClearLocks, and push us over 5
	// locks.
	added, err = lt.AddDiscoveredLock(
		ctx, newLock(&txnMeta, keys[9*20+15], lock.Intent),
		0, false, guards[9])
	require.True(t, added)
	require.NoError(t, err)
	// Enforcement keeps the 1 notRemovable lock, and another, since
	// minKeysLocked=2.
	require.Equal(t, int64(2), lt.lockCountForTesting())
	// Which means that in total, 4 more locks are cleared.
	require.Equal(t, int64(104), m.LocksShedDueToMemoryLimit.Count())
	require.Equal(t, int64(70), m.NumLockShedDueToMemoryLimitEvents.Count())
	// Restore minKeysLocked to 0.
	lt.lockTableLimitsMu.minKeysLocked = 0
	// Add locks to push us over 5 locks.
	for i := 16; i < 20; i++ {
		added, err = lt.AddDiscoveredLock(
			ctx, newLock(&txnMeta, keys[9*20+i], lock.Intent),
			0, false, guards[9])
		require.True(t, added)
		require.NoError(t, err)
	}
	// Only the 1 notRemovable lock remains.
	require.Equal(t, int64(1), lt.lockCountForTesting())
	// Locks were cleared when we got to 6 locks (and 5 of them were cleared) as
	// one of them is notRemovable.
	require.Equal(t, int64(109), m.LocksShedDueToMemoryLimit.Count())
	require.Equal(t, int64(71), m.NumLockShedDueToMemoryLimitEvents.Count())
}

// TestLockTableMaxLocksWithMultipleNotRemovableRefs tests the notRemovable
// ref counting.
func TestLockTableMaxLocksWithMultipleNotRemovableRefs(t *testing.T) {
	ctx := context.Background()
	m := TestingMakeLockTableMetricsCfg()
	lt := newLockTable(
		2, roachpb.RangeID(3), hlc.NewClockForTesting(nil), cluster.MakeTestingClusterSettings(),
		m.LocksShedDueToMemoryLimit, m.NumLockShedDueToMemoryLimitEvents,
	)
	lt.lockTableLimitsMu.minKeysLocked = 0
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
		ba := &kvpb.BatchRequest{}
		ba.Timestamp = hlc.Timestamp{WallTime: 1}
		req := Request{
			Timestamp:  ba.Timestamp,
			LatchSpans: latchSpans,
			LockSpans:  lockSpans,
			Batch:      ba,
		}
		ltg, err := lt.ScanAndEnqueue(ctx, req, nil)
		require.Nil(t, err)
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
			ctx, newLock(&txnMeta, keys[i/2], lock.Intent),
			0, false, guards[i])
		require.True(t, added)
		require.NoError(t, err)
	}
	// All the 3 locks are there.
	require.Equal(t, int64(3), lt.lockCountForTesting())
	// Remove one of the notRemovable refs from each lock.
	for i := 0; i < 6; i++ {
		if i%2 == 0 {
			lt.Dequeue(ctx, guards[i])
		}
	}
	// Add another lock using request 6.
	added, err := lt.AddDiscoveredLock(
		ctx, newLock(&txnMeta, keys[6/2], lock.Intent),
		0, false, guards[6])
	require.True(t, added)
	require.NoError(t, err)
	// There are 4 locks.
	require.Equal(t, int64(4), lt.lockCountForTesting())
	// Remove the remaining notRemovable refs.
	for i := 0; i < 6; i++ {
		if i%2 == 1 {
			lt.Dequeue(ctx, guards[i])
		}
	}
	lt.Dequeue(ctx, guards[6])
	// There are still 4 locks since tryClearLocks has not happened since the
	// ref counts went to 0.
	require.Equal(t, int64(4), lt.lockCountForTesting())
	// Add another lock using request 8.
	added, err = lt.AddDiscoveredLock(
		ctx, newLock(&txnMeta, keys[8/2], lock.Intent),
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
	locksToAcquire []lockToAcquire

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
		defer func() {
			if lg != nil {
				e.lm.Release(ctx, lg)
				lg = nil
			}
			if g != nil {
				e.lt.Dequeue(ctx, g)
				g = nil
			}
		}()
		var timer timeutil.Timer
		defer timer.Stop()
		var err error
		for {
			// Since we can't do a select involving latch acquisition and context
			// cancellation, the code makes sure to release latches when returning
			// early due to error. Otherwise other requests will get stuck and
			// group.Wait() will not return until the test times out.
			lg, err = e.lm.Acquire(ctx, item.request.LatchSpans, poison.Policy_Error, item.request.Batch)
			if err != nil {
				return err
			}
			var kvErr *Error
			g, kvErr = e.lt.ScanAndEnqueue(ctx, *item.request, g)
			if kvErr != nil {
				return kvErr.GoError()
			}
			if !g.ShouldWait() {
				break
			}
			e.lm.Release(ctx, lg)
			lg = nil
			var lastID uuid.UUID
		L:
			for {
				timer.Reset(time.Minute * 5)
				select {
				case <-g.NewStateChan():
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
					return errors.AssertionFailedf(
						"request %d has been waiting for more than 5 minutes; lock table state:\n%s\n",
						g.(*lockTableGuardImpl).seqNum,
						e.lt.String(),
					)
				}
				state, err := g.CurState()
				if err != nil {
					return err
				}
				switch state.kind {
				case doneWaiting:
					if !lastID.Equal(uuid.UUID{}) && item.request.Txn != nil {
						_, err = e.waitingFor(item.request.Txn.ID, lastID, uuid.UUID{})
						if err != nil {
							return err
						}
					}
					break L
				case waitSelf:
					if item.request.Txn == nil {
						return errors.Errorf("non-transactional request cannot waitSelf")
					}
				case waitFor, waitElsewhere:
					if item.request.Txn != nil {
						var aborted bool
						aborted, err = e.waitingFor(item.request.Txn.ID, lastID, state.txn.ID)
						if !aborted {
							lastID = state.txn.ID
						}
						if aborted {
							return err
						}
					}
				default:
					return errors.Errorf("unexpected state: %v", state.kind)
				}
			}
		}

		// Acquire locks.
		for _, toAcq := range item.locksToAcquire {
			err = e.acquireLock(item.request.Txn, toAcq)
			if err != nil {
				break
			}
		}
		return err
	}
	for i := range item.intents {
		if err := e.lt.UpdateLocks(ctx, &item.intents[i]); err != nil {
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
	locksToAcquire []lockToAcquire

	// Non-empty when transaction should release locks.
	finish uuid.UUID
}

// lockToAcquire is a lock that should be acquired by a request.
type lockToAcquire struct {
	key roachpb.Key
	str lock.Strength
	dur lock.Durability
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
	lm *spanlatch.Manager
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
	clock := hlc.NewClockForTesting(nil)
	settings := cluster.MakeTestingClusterSettings()
	lm := spanlatch.Make(
		nil, /* stopper */
		nil, /* slowReqs */
		settings,
		nil, /* latchWaitDurations */
		clock,
	)
	m := TestingMakeLockTableMetricsCfg()
	ltImpl := newLockTable(maxLocks, roachpb.RangeID(3), clock, settings,
		m.LocksShedDueToMemoryLimit, m.NumLockShedDueToMemoryLimitEvents)
	ltImpl.enabled = true
	lt := maybeWrapInVerifyingLockTable(ltImpl)
	ex := &workloadExecutor{
		lm:           &lm,
		lt:           lt,
		items:        items,
		transactions: make(map[uuid.UUID]*transactionState),
		doneWork:     make(chan *workItem),
		concurrency:  concurrency,
	}
	return ex
}

func (e *workloadExecutor) acquireLock(txn *roachpb.Transaction, toAcq lockToAcquire) error {
	ctx := context.Background()
	acq := roachpb.MakeLockAcquisition(
		txn.TxnMeta, toAcq.key, toAcq.dur, toAcq.str, txn.IgnoredSeqNums,
	)
	err := e.lt.AcquireLock(ctx, &acq)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	tstate, ok := e.transactions[txn.ID]
	if !ok {
		return errors.Errorf("testbug: lock acquiring request with txnID %v has no transaction", txn.ID)
	}
	tstate.acquiredLocks = append(tstate.acquiredLocks, toAcq.key)
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
	strs := []lock.Strength{lock.None, lock.Shared, lock.Exclusive, lock.Intent}

	const numKeys = 2
	var items []workloadItem
	var startedTxnIDs []uuid.UUID // inefficient queue, but ok for a test.
	const maxStartedTxns = 10
	const numRequests = 10000
	for i := 0; i < numRequests; i++ {
		ts := timestamps[rand.IntN(len(timestamps))]
		keysPerm := rand.Perm(len(keys))
		latchSpans := &spanset.SpanSet{}
		lockSpans := &lockspanset.LockSpanSet{}
		for i := 0; i < numKeys; i++ {
			span := roachpb.Span{Key: keys[keysPerm[i]]}
			str := strs[rand.IntN(len(strs))]
			sa, latchTs := latchAccessForLockStrength(str, ts)
			latchSpans.AddMVCC(sa, span, latchTs)
			lockSpans.Add(str, span)
		}
		var txn *roachpb.Transaction
		if rand.IntN(2) == 0 {
			txn = &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					ID:             nextUUID(&txnCounter),
					WriteTimestamp: ts,
				},
				ReadTimestamp: ts,
			}
		}
		ba := &kvpb.BatchRequest{}
		ba.Txn = txn
		ba.Timestamp = ts
		request := &Request{
			Txn:        ba.Txn,
			Timestamp:  ba.Timestamp,
			LatchSpans: latchSpans,
			LockSpans:  lockSpans,
			Batch:      ba,
		}
		items = append(items, workloadItem{request: request})
		if txn != nil {
			startedTxnIDs = append(startedTxnIDs, txn.ID)
		}
		randomMaxStartedTxns := rand.IntN(maxStartedTxns)
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

// TestLockTableConcurrentRequests is a general randomized test for the lock
// table.
func TestLockTableConcurrentRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	possibleNumRequests := []int{1000, 3000, 10000}
	possibleNumActiveTxns := []int{2, 4, 8, 16, 32}
	possibleProbTxnalReq := []float64{0.9, 1, 0.75}
	possibleProbCreateNewTxn := []float64{0.75, 0.25, 0.1}
	possibleProbOnlyRead := []float64{0.5, 0.25}

	numRequests := possibleNumRequests[rand.IntN(len(possibleNumRequests))]
	numActiveTxns := possibleNumActiveTxns[rand.IntN(len(possibleNumActiveTxns))]
	probTxnalReq := possibleProbTxnalReq[rand.IntN(len(possibleProbTxnalReq))] // rest will be non-txnal
	probCreateNewTxn := possibleProbCreateNewTxn[rand.IntN(len(possibleProbTxnalReq))]
	probDupAccessWithWeakerStr := 0.5
	probOnlyRead := possibleProbOnlyRead[rand.IntN(len(possibleProbOnlyRead))]

	if syncutil.DeadlockEnabled || util.RaceEnabled {
		// We've seen 10,000 requests to be too much when running a deadlock/race
		// build. Override numRequests to the lowest option (1,000) for such builds.
		numRequests = 1000
	}

	testLockTableConcurrentRequests(
		t, numActiveTxns, numRequests, probTxnalReq, probCreateNewTxn,
		probDupAccessWithWeakerStr, probOnlyRead,
	)
}

// testLockTableConcurrencyRequests runs a randomized test on the lock table
// with the specified number of transactions and requests. The caller can vary
// probabilities of various randomized parameters, such as:
// - the probability of creating transactional requests (as opposed to
// non-transactional ones).
// - the probability of creating a new transaction in favor of using an existing
// one.
// - probability of accessing a key that's being locked with duplicate access.
// - probability of creating read-only requests.
func testLockTableConcurrentRequests(
	t *testing.T,
	numActiveTxns int,
	numRequests int,
	probTxnalReq float64,
	probCreateNewTxn float64,
	probDupAccessWithWeakerStr float64,
	probOnlyRead float64,
) {
	t.Logf(
		"numRequests: %d; numActiveTxns: %d; probability(txn-al reqs): %.2f; "+
			"probability(new txns): %.2f; probability(duplicate access): %.2f",
		numRequests, numActiveTxns, probTxnalReq, probCreateNewTxn, probDupAccessWithWeakerStr,
	)
	txnCounter := uint128.FromInts(0, 0)
	numTxnsCreated := 0
	var timestamps []hlc.Timestamp
	for i := 0; i < 2; i++ {
		timestamps = append(timestamps, hlc.Timestamp{WallTime: int64(i + 1)})
	}
	var keys []roachpb.Key
	for i := 0; i < 10; i++ {
		keys = append(keys, roachpb.Key(string(rune('a'+i))))
	}
	strs := []lock.Strength{lock.None, lock.Shared, lock.Exclusive, lock.Intent}
	activeTxns := make([]*enginepb.TxnMeta, 0, numActiveTxns)
	var items []workloadItem
	for i := 0; i < numRequests; i++ {
		var txnMeta *enginepb.TxnMeta
		var ts hlc.Timestamp

		if rand.Float64() < probTxnalReq {
			// Transactional request.
			shouldCreateNewTxn := len(activeTxns) < numActiveTxns || rand.Float64() < probCreateNewTxn
			if shouldCreateNewTxn {
				numTxnsCreated++
				ts = timestamps[rand.IntN(len(timestamps))]
				txnMeta = &enginepb.TxnMeta{
					ID:             nextUUID(&txnCounter),
					WriteTimestamp: ts,
				}
				if len(activeTxns) == numActiveTxns {
					txnIndex := rand.IntN(numActiveTxns)
					// We've reached the maximum number of active transactions the test
					// desires; replace an existing transaction.
					oldTxn := activeTxns[txnIndex]
					items = append(items, workloadItem{finish: oldTxn.ID})
					activeTxns[txnIndex] = txnMeta
				} else {
					activeTxns = append(activeTxns, txnMeta)
				}
			} else {
				txnIndex := rand.IntN(numActiveTxns)
				txnMeta = activeTxns[txnIndex]
				ts = txnMeta.WriteTimestamp
			}
		} else {
			// Create a non-transactional request.
			ts = timestamps[rand.IntN(len(timestamps))]
		}
		keysPerm := rand.Perm(len(keys))
		latchSpans := &spanset.SpanSet{}
		lockSpans := &lockspanset.LockSpanSet{}
		onlyReads := txnMeta == nil && rand.Float64() < probOnlyRead
		numKeys := rand.IntN(len(keys)-1) + 1
		ba := &kvpb.BatchRequest{}
		ba.Timestamp = ts
		request := &Request{
			Timestamp:  ba.Timestamp,
			LatchSpans: latchSpans,
			LockSpans:  lockSpans,
			Batch:      ba,
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

			str := lock.None
			if !onlyReads && txnMeta != nil {
				// Randomly select a lock strength (including lock.None).
				str = strs[rand.IntN(len(strs))]
			}
			sa, latchTs := latchAccessForLockStrength(str, ts)
			latchSpans.AddMVCC(sa, span, latchTs)
			lockSpans.Add(str, span)
			if str != lock.None {
				// Randomly select a lock durability. Shared and Exclusive locks
				// can be unreplicated, but Intent can not.
				dur := lock.Replicated
				if str != lock.Intent && rand.IntN(2) == 0 {
					dur = lock.Unreplicated
				}
				toAcq := lockToAcquire{span.Key, str, dur}
				wi.locksToAcquire = append(wi.locksToAcquire, toAcq)
			}

			dupAccess := str != lock.None && // nothing weaker than lock.None
				rand.Float64() < probDupAccessWithWeakerStr

			if dupAccess {
				dupStr := lock.None // only thing weaker
				switch str {
				case lock.Shared:
				case lock.Exclusive:
					if rand.IntN(2) == 0 {
						dupStr = lock.Shared
					}
				case lock.Intent:
					rn := rand.IntN(3)
					if rn == 0 {
						dupStr = lock.Shared
					} else if rn == 1 {
						dupStr = lock.Exclusive
					}
				}
				sa, latchTs := latchAccessForLockStrength(dupStr, ts)
				latchSpans.AddMVCC(sa, span, latchTs)
				lockSpans.Add(dupStr, span)
			}
		}
		items = append(items, wi)
	}
	for i := range activeTxns {
		if txnMeta := activeTxns[i]; txnMeta != nil {
			items = append(items, workloadItem{finish: txnMeta.ID})
		}
	}
	t.Logf("txns creted: %d", numTxnsCreated) // avoid some multiplication
	concurrency := []int{numActiveTxns / 4, numActiveTxns, numActiveTxns * 2, numActiveTxns * 4}
	for _, c := range concurrency {
		if c == 0 {
			continue
		}
		t.Run(fmt.Sprintf("concurrency %d", c), func(t *testing.T) {
			exec := newWorkLoadExecutor(items, c)
			// TODO(arul): if the number of goroutines this test spawns becomes a
			// problem, we'll have to rethink some of the randomized parameters we're
			// giving it. In particular, as the number of pending transactions in the
			// system increases[*], very quickly a single guy becomes the bottleneck.
			// At that point, it's imperative for the test to make progress to uncork
			// this transaction -- otherwise, everything will block and we'll get a
			// timeout.
			//
			// [*] Note that this is an order of magnitude different from the number
			// of active transactions. Transactions will remain pending as long as
			// they have outstanding requests -- the number of pending transactions
			// can get quite high once things start to block.
			if err := exec.execute(false, numRequests); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type benchWorkItem struct {
	Request
	locksToAcquire []lockToAcquire
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
	ctx := context.Background()
	for {
		if lg, err = env.lm.Acquire(ctx, item.LatchSpans, poison.Policy_Error, item.Batch); err != nil {
			doneCh <- err
			return
		}
		var kvErr *Error
		g, kvErr = env.lt.ScanAndEnqueue(ctx, item.Request, g)
		if kvErr != nil {
			doneCh <- kvErr.GoError()
			return
		}
		atomic.AddUint64(env.numScanCalls, 1)
		if !g.ShouldWait() {
			break
		}
		if firstIter {
			atomic.AddUint64(env.numRequestsWaited, 1)
			firstIter = false
		}
		env.lm.Release(ctx, lg)
		for {
			<-g.NewStateChan()
			state, err := g.CurState()
			if err != nil {
				doneCh <- err
				return
			}
			if state.kind == doneWaiting {
				break
			}
		}
	}
	for _, toAcq := range item.locksToAcquire {
		acq := roachpb.MakeLockAcquisition(
			item.Txn.TxnMeta, toAcq.key, toAcq.dur, toAcq.str, item.Txn.IgnoredSeqNums,
		)
		if err = env.lt.AcquireLock(ctx, &acq); err != nil {
			doneCh <- err
			return
		}
	}
	env.lt.Dequeue(ctx, g)
	env.lm.Release(ctx, lg)
	if len(item.locksToAcquire) == 0 {
		doneCh <- nil
		return
	}
	// Release locks.
	if lg, err = env.lm.Acquire(ctx, item.LatchSpans, poison.Policy_Error, item.Batch); err != nil {
		doneCh <- err
		return
	}
	for _, toAcq := range item.locksToAcquire {
		intent := roachpb.LockUpdate{
			Span:   roachpb.Span{Key: toAcq.key},
			Txn:    item.Request.Txn.TxnMeta,
			Status: roachpb.COMMITTED,
		}
		if err = env.lt.UpdateLocks(ctx, &intent); err != nil {
			doneCh <- err
			return
		}
	}
	env.lm.Release(ctx, lg)
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
	ba := &kvpb.BatchRequest{}
	ba.Timestamp = ts
	wi := benchWorkItem{
		Request: Request{
			Timestamp:  ts,
			LatchSpans: latchSpans,
			LockSpans:  lockSpans,
			Batch:      ba,
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
			toAcq := lockToAcquire{key, lock.Exclusive, lock.Unreplicated}
			lockSpans.Add(toAcq.str, span)
			wi.locksToAcquire = append(wi.locksToAcquire, toAcq)
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
						clock := hlc.NewClockForTesting(nil)
						settings := cluster.MakeTestingClusterSettings()
						lm := spanlatch.Make(
							nil /* stopper */, nil /* slowReqs */, settings, nil /* latchWaitDurations */, clock,
						)
						m := TestingMakeLockTableMetricsCfg()
						lt := newLockTable(maxLocks, roachpb.RangeID(3), clock, settings,
							m.LocksShedDueToMemoryLimit, m.NumLockShedDueToMemoryLimitEvents,
						)
						lt.enabled = true
						env := benchEnv{
							lm:                &lm,
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
							log.KvExec.Infof(context.Background(), "num requests that waited: %d, num scan calls: %d\n",
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
			m := TestingMakeLockTableMetricsCfg()
			lt := newLockTable(
				maxLocks,
				roachpb.RangeID(3),
				hlc.NewClockForTesting(nil),
				cluster.MakeTestingClusterSettings(),
				m.LocksShedDueToMemoryLimit,
				m.NumLockShedDueToMemoryLimitEvents,
			)
			lt.enabled = true

			ctx := context.Background()
			txn := &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{ID: uuid.MakeV4()},
			}
			for i := 0; i < locks; i++ {
				k := roachpb.Key(fmt.Sprintf("%03d", i))
				acq := roachpb.MakeLockAcquisition(
					txn.TxnMeta, k, lock.Unreplicated, lock.Exclusive, txn.IgnoredSeqNums,
				)
				err := lt.AcquireLock(ctx, &acq)
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

func TestKeyLocksSafeFormat(t *testing.T) {
	l := &keyLocks{
		id:     1,
		key:    []byte("KEY"),
		endKey: []byte("END"),
	}
	holder := &txnLock{}
	l.holders.PushFront(holder)
	holder.txn = &enginepb.TxnMeta{ID: uuid.NamespaceDNS}
	holder.unreplicatedInfo.init()
	holder.unreplicatedInfo.ts = hlc.Timestamp{WallTime: 123, Logical: 7}
	require.NoError(t, holder.unreplicatedInfo.acquire(lock.Exclusive, 1, nil))
	require.NoError(t, holder.unreplicatedInfo.acquire(lock.Shared, 3, nil))
	replTS := hlc.Timestamp{WallTime: 125, Logical: 1}
	holder.replicatedInfo.acquire(lock.Intent, replTS)
	holder.replicatedInfo.acquire(lock.Shared, replTS)
	require.EqualValues(t,
		" lock: ‹\"KEY\"›\n  holder: txn: 6ba7b810-9dad-11d1-80b4-00c04fd430c8 epoch: 0, iso: Serializable, ts: 0.000000125,1, info: repl [Intent, Shared], unrepl [(str: Exclusive seq: 1), (str: Shared seq: 3)]\n",
		redact.Sprint(l))
	require.EqualValues(t,
		" lock: ‹×›\n  holder: txn: 6ba7b810-9dad-11d1-80b4-00c04fd430c8 epoch: 0, iso: Serializable, ts: 0.000000125,1, info: repl [Intent, Shared], unrepl [(str: Exclusive seq: 1), (str: Shared seq: 3)]\n",
		redact.Sprint(l).Redact())
}

// TestKeyLocksSafeFormatMultipleLockHolders ensures multiple lock holders on
// a single key are correctly formatted.
func TestKeyLocksSafeFormatMultipleLockHolders(t *testing.T) {
	kl := &keyLocks{
		id:     1,
		key:    []byte("KEY"),
		endKey: []byte("END"),
	}
	holder1 := &txnLock{}
	holder2 := &txnLock{}
	kl.holders.PushBack(holder1)
	kl.holders.PushBack(holder2)
	holder1.txn = &enginepb.TxnMeta{ID: uuid.NamespaceDNS}
	holder1.unreplicatedInfo.init()
	holder1.unreplicatedInfo.ts = hlc.Timestamp{WallTime: 123, Logical: 7}
	require.NoError(t, holder1.unreplicatedInfo.acquire(lock.Shared, 3, nil))
	holder2.txn = &enginepb.TxnMeta{ID: uuid.NamespaceURL}
	holder2.unreplicatedInfo.init()
	holder2.unreplicatedInfo.ts = hlc.Timestamp{WallTime: 125, Logical: 1}
	require.NoError(t, holder2.unreplicatedInfo.acquire(lock.Shared, 6, nil))
	require.EqualValues(t,
		" lock: ‹\"KEY\"›\n"+
			"  holders: txn: 6ba7b810-9dad-11d1-80b4-00c04fd430c8 epoch: 0, iso: Serializable, info: unrepl [(str: Shared seq: 3)]\n"+
			"           txn: 6ba7b811-9dad-11d1-80b4-00c04fd430c8 epoch: 0, iso: Serializable, info: unrepl [(str: Shared seq: 6)]\n",
		redact.Sprint(kl))
	require.EqualValues(t,
		" lock: ‹×›\n"+
			"  holders: txn: 6ba7b810-9dad-11d1-80b4-00c04fd430c8 epoch: 0, iso: Serializable, info: unrepl [(str: Shared seq: 3)]\n"+
			"           txn: 6ba7b811-9dad-11d1-80b4-00c04fd430c8 epoch: 0, iso: Serializable, info: unrepl [(str: Shared seq: 6)]\n",
		redact.Sprint(kl).Redact())
}

// TestLockStateSafeFormatWaitQueue ensures requests waiting in a lock's wait
// queue are formatted correctly.
func TestKeyLocksSafeFormatWaitQueue(t *testing.T) {
	kl := &keyLocks{
		id:     1,
		key:    []byte("KEY"),
		endKey: []byte("END"),
	}
	holder := &txnLock{}
	kl.holders.PushBack(holder)
	holder.txn = &enginepb.TxnMeta{ID: uuid.NamespaceDNS}
	holder.unreplicatedInfo.init()
	holder.unreplicatedInfo.ts = hlc.Timestamp{WallTime: 123, Logical: 7}
	require.NoError(t, holder.unreplicatedInfo.acquire(lock.Shared, 3, nil))
	waiter := queuedGuard{
		guard:  newLockTableGuardImpl(),
		active: true,
		order:  queueOrder{isPromoting: true, reqSeqNum: 11},
		mode:   lock.MakeModeIntent(hlc.Timestamp{WallTime: 123, Logical: 7}),
	}
	waiter.guard.txn = &roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{ID: uuid.NamespaceDNS},
	}
	kl.queuedLockingRequests.PushBack(&waiter)
	require.EqualValues(t,
		" lock: ‹\"KEY\"›\n"+
			"  holder: txn: 6ba7b810-9dad-11d1-80b4-00c04fd430c8 epoch: 0, iso: Serializable, info: unrepl [(str: Shared seq: 3)]\n"+
			"   queued locking requests:\n"+
			"    active: true req: 11 promoting: true, strength: Intent, txn: 6ba7b810-9dad-11d1-80b4-00c04fd430c8\n",
		redact.Sprint(kl))
	require.EqualValues(t,
		" lock: ‹×›\n"+
			"  holder: txn: 6ba7b810-9dad-11d1-80b4-00c04fd430c8 epoch: 0, iso: Serializable, info: unrepl [(str: Shared seq: 3)]\n"+
			"   queued locking requests:\n"+
			"    active: true req: 11 promoting: true, strength: Intent, txn: 6ba7b810-9dad-11d1-80b4-00c04fd430c8\n",
		redact.Sprint(kl).Redact())
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
		"kind":                  includeWhenDeciding,
		"txn":                   includeWhenDeciding,
		"key":                   includeWhenDeciding,
		"held":                  includeWhenDeciding,
		"queuedLockingRequests": doNotIncludeWhenDeciding,
		"queuedReaders":         doNotIncludeWhenDeciding,
		"guardStrength":         doNotIncludeWhenDeciding,
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
