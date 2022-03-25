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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/concmon"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

// TestConcurrencyManagerBasic verifies that sequences of requests interacting
// with a concurrency manager perform properly.
//
// The input files use the following DSL:
//
// new-txn      name=<txn-name> ts=<int>[,<int>] epoch=<int> [uncertainty-limit=<int>[,<int>]]
// new-request  name=<req-name> txn=<txn-name>|none ts=<int>[,<int>] [priority] [inconsistent] [wait-policy=<policy>] [lock-timeout] [max-lock-wait-queue-length=<int>] [poison-policy=[err|wait]]
//   <proto-name> [<field-name>=<field-value>...] (hint: see scanSingleRequest)
// sequence     req=<req-name> [eval-kind=<pess|opt|pess-after-opt]
// poison       req=<req-name>
// finish       req=<req-name>
//
// handle-write-intent-error  req=<req-name> txn=<txn-name> key=<key> lease-seq=<seq>
// handle-txn-push-error      req=<req-name> txn=<txn-name> key=<key>  TODO(nvanbenschoten): implement this
//
// check-opt-no-conflicts req=<req-name>
//
// on-lock-acquired  req=<req-name> key=<key> [seq=<seq>] [dur=r|u]
// on-lock-updated   req=<req-name> txn=<txn-name> key=<key> status=[committed|aborted|pending] [ts=<int>[,<int>]]
// on-txn-updated    txn=<txn-name> status=[committed|aborted|pending] [ts=<int>[,<int>]]
//
// on-lease-updated  leaseholder=<bool> lease-seq=<seq>
// on-split
// on-merge
// on-snapshot-applied
//
// debug-latch-manager
// debug-lock-table
// debug-disable-txn-pushes
// debug-set-clock           ts=<secs>
// debug-advance-clock       ts=<secs>
// debug-set-discovered-locks-threshold-to-consult-finalized-txn-cache n=<count>
// debug-set-max-locks n=<count>
// reset
//
func TestConcurrencyManagerBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, testutils.TestDataPath(t, "concurrency_manager"), func(t *testing.T, path string) {
		c := newCluster()
		c.enableTxnPushes()
		m := concurrency.NewManager(c.makeConfig())
		m.OnRangeLeaseUpdated(1, true /* isLeaseholder */) // enable
		c.m = m
		mon := concmon.NewMonitor()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "new-txn":
				var txnName string
				d.ScanArgs(t, "name", &txnName)
				ts := scanTimestamp(t, d)

				var epoch int
				d.ScanArgs(t, "epoch", &epoch)

				uncertaintyLimit := ts
				if d.HasArg("uncertainty-limit") {
					uncertaintyLimit = scanTimestampWithName(t, d, "uncertainty-limit")
				}

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
						Priority:       1, // not min or max
					},
					ReadTimestamp:          ts,
					GlobalUncertaintyLimit: uncertaintyLimit,
				}
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
				if txn != nil {
					txn = txn.Clone()
					txn.ReadTimestamp = ts
					txn.WriteTimestamp = ts
				}

				readConsistency := roachpb.CONSISTENT
				if d.HasArg("inconsistent") {
					readConsistency = roachpb.INCONSISTENT
				}

				waitPolicy := scanWaitPolicy(t, d, false /* required */)

				var lockTimeout time.Duration
				if d.HasArg("lock-timeout") {
					// A lock timeout of 1ns will be considered immediately expired
					// without a delay by the lockTableWaiter, ensuring that the lock
					// timeout logic deterministically fires.
					// See (*lockTableWaiterImpl).timeUntilDeadline.
					lockTimeout = 1 * time.Nanosecond
				}

				var maxLockWaitQueueLength int
				if d.HasArg("max-lock-wait-queue-length") {
					d.ScanArgs(t, "max-lock-wait-queue-length", &maxLockWaitQueueLength)
				}

				pp := scanPoisonPolicy(t, d)

				// Each roachpb.Request is provided on an indented line.
				reqs, reqUnions := scanRequests(t, d, c)
				latchSpans, lockSpans := c.collectSpans(t, txn, ts, reqs)

				c.requestsByName[reqName] = concurrency.Request{
					Txn:       txn,
					Timestamp: ts,
					// TODO(nvanbenschoten): test Priority
					ReadConsistency:        readConsistency,
					WaitPolicy:             waitPolicy,
					LockTimeout:            lockTimeout,
					MaxLockWaitQueueLength: maxLockWaitQueueLength,
					Requests:               reqUnions,
					LatchSpans:             latchSpans,
					LockSpans:              lockSpans,
					PoisonPolicy:           pp,
				}
				return ""

			case "sequence":
				var reqName string
				d.ScanArgs(t, "req", &reqName)
				req, ok := c.requestsByName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}
				evalKind := concurrency.PessimisticEval
				if d.HasArg("eval-kind") {
					var kind string
					d.ScanArgs(t, "eval-kind", &kind)
					switch kind {
					case "pess":
						evalKind = concurrency.PessimisticEval
					case "opt":
						evalKind = concurrency.OptimisticEval
					case "pess-after-opt":
						evalKind = concurrency.PessimisticAfterFailedOptimisticEval
					default:
						d.Fatalf(t, "unknown eval-kind: %s", kind)
					}
				}

				// Copy the request's latch and lock spans before handing them to
				// SequenceReq, because they may be destroyed once handed to the
				// concurrency manager.
				req.LatchSpans = req.LatchSpans.Copy()
				req.LockSpans = req.LockSpans.Copy()

				c.mu.Lock()
				prev := c.guardsByReqName[reqName]
				delete(c.guardsByReqName, reqName)
				c.mu.Unlock()

				opName := fmt.Sprintf("sequence %s", reqName)
				mon.RunAsync(opName, func(ctx context.Context) {
					guard, resp, err := m.SequenceReq(ctx, prev, req, evalKind)
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
				return c.waitAndCollect(t, mon)

			case "finish":
				var reqName string
				d.ScanArgs(t, "req", &reqName)
				guard, ok := c.guardsByReqName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}

				opName := fmt.Sprintf("finish %s", reqName)
				mon.RunSync(opName, func(ctx context.Context) {
					log.Event(ctx, "finishing request")
					m.FinishReq(guard)
					c.mu.Lock()
					delete(c.guardsByReqName, reqName)
					c.mu.Unlock()
				})
				return c.waitAndCollect(t, mon)

			case "poison":
				var reqName string
				d.ScanArgs(t, "req", &reqName)
				guard, ok := c.guardsByReqName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}

				opName := fmt.Sprintf("poison %s", reqName)
				mon.RunSync(opName, func(ctx context.Context) {
					log.Event(ctx, "poisoning request")
					m.PoisonReq(guard)
				})
				return c.waitAndCollect(t, mon)

			case "handle-write-intent-error":
				var reqName string
				d.ScanArgs(t, "req", &reqName)
				prev, ok := c.guardsByReqName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}

				var leaseSeq int
				d.ScanArgs(t, "lease-seq", &leaseSeq)

				// Each roachpb.Intent is provided on an indented line.
				var intents []roachpb.Intent
				singleReqLines := strings.Split(d.Input, "\n")
				for _, line := range singleReqLines {
					var err error
					d.Cmd, d.CmdArgs, err = datadriven.ParseLine(line)
					if err != nil {
						d.Fatalf(t, "error parsing single intent: %v", err)
					}
					if d.Cmd != "intent" {
						d.Fatalf(t, "expected \"intent\", found %s", d.Cmd)
					}

					var txnName string
					d.ScanArgs(t, "txn", &txnName)
					txn, ok := c.txnsByName[txnName]
					if !ok {
						d.Fatalf(t, "unknown txn %s", txnName)
					}

					var key string
					d.ScanArgs(t, "key", &key)

					intents = append(intents, roachpb.MakeIntent(&txn.TxnMeta, roachpb.Key(key)))
				}

				opName := fmt.Sprintf("handle write intent error %s", reqName)
				mon.RunAsync(opName, func(ctx context.Context) {
					seq := roachpb.LeaseSequence(leaseSeq)
					wiErr := &roachpb.WriteIntentError{Intents: intents}
					guard, err := m.HandleWriterIntentError(ctx, prev, seq, wiErr)
					if err != nil {
						log.Eventf(ctx, "handled %v, returned error: %v", wiErr, err)
						c.mu.Lock()
						delete(c.guardsByReqName, reqName)
						c.mu.Unlock()
					} else {
						log.Eventf(ctx, "handled %v, released latches", wiErr)
						c.mu.Lock()
						c.guardsByReqName[reqName] = guard
						c.mu.Unlock()
					}
				})
				return c.waitAndCollect(t, mon)

			case "check-opt-no-conflicts":
				var reqName string
				d.ScanArgs(t, "req", &reqName)
				g, ok := c.guardsByReqName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}
				reqs, _ := scanRequests(t, d, c)
				latchSpans, lockSpans := c.collectSpans(t, g.Req.Txn, g.Req.Timestamp, reqs)
				return fmt.Sprintf("no-conflicts: %t", g.CheckOptimisticNoConflicts(latchSpans, lockSpans))

			case "on-lock-acquired":
				var reqName string
				d.ScanArgs(t, "req", &reqName)
				guard, ok := c.guardsByReqName[reqName]
				if !ok {
					d.Fatalf(t, "unknown request: %s", reqName)
				}
				txn := guard.Req.Txn

				var key string
				d.ScanArgs(t, "key", &key)

				var seq int
				if d.HasArg("seq") {
					d.ScanArgs(t, "seq", &seq)
				}
				seqNum := enginepb.TxnSeq(seq)

				dur := lock.Unreplicated
				if d.HasArg("dur") {
					dur = scanLockDurability(t, d)
				}

				// Confirm that the request has a corresponding write request.
				found := false
				for _, ru := range guard.Req.Requests {
					req := ru.GetInner()
					keySpan := roachpb.Span{Key: roachpb.Key(key)}
					if roachpb.IsLocking(req) &&
						req.Header().Span().Contains(keySpan) &&
						req.Header().Sequence == seqNum {
						found = true
						break
					}
				}
				if !found {
					d.Fatalf(t, "missing corresponding write request")
				}

				txnAcquire := txn.Clone()
				txnAcquire.Sequence = seqNum

				mon.RunSync("acquire lock", func(ctx context.Context) {
					log.Eventf(ctx, "txn %s @ %s", txn.ID.Short(), key)
					acq := roachpb.MakeLockAcquisition(txnAcquire, roachpb.Key(key), dur)
					m.OnLockAcquired(ctx, &acq)
				})
				return c.waitAndCollect(t, mon)

			case "on-lock-updated":
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

				status, verb := scanTxnStatus(t, d)
				var ts hlc.Timestamp
				if d.HasArg("ts") {
					ts = scanTimestamp(t, d)
				}

				// Confirm that the request has a corresponding ResolveIntent.
				found := false
				for _, ru := range guard.Req.Requests {
					if riReq := ru.GetResolveIntent(); riReq != nil &&
						riReq.IntentTxn.ID == txn.ID &&
						riReq.Key.Equal(roachpb.Key(key)) &&
						riReq.Status == status {
						found = true
						break
					}
				}
				if !found {
					d.Fatalf(t, "missing corresponding resolve intent request")
				}

				txnUpdate := txn.Clone()
				txnUpdate.Status = status
				txnUpdate.WriteTimestamp.Forward(ts)

				mon.RunSync("update lock", func(ctx context.Context) {
					log.Eventf(ctx, "%s txn %s @ %s", verb, txn.ID.Short(), key)
					span := roachpb.Span{Key: roachpb.Key(key)}
					up := roachpb.MakeLockUpdate(txnUpdate, span)
					m.OnLockUpdated(ctx, &up)
				})
				return c.waitAndCollect(t, mon)

			case "on-txn-updated":
				var txnName string
				d.ScanArgs(t, "txn", &txnName)
				txn, ok := c.txnsByName[txnName]
				if !ok {
					d.Fatalf(t, "unknown txn %s", txnName)
				}

				status, verb := scanTxnStatus(t, d)
				var ts hlc.Timestamp
				if d.HasArg("ts") {
					ts = scanTimestamp(t, d)
				}

				mon.RunSync("update txn", func(ctx context.Context) {
					log.Eventf(ctx, "%s %s", verb, txnName)
					if err := c.updateTxnRecord(txn.ID, status, ts); err != nil {
						d.Fatalf(t, err.Error())
					}
				})
				return c.waitAndCollect(t, mon)

			case "on-lease-updated":
				var isLeaseholder bool
				d.ScanArgs(t, "leaseholder", &isLeaseholder)

				var leaseSeq int
				d.ScanArgs(t, "lease-seq", &leaseSeq)

				mon.RunSync("transfer lease", func(ctx context.Context) {
					if isLeaseholder {
						log.Event(ctx, "acquired")
					} else {
						log.Event(ctx, "released")
					}
					m.OnRangeLeaseUpdated(roachpb.LeaseSequence(leaseSeq), isLeaseholder)
				})
				return c.waitAndCollect(t, mon)

			case "on-split":
				mon.RunSync("split range", func(ctx context.Context) {
					log.Event(ctx, "complete")
					m.OnRangeSplit()
				})
				return c.waitAndCollect(t, mon)

			case "on-merge":
				mon.RunSync("merge range", func(ctx context.Context) {
					log.Event(ctx, "complete")
					m.OnRangeMerge()
				})
				return c.waitAndCollect(t, mon)

			case "on-snapshot-applied":
				mon.RunSync("snapshot replica", func(ctx context.Context) {
					log.Event(ctx, "applied")
					m.OnReplicaSnapshotApplied()
				})
				return c.waitAndCollect(t, mon)

			case "debug-latch-manager":
				metrics := m.LatchMetrics()
				output := []string{
					fmt.Sprintf("write count: %d", metrics.WriteCount),
					fmt.Sprintf(" read count: %d", metrics.ReadCount),
				}
				return strings.Join(output, "\n")

			case "debug-lock-table":
				return m.TestingLockTableString()

			case "debug-disable-txn-pushes":
				c.disableTxnPushes()
				return ""

			case "debug-set-clock":
				var secs int
				d.ScanArgs(t, "ts", &secs)

				nanos := int64(secs) * time.Second.Nanoseconds()
				if nanos < c.manual.UnixNano() {
					d.Fatalf(t, "manual clock must advance")
				}
				c.manual.Set(nanos)
				return ""

			case "debug-advance-clock":
				var secs int
				d.ScanArgs(t, "ts", &secs)
				c.manual.Increment(int64(secs) * time.Second.Nanoseconds())
				return ""

			case "debug-set-discovered-locks-threshold-to-consult-finalized-txn-cache":
				var n int
				d.ScanArgs(t, "n", &n)
				c.setDiscoveredLocksThresholdToConsultFinalizedTxnCache(n)
				return ""

			case "debug-set-max-locks":
				var n int
				d.ScanArgs(t, "n", &n)
				m.TestingSetMaxLocks(int64(n))
				return ""

			case "reset":
				if n := mon.NumMonitored(); n > 0 {
					d.Fatalf(t, "%d requests still in flight", n)
				}
				mon = concmon.NewMonitor()
				if err := c.reset(); err != nil {
					d.Fatalf(t, "could not reset cluster: %v", err)
				}
				// Reset request and txn namespace?
				if d.HasArg("namespace") {
					c.resetNamespace()
				}
				return ""

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

func scanRequests(
	t *testing.T, d *datadriven.TestData, c *cluster,
) ([]roachpb.Request, []roachpb.RequestUnion) {
	// Each roachpb.Request is provided on an indented line.
	var reqs []roachpb.Request
	singleReqLines := strings.Split(d.Input, "\n")
	for _, line := range singleReqLines {
		req := scanSingleRequest(t, d, line, c.txnsByName)
		reqs = append(reqs, req)
	}
	reqUnions := make([]roachpb.RequestUnion, len(reqs))
	for i, req := range reqs {
		reqUnions[i].MustSetInner(req)
	}
	return reqs, reqUnions
}

// cluster encapsulates the state of a running cluster and a set of requests.
// It serves as the test harness in TestConcurrencyManagerBasic - maintaining
// transaction and request declarations, recording the state of in-flight
// requests as they flow through the concurrency manager, and mocking out the
// interfaces that the concurrency manager interacts with.
type cluster struct {
	nodeDesc  *roachpb.NodeDescriptor
	rangeDesc *roachpb.RangeDescriptor
	st        *clustersettings.Settings
	manual    *hlc.ManualClock
	clock     *hlc.Clock
	m         concurrency.Manager

	// Definitions.
	txnCounter     uint32
	txnsByName     map[string]*roachpb.Transaction
	requestsByName map[string]concurrency.Request

	// Request state. Cleared on reset.
	mu              syncutil.Mutex
	guardsByReqName map[string]*concurrency.Guard
	txnRecords      map[uuid.UUID]*txnRecord
	txnPushes       map[uuid.UUID]*txnPush
}

type txnRecord struct {
	mu               syncutil.Mutex
	sig              chan struct{}
	txn              *roachpb.Transaction // immutable, modify fields below
	updatedStatus    roachpb.TransactionStatus
	updatedTimestamp hlc.Timestamp
}

type txnPush struct {
	ctx            context.Context
	pusher, pushee uuid.UUID
	count          int
}

func newCluster() *cluster {
	manual := hlc.NewManualClock(123 * time.Second.Nanoseconds())
	return &cluster{
		nodeDesc:  &roachpb.NodeDescriptor{NodeID: 1},
		rangeDesc: &roachpb.RangeDescriptor{RangeID: 1},
		st:        clustersettings.MakeTestingClusterSettings(),
		manual:    manual,
		clock:     hlc.NewClock(manual.UnixNano, time.Nanosecond),

		txnsByName:      make(map[string]*roachpb.Transaction),
		requestsByName:  make(map[string]concurrency.Request),
		guardsByReqName: make(map[string]*concurrency.Guard),
		txnRecords:      make(map[uuid.UUID]*txnRecord),
		txnPushes:       make(map[uuid.UUID]*txnPush),
	}
}

func (c *cluster) makeConfig() concurrency.Config {
	return concurrency.Config{
		NodeDesc:       c.nodeDesc,
		RangeDesc:      c.rangeDesc,
		Settings:       c.st,
		Clock:          c.clock,
		IntentResolver: c,
		TxnWaitMetrics: txnwait.NewMetrics(time.Minute),
	}
}

// PushTransaction implements the concurrency.IntentResolver interface.
func (c *cluster) PushTransaction(
	ctx context.Context, pushee *enginepb.TxnMeta, h roachpb.Header, pushType roachpb.PushTxnType,
) (*roachpb.Transaction, *roachpb.Error) {
	pusheeRecord, err := c.getTxnRecord(pushee.ID)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	var pusherRecord *txnRecord
	if h.Txn != nil {
		pusherID := h.Txn.ID
		pusherRecord, err = c.getTxnRecord(pusherID)
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		push, err := c.registerPush(ctx, pusherID, pushee.ID)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		defer c.unregisterPush(push)
	}
	for {
		// Is the pushee pushed?
		pusheeTxn, pusheeRecordSig := pusheeRecord.asTxn()
		var pushed bool
		switch pushType {
		case roachpb.PUSH_TIMESTAMP:
			pushed = h.Timestamp.Less(pusheeTxn.WriteTimestamp) || pusheeTxn.Status.IsFinalized()
		case roachpb.PUSH_ABORT, roachpb.PUSH_TOUCH:
			pushed = pusheeTxn.Status.IsFinalized()
		default:
			return nil, roachpb.NewErrorf("unexpected push type: %s", pushType)
		}
		if pushed {
			return pusheeTxn, nil
		}
		// If PUSH_TOUCH, return error instead of waiting.
		if pushType == roachpb.PUSH_TOUCH {
			log.Eventf(ctx, "pushee not abandoned")
			err := roachpb.NewTransactionPushError(*pusheeTxn)
			return nil, roachpb.NewError(err)
		}
		// Or the pusher aborted?
		var pusherRecordSig chan struct{}
		if pusherRecord != nil {
			var pusherTxn *roachpb.Transaction
			pusherTxn, pusherRecordSig = pusherRecord.asTxn()
			if pusherTxn.Status == roachpb.ABORTED {
				log.Eventf(ctx, "detected pusher aborted")
				err := roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_PUSHER_ABORTED)
				return nil, roachpb.NewError(err)
			}
		}
		// Wait until either record is updated.
		select {
		case <-pusheeRecordSig:
		case <-pusherRecordSig:
		case <-ctx.Done():
			return nil, roachpb.NewError(ctx.Err())
		}
	}
}

// ResolveIntent implements the concurrency.IntentResolver interface.
func (c *cluster) ResolveIntent(
	ctx context.Context, intent roachpb.LockUpdate, _ intentresolver.ResolveOptions,
) *roachpb.Error {
	log.Eventf(ctx, "resolving intent %s for txn %s with %s status", intent.Key, intent.Txn.ID.Short(), intent.Status)
	c.m.OnLockUpdated(ctx, &intent)
	return nil
}

// ResolveIntents implements the concurrency.IntentResolver interface.
func (c *cluster) ResolveIntents(
	ctx context.Context, intents []roachpb.LockUpdate, opts intentresolver.ResolveOptions,
) *roachpb.Error {
	log.Eventf(ctx, "resolving a batch of %d intent(s)", len(intents))
	for _, intent := range intents {
		if err := c.ResolveIntent(ctx, intent, opts); err != nil {
			return err
		}
	}
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
	r := &txnRecord{txn: txn, sig: make(chan struct{})}
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
	// Notify all listeners. This is a poor man's composable cond var.
	close(r.sig)
	r.sig = make(chan struct{})
	return nil
}

func (r *txnRecord) asTxn() (*roachpb.Transaction, chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	txn := r.txn.Clone()
	if r.updatedStatus > txn.Status {
		txn.Status = r.updatedStatus
	}
	txn.WriteTimestamp.Forward(r.updatedTimestamp)
	return txn, r.sig
}

func (c *cluster) registerPush(ctx context.Context, pusher, pushee uuid.UUID) (*txnPush, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p, ok := c.txnPushes[pusher]; ok {
		if pushee != p.pushee {
			return nil, errors.Errorf("pusher %s can't push two txns %s and %s at the same time",
				pusher.Short(), pushee.Short(), p.pushee.Short(),
			)
		}
		p.count++
		return p, nil
	}
	p := &txnPush{
		ctx:    ctx,
		count:  1,
		pusher: pusher,
		pushee: pushee,
	}
	c.txnPushes[pusher] = p
	return p, nil
}

func (c *cluster) unregisterPush(push *txnPush) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.txnPushes[push.pusher]
	if !ok {
		return
	}
	p.count--
	if p.count == 0 {
		delete(c.txnPushes, push.pusher)
	}
	if p.count < 0 {
		panic(fmt.Sprintf("negative count: %+v", p))
	}
}

// detectDeadlocks looks at all in-flight transaction pushes and determines
// whether any are blocked due to dependency cycles within transactions. If so,
// the method logs an event on the contexts of each of the members of the cycle.
func (c *cluster) detectDeadlocks() {
	// This cycle detection algorithm it not particularly efficient - at worst
	// it runs in O(n ^ 2) time. However, it's simple and effective at assigning
	// each member of each cycle a unique view of the cycle that it's a part of.
	// This works because we currently only allow a transaction to push a single
	// other transaction at a time.
	c.mu.Lock()
	defer c.mu.Unlock()
	var chain []uuid.UUID
	seen := make(map[uuid.UUID]struct{})
	for orig, origPush := range c.txnPushes {
		pusher := orig
		chain = append(chain[:0], orig)
		for id := range seen {
			delete(seen, id)
		}
		seen[pusher] = struct{}{}
		for {
			push, ok := c.txnPushes[pusher]
			if !ok {
				break
			}
			pusher = push.pushee
			chain = append(chain, pusher)
			if _, ok := seen[pusher]; ok {
				// Cycle detected!
				if pusher == orig {
					// The cycle we were looking for (i.e. starting at orig).
					var chainBuf strings.Builder
					for i, id := range chain {
						if i > 0 {
							chainBuf.WriteString("->")
						}
						chainBuf.WriteString(id.Short())
					}
					log.Eventf(origPush.ctx, "dependency cycle detected %s", chainBuf.String())
				}
				break
			}
			seen[pusher] = struct{}{}
		}
	}
}

func (c *cluster) enableTxnPushes() {
	concurrency.LockTableLivenessPushDelay.Override(context.Background(), &c.st.SV, 0*time.Millisecond)
	concurrency.LockTableDeadlockDetectionPushDelay.Override(context.Background(), &c.st.SV, 0*time.Millisecond)
}

func (c *cluster) disableTxnPushes() {
	concurrency.LockTableLivenessPushDelay.Override(context.Background(), &c.st.SV, time.Hour)
	concurrency.LockTableDeadlockDetectionPushDelay.Override(context.Background(), &c.st.SV, time.Hour)
}

func (c *cluster) setDiscoveredLocksThresholdToConsultFinalizedTxnCache(n int) {
	concurrency.DiscoveredLocksThresholdToConsultFinalizedTxnCache.Override(context.Background(), &c.st.SV, int64(n))
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
	metrics := c.m.LatchMetrics()
	if metrics.ReadCount+metrics.WriteCount > 0 {
		return errors.Errorf("outstanding latches")
	}
	// Clear the lock table by transferring the lease away and reacquiring it.
	c.m.OnRangeLeaseUpdated(1, false /* isLeaseholder */)
	c.m.OnRangeLeaseUpdated(1, true /* isLeaseholder */)
	return nil
}

// resetNamespace resets the entire cluster namespace, clearing both request
// definitions and transaction definitions.
func (c *cluster) resetNamespace() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txnCounter = 0
	c.txnsByName = make(map[string]*roachpb.Transaction)
	c.requestsByName = make(map[string]concurrency.Request)
	c.txnRecords = make(map[uuid.UUID]*txnRecord)
}

// collectSpans collects the declared spans for a set of requests.
// Its logic mirrors that in Replica.collectSpans.
func (c *cluster) collectSpans(
	t *testing.T, txn *roachpb.Transaction, ts hlc.Timestamp, reqs []roachpb.Request,
) (latchSpans, lockSpans *spanset.SpanSet) {
	latchSpans, lockSpans = &spanset.SpanSet{}, &spanset.SpanSet{}
	h := roachpb.Header{Txn: txn, Timestamp: ts}
	for _, req := range reqs {
		if cmd, ok := batcheval.LookupCommand(req.Method()); ok {
			cmd.DeclareKeys(c.rangeDesc, &h, req, latchSpans, lockSpans, 0)
		} else {
			t.Fatalf("unrecognized command %s", req.Method())
		}
	}

	// Commands may create a large number of duplicate spans. De-duplicate
	// them to reduce the number of spans we pass to the spanlatch manager.
	for _, s := range [...]*spanset.SpanSet{latchSpans, lockSpans} {
		s.SortAndDedup()
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	return latchSpans, lockSpans
}

func (c *cluster) waitAndCollect(t *testing.T, m *concmon.Monitor) string {
	m.WaitForAsyncGoroutinesToStall(t)
	c.detectDeadlocks()
	return m.CollectRecordings()
}
