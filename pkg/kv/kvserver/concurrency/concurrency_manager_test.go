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
	"io"
	"io/ioutil"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/maruel/panicparse/v2/stack"
	"github.com/petermattis/goid"
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
// debug-set-discovered-locks-threshold-to-consult-finalized-txn-cache n=<count>
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
		mon := newMonitor()
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
				mon.runAsync(opName, func(ctx context.Context) {
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
				mon.runSync(opName, func(ctx context.Context) {
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
				mon.runSync(opName, func(ctx context.Context) {
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
				mon.runAsync(opName, func(ctx context.Context) {
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

				mon.runSync("acquire lock", func(ctx context.Context) {
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

				mon.runSync("update lock", func(ctx context.Context) {
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

				mon.runSync("update txn", func(ctx context.Context) {
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

				mon.runSync("transfer lease", func(ctx context.Context) {
					if isLeaseholder {
						log.Event(ctx, "acquired")
					} else {
						log.Event(ctx, "released")
					}
					m.OnRangeLeaseUpdated(roachpb.LeaseSequence(leaseSeq), isLeaseholder)
				})
				return c.waitAndCollect(t, mon)

			case "on-split":
				mon.runSync("split range", func(ctx context.Context) {
					log.Event(ctx, "complete")
					m.OnRangeSplit()
				})
				return c.waitAndCollect(t, mon)

			case "on-merge":
				mon.runSync("merge range", func(ctx context.Context) {
					log.Event(ctx, "complete")
					m.OnRangeMerge()
				})
				return c.waitAndCollect(t, mon)

			case "on-snapshot-applied":
				mon.runSync("snapshot replica", func(ctx context.Context) {
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

			case "debug-set-discovered-locks-threshold-to-consult-finalized-txn-cache":
				var n int
				d.ScanArgs(t, "n", &n)
				c.setDiscoveredLocksThresholdToConsultFinalizedTxnCache(n)
				return ""

			case "reset":
				if n := mon.numMonitored(); n > 0 {
					d.Fatalf(t, "%d requests still in flight", n)
				}
				mon.resetSeqNums()
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
		OnContentionEvent: func(ev *roachpb.ContentionEvent) {
			ev.Duration = 1234 * time.Millisecond // for determinism
		},
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

func (c *cluster) waitAndCollect(t *testing.T, m *monitor) string {
	m.waitForAsyncGoroutinesToStall(t)
	c.detectDeadlocks()
	return m.collectRecordings()
}

// monitor tracks a set of running goroutines as they execute and captures
// tracing recordings from them. It is capable of watching its set of goroutines
// until they all mutually stall.
//
// It is NOT safe to use multiple monitors concurrently.
type monitor struct {
	seq int
	gs  map[*monitoredGoroutine]struct{}
	tr  *tracing.Tracer
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
		tr: tracing.NewTracer(),
		gs: make(map[*monitoredGoroutine]struct{}),
	}
}

func (m *monitor) runSync(opName string, fn func(context.Context)) {
	ctx, sp := m.tr.StartSpanCtx(context.Background(), opName, tracing.WithRecording(tracing.RecordingVerbose))
	g := &monitoredGoroutine{
		opSeq:  0, // synchronous
		opName: opName,
		ctx:    ctx,
		collect: func() tracing.Recording {
			return sp.GetConfiguredRecording()
		},
		cancel: sp.Finish,
	}
	m.gs[g] = struct{}{}
	fn(ctx)
	atomic.StoreInt32(&g.finished, 1)
}

func (m *monitor) runAsync(opName string, fn func(context.Context)) (cancel func()) {
	m.seq++
	ctx, sp := m.tr.StartSpanCtx(context.Background(), opName, tracing.WithRecording(tracing.RecordingVerbose))
	g := &monitoredGoroutine{
		opSeq:  m.seq,
		opName: opName,
		ctx:    ctx,
		collect: func() tracing.Recording {
			return sp.GetConfiguredRecording()
		},
		cancel: sp.Finish,
	}
	m.gs[g] = struct{}{}
	go func() {
		atomic.StoreInt64(&g.gID, goid.Get())
		fn(ctx)
		atomic.StoreInt32(&g.finished, 1)
	}()
	return cancel
}

func (m *monitor) numMonitored() int {
	return len(m.gs)
}

func (m *monitor) resetSeqNums() {
	m.seq = 0
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
				if prev > 0 {
					prev--
					continue
				}
				logs = append(logs, logRecord{
					g: g, value: log.Msg().StripMarkers(),
				})
				g.prevEvents++
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
		logValue := stripFileLinePrefix(log.value)
		fmt.Fprintf(&buf, "[%s] %s: %s", seq, log.g.opName, logValue)
	}
	return buf.String()
}

func stripFileLinePrefix(s string) string {
	return reFileLinePrefix.ReplaceAllString(s, "")
}

var reFileLinePrefix = regexp.MustCompile(`^[^:]+:\d+ `)

func (m *monitor) hasNewEvents(g *monitoredGoroutine) bool {
	events := 0
	rec := g.collect()
	for _, span := range rec {
		events += len(span.Logs)
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
	// monitored goroutines to be stalled and also both observe the exact same
	// goroutine state. The goroutine dump provides a consistent snapshot of all
	// goroutine states and statuses (runtime.Stack(all=true) stops the world).
	var status []*stack.Goroutine
	filter := funcName((*monitor).runAsync)
	testutils.SucceedsSoon(t, func() error {
		// Add a small fixed delay after each iteration. This is sufficient to
		// prevents false detection of stalls in a few cases, like when
		// receiving on a buffered channel that already has an element in it.
		defer time.Sleep(5 * time.Millisecond)

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
		log.Eventf(g.ctx, "blocked on %s in %s.%s",
			stat.State, stalledCall.Func.DirName, stalledCall.Func.Name)
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
	b := stacks(buf)
	s, _, err := stack.ScanSnapshot(bytes.NewBuffer(b), ioutil.Discard, stack.DefaultOpts())
	if err != io.EOF {
		t.Fatalf("could not parse goroutine dump: %v", err)
		return nil
	}

	matching := s.Goroutines[:0]
	for _, g := range s.Goroutines {
		for _, call := range g.Stack.Calls {
			if strings.Contains(call.Func.Complete, filter) {
				matching = append(matching, g)
				break
			}
		}
	}
	return matching
}

// stacks is a wrapper for runtime.Stack that attempts to recover the data for
// all goroutines. It uses the provided buffer to avoid repeat allocations.
func stacks(buf *[]byte) []byte {
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
	return truncBuf
}

func firstNonStdlib(calls []stack.Call) stack.Call {
	for _, call := range calls {
		if call.Location != stack.Stdlib {
			return call
		}
	}
	panic("unexpected")
}
