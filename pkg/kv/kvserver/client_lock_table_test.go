// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestClientLockTableDataDriven tests the behavior of lock table and
// concurrency manager using end-to-end requests from the client.
//
// Each file runs in the context of a single test server.
func TestClientLockTableDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	datadriven.Walk(t, datapathutils.TestDataPath(t, "lock_table"), func(t *testing.T, path string) {
		defer log.Scope(t).Close(t)

		srv, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{
			// TODO(ssd): We could remove this if we mad
			// it easier to get something like a "scratch
			// range" in a secondary tenant keyspace
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		})
		defer srv.Stopper().Stop(ctx)

		s := srv.StorageLayer()
		rangeStartKey, err := s.ScratchRange()
		require.NoError(t, err)

		store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
		require.NoError(t, err)
		evalCtx := newEvalCtx(t, rangeStartKey, store.StateEngine(), db)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {

			switch d.Cmd {
			case "new-txn":
				evalCtx.newTxn(ctx, d)
				return ""
			case "batch":
				txn := evalCtx.mustGetTxn(d)
				b := txn.NewBatch()
				b.Header.WaitPolicy = evalCtx.getLockWaitPolicy(d)
				for _, line := range strings.Split(d.Input, "\n") {
					if err := evalCtx.cmdInBatch(line, b); err != nil {
						d.Fatalf(t, "batch command error: %v", err.Error())
					}
				}
				err := txn.Run(ctx, b)
				if err != nil {
					return fmt.Sprintf("error: %s", err.Error())
				}
				return ""
			case "get":
				txn := evalCtx.mustGetTxn(d)
				key := evalCtx.getKey(d)

				if d.HasArg("lock") {
					str := evalCtx.getLockStr(d)
					dur := evalCtx.getLockDurability(d)

					b := txn.NewBatch()
					b.Header.WaitPolicy = evalCtx.getLockWaitPolicy(d)

					switch str {
					case lock.Exclusive:
						b.GetForUpdate(key, dur)
					case lock.Shared:
						b.GetForShare(key, dur)
					}
					b.Requests()[0].GetGet().LockNonExisting = d.HasArg("lock-non-existing")
					err := txn.Run(ctx, b)
					if err != nil {
						return fmt.Sprintf("error: %s", err.Error())
					}
					res := b.Results[0]
					if res.Err != nil {
						return fmt.Sprintf("error: %s", err.Error())
					}
					return fmt.Sprintf("get: %s", res.Rows[0].String())
				} else {
					kv, err := txn.Get(ctx, key)
					if err != nil {
						return fmt.Sprintf("error: %s", err.Error())
					}
					return fmt.Sprintf("get: %s", kv.String())
				}
			case "put":
				txn := evalCtx.mustGetTxn(d)
				b := txn.NewBatch()
				b.Header.WaitPolicy = evalCtx.getLockWaitPolicy(d)
				evalCtx.putInBatch(d, b)
				err := txn.Run(ctx, b)
				if err != nil {
					return fmt.Sprintf("error: %s", err.Error())
				}
				return ""
			case "commit":
				if err := evalCtx.commitTxn(ctx, d); err != nil {
					return fmt.Sprintf("error: %s", err.Error())
				}
				return ""
			case "rollback":
				if err := evalCtx.rollbackTxn(ctx, d); err != nil {
					return fmt.Sprintf("error: %s", err.Error())
				}
				return ""
			case "print-in-memory-lock-table":
				rangeDesc, err := s.LookupRange(rangeStartKey)
				if err != nil {
					d.Fatalf(t, "lookup range: %s", err)
				}
				r, err := store.GetReplica(rangeDesc.RangeID)
				if err != nil {
					d.Fatalf(t, "get replica: %s", err)
				}
				return evalCtx.scrubTS(evalCtx.replaceAllTxnUUIDs(r.GetConcurrencyManager().TestingLockTableString()))
			case "print-replicated-lock-table":
				startKey := evalCtx.getNamedKey("start", d)
				endKey := evalCtx.getNamedKey("end", d)
				actual := evalCtx.printLockTable(ctx, d, startKey, endKey)
				// Because of pipelined writes, the lock table might not be up to date
				// if we check it too quickly, so we retry a few times.
				const maxRetries = 100
				for try := 0; try < maxRetries && actual != d.Expected; try++ {
					time.Sleep(100 * time.Millisecond)
					actual = evalCtx.printLockTable(ctx, d, startKey, endKey)
				}
				return actual
			case "exec-sql":
				_, err := sqlDB.Exec(d.Input)
				if err != nil {
					return fmt.Sprintf("error: %s", err.Error())
				}
				return ""
			case "flush-lock-table":
				startKey := evalCtx.getNamedKey("start", d)
				endKey := evalCtx.getNamedKey("end", d)
				b := db.NewBatch()
				b.AddRawRequest(&kvpb.FlushLockTableRequest{
					RequestHeader: kvpb.RequestHeader{
						Key:    startKey,
						EndKey: endKey,
					},
				})
				if err := db.Run(ctx, b); err != nil {
					return fmt.Sprintf("error: %s", err.Error())
				}
				return ""
			default:
				d.Fatalf(t, "unknown command: %s", d.Cmd)
				return ""
			}
		})
	})
}

type evalCtx struct {
	t             *testing.T
	s             storage.Reader
	db            *kv.DB
	rangeStartKey roachpb.Key

	txnsByName       map[string]*kv.Txn
	txnsByUUIDToName map[string]string
}

func newEvalCtx(t *testing.T, rsk roachpb.Key, s storage.Reader, db *kv.DB) *evalCtx {
	return &evalCtx{
		t:             t,
		s:             s,
		db:            db,
		rangeStartKey: rsk,

		txnsByName:       make(map[string]*kv.Txn),
		txnsByUUIDToName: make(map[string]string),
	}
}

var (
	txnKey = "txn"
	keyKey = "k"
	valKey = "v"
)

func (e *evalCtx) newTxn(ctx context.Context, d *datadriven.TestData) {
	var name string
	d.ScanArgs(e.t, txnKey, &name)
	if _, ok := e.txnsByName[name]; ok {
		d.Fatalf(e.t, "txn %q already exists", name)
	} else {
		txn := e.db.NewTxn(ctx, name)

		e.txnsByName[name] = txn
		e.txnsByUUIDToName[txn.ID().String()] = name
	}
}

func (e *evalCtx) mustGetTxn(d *datadriven.TestData) *kv.Txn {
	var txnName string
	d.ScanArgs(e.t, txnKey, &txnName)
	txn, ok := e.txnsByName[txnName]
	if !ok {
		d.Fatalf(e.t, "txn %q doesn't exists", txnName)
	}
	return txn
}

func (e *evalCtx) mustGetAndRemoveTxn(d *datadriven.TestData) *kv.Txn {
	var txnName string
	d.ScanArgs(e.t, txnKey, &txnName)
	txn, ok := e.txnsByName[txnName]
	if !ok {
		d.Fatalf(e.t, "txn %q doesn't exists", txnName)
	}
	delete(e.txnsByName, txnName)
	delete(e.txnsByUUIDToName, txn.ID().String())
	return txn
}

func (e *evalCtx) commitTxn(ctx context.Context, d *datadriven.TestData) error {
	txn := e.mustGetAndRemoveTxn(d)
	return txn.Commit(ctx)
}

func (e *evalCtx) rollbackTxn(ctx context.Context, d *datadriven.TestData) error {
	txn := e.mustGetAndRemoveTxn(d)
	return txn.Rollback(ctx)
}

func (e *evalCtx) getNamedKey(name string, d *datadriven.TestData) roachpb.Key {
	e.t.Helper()
	var keyS string
	d.ScanArgs(e.t, name, &keyS)
	return append(e.rangeStartKey.Clone(), []byte(keyS)...)
}

func (e *evalCtx) getKey(d *datadriven.TestData) roachpb.Key {
	return e.getNamedKey(keyKey, d)
}

func (e *evalCtx) getValue(d *datadriven.TestData) []byte {
	e.t.Helper()
	var valueS string
	d.ScanArgs(e.t, valKey, &valueS)
	return []byte(valueS)
}

func (e *evalCtx) getLockStr(d *datadriven.TestData) lock.Strength {
	var lockStr string
	d.ScanArgs(e.t, "lock", &lockStr)
	return lock.Strength(lock.Strength_value[lockStr])
}

func (e *evalCtx) getLockDurability(d *datadriven.TestData) kvpb.KeyLockingDurabilityType {
	var lockDur string
	// TODO(ssd): I tend to think in terms of the lock package while the
	// client takes arguments in terms of the kvpb package. This creates
	// a small mess.
	d.ScanArgs(e.t, "dur", &lockDur)
	var dur kvpb.KeyLockingDurabilityType
	switch lock.Durability(lock.Durability_value[lockDur]) {
	case lock.Replicated:
		dur = kvpb.GuaranteedDurability
	default:
		dur = kvpb.BestEffort
	}
	return dur
}

func (e *evalCtx) getLockWaitPolicy(d *datadriven.TestData) lock.WaitPolicy {
	if d.HasArg("wait") {
		var waitPolicyStr string
		d.ScanArgs(e.t, "wait", &waitPolicyStr)
		return lock.WaitPolicy(lock.WaitPolicy_value[waitPolicyStr])
	} else {
		return lock.WaitPolicy_Block
	}
}

func (e *evalCtx) cmdInBatch(cmdStr string, b *kv.Batch) error {
	d := datadriven.TestData{}
	var err error
	d.Cmd, d.CmdArgs, err = datadriven.ParseLine(cmdStr)
	if err != nil {
		return err
	}
	switch d.Cmd {
	case "put":
		e.putInBatch(&d, b)
	default:
		return errors.Newf("unknown command: %s", d.Cmd)
	}
	return nil
}

func (e *evalCtx) putInBatch(d *datadriven.TestData, b *kv.Batch) {
	key := e.getKey(d)
	value := e.getValue(d)
	b.Put(key, value)
}

func (e *evalCtx) printLockTable(
	ctx context.Context, d *datadriven.TestData, startKey, endKey roachpb.Key,
) string {
	var outBuf strings.Builder
	ltStart, _ := keys.LockTableSingleKey(startKey, nil)
	ltEnd, _ := keys.LockTableSingleKey(endKey, nil)
	iter, err := storage.NewLockTableIterator(ctx, e.s, storage.LockTableIteratorOptions{
		LowerBound:  ltStart,
		UpperBound:  ltEnd,
		MatchMinStr: lock.Shared,
	})
	if err != nil {
		d.Fatalf(e.t, "lock table iter: %s", err.Error())
	}
	defer iter.Close()

	for valid, err := iter.SeekEngineKeyGE(storage.EngineKey{Key: ltStart}); ; valid, err = iter.NextEngineKey() {
		if err != nil {
			d.Fatalf(e.t, "next engine key: %s", err.Error())
		}
		if !valid {
			break
		}
		engineKey, err := iter.EngineKey()
		if err != nil {
			d.Fatalf(e.t, "engine key: %s", err.Error())
		}
		ltKey, err := engineKey.ToLockTableKey()
		if err != nil {
			d.Fatalf(e.t, "lock table key: %s", err.Error())
		}
		fmt.Fprintf(&outBuf, "key: %s, str: %s, txn: %s\n", ltKey.Key, ltKey.Strength, e.uuidToTxnName(ltKey.TxnUUID.String()))
	}
	return outBuf.String()
}

func (e *evalCtx) uuidToTxnName(uuid string) string {
	if name, ok := e.txnsByUUIDToName[uuid]; ok {
		return name
	} else {
		return "unknown txn"
	}
}

func (e *evalCtx) replaceAllTxnUUIDs(input string) string {
	for uuid, name := range e.txnsByUUIDToName {
		input = strings.Replace(input, uuid, name, -1)
	}
	return input
}

// ts: 1739967161.672801000,0,
var tsRegex = regexp.MustCompile(`ts: [0-9]+\.[0-9]+,[0-9],`)

func (e *evalCtx) scrubTS(input string) string {
	return tsRegex.ReplaceAllString(input, "ts: <stripped>,")
}
