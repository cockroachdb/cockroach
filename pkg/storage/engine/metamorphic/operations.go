// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metamorphic

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// opRun represents one operation run; an mvccOp reference as well as bound
// arguments.
type opRun struct {
	op   *mvccOp
	args []operand
}

// An mvccOp instance represents one type of an operation. The run and
// dependantOps commands should be stateless, with all state stored in the
// passed-in test runner or its operand managers.
type mvccOp struct {
	// Name of the operation. Used in file output and parsing.
	name string
	// Function to call to run this operation.
	run func(ctx context.Context, m *metaTestRunner, args ...operand) string
	// Returns a list of operation runs that must happen before this operation.
	// Note that openers for non-existent operands are handled separately and
	// don't need to be handled here.
	dependentOps func(m *metaTestRunner, args ...operand) []opRun
	// Operands this operation expects. Passed in the same order to run and
	// dependentOps.
	operands []operandType
	// weight is used to denote frequency of this operation to the TPCC-style
	// deck.
	//
	// Note that the generator tends to bias towards opener operations; since
	// an opener can be generated outside of the deck shuffle, in resolveAndRunOp
	// to create an instance of an operand that does not exist. To counter this
	// bias, we try to keep the sum of opener operations to be less than half
	// the sum of "closer" operations for an operand type, to prevent too many
	// of that type of object from accumulating throughout the run.
	weight int
}

// Helper function to generate iterator_close opRuns for all iterators on a
// passed-in Batch.
func closeItersOnBatch(m *metaTestRunner, reader engine.Reader) (results []opRun) {
	// No need to close iters on non-batches (i.e. engines).
	if batch, ok := reader.(engine.Batch); ok {
		// Close all iterators for this batch first.
		iterManager := m.managers[operandIterator].(*iteratorManager)
		for _, iter := range iterManager.readerToIter[batch] {
			results = append(results, opRun{
				op:   m.nameToOp["iterator_close"],
				args: []operand{iter},
			})
		}
	}
	return
}

// Returns true if the specified iterator is a batch iterator.
func isBatchIterator(m *metaTestRunner, iter engine.Iterator) bool {
	found := false
	iterManager := m.managers[operandIterator].(*iteratorManager)
	for _, iter2 := range iterManager.readerToIter[m.engine] {
		if iter2 == iter {
			found = true
			break
		}
	}
	return !found
}

// Helper function to run MVCCScan given a key range and a reader.
func runMvccScan(
	ctx context.Context, m *metaTestRunner, reverse bool, inconsistent bool, args []operand,
) string {
	key := args[0].(engine.MVCCKey)
	endKey := args[1].(engine.MVCCKey)
	if endKey.Less(key) {
		key, endKey = endKey, key
	}
	var ts hlc.Timestamp
	var txn *roachpb.Transaction
	if inconsistent {
		ts = args[2].(hlc.Timestamp)
	} else {
		txn = args[2].(*roachpb.Transaction)
		ts = txn.ReadTimestamp
	}
	// While MVCCScanning on a batch works in Pebble, it does not in rocksdb.
	// This is due to batch iterators not supporting SeekForPrev. For now, use
	// m.engine instead of a readWriterManager-generated engine.Reader, otherwise
	// we will try MVCCScanning on batches and produce diffs between runs on
	// different engines that don't point to an actual issue.
	result, err := engine.MVCCScan(ctx, m.engine, key.Key, endKey.Key, math.MaxInt64, ts, engine.MVCCScanOptions{
		Inconsistent: inconsistent,
		Tombstones:   true,
		Reverse:      reverse,
		Txn:          txn,
	})
	if err != nil {
		return fmt.Sprintf("error: %s", err)
	}
	return fmt.Sprintf("kvs = %v, intents = %v", result.KVs, result.Intents)
}

// Prints the key where an iterator is positioned, or valid = false if invalid.
func printIterState(iter engine.Iterator) string {
	if ok, err := iter.Valid(); !ok || err != nil {
		if err != nil {
			return fmt.Sprintf("valid = %v, err = %s", ok, err.Error())
		}
		return "valid = false"
	}
	return fmt.Sprintf("key = %s", iter.UnsafeKey().String())
}

// List of operations, where each operation is defined as one instance of mvccOp.
//
// TODO(itsbilal): Add more missing MVCC operations, such as:
//  - MVCCBlindPut
//  - MVCCMerge
//  - MVCCIncrement
//  - MVCCResolveWriteIntent in the aborted case
//  - and any others that would be important to test.
var operations = []mvccOp{
	{
		name: "mvcc_inconsistent_get",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			reader := args[0].(engine.Reader)
			key := args[1].(engine.MVCCKey)
			ts := args[2].(hlc.Timestamp)
			// TODO: Specify these bools as operands instead of having a separate
			// operation for inconsistent cases. This increases visibility for anyone
			// reading the output file.
			val, intent, err := engine.MVCCGet(ctx, reader, key.Key, ts, engine.MVCCGetOptions{
				Inconsistent: true,
				Tombstones:   true,
				Txn:          nil,
			})
			if err != nil {
				return fmt.Sprintf("error: %s", err)
			}
			return fmt.Sprintf("val = %v, intent = %v", val, intent)
		},
		operands: []operandType{
			operandReadWriter,
			operandMVCCKey,
			operandPastTS,
		},
		weight: 100,
	},
	{
		name: "mvcc_get",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			reader := args[0].(engine.Reader)
			key := args[1].(engine.MVCCKey)
			txn := args[2].(*roachpb.Transaction)
			val, intent, err := engine.MVCCGet(ctx, reader, key.Key, txn.ReadTimestamp, engine.MVCCGetOptions{
				Inconsistent: false,
				Tombstones:   true,
				Txn:          txn,
			})
			if err != nil {
				return fmt.Sprintf("error: %s", err)
			}
			return fmt.Sprintf("val = %v, intent = %v", val, intent)
		},
		operands: []operandType{
			operandReadWriter,
			operandMVCCKey,
			operandTransaction,
		},
		weight: 100,
	},
	{
		name: "mvcc_put",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			writer := args[0].(engine.ReadWriter)
			key := args[1].(engine.MVCCKey)
			value := roachpb.MakeValueFromBytes(args[2].([]byte))
			txn := args[3].(*roachpb.Transaction)
			txn.Sequence++

			err := engine.MVCCPut(ctx, writer, nil, key.Key, txn.WriteTimestamp, value, txn)
			if err != nil {
				return fmt.Sprintf("error: %s", err)
			}

			// Update the txn's intent spans to account for this intent being written.
			txn.IntentSpans = append(txn.IntentSpans, roachpb.Span{
				Key: key.Key,
			})
			// Track this write in the txn manager. This ensures the batch will be
			// committed before the transaction is committed
			m.managers[operandTransaction].(*txnManager).trackWriteOnBatch(writer, txn)
			return "ok"
		},
		operands: []operandType{
			operandReadWriter,
			operandMVCCKey,
			operandValue,
			operandTransaction,
		},
		weight: 500,
	},
	{
		name: "mvcc_conditional_put",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			writer := args[0].(engine.ReadWriter)
			key := args[1].(engine.MVCCKey)
			value := roachpb.MakeValueFromBytes(args[2].([]byte))
			expVal := roachpb.MakeValueFromBytes(args[3].([]byte))
			txn := args[4].(*roachpb.Transaction)
			txn.Sequence++

			err := engine.MVCCConditionalPut(ctx, writer, nil, key.Key, txn.WriteTimestamp, value, &expVal, true, txn)
			if err != nil {
				return fmt.Sprintf("error: %s", err)
			}

			// Update the txn's intent spans to account for this intent being written.
			txn.IntentSpans = append(txn.IntentSpans, roachpb.Span{
				Key: key.Key,
			})
			// Track this write in the txn manager. This ensures the batch will be
			// committed before the transaction is committed
			m.managers[operandTransaction].(*txnManager).trackWriteOnBatch(writer, txn)
			return "ok"
		},
		operands: []operandType{
			operandReadWriter,
			operandMVCCKey,
			operandValue,
			operandValue,
			operandTransaction,
		},
		weight: 50,
	},
	{
		name: "mvcc_init_put",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			writer := args[0].(engine.ReadWriter)
			key := args[1].(engine.MVCCKey)
			value := roachpb.MakeValueFromBytes(args[2].([]byte))
			txn := args[3].(*roachpb.Transaction)
			txn.Sequence++

			err := engine.MVCCInitPut(ctx, writer, nil, key.Key, txn.WriteTimestamp, value, false, txn)
			if err != nil {
				return fmt.Sprintf("error: %s", err)
			}

			// Update the txn's intent spans to account for this intent being written.
			txn.IntentSpans = append(txn.IntentSpans, roachpb.Span{
				Key: key.Key,
			})
			// Track this write in the txn manager. This ensures the batch will be
			// committed before the transaction is committed
			m.managers[operandTransaction].(*txnManager).trackWriteOnBatch(writer, txn)
			return "ok"
		},
		operands: []operandType{
			operandReadWriter,
			operandMVCCKey,
			operandValue,
			operandTransaction,
		},
		weight: 50,
	},
	{
		name: "mvcc_delete_range",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			writer := args[0].(engine.ReadWriter)
			key := args[1].(engine.MVCCKey).Key
			endKey := args[2].(engine.MVCCKey).Key
			txn := args[3].(*roachpb.Transaction)
			txn.Sequence++

			if endKey.Compare(key) < 0 {
				key, endKey = endKey, key
			}

			keys, _, _, err := engine.MVCCDeleteRange(ctx, writer, nil, key, endKey, 0, txn.WriteTimestamp, txn, true)
			if err != nil {
				return fmt.Sprintf("error: %s", err)
			}

			// Update the txn's intent spans to account for this intent being written.
			for _, key := range keys {
				txn.IntentSpans = append(txn.IntentSpans, roachpb.Span{
					Key: key,
				})
			}
			// Track this write in the txn manager. This ensures the batch will be
			// committed before the transaction is committed
			m.managers[operandTransaction].(*txnManager).trackWriteOnBatch(writer, txn)
			return fmt.Sprintf("keys = %v", keys)
		},
		dependentOps: func(m *metaTestRunner, args ...operand) (results []opRun) {
			return closeItersOnBatch(m, args[0].(engine.Reader))
		},
		operands: []operandType{
			operandReadWriter,
			operandMVCCKey,
			operandMVCCKey,
			operandTransaction,
		},
		weight: 20,
	},
	{
		name: "mvcc_clear_time_range",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			writer := args[0].(engine.ReadWriter)
			key := args[1].(engine.MVCCKey).Key
			endKey := args[2].(engine.MVCCKey).Key
			startTime := args[3].(hlc.Timestamp)
			endTime := args[3].(hlc.Timestamp)

			if endKey.Compare(key) < 0 {
				key, endKey = endKey, key
			}
			if endTime.Less(startTime) {
				startTime, endTime = endTime, startTime
			}

			span, err := engine.MVCCClearTimeRange(ctx, writer, nil, key, endKey, startTime, endTime, math.MaxInt64)
			if err != nil {
				return fmt.Sprintf("error: %s", err)
			}
			return fmt.Sprintf("ok, span = %v", span)
		},
		dependentOps: func(m *metaTestRunner, args ...operand) (results []opRun) {
			return closeItersOnBatch(m, args[0].(engine.Reader))
		},
		operands: []operandType{
			operandReadWriter,
			operandMVCCKey,
			operandMVCCKey,
			operandPastTS,
			operandPastTS,
		},
		weight: 20,
	},
	{
		name: "mvcc_delete",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			writer := args[0].(engine.ReadWriter)
			key := args[1].(engine.MVCCKey)
			txn := args[2].(*roachpb.Transaction)
			txn.Sequence++

			err := engine.MVCCDelete(ctx, writer, nil, key.Key, txn.WriteTimestamp, txn)
			if err != nil {
				return fmt.Sprintf("error: %s", err)
			}

			// Update the txn's intent spans to account for this intent being written.
			txn.IntentSpans = append(txn.IntentSpans, roachpb.Span{
				Key: key.Key,
			})
			// Track this write in the txn manager. This ensures the batch will be
			// committed before the transaction is committed
			m.managers[operandTransaction].(*txnManager).trackWriteOnBatch(writer, txn)
			return "ok"
		},
		operands: []operandType{
			operandReadWriter,
			operandMVCCKey,
			operandTransaction,
		},
		weight: 100,
	},
	{
		name: "mvcc_find_split_key",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			key, _ := keys.Addr(args[0].(engine.MVCCKey).Key)
			endKey, _ := keys.Addr(args[1].(engine.MVCCKey).Key)
			splitSize := int64(1024)

			splitKey, err := engine.MVCCFindSplitKey(ctx, m.engine, key, endKey, splitSize)
			if err != nil {
				return fmt.Sprintf("error: %s", err)
			}

			return fmt.Sprintf("ok, splitSize = %d, splitKey = %v", splitSize, splitKey)
		},
		operands: []operandType{
			operandMVCCKey,
			operandMVCCKey,
		},
		weight: 20,
	},
	{
		name: "mvcc_scan",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			return runMvccScan(ctx, m, false, false, args)
		},
		operands: []operandType{
			operandMVCCKey,
			operandMVCCKey,
			operandTransaction,
		},
		weight: 100,
	},
	{
		name: "mvcc_inconsistent_scan",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			return runMvccScan(ctx, m, false, true, args)
		},
		operands: []operandType{
			operandMVCCKey,
			operandMVCCKey,
			operandPastTS,
		},
		weight: 100,
	},
	{
		name: "mvcc_reverse_scan",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			return runMvccScan(ctx, m, true, false, args)
		},
		operands: []operandType{
			operandMVCCKey,
			operandMVCCKey,
			operandTransaction,
		},
		weight: 100,
	},
	{
		name: "txn_open",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			txn := m.managers[operandTransaction].(*txnManager).open()
			return m.managers[operandTransaction].toString(txn)
		},
		operands: []operandType{},
		weight:   40,
	},
	{
		name: "txn_commit",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			txnManager := m.managers[operandTransaction].(*txnManager)
			txn := args[0].(*roachpb.Transaction)
			txn.Status = roachpb.COMMITTED
			txnManager.close(txn)

			return "ok"
		},
		dependentOps: func(m *metaTestRunner, args ...operand) (result []opRun) {
			txnManager := m.managers[operandTransaction].(*txnManager)
			txn := args[0].(*roachpb.Transaction)

			// A transaction could have in-flight writes in some batches. Get a list
			// of all those batches, and dispatch batch_commit operations for them.
			for batch := range txnManager.openBatches[txn] {
				result = append(result, opRun{
					op:   m.nameToOp["batch_commit"],
					args: []operand{batch},
				})
			}
			return
		},
		operands: []operandType{
			operandTransaction,
		},
		weight: 100,
	},
	{
		name: "batch_open",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			batch := m.managers[operandReadWriter].(*readWriterManager).open()
			return m.managers[operandReadWriter].toString(batch)
		},
		operands: []operandType{},
		weight:   40,
	},
	{
		name: "batch_commit",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			if batch, ok := args[0].(engine.Batch); ok {
				err := batch.Commit(false)
				m.managers[operandReadWriter].close(batch)
				m.managers[operandTransaction].(*txnManager).clearBatch(batch)
				if err != nil {
					return err.Error()
				}
				return "ok"
			}
			return "noop"
		},
		dependentOps: func(m *metaTestRunner, args ...operand) (results []opRun) {
			return closeItersOnBatch(m, args[0].(engine.Reader))
		},
		operands: []operandType{
			operandReadWriter,
		},
		weight: 100,
	},
	{
		name: "iterator_open",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			iterManager := m.managers[operandIterator].(*iteratorManager)
			key := args[1].(engine.MVCCKey)
			endKey := args[2].(engine.MVCCKey)
			if endKey.Less(key) {
				key, endKey = endKey, key
			}
			iter := iterManager.open(args[0].(engine.ReadWriter), engine.IterOptions{
				Prefix:     false,
				LowerBound: key.Key,
				UpperBound: endKey.Key.Next(),
			})
			if _, ok := args[0].(engine.Batch); ok {
				// When Next()-ing on a newly initialized batch iter without a key,
				// pebble's iterator stays invalid while RocksDB's finds the key after
				// the first key. This is a known difference. For now seek the iterator
				// to standardize behavior for this test.
				iter.SeekGE(key)
			}

			return iterManager.toString(iter)
		},
		dependentOps: func(m *metaTestRunner, args ...operand) (results []opRun) {
			return closeItersOnBatch(m, args[0].(engine.Reader))
		},
		operands: []operandType{
			operandReadWriter,
			operandMVCCKey,
			operandMVCCKey,
		},
		weight: 20,
	},
	{
		name: "iterator_close",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			iterManager := m.managers[operandIterator].(*iteratorManager)
			iterManager.close(args[0].(engine.Iterator))

			return "ok"
		},
		operands: []operandType{
			operandIterator,
		},
		weight: 50,
	},
	{
		name: "iterator_seekge",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			iter := args[0].(engine.Iterator)
			key := args[1].(engine.MVCCKey)
			if isBatchIterator(m, iter) {
				// RocksDB batch iterators do not account for lower bounds consistently:
				// https://github.com/cockroachdb/cockroach/issues/44512
				// In the meantime, ensure the SeekGE key >= lower bound.
				iterManager := m.managers[operandIterator].(*iteratorManager)
				lowerBound := iterManager.iterToInfo[iter].lowerBound
				if key.Key.Compare(lowerBound) < 0 {
					key.Key = lowerBound
				}
			}
			iter.SeekGE(key)

			return printIterState(iter)
		},
		operands: []operandType{
			operandIterator,
			operandMVCCKey,
		},
		weight: 50,
	},
	{
		name: "iterator_seeklt",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			iter := args[0].(engine.Iterator)
			key := args[1].(engine.MVCCKey)
			// Check if this is an engine or batch iterator. If batch iterator, do
			// nothing.
			if isBatchIterator(m, iter) {
				return "noop due to missing seekLT support in rocksdb batch iterators"
			}

			iter.SeekLT(key)

			return printIterState(iter)
		},
		operands: []operandType{
			operandIterator,
			operandMVCCKey,
		},
		weight: 50,
	},
	{
		name: "iterator_next",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			iter := args[0].(engine.Iterator)
			// The rocksdb iterator does not treat kindly to a Next() if it is already
			// invalid. Don't run next if that is the case.
			if ok, err := iter.Valid(); !ok || err != nil {
				if err != nil {
					return fmt.Sprintf("valid = %v, err = %s", ok, err.Error())
				}
				return "valid = false"
			}
			iter.Next()

			return printIterState(iter)
		},
		operands: []operandType{
			operandIterator,
		},
		weight: 100,
	},
	{
		name: "iterator_nextkey",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			iter := args[0].(engine.Iterator)
			// The rocksdb iterator does not treat kindly to a NextKey() if it is
			// already invalid. Don't run NextKey if that is the case.
			if ok, err := iter.Valid(); !ok || err != nil {
				if err != nil {
					return fmt.Sprintf("valid = %v, err = %s", ok, err.Error())
				}
				return "valid = false"
			}
			iter.NextKey()

			return printIterState(iter)
		},
		operands: []operandType{
			operandIterator,
		},
		weight: 100,
	},
	{
		name: "iterator_prev",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			iter := args[0].(engine.Iterator)
			if isBatchIterator(m, iter) {
				return "noop due to missing Prev support in rocksdb batch iterators"
			}
			// The rocksdb iterator does not treat kindly to a Prev() if it is already
			// invalid. Don't run prev if that is the case.
			if ok, err := iter.Valid(); !ok || err != nil {
				if err != nil {
					return fmt.Sprintf("valid = %v, err = %s", ok, err.Error())
				}
				return "valid = false"
			}
			iter.Prev()

			return printIterState(iter)
		},
		operands: []operandType{
			operandIterator,
		},
		weight: 100,
	},
	{
		// Note that this is not an MVCC* operation; unlike MVCC{Put,Get,Scan}, etc,
		// it does not respect transactions. This often yields interesting
		// behavior.
		name: "delete_range",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			key := args[0].(engine.MVCCKey).Key
			endKey := args[1].(engine.MVCCKey).Key
			if endKey.Compare(key) < 0 {
				key, endKey = endKey, key
			} else if endKey.Equal(key) {
				// Range tombstones where start = end can exhibit different behavior on
				// different engines; rocks treats it as a point delete, while pebble
				// treats it as a nonexistent tombstone. For the purposes of this test,
				// standardize behavior.
				endKey = endKey.Next()
			}
			// All ClearRange calls in Cockroach usually happen with metadata keys, so
			// mimic the same behavior here.
			err := m.engine.ClearRange(engine.MakeMVCCMetadataKey(key), engine.MakeMVCCMetadataKey(endKey))
			if err != nil {
				return fmt.Sprintf("error: %s", err.Error())
			}
			return "ok"
		},
		operands: []operandType{
			operandMVCCKey,
			operandMVCCKey,
		},
		weight: 20,
	},
	{
		name: "compact",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			key := args[0].(engine.MVCCKey).Key
			endKey := args[1].(engine.MVCCKey).Key
			if endKey.Compare(key) < 0 {
				key, endKey = endKey, key
			}
			err := m.engine.CompactRange(key, endKey, false)
			if err != nil {
				return fmt.Sprintf("error: %s", err.Error())
			}
			return "ok"
		},
		operands: []operandType{
			operandMVCCKey,
			operandMVCCKey,
		},
		weight: 10,
	},
	{
		name: "ingest",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			sstPath := filepath.Join(m.path, "ingest.sst")
			f, err := os.Create(sstPath)
			if err != nil {
				return fmt.Sprintf("error = %s", err.Error())
			}
			defer f.Close()

			var keys []engine.MVCCKey
			for _, arg := range args {
				keys = append(keys, arg.(engine.MVCCKey))
			}
			// SST Writer expects keys in sorted order, so sort them first.
			sort.Slice(keys, func(i, j int) bool {
				return keys[i].Less(keys[j])
			})

			sstWriter := engine.MakeIngestionSSTWriter(f)
			for _, key := range keys {
				_ = sstWriter.Put(key, []byte("ingested"))
			}
			if err := sstWriter.Finish(); err != nil {
				return fmt.Sprintf("error = %s", err.Error())
			}
			sstWriter.Close()

			if err := m.engine.IngestExternalFiles(ctx, []string{sstPath}); err != nil {
				return fmt.Sprintf("error = %s", err.Error())
			}

			return "ok"
		},
		operands: []operandType{
			operandMVCCKey,
			operandMVCCKey,
			operandMVCCKey,
			operandMVCCKey,
			operandMVCCKey,
		},
		weight: 10,
	},
	{
		name: "restart",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			if !m.restarts {
				m.printComment("no-op due to restarts being disabled")
				return "ok"
			}

			oldEngineName, newEngineName := m.restart()
			m.printComment(fmt.Sprintf("restarting: %s -> %s", oldEngineName, newEngineName))
			return "ok"
		},
		operands: nil,
		weight:   4,
	},
}
