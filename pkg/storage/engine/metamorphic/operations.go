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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
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
		iterManager := m.managers[OPERAND_ITERATOR].(*iteratorManager)
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
	iterManager := m.managers[OPERAND_ITERATOR].(*iteratorManager)
	for _, iter2 := range iterManager.readerToIter[m.engine] {
		if iter2 == iter {
			found = true
			break
		}
	}
	return !found
}

// Helper function to run MVCCScan given a key range and a reader.
func runMvccScan(ctx context.Context, m *metaTestRunner, reverse bool, args []operand) string {
	key := args[0].(engine.MVCCKey)
	endKey := args[1].(engine.MVCCKey)
	txn := args[2].(*roachpb.Transaction)
	if endKey.Less(key) {
		tmpKey := endKey
		endKey = key
		key = tmpKey
	}
	// While MVCCScanning on a batch works in Pebble, it does not in rocksdb.
	// This is due to batch iterators not supporting SeekForPrev. For now, use
	// m.engine instead of a readWriterManager-generated engine.Reader, otherwise
	// we will try MVCCScanning on batches and produce diffs between runs on
	// different engines that don't point to an actual issue.
	kvs, _, intent, err := engine.MVCCScan(ctx, m.engine, key.Key, endKey.Key, math.MaxInt64, txn.ReadTimestamp, engine.MVCCScanOptions{
		Inconsistent: false,
		Tombstones:   true,
		Reverse:      reverse,
		Txn:          txn,
	})
	if err != nil {
		return fmt.Sprintf("error: %s", err)
	}
	return fmt.Sprintf("kvs = %v, intent = %v", kvs, intent)
}

// Prints the key where an iterator is positioned, or valid = false if invalid.
func printIterState(iter engine.Iterator) string {
	if ok, err := iter.Valid(); !ok || err != nil {
		if err != nil {
			return fmt.Sprintf("valid = %v, err = %s", ok, err.Error())
		} else {
			return "valid = false"
		}
	}
	return fmt.Sprintf("key = %s", iter.UnsafeKey().String())
}

// List of operations, where each operation is defined as one instance of mvccOp.
var operations = []mvccOp{
	{
		name: "mvcc_get",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			reader := args[0].(engine.Reader)
			key := args[1].(engine.MVCCKey)
			val, intent, err := engine.MVCCGet(ctx, reader, key.Key, key.Timestamp, engine.MVCCGetOptions{
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
			OPERAND_READWRITER,
			OPERAND_MVCC_KEY,
		},
		weight: 10,
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
			// If this write happened on a batch, track that in the txn manager so
			// that the batch is committed before the transaction is aborted or
			// committed.
			if batch, ok := writer.(engine.Batch); ok {
				txnManager := m.managers[OPERAND_TRANSACTION].(*txnManager)
				txnManager.inFlightBatches[txn] = append(txnManager.inFlightBatches[txn], batch)
			}
			return "ok"
		},
		operands: []operandType{
			OPERAND_READWRITER,
			OPERAND_MVCC_KEY,
			OPERAND_VALUE,
			OPERAND_TRANSACTION,
		},
		weight: 30,
	},
	{
		name: "mvcc_scan",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			return runMvccScan(ctx, m, false, args)
		},
		operands: []operandType{
			OPERAND_MVCC_KEY,
			OPERAND_MVCC_KEY,
			OPERAND_TRANSACTION,
		},
		weight: 10,
	},
	{
		name: "mvcc_reverse_scan",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			return runMvccScan(ctx, m, true, args)
		},
		operands: []operandType{
			OPERAND_MVCC_KEY,
			OPERAND_MVCC_KEY,
			OPERAND_TRANSACTION,
		},
		weight: 10,
	},
	{
		name: "txn_open",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			txn := m.managers[OPERAND_TRANSACTION].(*txnManager).open()
			return m.managers[OPERAND_TRANSACTION].toString(txn)
		},
		operands: []operandType{},
		weight:   4,
	},
	{
		name: "txn_commit",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			txnManager := m.managers[OPERAND_TRANSACTION].(*txnManager)
			txn := args[0].(*roachpb.Transaction)
			txn.Status = roachpb.COMMITTED
			txnManager.close(txn)

			return "ok"
		},
		dependentOps: func(m *metaTestRunner, args ...operand) (result []opRun) {
			txnManager := m.managers[OPERAND_TRANSACTION].(*txnManager)
			txn := args[0].(*roachpb.Transaction)
			closedBatches := make(map[engine.Batch]struct{})

			// A transaction could have in-flight writes in some batches. Get a list
			// of all those batches, and dispatch batch_commit operations for them.
			for _, batch := range txnManager.inFlightBatches[txn] {
				if _, ok := closedBatches[batch]; ok {
					continue
				}
				closedBatches[batch] = struct{}{}

				result = append(result, opRun{
					op:   m.nameToOp["batch_commit"],
					args: []operand{batch},
				})
			}
			return
		},
		operands: []operandType{
			OPERAND_TRANSACTION,
		},
		weight: 10,
	},
	{
		name: "batch_open",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			batch := m.managers[OPERAND_READWRITER].(*readWriterManager).open()
			return m.managers[OPERAND_READWRITER].toString(batch)
		},
		operands: []operandType{},
		weight:   4,
	},
	{
		name: "batch_commit",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			if batch, ok := args[0].(engine.Batch); ok {
				err := batch.Commit(false)
				m.managers[OPERAND_READWRITER].close(batch)
				m.managers[OPERAND_TRANSACTION].(*txnManager).clearBatch(batch)
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
			OPERAND_READWRITER,
		},
		weight: 10,
	},
	{
		name: "iterator_open",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			iterManager := m.managers[OPERAND_ITERATOR].(*iteratorManager)
			key := args[1].(engine.MVCCKey)
			endKey := args[2].(engine.MVCCKey)
			if endKey.Less(key) {
				tmpKey := key
				key = endKey
				endKey = tmpKey
			}
			iter := iterManager.open(args[0].(engine.ReadWriter), engine.IterOptions{
				Prefix:     false,
				LowerBound: key.Key,
				UpperBound: endKey.Key.Next(),
			})

			return iterManager.toString(iter)
		},
		dependentOps: func(m *metaTestRunner, args ...operand) (results []opRun) {
			return closeItersOnBatch(m, args[0].(engine.Reader))
		},
		operands: []operandType{
			OPERAND_READWRITER,
			OPERAND_MVCC_KEY,
			OPERAND_MVCC_KEY,
		},
		weight: 2,
	},
	{
		name: "iterator_close",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			iterManager := m.managers[OPERAND_ITERATOR].(*iteratorManager)
			iterManager.close(args[0].(engine.Iterator))

			return "ok"
		},
		operands: []operandType{
			OPERAND_ITERATOR,
		},
		weight: 5,
	},
	{
		name: "iterator_seekge",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			iter := args[0].(engine.Iterator)
			key := args[1].(engine.MVCCKey)
			iter.SeekGE(key)

			return printIterState(iter)
		},
		operands: []operandType{
			OPERAND_ITERATOR,
			OPERAND_MVCC_KEY,
		},
		weight: 5,
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
			OPERAND_ITERATOR,
			OPERAND_MVCC_KEY,
		},
		weight: 5,
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
				} else {
					return "valid = false"
				}
			}
			iter.Next()

			return printIterState(iter)
		},
		operands: []operandType{
			OPERAND_ITERATOR,
		},
		weight: 10,
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
				} else {
					return "valid = false"
				}
			}
			iter.NextKey()

			return printIterState(iter)
		},
		operands: []operandType{
			OPERAND_ITERATOR,
		},
		weight: 10,
	},
	{
		name: "iterator_prev",
		run: func(ctx context.Context, m *metaTestRunner, args ...operand) string {
			iter := args[0].(engine.Iterator)
			if isBatchIterator(m, iter) {
				return "noop due to missing seekLT support in rocksdb batch iterators"
			}
			// The rocksdb iterator does not treat kindly to a Prev() if it is already
			// invalid. Don't run prev if that is the case.
			if ok, err := iter.Valid(); !ok || err != nil {
				if err != nil {
					return fmt.Sprintf("valid = %v, err = %s", ok, err.Error())
				} else {
					return "valid = false"
				}
			}
			iter.Prev()

			return printIterState(iter)
		},
		operands: []operandType{
			OPERAND_ITERATOR,
		},
		weight: 10,
	},
}
