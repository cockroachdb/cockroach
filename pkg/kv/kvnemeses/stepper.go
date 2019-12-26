// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemeses

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

// Stepper incrementally constructs KV traffic designed to maximally test edge
// cases.
//
// Each Step consists of one or more Operations which are expected to be run in
// parallel, exercising concurrency. However, to abide by KV interface usage
// invariants, a Step must fully finish executing before moving onto the next
// Step.
//
// An Operation is a unit of work that must be done serially. It often
// corresponds 1:1 to a single call to some method on the KV api (such as Get or
// Put), but some Operations have a set of steps (such as using a transaction).
//
// A Step intentionally does not have deterministic results. For example, a step
// could contain an Operation to update a key and a Step to delete the same key.
// The outcome would be race-y. However, a Step intentionally contains few
// Operations so that it's possible to enumerate all orderings and verify that
// the results of executing the Step matches one of them.
type Stepper struct {
	clock        *hlc.Clock
	nextValue    int
	nextTxnID    int
	keys         map[string]struct{}
	splits       map[string]struct{}
	mergedSplits map[string]struct{}
	txns         map[string]struct{}

	// stepTxns contains the txns that have already been used in the step
	// currently being generated. Similarly stepSplits and stepMerges.
	stepTxns   map[string]struct{}
	stepSplits map[string]struct{}
	stepMerges map[string]struct{}
}

// MakeStepper constructs a Stepper.
//
// TODO(dan): Make all the random weights configurable.
func MakeStepper() *Stepper {
	return &Stepper{
		clock:        hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		keys:         make(map[string]struct{}),
		splits:       make(map[string]struct{}),
		mergedSplits: make(map[string]struct{}),
		txns:         make(map[string]struct{}),
		stepTxns:     make(map[string]struct{}),
		stepSplits:   make(map[string]struct{}),
		stepMerges:   make(map[string]struct{}),
	}
}

var stepperSpan = roachpb.Span{
	Key:    roachpb.Key(keys.MakeTablePrefix(50)),
	EndKey: roachpb.Key(keys.MakeTablePrefix(51)),
}

// DataSpan returns a span that contains all of the operations created by this
// Stepper.
func (s *Stepper) DataSpan() roachpb.Span {
	return stepperSpan
}

// RandStep returns a single randomly generated next step to execute. A user may
// execute this step and then call RandStep again, but this is not required.
// Steps may be computed in advance.
//
// RandStep is not concurrency safe.
func (s *Stepper) RandStep(rng *rand.Rand) Step {
	for txnID := range s.stepTxns {
		s.txns[txnID] = struct{}{}
		delete(s.stepTxns, txnID)
	}
	for key := range s.stepMerges {
		if _, ok := s.stepSplits[key]; !ok {
			// The key was merged and not concurrently split, so we know it's gone.
			delete(s.splits, key)
		}
		delete(s.stepMerges, key)
	}
	for key := range s.stepSplits {
		delete(s.stepSplits, key)
	}
	if len(s.stepTxns) > 0 || len(s.stepSplits) > 0 || len(s.stepMerges) > 0 {
		panic(`unreachable`)
	}

	numOps := rand.Intn(4) + 1
	ops := make([]Operation, numOps)
	for idx := range ops {
		switch rng.Intn(5) {
		case 0:
			// TODO(dan): A Batch with two puts of the same key currently errors when
			// run outside of a transaction, but nothing in Stepper currently prevents
			// this.
			//
			// ops[idx] = s.randBatch(rng)
			ops[idx] = s.randOp(rng)
		case 1:
			ops[idx] = s.randTxnOp(rng)
		case 2:
			ops[idx] = s.randOp(rng)
		case 3:
			ops[idx] = s.randSplit(rng)
		case 4:
			ops[idx] = s.randMerge(rng)
		default:
			panic(`unreachable`)
		}
	}

	return step(ops...)
}

func (s *Stepper) randOp(rng *rand.Rand) Operation {
	switch rng.Intn(2) {
	case 0:
		return s.randGet(rng)
	case 1:
		return s.randPut(rng)
	default:
		panic(`unreachable`)
	}
}

func (s *Stepper) randGet(rng *rand.Rand) Operation {
	if len(s.keys) == 0 || rng.Intn(100) < 10 {
		return get(randKey(rng))
	}
	key := randMapKey(rng, s.keys)
	return get(key)
}

func (s *Stepper) randPut(rng *rand.Rand) Operation {
	value := `v-` + strconv.Itoa(s.nextValue)
	s.nextValue++
	if len(s.keys) == 0 || rng.Intn(100) < 10 {
		key := randKey(rng)
		s.keys[key] = struct{}{}
		return put(key, value)
	}
	key := randMapKey(rng, s.keys)
	return put(key, value)
}

func (s *Stepper) randSplit(rng *rand.Rand) Operation {
	if len(s.splits) == 0 || rng.Intn(100) < 10 {
		return split(randKey(rng))
	}
	key := randMapKey(rng, s.splits)
	s.splits[key] = struct{}{}
	s.stepSplits[key] = struct{}{}
	return split(key)
}

func (s *Stepper) randMerge(rng *rand.Rand) Operation {
	if len(s.splits) == 0 || rng.Intn(100) < 10 {
		return merge(randKey(rng))
	}
	key := randMapKey(rng, s.splits)
	s.mergedSplits[key] = struct{}{}
	s.stepMerges[key] = struct{}{}
	return merge(key)
}

func (s *Stepper) randBatch(rng *rand.Rand) Operation {
	numOps := rng.Intn(4)
	ops := make([]Operation, numOps)
	for i := range ops {
		ops[i] = s.randOp(rng)
	}
	return batch(ops...)
}

func (s *Stepper) randTxnOp(rng *rand.Rand) Operation {
	if len(s.txns) == 0 || rng.Intn(100) < 10 {
		txnID := strconv.Itoa(s.nextTxnID)
		s.nextTxnID++
		s.stepTxns[txnID] = struct{}{}
		return beginTxn(txnID)
	}
	txnID := randMapKey(rng, s.txns)
	switch rng.Intn(3) {
	case 0:
		delete(s.txns, txnID)
		s.stepTxns[txnID] = struct{}{}

		numOps := rng.Intn(4)
		ops := make([]Operation, numOps)
		for i := range ops {
			switch rng.Intn(2) {
			case 0:
				ops[i] = s.randBatch(rng)
			case 1:
				ops[i] = s.randOp(rng)
			default:
				panic(`unreachable`)
			}
		}
		return useTxn(txnID, ops...)
	case 1:
		delete(s.txns, txnID)
		// TODO(dan): Add the txn's keys to s.keys.
		return commitTxn(txnID)
	case 2:
		delete(s.txns, txnID)
		return rollbackTxn(txnID)
	default:
		panic(`unreachable`)
	}
}

func randKey(rng *rand.Rand) string {
	u, err := uuid.NewGenWithReader(rng).NewV4()
	if err != nil {
		panic(err)
	}
	key := stepperSpan.Key
	key = encoding.EncodeStringAscending(key, u.String())
	return string(key)
}

func randMapKey(rng *rand.Rand, m map[string]struct{}) string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys[rng.Intn(len(keys))]
}

// ValidateStep verifies that the given Step adheres to the KV api contract.
func ValidateStep(s Step) error {
	usedTxns := make(map[string]struct{})
	useTxn := func(txnID string) error {
		if _, ok := usedTxns[txnID]; ok {
			return errors.Errorf("txn %s used concurrently", txnID)
		}
		usedTxns[txnID] = struct{}{}
		return nil
	}

	for _, op := range s.Ops {
		switch t := op.GetValue().(type) {
		case *BeginTxnOperation:
			if err := useTxn(t.TxnID); err != nil {
				return err
			}
		case *UseTxnOperation:
			if err := useTxn(t.TxnID); err != nil {
				return err
			}
		case *CommitTxnOperation:
			if err := useTxn(t.TxnID); err != nil {
				return err
			}
		case *RollbackTxnOperation:
			if err := useTxn(t.TxnID); err != nil {
				return err
			}
		}
	}
	return nil
}

func step(ops ...Operation) Step {
	return Step{Ops: ops}
}

func batch(ops ...Operation) Operation {
	return Operation{Batch: &BatchOperation{Ops: ops}}
}

func beginTxn(txnID string) Operation {
	return Operation{BeginTxn: &BeginTxnOperation{TxnID: txnID}}
}

func useTxn(txnID string, ops ...Operation) Operation {
	return Operation{UseTxn: &UseTxnOperation{TxnID: txnID, Ops: ops}}
}

func commitTxn(txnID string) Operation {
	return Operation{CommitTxn: &CommitTxnOperation{TxnID: txnID}}
}

func rollbackTxn(txnID string) Operation {
	return Operation{RollbackTxn: &RollbackTxnOperation{TxnID: txnID}}
}

func get(key string) Operation {
	return Operation{Get: &GetOperation{Key: []byte(key)}}
}

func put(key, value string) Operation {
	return Operation{Put: &PutOperation{Key: []byte(key), Value: []byte(value)}}
}

func split(key string) Operation {
	return Operation{Split: &SplitOperation{Key: []byte(key)}}
}

func merge(key string) Operation {
	return Operation{Merge: &MergeOperation{Key: []byte(key)}}
}
