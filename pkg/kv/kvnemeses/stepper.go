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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// OpP is a key used to configure the relative proportions of various options.
type OpP string

// The various values of OpP.
const (
	OpPGetExisting   OpP = "GetExisting"
	OpPGetMissing    OpP = "GetMissing"
	OpPPutExisting   OpP = "PutExisting"
	OpPPutMissing    OpP = "PutMissing"
	OpPBatch         OpP = "Batch"
	OpPClosureTxn    OpP = "ClosureTxn"
	OpPSplitNew      OpP = "SplitNew"
	OpPSplitPrevious OpP = "SplitPrevious"
	OpPMergeNotSplit OpP = "MergeNotSplit"
	OpPMergeIsSplit  OpP = "MergeIsSplit"
)

// StepperConfig configures the releative probabilities of producing various
// operations.
type StepperConfig map[OpP]int

// StepperDataSpan returns a span that contains all of the operations created by
// this Stepper.
func StepperDataSpan() roachpb.Span {
	return roachpb.Span{
		Key:    roachpb.Key(keys.MakeTablePrefix(50)),
		EndKey: roachpb.Key(keys.MakeTablePrefix(51)),
	}
}

// Stepper incrementally constructs KV traffic designed to maximally test edge
// cases.
//
// The expected usage is that a number of concurrent worker threads will each
// repeatedly ask for a Step, finish executing it, then ask for the next Step.
//
// A Step consists of a single Operation, which is a unit of work that must be
// done serially. It often corresponds 1:1 to a single call to some method on
// the KV api (such as Get or Put), but some Operations have a set of steps
// (such as using a transaction).
//
// Stepper intentionally does not have deterministic results. For example, one
// worker thread could have an Step to update a key and another thread could
// concurrently have a Step to delete the same key. The outcome would be race-y.
type Stepper struct {
	Config StepperConfig

	nextValue int

	// keys is the set of every key that has been written to, including those in
	// rolled back transactions.
	keys map[string]struct{}

	// currentSplits is approximately the set of every split that has been made
	// within DataSpan. The exact accounting is hard because Stepper can hand out
	// a concurrent split and merge for the same key, which is racey. Luckily we
	// don't need exact accounting.
	currentSplits map[string]struct{}
	// historicalSplits is the set of every key that has been split at, regardless
	// of whether it has been merged since.
	historicalSplits map[string]struct{}
}

// MakeStepper constructs a Stepper.
func MakeStepper(config StepperConfig) *Stepper {
	return &Stepper{
		Config:           config,
		keys:             make(map[string]struct{}),
		currentSplits:    make(map[string]struct{}),
		historicalSplits: make(map[string]struct{}),
	}
}

// RandStep returns a single randomly generated next operation to execute.
//
// RandStep is not concurrency safe.
func (s *Stepper) RandStep(rng *rand.Rand) Step {
	allowed := make(map[OpP]opGenFunc)
	s.registerClientOps(allowed)
	allowed[OpPBatch] = randBatch
	allowed[OpPClosureTxn] = randClosureTxn
	allowed[OpPSplitNew] = randSplitNew
	allowed[OpPMergeNotSplit] = randMergeNotSplit

	if len(s.historicalSplits) > 0 {
		allowed[OpPSplitPrevious] = randSplitPrevious
	}
	if len(s.currentSplits) > 0 {
		allowed[OpPMergeIsSplit] = randMergeIsSplit
	}

	return step(s.selectOp(rng, allowed))
}

type opGenFunc func(*Stepper, *rand.Rand) Operation

func (s *Stepper) selectOp(rng *rand.Rand, contextuallyValid map[OpP]opGenFunc) Operation {
	var total int
	for op := range contextuallyValid {
		total += s.Config[op]
	}
	target := rng.Intn(total)
	var sum int
	for op, fn := range contextuallyValid {
		sum += s.Config[op]
		if sum > target {
			return fn(s, rng)
		}
	}
	panic(`unreachable`)
}

func (s *Stepper) registerClientOps(allowed map[OpP]opGenFunc) {
	allowed[OpPGetMissing] = randGetMissing
	allowed[OpPPutMissing] = randPutMissing
	if len(s.keys) > 0 {
		allowed[OpPGetExisting] = randGetExisting
		allowed[OpPPutExisting] = randPutExisting
	}
}

func randGetMissing(s *Stepper, rng *rand.Rand) Operation {
	return get(randKey(rng))
}

func randGetExisting(s *Stepper, rng *rand.Rand) Operation {
	key := randMapKey(rng, s.keys)
	return get(key)
}

func randPutMissing(s *Stepper, rng *rand.Rand) Operation {
	value := s.getNextValue()
	key := randKey(rng)
	s.keys[key] = struct{}{}
	return put(key, value)
}

func randPutExisting(s *Stepper, rng *rand.Rand) Operation {
	value := s.getNextValue()
	key := randMapKey(rng, s.keys)
	return put(key, value)
}

func randSplitNew(s *Stepper, rng *rand.Rand) Operation {
	key := randKey(rng)
	s.currentSplits[key] = struct{}{}
	s.historicalSplits[key] = struct{}{}
	return split(key)
}

func randSplitPrevious(s *Stepper, rng *rand.Rand) Operation {
	key := randMapKey(rng, s.historicalSplits)
	s.currentSplits[key] = struct{}{}
	return split(key)
}

func randMergeNotSplit(s *Stepper, rng *rand.Rand) Operation {
	key := randKey(rng)
	return merge(key)
}

func randMergeIsSplit(s *Stepper, rng *rand.Rand) Operation {
	key := randMapKey(rng, s.currentSplits)
	// Assume that this split actually got merged, even though we may have handed
	// out a concurrent split for the same key.
	delete(s.currentSplits, key)
	return merge(key)
}

func randBatch(s *Stepper, rng *rand.Rand) Operation {
	allowed := make(map[OpP]opGenFunc)
	s.registerClientOps(allowed)

	numOps := rng.Intn(4)
	ops := make([]Operation, numOps)
	for i := range ops {
		ops[i] = s.selectOp(rng, allowed)
	}
	return batch(ops...)
}

func randClosureTxn(s *Stepper, rng *rand.Rand) Operation {
	allowed := make(map[OpP]opGenFunc)
	s.registerClientOps(allowed)
	allowed[OpPBatch] = randBatch

	numOps := rng.Intn(4)
	ops := make([]Operation, numOps)
	for i := range ops {
		ops[i] = s.selectOp(rng, allowed)
	}
	typ := ClosureTxnType(rng.Intn(2))
	return closureTxn(typ, ops...)
}

func (s *Stepper) getNextValue() string {
	value := `v-` + strconv.Itoa(s.nextValue)
	s.nextValue++
	return value
}

func randKey(rng *rand.Rand) string {
	u, err := uuid.NewGenWithReader(rng).NewV4()
	if err != nil {
		panic(err)
	}
	key := StepperDataSpan().Key
	key = encoding.EncodeStringAscending(key, u.Short())
	return string(key)
}

func randMapKey(rng *rand.Rand, m map[string]struct{}) string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return randKey(rng)
	}
	return keys[rng.Intn(len(keys))]
}

func step(op Operation) Step {
	return Step{Op: op}
}

func batch(ops ...Operation) Operation {
	return Operation{Batch: &BatchOperation{Ops: ops}}
}

func closureTxn(typ ClosureTxnType, ops ...Operation) Operation {
	return Operation{ClosureTxn: &ClosureTxnOperation{Ops: ops, Type: typ}}
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
