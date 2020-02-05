// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"math/rand"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// OpP is a key used to configure the relative proportions of various options.
type OpP string

// The various values of OpP. In the following, wording such as "likely exists"
// or "definitely doesn't exist" is according to previously generated steps.
// "likely" is a result of non-determinism due to concurrent execution of the
// generated operations.
const (
	// OpPGetExisting is an operation that Gets a key that likely exists.
	OpPGetExisting OpP = "GetExisting"

	// OpPGetMissing is an operation that Gets a key that definitely doesn't
	// exist.
	OpPGetMissing OpP = "GetMissing"

	// OpPPutExisting is an operation that Puts a key that likely exists.
	OpPPutExisting OpP = "PutExisting"

	// OpPPutMissing is an operation that Puts a key that definitely doesn't
	// exist.
	OpPPutMissing OpP = "PutMissing"

	// OpPBatch is an operation that represents a series of Operations performed
	// on a client.Batch. These can be run in various ways including client.DB.Run
	// or client.Txn.Run.
	OpPBatch OpP = "Batch"

	// OpPBatch is an operation that represents a series of Operations performed
	// inside a closure handed to client.DB.Txn.
	OpPClosureTxn OpP = "ClosureTxn"

	// OpPClosureTxnCommitInBatch is the same as OpPBatch except that the
	// transaction is committed by client.Txn.CommitInBatch instead of by db.Txn
	// after running the closure.
	OpPClosureTxnCommitInBatch OpP = "ClosureTxnCommitInBatch"

	// OpPSplitNew is an operation that Splits at a key that has never previously
	// been a split point.
	OpPSplitNew OpP = "SplitNew"

	// OpPSplitAgain is an operation that Splits at a key that likely has
	// previously been a split point, though it may or may not have been merged
	// since.
	OpPSplitAgain OpP = "SplitAgain"

	// OpPMergeNotSplit is an operation that Merges at a key that has never been
	// split at (meaning this should be a no-op).
	OpPMergeNotSplit OpP = "MergeNotSplit"

	// OpPMergeIsSplit is an operation that Merges at a key that is likely to
	// currently be split.
	OpPMergeIsSplit OpP = "MergeIsSplit"

	// OpPChangeReplicas is an operation that adds and/or removes replicas from
	// the range containing a key.
	OpPChangeReplicas OpP = "ChangeReplicas"
)

// GeneratorConfig configures the relative probabilities of producing various
// operations.
type GeneratorConfig struct {
	OpPs                  map[OpP]int
	NumNodes, NumReplicas int
}

// newAllOperationsConfig returns a GeneratorConfig that exercises *all*
// options. You probably want NewDefaultConfig. Most of the time, these will be
// the same, but having both allows us to merge code for operations that do not
// yet pass (for example, if the new operation finds a kv bug or edge case).
func newAllOperationsConfig() GeneratorConfig {
	return GeneratorConfig{OpPs: map[OpP]int{
		OpPGetMissing:              1,
		OpPGetExisting:             1,
		OpPPutMissing:              1,
		OpPPutExisting:             1,
		OpPBatch:                   1,
		OpPClosureTxn:              5,
		OpPClosureTxnCommitInBatch: 5,
		OpPSplitNew:                1,
		OpPSplitAgain:              1,
		OpPMergeNotSplit:           1,
		OpPMergeIsSplit:            1,
		OpPChangeReplicas:          1,
	}}
}

// NewDefaultConfig returns a GeneratorConfig that is a reasonable default
// starting point for general KV usage. Nemesis test variants that want to
// stress particular areas may want to start with this and eliminate some
// operations/make some operations more likely.
func NewDefaultConfig() GeneratorConfig {
	config := newAllOperationsConfig()
	// TODO(dan): This fails with a WriteTooOld error if the same key is Put twice
	// in a single batch. However, if the same Batch is committed using txn.Run,
	// then it works and only the last one is materialized. We could make the
	// db.Run behavior match txn.Run by ensuring that all requests in a
	// nontransactional batch are disjoint and upgrading to a transactional batch
	// (see CrossRangeTxnWrapperSender) if they are. roachpb.SpanGroup can be used
	// to efficiently check this.
	config.OpPs[OpPBatch] = 0
	return config
}

// GeneratorDataSpan returns a span that contains all of the operations created
// by this Generator.
func GeneratorDataSpan() roachpb.Span {
	return roachpb.Span{
		Key:    roachpb.Key(keys.MakeTablePrefix(50)),
		EndKey: roachpb.Key(keys.MakeTablePrefix(51)),
	}
}

// GetReplicasFn is a function that returns the current replicas for the range
// containing a key.
type GetReplicasFn func(roachpb.Key) []roachpb.ReplicationTarget

// Generator incrementally constructs KV traffic designed to maximally test edge
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
// Generator in itself is deterministic, but it's intended usage is that
// multiple worker goroutines take turns pulling steps (sequentially) which they
// then execute concurrently. To improve the efficiency of this pattern,
// Generator will track which splits and merges could possibly have taken place
// and takes this into account when generating operations. For example,
// Generator won't take a OpMergeIsSplit step if it has never previously emitted
// a split, but it may emit an OpMerge once it has produced an OpSplit even
// though the worker executing the split may find that the merge has not yet
// been executed.
type Generator struct {
	// TODO(dan): This is awkward, unify Generator and generator.
	mu struct {
		syncutil.Mutex
		generator
	}
}

// MakeGenerator constructs a Generator.
func MakeGenerator(config GeneratorConfig, replicasFn GetReplicasFn) (*Generator, error) {
	if config.NumNodes <= 0 {
		return nil, errors.Errorf(`NumNodes must be positive got: %d`, config.NumNodes)
	}
	if config.NumReplicas <= 0 {
		return nil, errors.Errorf(`NumReplicas must be positive got: %d`, config.NumReplicas)
	}
	if config.NumReplicas > config.NumNodes {
		return nil, errors.Errorf(`NumReplicas (%d) must <= NumNodes (%d)`,
			config.NumReplicas, config.NumNodes)
	}
	g := &Generator{}
	g.mu.generator = generator{
		Config:           config,
		replicasFn:       replicasFn,
		keys:             make(map[string]struct{}),
		currentSplits:    make(map[string]struct{}),
		historicalSplits: make(map[string]struct{}),
	}
	return g, nil
}

// RandStep returns a single randomly generated next operation to execute.
//
// RandStep is concurrency safe.
func (g *Generator) RandStep(rng *rand.Rand) Step {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mu.RandStep(rng)
}

type generator struct {
	Config     GeneratorConfig
	replicasFn GetReplicasFn

	nextValue int

	// keys is the set of every key that has been written to, including those in
	// rolled back transactions.
	keys map[string]struct{}

	// currentSplits is approximately the set of every split that has been made
	// within DataSpan. The exact accounting is hard because Generator can hand
	// out a concurrent split and merge for the same key, which is racey. These
	// races can result in a currentSplit that is not in fact a split at the KV
	// level. Luckily we don't need exact accounting.
	currentSplits map[string]struct{}
	// historicalSplits is the set of every key for which a split has been
	// emitted, regardless of whether the split has since been applied or been
	// merged away again.
	historicalSplits map[string]struct{}
}

// RandStep returns a single randomly generated next operation to execute.
//
// RandStep is not concurrency safe.
func (g *generator) RandStep(rng *rand.Rand) Step {
	allowed := make(map[OpP]opGenFunc)
	g.registerClientOps(allowed)
	allowed[OpPBatch] = randBatch
	allowed[OpPClosureTxn] = randClosureTxn
	allowed[OpPClosureTxnCommitInBatch] = randClosureTxnCommitInBatch
	allowed[OpPSplitNew] = randSplitNew
	allowed[OpPMergeNotSplit] = randMergeNotSplit

	if len(g.historicalSplits) > 0 {
		allowed[OpPSplitAgain] = randSplitAgain
	}
	if len(g.currentSplits) > 0 {
		allowed[OpPMergeIsSplit] = randMergeIsSplit
	}
	if g.Config.NumNodes > g.Config.NumReplicas {
		allowed[OpPChangeReplicas] = randChangeReplicas
	}

	return step(g.selectOp(rng, allowed))
}

type opGenFunc func(*generator, *rand.Rand) Operation

func (g *generator) selectOp(rng *rand.Rand, contextuallyValid map[OpP]opGenFunc) Operation {
	var total int
	for op := range contextuallyValid {
		total += g.Config.OpPs[op]
	}
	target := rng.Intn(total)
	var sum int
	for op, fn := range contextuallyValid {
		sum += g.Config.OpPs[op]
		if sum > target {
			return fn(g, rng)
		}
	}
	panic(`unreachable`)
}

func (g *generator) registerClientOps(allowed map[OpP]opGenFunc) {
	allowed[OpPGetMissing] = randGetMissing
	allowed[OpPPutMissing] = randPutMissing
	if len(g.keys) > 0 {
		allowed[OpPGetExisting] = randGetExisting
		allowed[OpPPutExisting] = randPutExisting
	}
}

func randGetMissing(_ *generator, rng *rand.Rand) Operation {
	return get(randKey(rng))
}

func randGetExisting(g *generator, rng *rand.Rand) Operation {
	key := randMapKey(rng, g.keys)
	return get(key)
}

func randPutMissing(g *generator, rng *rand.Rand) Operation {
	value := g.getNextValue()
	key := randKey(rng)
	g.keys[key] = struct{}{}
	return put(key, value)
}

func randPutExisting(g *generator, rng *rand.Rand) Operation {
	value := g.getNextValue()
	key := randMapKey(rng, g.keys)
	return put(key, value)
}

func randSplitNew(g *generator, rng *rand.Rand) Operation {
	key := randKey(rng)
	g.currentSplits[key] = struct{}{}
	g.historicalSplits[key] = struct{}{}
	return split(key)
}

func randSplitAgain(g *generator, rng *rand.Rand) Operation {
	key := randMapKey(rng, g.historicalSplits)
	g.currentSplits[key] = struct{}{}
	return split(key)
}

func randMergeNotSplit(g *generator, rng *rand.Rand) Operation {
	key := randKey(rng)
	return merge(key)
}

func randMergeIsSplit(g *generator, rng *rand.Rand) Operation {
	key := randMapKey(rng, g.currentSplits)
	// Assume that this split actually got merged, even though we may have handed
	// out a concurrent split for the same key.
	delete(g.currentSplits, key)
	return merge(key)
}

func randChangeReplicas(g *generator, rng *rand.Rand) Operation {
	key := randKey(rng)
	current := g.replicasFn(roachpb.Key(key))
	var changes []roachpb.ReplicationChange
	if len(current) > g.Config.NumReplicas {
		changes = append(changes, roachpb.ReplicationChange{
			ChangeType: roachpb.REMOVE_REPLICA,
			Target:     current[rng.Intn(len(current))],
		})
	} else if len(current) < g.Config.NumNodes {
		candidatesMap := make(map[roachpb.ReplicationTarget]struct{})
		for i := 0; i < g.Config.NumNodes; i++ {
			t := roachpb.ReplicationTarget{NodeID: roachpb.NodeID(i + 1), StoreID: roachpb.StoreID(i + 1)}
			candidatesMap[t] = struct{}{}
		}
		for _, replica := range current {
			delete(candidatesMap, replica)
		}
		var candidates []roachpb.ReplicationTarget
		for candidate := range candidatesMap {
			candidates = append(candidates, candidate)
		}
		candidate := candidates[rng.Intn(len(candidates))]
		changes = append(changes, roachpb.ReplicationChange{
			ChangeType: roachpb.ADD_REPLICA,
			Target:     candidate,
		})
		// Sometimes test atomic swaps
		if len(current) == g.Config.NumReplicas && rng.Intn(2) == 0 {
			changes = append(changes, roachpb.ReplicationChange{
				ChangeType: roachpb.REMOVE_REPLICA,
				Target:     current[rng.Intn(len(current))],
			})
		}
	}
	return changeReplicas(key, changes...)
}

func randBatch(g *generator, rng *rand.Rand) Operation {
	allowed := make(map[OpP]opGenFunc)
	g.registerClientOps(allowed)

	numOps := rng.Intn(4)
	ops := make([]Operation, numOps)
	for i := range ops {
		ops[i] = g.selectOp(rng, allowed)
	}
	return batch(ops...)
}

func randClosureTxn(g *generator, rng *rand.Rand) Operation {
	allowed := make(map[OpP]opGenFunc)
	g.registerClientOps(allowed)
	allowed[OpPBatch] = randBatch

	numOps := rng.Intn(4)
	ops := make([]Operation, numOps)
	for i := range ops {
		ops[i] = g.selectOp(rng, allowed)
	}
	typ := ClosureTxnType(rng.Intn(2))
	return closureTxn(typ, ops...)
}

func randClosureTxnCommitInBatch(g *generator, rng *rand.Rand) Operation {
	o := randClosureTxn(g, rng)
	o.ClosureTxn.CommitInBatch = randBatch(g, rng).Batch
	o.ClosureTxn.Type = ClosureTxnType_Commit
	return o
}

func (g *generator) getNextValue() string {
	value := `v-` + strconv.Itoa(g.nextValue)
	g.nextValue++
	return value
}

func randKey(rng *rand.Rand) string {
	u, err := uuid.NewGenWithReader(rng).NewV4()
	if err != nil {
		panic(err)
	}
	key := GeneratorDataSpan().Key
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

func opSlice(ops ...Operation) []Operation {
	return ops
}

func closureTxn(typ ClosureTxnType, ops ...Operation) Operation {
	return Operation{ClosureTxn: &ClosureTxnOperation{Ops: ops, Type: typ}}
}

func closureTxnCommitInBatch(commitInBatch []Operation, ops ...Operation) Operation {
	o := closureTxn(ClosureTxnType_Commit, ops...)
	if len(commitInBatch) > 0 {
		o.ClosureTxn.CommitInBatch = &BatchOperation{Ops: commitInBatch}
	}
	return o
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

func changeReplicas(key string, changes ...roachpb.ReplicationChange) Operation {
	return Operation{ChangeReplicas: &ChangeReplicasOperation{Key: []byte(key), Changes: changes}}
}
