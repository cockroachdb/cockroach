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
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// GeneratorConfig contains all the tunable knobs necessary to run a Generator.
type GeneratorConfig struct {
	Ops                   OperationConfig
	NumNodes, NumReplicas int
}

// OperationConfig configures the relative probabilities of producing various
// operations.
//
// In this struct and all sub-configurations, wording such as "likely exists" or
// "definitely doesn't exist" is according to previously generated steps.
// "likely" is a result of non-determinism due to concurrent execution of the
// generated operations.
type OperationConfig struct {
	DB             ClientOperationConfig
	Batch          BatchOperationConfig
	ClosureTxn     ClosureTxnConfig
	Split          SplitConfig
	Merge          MergeConfig
	ChangeReplicas ChangeReplicasConfig
	ChangeLease    ChangeLeaseConfig
	ChangeZone     ChangeZoneConfig
}

// ClosureTxnConfig configures the relative probability of running some
// operations in a transaction by using the closure-based kv.DB.Txn method. This
// family of operations mainly varies in how it commits (or doesn't commit). The
// composition of the operations in the txn is controlled by TxnClientOps and
// TxnBatchOps
type ClosureTxnConfig struct {
	TxnClientOps ClientOperationConfig
	TxnBatchOps  BatchOperationConfig

	// Commit is a transaction that commits normally.
	Commit int
	// Rollback is a transaction that encounters an error at the end and has to
	// roll back.
	Rollback int
	// CommitInBatch is a transaction that commits via the CommitInBatchMethod.
	// This is an important part of the 1pc txn fastpath.
	CommitInBatch int
	// When CommitInBatch is selected, CommitBatchOps controls the composition of
	// the kv.Batch used.
	CommitBatchOps ClientOperationConfig
}

// ClientOperationConfig configures the relative probabilities of the
// bread-and-butter kv operations such as Get/Put/Delete/etc. These can all be
// run on a DB, a Txn, or a Batch.
type ClientOperationConfig struct {
	// GetMissing is an operation that Gets a key that definitely doesn't exist.
	GetMissing int
	// GetMissingForUpdate is an operation that Gets a key that definitely
	// doesn't exist using a locking read.
	GetMissingForUpdate int
	// GetExisting is an operation that Gets a key that likely exists.
	GetExisting int
	// GetExistingForUpdate is an operation that Gets a key that likely exists
	// using a locking read.
	GetExistingForUpdate int
	// PutMissing is an operation that Puts a key that definitely doesn't exist.
	PutMissing int
	// PutExisting is an operation that Puts a key that likely exists.
	PutExisting int
	// Scan is an operation that Scans a key range that may contain values.
	Scan int
	// ScanForUpdate is an operation that Scans a key range that may contain
	// values using a per-key locking scan.
	ScanForUpdate int
	// ReverseScan is an operation that Scans a key range that may contain
	// values in reverse key order.
	ReverseScan int
	// ReverseScanForUpdate is an operation that Scans a key range that may
	// contain values using a per-key locking scan in reverse key order.
	ReverseScanForUpdate int
	// DeleteMissing is an operation that Deletes a key that definitely doesn't exist.
	DeleteMissing int
	// DeleteExisting is an operation that Deletes a key that likely exists.
	DeleteExisting int
	// DeleteRange is an operation that Deletes a key range that may contain values.
	DeleteRange int
	// DeleteRange is an operation that invokes DeleteRangeUsingTombstone.
	DeleteRangeUsingTombstone int
	// AddSSTable is an operations that ingests an SSTable with random KV pairs.
	AddSSTable int
}

// BatchOperationConfig configures the relative probability of generating a
// kv.Batch of some number of operations as well as the composition of the
// operations in the batch itself. These can be run in various ways including
// kv.DB.Run or kv.Txn.Run.
type BatchOperationConfig struct {
	Batch int
	Ops   ClientOperationConfig
}

// SplitConfig configures the relative probability of generating a Split
// operation.
type SplitConfig struct {
	// SplitNew is an operation that Splits at a key that has never previously
	// been a split point.
	SplitNew int
	// SplitAgain is an operation that Splits at a key that likely has
	// previously been a split point, though it may or may not have been merged
	// since.
	SplitAgain int
}

// MergeConfig configures the relative probability of generating a Merge
// operation.
type MergeConfig struct {
	// MergeNotSplit is an operation that Merges at a key that has never been
	// split at (meaning this should be a no-op).
	MergeNotSplit int
	// MergeIsSplit is an operation that Merges at a key that is likely to
	// currently be split.
	MergeIsSplit int
}

// ChangeReplicasConfig configures the relative probability of generating a
// ChangeReplicas operation.
type ChangeReplicasConfig struct {
	// AddReplica adds a single replica.
	AddReplica int
	// RemoveReplica removes a single replica.
	RemoveReplica int
	// AtomicSwapReplica adds 1 replica and removes 1 replica in a single
	// ChangeReplicas call.
	AtomicSwapReplica int
}

// ChangeLeaseConfig configures the relative probability of generating an
// operation that causes a leaseholder change.
type ChangeLeaseConfig struct {
	// Transfer the lease to a random replica.
	TransferLease int
}

// ChangeZoneConfig configures the relative probability of generating a zone
// configuration change operation.
type ChangeZoneConfig struct {
	// ToggleGlobalReads sets global_reads to a new value.
	ToggleGlobalReads int
}

// newAllOperationsConfig returns a GeneratorConfig that exercises *all*
// options. You probably want NewDefaultConfig. Most of the time, these will be
// the same, but having both allows us to merge code for operations that do not
// yet pass (for example, if the new operation finds a kv bug or edge case).
func newAllOperationsConfig() GeneratorConfig {
	clientOpConfig := ClientOperationConfig{
		GetMissing:                1,
		GetMissingForUpdate:       1,
		GetExisting:               1,
		GetExistingForUpdate:      1,
		PutMissing:                1,
		PutExisting:               1,
		Scan:                      1,
		ScanForUpdate:             1,
		ReverseScan:               1,
		ReverseScanForUpdate:      1,
		DeleteMissing:             1,
		DeleteExisting:            1,
		DeleteRange:               1,
		DeleteRangeUsingTombstone: 1,
		AddSSTable:                1,
	}
	batchOpConfig := BatchOperationConfig{
		Batch: 4,
		Ops:   clientOpConfig,
	}
	return GeneratorConfig{Ops: OperationConfig{
		DB:    clientOpConfig,
		Batch: batchOpConfig,
		ClosureTxn: ClosureTxnConfig{
			Commit:         5,
			Rollback:       5,
			CommitInBatch:  5,
			TxnClientOps:   clientOpConfig,
			TxnBatchOps:    batchOpConfig,
			CommitBatchOps: clientOpConfig,
		},
		Split: SplitConfig{
			SplitNew:   1,
			SplitAgain: 1,
		},
		Merge: MergeConfig{
			MergeNotSplit: 1,
			MergeIsSplit:  1,
		},
		ChangeReplicas: ChangeReplicasConfig{
			AddReplica:        1,
			RemoveReplica:     1,
			AtomicSwapReplica: 1,
		},
		ChangeLease: ChangeLeaseConfig{
			TransferLease: 1,
		},
		ChangeZone: ChangeZoneConfig{
			ToggleGlobalReads: 1,
		},
	}}
}

// NewDefaultConfig returns a GeneratorConfig that is a reasonable default
// starting point for general KV usage. Nemesis test variants that want to
// stress particular areas may want to start with this and eliminate some
// operations/make some operations more likely.
func NewDefaultConfig() GeneratorConfig {
	config := newAllOperationsConfig()
	// DeleteRangeUsingTombstone does not support transactions.
	config.Ops.ClosureTxn.TxnClientOps.DeleteRangeUsingTombstone = 0
	config.Ops.ClosureTxn.TxnBatchOps.Ops.DeleteRangeUsingTombstone = 0
	config.Ops.ClosureTxn.CommitBatchOps.DeleteRangeUsingTombstone = 0
	// DeleteRangeUsingTombstone does in principle support batches, but
	// in kvnemesis we don't let it span ranges non-atomically (as it
	// is allowed to do in CRDB). The generator already tries to avoid
	// crossing range boundaries quite a fair bit, so we could enable this
	// after some investigation to ensure that significant enough coverage
	// remains on the batch path.
	// Note also that at the time of writing `config.Ops.Batch` is cleared in its
	// entirety below, so changing this line alonewon't have an effect.
	config.Ops.Batch.Ops.DeleteRangeUsingTombstone = 0
	// TODO(sarkesian): Enable DeleteRange in comingled batches once #71236 is fixed.
	config.Ops.ClosureTxn.CommitBatchOps.DeleteRange = 0
	config.Ops.ClosureTxn.TxnBatchOps.Ops.DeleteRange = 0
	// TODO(dan): This fails with a WriteTooOld error if the same key is Put twice
	// in a single batch. However, if the same Batch is committed using txn.Run,
	// then it works and only the last one is materialized. We could make the
	// db.Run behavior match txn.Run by ensuring that all requests in a
	// nontransactional batch are disjoint and upgrading to a transactional batch
	// (see CrossRangeTxnWrapperSender) if they are. roachpb.SpanGroup can be used
	// to efficiently check this.
	//
	// TODO(tbg): could make this `config.Ops.Batch.Ops.PutExisting = 0` (and
	// DeleteRange, etc, all ops that can overwrite existing keys basically), as
	// #46081 has long been fixed. Then file an issue about generating
	// non-self-overlapping operations for batches.
	config.Ops.Batch = BatchOperationConfig{}
	// TODO(tbg): should be able to remove the two lines below, since
	// #45586 has already been addressed.
	config.Ops.ClosureTxn.CommitBatchOps.GetExisting = 0
	config.Ops.ClosureTxn.CommitBatchOps.GetMissing = 0
	// AddSSTable cannot be used in transactions, nor in batches.
	config.Ops.Batch.Ops.AddSSTable = 0
	config.Ops.ClosureTxn.CommitBatchOps.AddSSTable = 0
	config.Ops.ClosureTxn.TxnClientOps.AddSSTable = 0
	config.Ops.ClosureTxn.TxnBatchOps.Ops.AddSSTable = 0
	return config
}

// GeneratorDataTableID is the table ID that corresponds to GeneratorDataSpan.
// This must be a table ID that is not used in a new cluster.
var GeneratorDataTableID = bootstrap.TestingMinUserDescID()

// GeneratorDataSpan returns a span that contains all of the operations created
// by this Generator.
func GeneratorDataSpan() roachpb.Span {
	return roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(GeneratorDataTableID),
		EndKey: keys.SystemSQLCodec.TablePrefix(GeneratorDataTableID + 1),
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

	seqGen kvnemesisutil.Seq

	// keys is the set of every key that has been written to, including those
	// deleted or in rolled back transactions.
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
	var allowed []opGen
	g.registerClientOps(&allowed, &g.Config.Ops.DB)
	g.registerBatchOps(&allowed, &g.Config.Ops.Batch)
	g.registerClosureTxnOps(&allowed, &g.Config.Ops.ClosureTxn)

	addOpGen(&allowed, randSplitNew, g.Config.Ops.Split.SplitNew)
	if len(g.historicalSplits) > 0 {
		addOpGen(&allowed, randSplitAgain, g.Config.Ops.Split.SplitAgain)
	}

	addOpGen(&allowed, randMergeNotSplit, g.Config.Ops.Merge.MergeNotSplit)
	if len(g.currentSplits) > 0 {
		addOpGen(&allowed, randMergeIsSplit, g.Config.Ops.Merge.MergeIsSplit)
	}

	key := randKey(rng)
	current := g.replicasFn(roachpb.Key(key))
	if len(current) < g.Config.NumNodes {
		addReplicaFn := makeAddReplicaFn(key, current, false /* atomicSwap */)
		addOpGen(&allowed, addReplicaFn, g.Config.Ops.ChangeReplicas.AddReplica)
	}
	if len(current) == g.Config.NumReplicas && len(current) < g.Config.NumNodes {
		atomicSwapReplicaFn := makeAddReplicaFn(key, current, true /* atomicSwap */)
		addOpGen(&allowed, atomicSwapReplicaFn, g.Config.Ops.ChangeReplicas.AtomicSwapReplica)
	}
	if len(current) > g.Config.NumReplicas {
		removeReplicaFn := makeRemoveReplicaFn(key, current)
		addOpGen(&allowed, removeReplicaFn, g.Config.Ops.ChangeReplicas.RemoveReplica)
	}
	transferLeaseFn := makeTransferLeaseFn(key, current)
	addOpGen(&allowed, transferLeaseFn, g.Config.Ops.ChangeLease.TransferLease)

	addOpGen(&allowed, toggleGlobalReads, g.Config.Ops.ChangeZone.ToggleGlobalReads)

	return step(g.selectOp(rng, allowed))
}

func (g *generator) nextSeq() kvnemesisutil.Seq {
	g.seqGen++
	return g.seqGen
}

type opGenFunc func(*generator, *rand.Rand) Operation

type opGen struct {
	fn     opGenFunc
	weight int
}

func addOpGen(valid *[]opGen, fn opGenFunc, weight int) {
	*valid = append(*valid, opGen{fn: fn, weight: weight})
}

func (g *generator) selectOp(rng *rand.Rand, contextuallyValid []opGen) Operation {
	var total int
	for _, x := range contextuallyValid {
		total += x.weight
	}
	target := rng.Intn(total)
	var sum int
	for _, x := range contextuallyValid {
		sum += x.weight
		if sum > target {
			return x.fn(g, rng)
		}
	}
	panic(`unreachable`)
}

func (g *generator) registerClientOps(allowed *[]opGen, c *ClientOperationConfig) {
	addOpGen(allowed, randGetMissing, c.GetMissing)
	addOpGen(allowed, randGetMissingForUpdate, c.GetMissingForUpdate)
	addOpGen(allowed, randPutMissing, c.PutMissing)
	addOpGen(allowed, randDelMissing, c.DeleteMissing)
	if len(g.keys) > 0 {
		addOpGen(allowed, randGetExisting, c.GetExisting)
		addOpGen(allowed, randGetExistingForUpdate, c.GetExistingForUpdate)
		addOpGen(allowed, randPutExisting, c.PutExisting)
		addOpGen(allowed, randDelExisting, c.DeleteExisting)
	}
	addOpGen(allowed, randScan, c.Scan)
	addOpGen(allowed, randScanForUpdate, c.ScanForUpdate)
	addOpGen(allowed, randReverseScan, c.ReverseScan)
	addOpGen(allowed, randReverseScanForUpdate, c.ReverseScanForUpdate)
	addOpGen(allowed, randDelRange, c.DeleteRange)
	addOpGen(allowed, randDelRangeUsingTombstone, c.DeleteRangeUsingTombstone)
	addOpGen(allowed, randAddSSTable, c.AddSSTable)
}

func (g *generator) registerBatchOps(allowed *[]opGen, c *BatchOperationConfig) {
	addOpGen(allowed, makeRandBatch(&c.Ops), c.Batch)
}

func randGetMissing(_ *generator, rng *rand.Rand) Operation {
	return get(randKey(rng))
}

func randGetMissingForUpdate(_ *generator, rng *rand.Rand) Operation {
	op := get(randKey(rng))
	op.Get.ForUpdate = true
	return op
}

func randGetExisting(g *generator, rng *rand.Rand) Operation {
	key := randMapKey(rng, g.keys)
	return get(key)
}

func randGetExistingForUpdate(g *generator, rng *rand.Rand) Operation {
	key := randMapKey(rng, g.keys)
	op := get(key)
	op.Get.ForUpdate = true
	return op
}

func randPutMissing(g *generator, rng *rand.Rand) Operation {
	seq := g.nextSeq()
	key := randKey(rng)
	g.keys[key] = struct{}{}
	return put(key, seq)
}

func randPutExisting(g *generator, rng *rand.Rand) Operation {
	seq := g.nextSeq()
	key := randMapKey(rng, g.keys)
	return put(key, seq)
}

func randAddSSTable(g *generator, rng *rand.Rand) Operation {
	ctx := context.Background()

	sstTimestamp := hlc.MinTimestamp // replaced via SSTTimestampToRequestTimestamp
	numKeys := rng.Intn(16) + 1      // number of point keys
	probReplace := 0.2               // probability to replace existing key, if possible
	probTombstone := 0.2             // probability to write a tombstone
	asWrites := rng.Float64() < 0.2  // IngestAsWrites

	// AddSSTable requests cannot span multiple ranges, so we try to fit them
	// within an existing range. This may race with a concurrent split, in which
	// case the AddSSTable will fail, but that's ok -- most should still succeed.
	rangeStart, rangeEnd := randRangeSpan(rng, g.currentSplits)
	rangeKeys := keysBetween(g.keys, rangeStart, rangeEnd)

	// Generate keys first, to write them in order and without duplicates. We pick
	// either existing or new keys depending on probReplace, making sure they're
	// unique.
	//
	// TODO(erikgrinaker): For now, only ingest point keys. We currently don't
	// ingest range keys in production code, although we soon will.
	sstKeys := []string{}
	sstKeysMap := map[string]struct{}{}
	for len(sstKeys) < numKeys {
		var key string
		if len(rangeKeys) > 0 && rng.Float64() < probReplace {
			// Pick a random existing key when appropriate.
			key = rangeKeys[rng.Intn(len(rangeKeys))]
		} else {
			// Generate a new random key in the range.
			key = randKeyBetween(rng, rangeStart, rangeEnd)
		}
		if _, ok := sstKeysMap[key]; !ok {
			sstKeysMap[key] = struct{}{}
			sstKeys = append(sstKeys, key)
		}
	}
	sort.Strings(sstKeys)

	sstSpan := roachpb.Span{
		Key:    roachpb.Key(sstKeys[0]),
		EndKey: roachpb.Key(tk(fk(sstKeys[len(sstKeys)-1]) + 1)),
	}

	// Unlike other write operations, AddSSTable sends raw MVCC values directly
	// through to storage. We therefore don't need to pass the sequence number via
	// the RequestHeader, but instead write them directly into the MVCCValueHeader
	// of the MVCC values.
	seq := g.nextSeq()
	sstValueHeader := enginepb.MVCCValueHeader{}
	sstValueHeader.KVNemesisSeq.Set(seq)
	sstValue := storage.MVCCValue{
		MVCCValueHeader: sstValueHeader,
		Value:           roachpb.MakeValueFromString(sv(seq)),
	}
	sstTombstone := storage.MVCCValue{MVCCValueHeader: sstValueHeader}

	// Write key/value pairs to the SST.
	f := &storage.MemFile{}
	st := cluster.MakeTestingClusterSettings()
	w := storage.MakeIngestionSSTWriter(ctx, st, f)
	defer w.Close()

	for _, key := range sstKeys {
		v := sstValue
		if rng.Float64() < probTombstone {
			v = sstTombstone
		}
		err := w.PutMVCC(storage.MVCCKey{Key: roachpb.Key(key), Timestamp: sstTimestamp}, v)
		if err != nil {
			panic(err)
		}
	}
	if err := w.Finish(); err != nil {
		panic(err)
	}

	return addSSTable(f.Data(), sstSpan, sstTimestamp, seq, asWrites)
}

func randScan(g *generator, rng *rand.Rand) Operation {
	key, endKey := randSpan(rng)
	return scan(key, endKey)
}

func randScanForUpdate(g *generator, rng *rand.Rand) Operation {
	op := randScan(g, rng)
	op.Scan.ForUpdate = true
	return op
}

func randReverseScan(g *generator, rng *rand.Rand) Operation {
	op := randScan(g, rng)
	op.Scan.Reverse = true
	return op
}

func randReverseScanForUpdate(g *generator, rng *rand.Rand) Operation {
	op := randReverseScan(g, rng)
	op.Scan.ForUpdate = true
	return op
}

func randDelMissing(g *generator, rng *rand.Rand) Operation {
	key := randKey(rng)
	g.keys[key] = struct{}{}
	seq := g.nextSeq()
	return del(key, seq)
}

func randDelExisting(g *generator, rng *rand.Rand) Operation {
	key := randMapKey(rng, g.keys)
	seq := g.nextSeq()
	return del(key, seq)
}

func randDelRange(g *generator, rng *rand.Rand) Operation {
	// We don't write any new keys to `g.keys` on a DeleteRange operation,
	// because DelRange(..) only deletes existing keys.
	key, endKey := randSpan(rng)
	seq := g.nextSeq()
	return delRange(key, endKey, seq)
}

func randDelRangeUsingTombstone(g *generator, rng *rand.Rand) Operation {
	return randDelRangeUsingTombstoneImpl(g.currentSplits, g.keys, g.nextSeq, rng)
}

func randDelRangeUsingTombstoneImpl(
	currentSplits, keys map[string]struct{}, nextSeq func() kvnemesisutil.Seq, rng *rand.Rand,
) Operation {
	yn := func(probY float64) bool {
		return rng.Float64() <= probY
	}

	var k, ek string
	if yn(0.90) {
		// 90% chance of picking an entire existing range.
		//
		// In kvnemesis, DeleteRangeUsingTombstone is prevented from spanning ranges since
		// CRDB executes such requests non-atomically and so we can't verify them
		// well. Thus, pick spans that are likely single-range most of the time.
		//
		// 75% (of the 90%) of the time we'll also modify the bounds.
		k, ek = randRangeSpan(rng, currentSplits)
		if yn(0.5) {
			// In 50% of cases, move startKey forward.
			k = randKeyBetween(rng, k, ek)
		}
		if yn(0.5) {
			// In 50% of cases, move endKey backward.
			nk := fk(k) + 1
			nek := fk(ek)
			if nek < math.MaxUint64 {
				nek++
			}
			ek = randKeyBetween(rng, tk(nk), tk(nek))
		}
	} else if yn(0.5) {
		// (100%-90%)*50% = 5% chance of turning the span we have now into a
		// point write. Half the time random key, otherwise prefer existing key.
		if yn(0.5) || len(keys) == 0 {
			k = randKey(rng)
		} else {
			k = randMapKey(rng, keys)
		}
		ek = tk(fk(k) + 1)
	} else {
		// 5% chance of picking a completely random span. This will often span range
		// boundaries and be rejected, so these are essentially doomed to fail.
		k, ek = randKey(rng), randKey(rng)
		if ek < k {
			// NB: if they're equal, that's just tough luck; we'll have an empty range.
			k, ek = ek, k
		}
	}

	return delRangeUsingTombstone(k, ek, nextSeq())
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

func makeRemoveReplicaFn(key string, current []roachpb.ReplicationTarget) opGenFunc {
	return func(g *generator, rng *rand.Rand) Operation {
		change := roachpb.ReplicationChange{
			ChangeType: roachpb.REMOVE_VOTER,
			Target:     current[rng.Intn(len(current))],
		}
		return changeReplicas(key, change)
	}
}

func makeAddReplicaFn(key string, current []roachpb.ReplicationTarget, atomicSwap bool) opGenFunc {
	return func(g *generator, rng *rand.Rand) Operation {
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
		changes := []roachpb.ReplicationChange{{
			ChangeType: roachpb.ADD_VOTER,
			Target:     candidate,
		}}
		if atomicSwap {
			changes = append(changes, roachpb.ReplicationChange{
				ChangeType: roachpb.REMOVE_VOTER,
				Target:     current[rng.Intn(len(current))],
			})
		}
		return changeReplicas(key, changes...)
	}
}

func makeTransferLeaseFn(key string, current []roachpb.ReplicationTarget) opGenFunc {
	return func(g *generator, rng *rand.Rand) Operation {
		target := current[rng.Intn(len(current))]
		return transferLease(key, target.StoreID)
	}
}

func toggleGlobalReads(_ *generator, _ *rand.Rand) Operation {
	return changeZone(ChangeZoneType_ToggleGlobalReads)
}

func makeRandBatch(c *ClientOperationConfig) opGenFunc {
	return func(g *generator, rng *rand.Rand) Operation {
		var allowed []opGen
		g.registerClientOps(&allowed, c)
		numOps := rng.Intn(4)
		ops := make([]Operation, numOps)
		for i := range ops {
			ops[i] = g.selectOp(rng, allowed)
		}
		return batch(ops...)
	}
}

func (g *generator) registerClosureTxnOps(allowed *[]opGen, c *ClosureTxnConfig) {
	addOpGen(allowed,
		makeClosureTxn(ClosureTxnType_Commit, &c.TxnClientOps, &c.TxnBatchOps, nil /* commitInBatch*/), c.Commit)
	addOpGen(allowed,
		makeClosureTxn(ClosureTxnType_Rollback, &c.TxnClientOps, &c.TxnBatchOps, nil /* commitInBatch*/), c.Rollback)
	addOpGen(allowed,
		makeClosureTxn(ClosureTxnType_Commit, &c.TxnClientOps, &c.TxnBatchOps, &c.CommitBatchOps), c.CommitInBatch)
}

func makeClosureTxn(
	txnType ClosureTxnType,
	txnClientOps *ClientOperationConfig,
	txnBatchOps *BatchOperationConfig,
	commitInBatch *ClientOperationConfig,
) opGenFunc {
	return func(g *generator, rng *rand.Rand) Operation {
		var allowed []opGen
		g.registerClientOps(&allowed, txnClientOps)
		g.registerBatchOps(&allowed, txnBatchOps)
		numOps := rng.Intn(4)
		ops := make([]Operation, numOps)
		for i := range ops {
			ops[i] = g.selectOp(rng, allowed)
		}
		op := closureTxn(txnType, ops...)
		if commitInBatch != nil {
			if txnType != ClosureTxnType_Commit {
				panic(errors.AssertionFailedf(`CommitInBatch must commit got: %s`, txnType))
			}
			op.ClosureTxn.CommitInBatch = makeRandBatch(commitInBatch)(g, rng).Batch
		}
		return op
	}
}

// fk stands for "from key", i.e. decode the uint64 the key represents.
// Panics on error.
func fk(k string) uint64 {
	k = k[len(GeneratorDataSpan().Key):]
	_, s, err := encoding.DecodeUnsafeStringAscendingDeepCopy([]byte(k), nil)
	if err != nil {
		panic(err)
	}
	sl, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return binary.BigEndian.Uint64(sl)
}

// tk stands for toKey, i.e. encode the uint64 into its key representation.
func tk(n uint64) string {
	var sl [8]byte
	binary.BigEndian.PutUint64(sl[:8], n)
	s := hex.EncodeToString(sl[:8])
	key := GeneratorDataSpan().Key
	key = encoding.EncodeStringAscending(key, s)
	return string(key)
}

// keysBetween returns the keys between the given [start,end) span
// in an undefined order. It takes a map for use with g.keys.
func keysBetween(keys map[string]struct{}, start, end string) []string {
	between := []string{}
	s, e := fk(start), fk(end)
	for key := range keys {
		if nk := fk(key); nk >= s && nk < e {
			between = append(between, key)
		}
	}
	return between
}

func randKey(rng *rand.Rand) string {
	// Avoid the endpoints because having point writes at the
	// endpoints complicates randRangeSpan.
	n := rng.Uint64()
	if n == 0 {
		n++
	}
	if n == math.MaxUint64 {
		n--
	}
	return tk(n)
}

// Interprets the provided map as the split points of the key space and returns
// the boundaries of a random range.
func randRangeSpan(rng *rand.Rand, curOrHistSplits map[string]struct{}) (string, string) {
	keys := make([]string, 0, len(curOrHistSplits))
	for key := range curOrHistSplits {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		// No splits.
		return tk(0), tk(math.MaxUint64)
	}
	idx := rng.Intn(len(keys) + 1)
	if idx == len(keys) {
		// Last range.
		return keys[idx-1], tk(math.MaxUint64)
	}
	if idx == 0 {
		// First range. We avoid having splits at 0 so this will be a well-formed
		// range. (If it isn't, we'll likely catch an error because we'll send an
		// ill-formed request and kvserver will error it out).
		return tk(0), keys[0]
	}
	return keys[idx-1], keys[idx]
}

func randMapKey(rng *rand.Rand, m map[string]struct{}) string {
	if len(m) == 0 {
		return randKey(rng)
	}
	k, ek := randRangeSpan(rng, m)
	// If there is only one key in the map we will get [0,x) or [x,max)
	// back and want to return `x` to avoid the endpoints, which are
	// reserved.
	if fk(k) == 0 {
		return ek
	}
	return k
}

// Returns a key that falls into `[k,ek)`.
func randKeyBetween(rng *rand.Rand, k, ek string) string {
	a, b := fk(k), fk(ek)
	if b <= a {
		b = a + 1 // we will return `a`
	}
	defer func() {
		if r := recover(); r != nil {
			panic(fmt.Sprintf("a=%d b=%d b-a=%d: %v", a, b, int64(b-a), r))
		}
	}()
	return tk(a + (rng.Uint64() % (b - a)))
}

func randSpan(rng *rand.Rand) (string, string) {
	key, endKey := randKey(rng), randKey(rng)
	if endKey < key {
		key, endKey = endKey, key
	} else if endKey == key {
		endKey = string(roachpb.Key(key).Next())
	}
	return key, endKey
}

func step(op Operation) Step {
	return Step{Op: op}
}

func batch(ops ...Operation) Operation {
	return Operation{Batch: &BatchOperation{
		Ops: ops,
	}}
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

func getForUpdate(key string) Operation {
	return Operation{Get: &GetOperation{Key: []byte(key), ForUpdate: true}}
}

func put(key string, seq kvnemesisutil.Seq) Operation {
	return Operation{Put: &PutOperation{Key: []byte(key), Seq: seq}}
}

func scan(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey)}}
}

func scanForUpdate(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), ForUpdate: true}}
}

func reverseScan(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), Reverse: true}}
}

func reverseScanForUpdate(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), Reverse: true, ForUpdate: true}}
}

func del(key string, seq kvnemesisutil.Seq) Operation {
	return Operation{Delete: &DeleteOperation{
		Key: []byte(key),
		Seq: seq,
	}}
}

func delRange(key, endKey string, seq kvnemesisutil.Seq) Operation {
	return Operation{DeleteRange: &DeleteRangeOperation{Key: []byte(key), EndKey: []byte(endKey), Seq: seq}}
}

func delRangeUsingTombstone(key, endKey string, seq kvnemesisutil.Seq) Operation {
	return Operation{DeleteRangeUsingTombstone: &DeleteRangeUsingTombstoneOperation{Key: []byte(key), EndKey: []byte(endKey), Seq: seq}}
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

func transferLease(key string, target roachpb.StoreID) Operation {
	return Operation{TransferLease: &TransferLeaseOperation{Key: []byte(key), Target: target}}
}

func changeZone(changeType ChangeZoneType) Operation {
	return Operation{ChangeZone: &ChangeZoneOperation{Type: changeType}}
}

func addSSTable(
	data []byte, span roachpb.Span, sstTimestamp hlc.Timestamp, seq kvnemesisutil.Seq, asWrites bool,
) Operation {
	return Operation{AddSSTable: &AddSSTableOperation{
		Data:         data,
		Span:         span,
		SSTTimestamp: sstTimestamp,
		Seq:          seq,
		AsWrites:     asWrites,
	}}
}
