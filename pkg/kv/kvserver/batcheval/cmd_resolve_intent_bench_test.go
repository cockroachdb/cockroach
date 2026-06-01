// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testfixtures"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// BenchmarkResolveIntent measures the cost of resolving intents using point
// resolves (ResolveIntent, one per intent) versus ranged resolves
// (ResolveIntentRange, one per transaction). It tests three resolve methods:
//
//   - "point": one ResolveIntent per intent key
//   - "range": one ResolveIntentRange per txn spanning the entire key space
//   - "range-limited": one ResolveIntentRange per txn spanning only that txn's
//     min to max intent key
//
// It also varies the transaction status being resolved (PENDING push,
// COMMITTED, ABORTED) since these have different cost profiles — PENDING
// rewrites the intent in place, while COMMITTED/ABORTED remove it.
func BenchmarkResolveIntent(b *testing.B) {
	defer log.Scope(b).Close(b)

	const generatorSeed = 42 // Change to bust the artifact cache.

	for _, totalKeys := range []int{
		10_000,
		100_000,
		// 1_000_000, // Uncomment for large-range testing; fixture generation is slow.
	} {
		for _, numTxns := range []int{1, 5, 10} {
			for _, intentsPerTxn := range []int{1, 10, 100} {
				for _, dist := range []string{"forward", "random"} {
					for _, status := range []roachpb.TransactionStatus{
						roachpb.PENDING,
						roachpb.COMMITTED,
						roachpb.ABORTED,
					} {
						for _, method := range []string{"point", "range", "range-limited"} {
							cfg := benchSetupConfig{
								totalKeys:     totalKeys,
								numTxns:       numTxns,
								intentsPerTxn: intentsPerTxn,
								intentDist:    dist,
								generatorSeed: generatorSeed,
							}
							name := cfg.testName(fmt.Sprintf(
								"status=%s/method=%s", status, method))
							b.Run(name, func(b *testing.B) {
								runResolveIntentBenchmark(b, cfg, method, status)
							})
						}
					}
				}
			}
		}
	}
}

// runResolveIntentBenchmark runs a single benchmark configuration, resolving
// intents using the specified method ("point", "range", or "range-limited")
// and transaction status.
func runResolveIntentBenchmark(
	b *testing.B, cfg benchSetupConfig, method string, status roachpb.TransactionStatus,
) {
	ctx := context.Background()
	eng, txns, keysByTxn := setupResolveIntentBenchData(b, cfg)
	defer eng.Close()

	const nodeID = roachpb.NodeID(1)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := (&batcheval.MockEvalCtx{
		ClusterSettings: st,
		NodeID:          nodeID,
		AbortSpan:       abortspan.New(1),
	}).EvalContext()
	startKey := resolveIntentBenchKey(0)
	endKey := resolveIntentBenchKey(cfg.totalKeys)

	// Resolve at a timestamp after the intent write (WallTime: 2).
	resolvedTS := hlc.Timestamp{WallTime: 3}
	var clockWhilePending roachpb.ObservedTimestamp
	if status == roachpb.PENDING {
		clockWhilePending = roachpb.ObservedTimestamp{
			NodeID:    nodeID,
			Timestamp: hlc.ClockTimestamp{WallTime: 3},
		}
	}
	for t := range txns {
		txns[t].WriteTimestamp = resolvedTS
	}

	// Precompute point resolves in key order and per-txn key bounds for
	// range-limited resolves so that neither sorting nor bound computation
	// is measured by the benchmark.
	type pointResolve struct {
		txnIdx int
		key    roachpb.Key
	}
	type keyBounds struct{ min, max roachpb.Key }
	var pointResolves []pointResolve
	txnBounds := make([]keyBounds, len(txns))
	for t, keys := range keysByTxn {
		for _, key := range keys {
			pointResolves = append(pointResolves, pointResolve{txnIdx: t, key: key})
			if txnBounds[t].min == nil || key.Compare(txnBounds[t].min) < 0 {
				txnBounds[t].min = key
			}
			if txnBounds[t].max == nil || key.Compare(txnBounds[t].max) > 0 {
				txnBounds[t].max = key
			}
		}
	}
	sort.Slice(pointResolves, func(i, j int) bool {
		return pointResolves[i].key.Compare(pointResolves[j].key) < 0
	})

	runIter := func() {
		batch := eng.NewBatch()
		defer batch.Close()

		switch method {
		case "point":
			for _, pr := range pointResolves {
				ri := kvpb.ResolveIntentRequest{
					IntentTxn:         txns[pr.txnIdx].TxnMeta,
					Status:            status,
					ClockWhilePending: clockWhilePending,
				}
				ri.Key = pr.key
				resp := &kvpb.ResolveIntentResponse{}
				_, err := batcheval.ResolveIntent(ctx, batch, batcheval.CommandArgs{
					EvalCtx: evalCtx,
					Args:    &ri,
				}, resp)
				require.NoError(b, err)
				require.Greater(b, resp.NumBytes, int64(0))
			}
		case "range":
			for _, txn := range txns {
				rir := kvpb.ResolveIntentRangeRequest{
					IntentTxn:         txn.TxnMeta,
					Status:            status,
					ClockWhilePending: clockWhilePending,
				}
				rir.Key = startKey
				rir.EndKey = endKey
				resp := &kvpb.ResolveIntentRangeResponse{}
				_, err := batcheval.ResolveIntentRange(ctx, batch, batcheval.CommandArgs{
					EvalCtx: evalCtx,
					Args:    &rir,
				}, resp)
				require.NoError(b, err)
				require.Greater(b, resp.NumBytes, int64(0))
			}
		case "range-limited":
			for t, txn := range txns {
				rir := kvpb.ResolveIntentRangeRequest{
					IntentTxn:         txn.TxnMeta,
					Status:            status,
					ClockWhilePending: clockWhilePending,
				}
				rir.Key = txnBounds[t].min
				rir.EndKey = txnBounds[t].max.Next()
				resp := &kvpb.ResolveIntentRangeResponse{}
				_, err := batcheval.ResolveIntentRange(ctx, batch, batcheval.CommandArgs{
					EvalCtx: evalCtx,
					Args:    &rir,
				}, resp)
				require.NoError(b, err)
				require.Greater(b, resp.NumBytes, int64(0))
			}
		default:
			b.Fatalf("unknown resolve method: %s", method)
		}
	}

	totalIntents := cfg.totalIntents()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runIter()
	}
	b.StopTimer()
	b.ReportMetric(
		float64(b.Elapsed().Nanoseconds())/float64(int64(b.N)*int64(totalIntents)),
		"ns/intent")
}

type benchSetupConfig struct {
	totalKeys     int
	numTxns       int
	intentsPerTxn int
	generatorSeed int64
	intentDist    string
}

func (c benchSetupConfig) testName(addition string) string {
	return fmt.Sprintf("keys=%d/txns=%d/intents=%d/dist=%s/%s",
		c.totalKeys, c.numTxns, c.intentsPerTxn, c.intentDist, addition)
}

func (c benchSetupConfig) fixtureName() string {
	return fmt.Sprintf("resolve_intent_bench_%d_%d_%d_%s_%d",
		c.totalKeys, c.numTxns, c.intentsPerTxn, c.intentDist, c.generatorSeed)
}

func (c benchSetupConfig) totalIntents() int {
	return c.numTxns * c.intentsPerTxn
}

// intentKeyGenerator returns a function that produces the next intent key on
// each call.
//
//   - "forward": sequential keys starting at position 0
//   - "random": random positions using fixed seed for deterministic test fixtures
func (c benchSetupConfig) intentKeyGenerator() func() roachpb.Key {
	switch c.intentDist {
	case "forward":
		pos := -1
		return func() roachpb.Key {
			pos++
			return resolveIntentBenchKey(pos)
		}
	case "random":
		pos := -1
		rng := randutil.NewTestRandWithSeed(c.generatorSeed)
		positions := rng.Perm(c.totalKeys)[:c.totalIntents()]
		return func() roachpb.Key {
			pos++
			return resolveIntentBenchKey(positions[pos])
		}
	default:
		panic(fmt.Sprintf("unknown intent distribution: %s", c.intentDist))
	}
}

// resolveIntentBenchKey produces a key for index i in the benchmark key space.
func resolveIntentBenchKey(i int) roachpb.Key {
	return encoding.EncodeUvarintAscending([]byte("key-"), uint64(i))
}

// setupResolveIntentBenchData creates and caches an on-disk Pebble store
// populated with committed data and intents. The returned engine is
// opened read-write from a copy of the cached fixture so that each benchmark
// gets a clean starting state.
func setupResolveIntentBenchData(
	b *testing.B, cfg benchSetupConfig,
) (storage.Engine, []*roachpb.Transaction, [][]roachpb.Key) {
	ctx := context.Background()

	keysByTxn := make([][]roachpb.Key, cfg.numTxns)
	txns := make([]*roachpb.Transaction, cfg.numTxns)
	nextKey := cfg.intentKeyGenerator()
	for i := range txns {
		keysByTxn[i] = make([]roachpb.Key, 0, cfg.intentsPerTxn)
		for range cfg.intentsPerTxn {
			keysByTxn[i] = append(keysByTxn[i], nextKey())
		}
		txn := roachpb.MakeTransaction(
			fmt.Sprintf("txn-%d", i), keysByTxn[i][0], 0, 0, hlc.Timestamp{WallTime: 2},
			0, 1, 0, false, /* omitInRangefeeds */
		)
		// Use deterministic IDs so resolves match intents in cached fixtures.
		txn.ID = uuid.FromUint128(uint128.FromInts(0, uint64(i+1)))
		txns[i] = &txn
	}

	dir := testfixtures.ReuseOrGenerate(b, cfg.fixtureName(), func(dir string) {
		createBenchmarkData(b, ctx, dir, cfg, txns, keysByTxn)
	})

	// Open the fixture read-write so the benchmark can create batches on top.
	testutils.ReadAllFiles(filepath.Join(dir, "*"))
	settings := cluster.MakeTestingClusterSettings()
	env, err := fs.InitEnv(ctx, vfs.Default, dir, fs.EnvConfig{
		RW:      fs.ReadWrite,
		Version: settings.Version,
	}, nil /* statsCollector */)
	require.NoError(b, err)

	eng, err := storage.Open(ctx, env, settings)
	require.NoError(b, err)

	// Set transaction sequences to match what was written.
	for t := range txns {
		txns[t].Sequence = enginepb.TxnSeq(cfg.intentsPerTxn)
	}
	return eng, txns, keysByTxn
}

// createBenchmarkData populates a Pebble store at dir with cfg.totalKeys
// committed MVCC values and intents for the specified transactions.
func createBenchmarkData(
	b testing.TB,
	ctx context.Context,
	dir string,
	cfg benchSetupConfig,
	txns []*roachpb.Transaction,
	keysByTxn [][]roachpb.Key,
) {
	committedTS := hlc.Timestamp{WallTime: 1}
	intentTS := hlc.Timestamp{WallTime: 2}

	settings := cluster.MakeTestingClusterSettings()
	env, err := fs.InitEnv(ctx, vfs.Default, dir, fs.EnvConfig{
		RW:      fs.ReadWrite,
		Version: settings.Version,
	}, nil /* statsCollector */)
	require.NoError(b, err)

	genEng, err := storage.Open(ctx, env, settings)
	require.NoError(b, err)

	// Write all keys as committed data at committedTS.
	{
		batch := genEng.NewBatch()
		val := roachpb.MakeValueFromBytes([]byte("committed"))
		for i := 0; i < cfg.totalKeys; i++ {
			key := resolveIntentBenchKey(i)
			_, err := storage.MVCCPut(
				ctx, batch, key, committedTS, val, storage.MVCCWriteOptions{},
			)
			require.NoError(b, err)
		}
		require.NoError(b, batch.Commit(false /* sync */))
		batch.Close()
	}

	// Write intents on selected keys at intentTS.
	{
		batch := genEng.NewBatch()
		val := roachpb.MakeValueFromBytes([]byte("intent-value"))
		for i := range txns {
			txn := txns[i]
			for _, key := range keysByTxn[i] {
				txn.Sequence++
				_, err := storage.MVCCPut(
					ctx, batch, key, intentTS, val, storage.MVCCWriteOptions{Txn: txn},
				)
				require.NoError(b, err)
			}
		}
		require.NoError(b, batch.Commit(false /* sync */))
		batch.Close()
	}

	require.NoError(b, genEng.Flush())
	genEng.Close()
}
