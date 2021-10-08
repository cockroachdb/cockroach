// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gc

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculateThreshold(t *testing.T) {
	for _, c := range []struct {
		gcTTL time.Duration
		ts    hlc.Timestamp
	}{
		{
			ts:    hlc.Timestamp{WallTime: time.Hour.Nanoseconds(), Logical: 0},
			gcTTL: time.Second,
		},
	} {
		require.Equal(t, c.ts, TimestampForThreshold(CalculateThreshold(c.ts, c.gcTTL), c.gcTTL))
	}
}

type collectingGCer struct {
	keys [][]roachpb.GCRequest_GCKey
}

func (c *collectingGCer) GC(_ context.Context, keys []roachpb.GCRequest_GCKey) error {
	c.keys = append(c.keys, keys)
	return nil
}

func TestBatchingInlineGCer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	c := &collectingGCer{}
	m := makeBatchingInlineGCer(c, func(err error) { t.Error(err) })
	if m.max == 0 {
		t.Fatal("did not init max")
	}
	m.max = 10 // something reasonable for this unit test

	long := roachpb.GCRequest_GCKey{
		Key: bytes.Repeat([]byte("x"), m.max-1),
	}
	short := roachpb.GCRequest_GCKey{
		Key: roachpb.Key("q"),
	}

	m.FlushingAdd(ctx, long.Key)
	require.Nil(t, c.keys) // no flush

	m.FlushingAdd(ctx, short.Key)
	// Flushed long and short.
	require.Len(t, c.keys, 1)
	require.Len(t, c.keys[0], 2)
	require.Equal(t, long, c.keys[0][0])
	require.Equal(t, short, c.keys[0][1])
	// Reset itself properly.
	require.Nil(t, m.gcKeys)
	require.Zero(t, m.size)

	m.FlushingAdd(ctx, short.Key)
	require.Len(t, c.keys, 1) // no flush

	m.Flush(ctx)
	require.Len(t, c.keys, 2) // flushed
	require.Len(t, c.keys[1], 1)
	require.Equal(t, short, c.keys[1][0])
	// Reset itself properly.
	require.Nil(t, m.gcKeys)
	require.Zero(t, m.size)
}

// TestIntentAgeThresholdSetting verifies that the GC intent resolution threshold can be
// adjusted. It uses short and long threshold to verify that intents inserted between two
// thresholds are not considered for resolution when threshold is high (1st attempt) and
// considered when threshold is low (2nd attempt).
func TestIntentAgeThresholdSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Test event timeline.
	now := 3 * time.Hour
	intentShortThreshold := 5 * time.Minute
	intentLongThreshold := 2 * time.Hour
	intentTs := now - (intentShortThreshold+intentLongThreshold)/2

	// Prepare test intents in MVCC.
	key := []byte("a")
	value := roachpb.Value{RawBytes: []byte("0123456789")}
	intentHlc := hlc.Timestamp{
		WallTime: intentTs.Nanoseconds(),
	}
	txn := roachpb.MakeTransaction("txn", key, roachpb.NormalUserPriority, intentHlc, 1000)
	require.NoError(t, storage.MVCCPut(ctx, eng, nil, key, intentHlc, value, &txn))
	require.NoError(t, eng.Flush())

	// Prepare test fixtures for GC run.
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey(key),
		EndKey:   roachpb.RKey("b"),
	}
	gcTTL := time.Second
	snap := eng.NewSnapshot()
	nowTs := hlc.Timestamp{
		WallTime: now.Nanoseconds(),
	}
	fakeGCer := makeFakeGCer()

	// Test GC desired behavior.
	info, err := Run(ctx, &desc, snap, nowTs, nowTs, RunOptions{IntentAgeThreshold: intentLongThreshold}, gcTTL, &fakeGCer, fakeGCer.resolveIntents,
		fakeGCer.resolveIntentsAsync)
	require.NoError(t, err, "GC Run shouldn't fail")
	assert.Zero(t, info.IntentsConsidered,
		"Expected no intents considered by GC with default threshold")

	info, err = Run(ctx, &desc, snap, nowTs, nowTs, RunOptions{IntentAgeThreshold: intentShortThreshold}, gcTTL, &fakeGCer, fakeGCer.resolveIntents,
		fakeGCer.resolveIntentsAsync)
	require.NoError(t, err, "GC Run shouldn't fail")
	assert.Equal(t, 1, info.IntentsConsidered,
		"Expected 1 intents considered by GC with short threshold")
}

func TestIntentCleanupBatching(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	intentThreshold := 2 * time.Hour
	now := 3 * intentThreshold
	intentTs := now - intentThreshold*2

	// Prepare test intents in MVCC.
	txnPrefixes := []byte{'a', 'b', 'c'}
	objectKeys := []byte{'a', 'b', 'c', 'd', 'e'}
	value := roachpb.Value{RawBytes: []byte("0123456789")}
	intentHlc := hlc.Timestamp{
		WallTime: intentTs.Nanoseconds(),
	}
	for _, prefix := range txnPrefixes {
		key := []byte{prefix, objectKeys[0]}
		txn := roachpb.MakeTransaction("txn", key, roachpb.NormalUserPriority, intentHlc, 1000)
		for _, suffix := range objectKeys {
			key := []byte{prefix, suffix}
			require.NoError(t, storage.MVCCPut(ctx, eng, nil, key, intentHlc, value, &txn))
		}
		require.NoError(t, eng.Flush())
	}
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey([]byte{txnPrefixes[0], objectKeys[0]}),
		EndKey:   roachpb.RKey("z"),
	}
	gcTTL := time.Second
	snap := eng.NewSnapshot()
	nowTs := hlc.Timestamp{
		WallTime: now.Nanoseconds(),
	}

	// Base GCer will cleanup all intents in one go and its result is used as a baseline
	// to compare batched runs for checking completeness.
	baseGCer := makeFakeGCer()
	_, err := Run(ctx, &desc, snap, nowTs, nowTs, RunOptions{IntentAgeThreshold: intentAgeThreshold}, gcTTL, &baseGCer, baseGCer.resolveIntents,
		baseGCer.resolveIntentsAsync)
	if err != nil {
		t.Fatal("Can't prepare test fixture. Non batched GC run fails.")
	}
	baseGCer.normalize()

	var batchSize int64 = 7
	fakeGCer := makeFakeGCer()
	info, err := Run(ctx, &desc, snap, nowTs, nowTs,
		RunOptions{IntentAgeThreshold: intentAgeThreshold, MaxIntentsPerIntentCleanupBatch: batchSize}, gcTTL,
		&fakeGCer, fakeGCer.resolveIntents, fakeGCer.resolveIntentsAsync)
	require.NoError(t, err, "GC Run shouldn't fail")
	maxIntents := 0
	for _, batch := range fakeGCer.batches {
		if intents := len(batch); intents > maxIntents {
			maxIntents = intents
		}
	}
	require.Equal(t, int64(maxIntents), batchSize, "Batch size")
	require.Equal(t, 15, info.ResolveTotal)
	fakeGCer.normalize()
	require.EqualValues(t, baseGCer, fakeGCer, "GC result with batching")
}

type testResolver [][]roachpb.Intent

func (r *testResolver) resolveBatch(_ context.Context, batch []roachpb.Intent) error {
	batchCopy := make([]roachpb.Intent, len(batch))
	copy(batchCopy, batch)
	*r = append(*r, batchCopy)
	return nil
}

func (r *testResolver) assertInvariants(t *testing.T, opts intentBatcherOptions) {
	tc := 0
	for _, b := range *r {
		tc += len(b)
	}
	for i, batch := range *r {
		require.Greaterf(t, len(batch), 0, fmt.Sprintf("Batch %d/%d should not be empty", i+1, len(*r)))
		var totalKeyBytes = 0
		// Calculate batch size across dimensions.
		txnMap := make(map[uuid.UUID]bool)
		for _, intent := range batch {
			txnMap[intent.Txn.ID] = true
			totalKeyBytes += len(intent.Key)
		}
		// Validate that limits are not breached if set.
		if opts.maxIntentsPerIntentCleanupBatch > 0 {
			require.LessOrEqual(t, int64(len(batch)), opts.maxIntentsPerIntentCleanupBatch,
				fmt.Sprintf("Batch size exceeded in batch %d/%d", i+1, len(*r)))
		}
		// Last key could overspill over limit, but that's ok.
		if opts.maxIntentKeyBytesPerIntentCleanupBatch > 0 {
			require.Less(t, int64(totalKeyBytes-len(batch[len(batch)-1].Key)), opts.maxIntentKeyBytesPerIntentCleanupBatch,
				fmt.Sprintf("Byte limit was exceeded for more than the last key in batch %d/%d", i+1, len(*r)))
		}
		if opts.maxTxnsPerIntentCleanupBatch > 0 {
			require.LessOrEqual(t, int64(len(txnMap)), opts.maxTxnsPerIntentCleanupBatch,
				fmt.Sprintf("Max transactions per cleanup batch %d/%d", i+1, len(*r)))
		}
		// Validate that at least one of thresholds reached.
		require.True(t, i == len(*r)-1 ||
			int64(len(batch)) == opts.maxIntentsPerIntentCleanupBatch ||
			int64(totalKeyBytes) >= opts.maxIntentKeyBytesPerIntentCleanupBatch ||
			int64(len(txnMap)) == opts.maxTxnsPerIntentCleanupBatch,
			fmt.Sprintf("None of batch thresholds were reached in batch %d/%d", i+1, len(*r)))
	}
}

type testIntent struct {
	key  roachpb.Key
	meta *enginepb.MVCCMetadata
}

func generateScattered(total int, txns int, maxKeySize int, random *rand.Rand) []testIntent {
	var txnIds []uuid.UUID
	for len(txnIds) < txns {
		txnIds = append(txnIds, uuid.FastMakeV4())
	}
	var intents []testIntent
	for len(intents) < total {
		intents = append(intents,
			testIntent{randomLengthKey(random, maxKeySize),
				&enginepb.MVCCMetadata{Txn: &enginepb.TxnMeta{ID: txnIds[random.Intn(len(txnIds))]}}})
	}
	return intents
}

// Random number from 1 to n inclusive.
func intnFrom1(rnd *rand.Rand, n int) int {
	return rnd.Intn(n) + 1
}

func randomLengthKey(rnd *rand.Rand, maxLength int) roachpb.Key {
	return make([]byte, intnFrom1(rnd, maxLength))
}

func generateSequential(total int, maxTxnSize int, maxKeySize int, random *rand.Rand) []testIntent {
	var intents []testIntent
	var txnUUID uuid.UUID
	leftForTransaction := 0
	for ; len(intents) < total; leftForTransaction-- {
		if leftForTransaction == 0 {
			leftForTransaction = intnFrom1(random, maxTxnSize)
			txnUUID = uuid.FastMakeV4()
		}
		intents = append(intents,
			testIntent{randomLengthKey(random, maxKeySize),
				&enginepb.MVCCMetadata{Txn: &enginepb.TxnMeta{ID: txnUUID}}})
	}
	return intents
}

func TestGCIntentBatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	rand, _ := randutil.NewTestPseudoRand()

	for _, s := range []struct {
		name    string
		intents []testIntent
	}{
		{"sequential", generateSequential(1000, 20, 20, rand)},
		{"scattered", generateScattered(1000, 100, 20, rand)},
	} {
		for _, batchSize := range []int64{0, 1, 10, 100, 1000} {
			for _, byteCount := range []int64{0, 1, 10, 100, 1000} {
				for _, txnCount := range []int64{0, 1, 10, 100, 1000} {
					t.Run(fmt.Sprintf("batch=%d,bytes=%d,txns=%d,txn_intents=%s", batchSize, byteCount, txnCount, s.name), func(t *testing.T) {
						info := Info{}
						opts := intentBatcherOptions{
							maxIntentsPerIntentCleanupBatch:        batchSize,
							maxIntentKeyBytesPerIntentCleanupBatch: byteCount,
							maxTxnsPerIntentCleanupBatch:           txnCount,
						}
						resolver := testResolver{}

						batcher := newIntentBatcher(resolver.resolveBatch, opts, &info)

						for _, intent := range s.intents {
							require.NoError(t, batcher.addAndMaybeFlushIntents(ctx, intent.key, intent.meta))
						}
						require.NoError(t, batcher.maybeFlushPendingIntents(ctx))
						resolver.assertInvariants(t, opts)
					})
				}
			}
		}
	}
}

func TestGCIntentBatcherErrorHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	opts := intentBatcherOptions{maxIntentsPerIntentCleanupBatch: 1}

	key1 := []byte("key1")
	key2 := []byte("key2")
	txn1 := enginepb.MVCCMetadata{Txn: &enginepb.TxnMeta{ID: uuid.FastMakeV4()}}

	// Verify intent cleanup error is propagated to caller.
	info := Info{}
	batcher := newIntentBatcher(func(ctx context.Context, intents []roachpb.Intent) error {
		return errors.New("having trouble cleaning up intents")
	}, opts, &info)
	require.NoError(t, batcher.addAndMaybeFlushIntents(context.Background(), key1, &txn1))
	require.Error(t, batcher.addAndMaybeFlushIntents(context.Background(), key2, &txn1))
	require.Equal(t, 0, info.ResolveTotal)

	// Verify that flush propagates error to caller.
	info = Info{}
	batcher = newIntentBatcher(func(ctx context.Context, intents []roachpb.Intent) error {
		return errors.New("having trouble cleaning up intents")
	}, opts, &info)
	require.NoError(t, batcher.addAndMaybeFlushIntents(context.Background(), key1, &txn1))
	require.Error(t, batcher.maybeFlushPendingIntents(context.Background()))
	require.Equal(t, 0, info.ResolveTotal)

	// Verify that canceled context is propagated even if there's nothing to cleanup.
	info = Info{}
	ctx, cancel := context.WithCancel(context.Background())
	batcher = newIntentBatcher(func(ctx context.Context, intents []roachpb.Intent) error {
		return nil
	}, opts, &info)
	cancel()
	require.Error(t, batcher.maybeFlushPendingIntents(ctx))
}
