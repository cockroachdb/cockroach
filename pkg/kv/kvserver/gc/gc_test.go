// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gc

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
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
	keys [][]kvpb.GCRequest_GCKey
}

func (c *collectingGCer) GC(
	_ context.Context,
	keys []kvpb.GCRequest_GCKey,
	_ []kvpb.GCRequest_GCRangeKey,
	_ *kvpb.GCRequest_GCClearRange,
) error {
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

	long := kvpb.GCRequest_GCKey{
		Key: bytes.Repeat([]byte("x"), m.max-1),
	}
	short := kvpb.GCRequest_GCKey{
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

// TestLockAgeThresholdSetting verifies that the GC lock resolution
// threshold can be adjusted. It uses short and long threshold to verify that
// intents or other locks inserted between two thresholds are not considered for
// resolution when threshold is high (1st attempt) and considered when threshold
// is low (2nd attempt).
func TestLockAgeThresholdSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Test event timeline.
	now := 3 * time.Hour
	lockShortThreshold := 5 * time.Minute
	lockLongThreshold := 2 * time.Hour
	lockTs := now - (lockShortThreshold+lockLongThreshold)/2

	// Prepare test locks in MVCC.
	key := []byte("a")
	makeKey := func(local bool, str lock.Strength) roachpb.Key {
		strKey := append(key, byte(str))
		if local {
			strKey = keys.MakeRangeKeyPrefix(strKey)
		}
		return strKey
	}
	value := roachpb.Value{RawBytes: []byte("0123456789")}
	intentHlc := hlc.Timestamp{
		WallTime: lockTs.Nanoseconds(),
	}
	makeTxn := func() roachpb.Transaction {
		return roachpb.MakeTransaction("txn", key, isolation.Serializable, roachpb.NormalUserPriority, intentHlc, 1000, 0, 0, false /* omitInRangefeeds */)
	}
	txn1, txn2 := makeTxn(), makeTxn()
	for _, local := range []bool{false, true} {
		// Write two intents -- one for a global key, and another for a local key.
		_, err := storage.MVCCPut(ctx, eng, makeKey(local, lock.Intent), intentHlc, value, storage.MVCCWriteOptions{Txn: &txn1})
		require.NoError(t, err)
		// Acquire some shared and exclusive locks as well.
		for _, txn := range []*roachpb.Transaction{&txn1, &txn2} {
			require.NoError(t, storage.MVCCAcquireLock(ctx, eng, &txn.TxnMeta, txn.IgnoredSeqNums, lock.Shared, makeKey(local, lock.Shared), nil, 0, 0))
		}
		require.NoError(t, storage.MVCCAcquireLock(ctx, eng, &txn1.TxnMeta, txn1.IgnoredSeqNums, lock.Exclusive, makeKey(local, lock.Exclusive), nil, 0, 0))
	}
	require.NoError(t, eng.Flush())

	// Prepare test fixtures for GC run.
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey(key),
		EndKey:   roachpb.RKey("b"),
	}
	gcTTL := time.Second
	snap := eng.NewSnapshot()
	defer snap.Close()
	nowTs := hlc.Timestamp{
		WallTime: now.Nanoseconds(),
	}
	gcer := makeFakeGCer()

	// Test GC desired behavior.
	info, err := Run(ctx, &desc, snap, nowTs, nowTs,
		RunOptions{
			LockAgeThreshold:    lockLongThreshold,
			TxnCleanupThreshold: txnCleanupThreshold,
		}, gcTTL, &gcer, gcer.resolveIntents,
		gcer.resolveIntentsAsync)
	require.NoError(t, err, "GC Run shouldn't fail")
	require.Zero(t, info.LocksConsidered,
		"Expected no locks considered by GC with default threshold")
	require.Zero(t, len(gcer.locks))

	info, err = Run(ctx, &desc, snap, nowTs, nowTs,
		RunOptions{
			LockAgeThreshold:    lockShortThreshold,
			TxnCleanupThreshold: txnCleanupThreshold,
		}, gcTTL, &gcer, gcer.resolveIntents,
		gcer.resolveIntentsAsync)
	require.NoError(t, err, "GC Run shouldn't fail")
	require.Equal(t, 8, info.LocksConsidered,
		"Expected 8 locks considered by GC with short threshold")
	require.Equal(t, 8, len(gcer.locks))
}

func TestIntentCleanupBatching(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	intentThreshold := 2 * time.Hour
	now := 3 * intentThreshold
	lockTs := now - intentThreshold*2

	// Prepare test locks using various transactions, keys, and lock strengths.
	txnPrefixes := []byte{'a', 'b', 'c'}
	objectKeys := []byte{'a', 'b', 'c', 'd', 'e'}
	value := roachpb.Value{RawBytes: []byte("0123456789")}
	intentHlc := hlc.Timestamp{
		WallTime: lockTs.Nanoseconds(),
	}
	for i, prefix := range txnPrefixes {
		key := []byte{prefix, objectKeys[0]}
		txn := roachpb.MakeTransaction("txn", key, isolation.Serializable, roachpb.NormalUserPriority, intentHlc, 1000, 0, 0, false /* omitInRangefeeds */)
		for j, suffix := range objectKeys {
			key := []byte{prefix, suffix}
			idx := i*len(objectKeys) + j
			switch idx % 3 {
			case 0:
				require.NoError(t, storage.MVCCAcquireLock(ctx, eng, &txn.TxnMeta, txn.IgnoredSeqNums, lock.Shared, key, nil, 0, 0))
			case 1:
				require.NoError(t, storage.MVCCAcquireLock(ctx, eng, &txn.TxnMeta, txn.IgnoredSeqNums, lock.Exclusive, key, nil, 0, 0))
			case 2:
				_, err := storage.MVCCPut(ctx, eng, key, intentHlc, value, storage.MVCCWriteOptions{Txn: &txn})
				require.NoError(t, err)
			default:
				t.Fatal("unexpected")
			}
		}
		require.NoError(t, eng.Flush())
	}
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey([]byte{txnPrefixes[0], objectKeys[0]}),
		EndKey:   roachpb.RKey("z"),
	}
	gcTTL := time.Second
	snap := eng.NewSnapshot()
	defer snap.Close()
	nowTs := hlc.Timestamp{
		WallTime: now.Nanoseconds(),
	}

	// Base GCer will cleanup all locks in one go and its result is used as a baseline
	// to compare batched runs for checking completeness.
	baseGCer := makeFakeGCer()
	_, err := Run(ctx, &desc, snap, nowTs, nowTs, RunOptions{
		LockAgeThreshold:    lockAgeThreshold,
		TxnCleanupThreshold: txnCleanupThreshold,
	},
		gcTTL, &baseGCer, baseGCer.resolveIntents,
		baseGCer.resolveIntentsAsync)
	if err != nil {
		t.Fatal("Can't prepare test fixture. Non batched GC run fails.")
	}
	baseGCer.normalize()

	var batchSize int64 = 7
	gcer := makeFakeGCer()
	info, err := Run(ctx, &desc, snap, nowTs, nowTs,
		RunOptions{
			LockAgeThreshold:              lockAgeThreshold,
			MaxLocksPerIntentCleanupBatch: batchSize,
			TxnCleanupThreshold:           txnCleanupThreshold,
		},
		gcTTL,
		&gcer, gcer.resolveIntents, gcer.resolveIntentsAsync)
	require.NoError(t, err, "GC Run shouldn't fail")
	maxIntents := 0
	for _, batch := range gcer.batches {
		if intents := len(batch); intents > maxIntents {
			maxIntents = intents
		}
	}
	require.Equal(t, int64(maxIntents), batchSize, "Batch size")
	require.Equal(t, 15, info.ResolveTotal)
	gcer.normalize()
	require.EqualValues(t, baseGCer, gcer, "GC result with batching")
}

type testResolver [][]roachpb.Lock

func (r *testResolver) resolveBatch(_ context.Context, batch []roachpb.Lock) error {
	batchCopy := make([]roachpb.Lock, len(batch))
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
		if opts.maxLocksPerIntentCleanupBatch > 0 {
			require.LessOrEqual(t, int64(len(batch)), opts.maxLocksPerIntentCleanupBatch,
				fmt.Sprintf("Batch size exceeded in batch %d/%d", i+1, len(*r)))
		}
		// Last key could overspill over limit, but that's ok.
		if opts.maxLockKeyBytesPerIntentCleanupBatch > 0 {
			require.Less(t, int64(totalKeyBytes-len(batch[len(batch)-1].Key)),
				opts.maxLockKeyBytesPerIntentCleanupBatch,
				fmt.Sprintf("Byte limit was exceeded for more than the last key in batch %d/%d", i+1,
					len(*r)))
		}
		if opts.maxTxnsPerIntentCleanupBatch > 0 {
			require.LessOrEqual(t, int64(len(txnMap)), opts.maxTxnsPerIntentCleanupBatch,
				fmt.Sprintf("Max transactions per cleanup batch %d/%d", i+1, len(*r)))
		}
		// Validate that at least one of thresholds reached.
		require.True(t, i == len(*r)-1 ||
			int64(len(batch)) == opts.maxLocksPerIntentCleanupBatch ||
			int64(totalKeyBytes) >= opts.maxLockKeyBytesPerIntentCleanupBatch ||
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
		txnIds = append(txnIds, uuid.MakeV4())
	}
	var intents []testIntent
	for len(intents) < total {
		intents = append(intents,
			testIntent{
				randomLengthKey(random, maxKeySize),
				&enginepb.MVCCMetadata{Txn: &enginepb.TxnMeta{ID: txnIds[random.Intn(len(txnIds))]}},
			})
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
			txnUUID = uuid.MakeV4()
		}
		intents = append(intents,
			testIntent{
				randomLengthKey(random, maxKeySize),
				&enginepb.MVCCMetadata{Txn: &enginepb.TxnMeta{ID: txnUUID}},
			})
	}
	return intents
}

func TestGCIntentBatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	rand, _ := randutil.NewTestRand()

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
					t.Run(fmt.Sprintf("batch=%d,bytes=%d,txns=%d,txn_intents=%s", batchSize, byteCount,
						txnCount, s.name), func(t *testing.T) {
						info := Info{}
						opts := intentBatcherOptions{
							maxLocksPerIntentCleanupBatch:        batchSize,
							maxLockKeyBytesPerIntentCleanupBatch: byteCount,
							maxTxnsPerIntentCleanupBatch:         txnCount,
						}
						resolver := testResolver{}

						batcher := newIntentBatcher(resolver.resolveBatch, opts, &info)

						for _, intent := range s.intents {
							require.NoError(t, batcher.addAndMaybeFlushIntents(ctx, intent.key, lock.Intent, intent.meta))
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

	opts := intentBatcherOptions{maxLocksPerIntentCleanupBatch: 1}

	key1 := []byte("key1")
	key2 := []byte("key2")
	txn1 := enginepb.MVCCMetadata{Txn: &enginepb.TxnMeta{ID: uuid.MakeV4()}}

	// Verify intent cleanup error is propagated to caller.
	info := Info{}
	batcher := newIntentBatcher(func(ctx context.Context, locks []roachpb.Lock) error {
		return errors.New("having trouble cleaning up intents")
	}, opts, &info)
	require.NoError(t, batcher.addAndMaybeFlushIntents(context.Background(), key1, lock.Intent, &txn1))
	require.Error(t, batcher.addAndMaybeFlushIntents(context.Background(), key2, lock.Intent, &txn1))
	require.Equal(t, 0, info.ResolveTotal)

	// Verify that flush propagates error to caller.
	info = Info{}
	batcher = newIntentBatcher(func(ctx context.Context, locks []roachpb.Lock) error {
		return errors.New("having trouble cleaning up intents")
	}, opts, &info)
	require.NoError(t, batcher.addAndMaybeFlushIntents(context.Background(), key1, lock.Intent, &txn1))
	require.Error(t, batcher.maybeFlushPendingIntents(context.Background()))
	require.Equal(t, 0, info.ResolveTotal)

	// Verify that canceled context is propagated even if there's nothing to cleanup.
	info = Info{}
	ctx, cancel := context.WithCancel(context.Background())
	batcher = newIntentBatcher(func(ctx context.Context, locks []roachpb.Lock) error {
		return nil
	}, opts, &info)
	cancel()
	require.Error(t, batcher.maybeFlushPendingIntents(ctx))
}

/*
Table data format:
(see data below for examples referenced in brackets after each explanation)

Top row contains keys separated by one or more spaces [a to j].

Data rows start with timestamp followed by values aligned with the first
character of a key in top row.

Timestamp could be prefixed with '>' to set GC timestamp [gc threshold is 5].
Intersection of key and timestamp defines value for the key [a@9 == 'A',
a@2 == 'c'].
If value contains any upper case characters it is expected that its key should
not be garbage collected [A, B, D, E].

Special value of . means a tombstone that should be collected [a@4 should be
garbage collected].
Special value of * means a tombstone that should not be collected [tombstone c@8
should not be garbage collected].

Values prefixed with ! are intents and follow the same expectation rules as
other values.

Empty lines and horizontal and vertical axis separators are ignored.

var data = `
   | a b c  d e f g h i j
---+----------------------
 9 | A   !E
 8 |     *
 7 |
 6 | B D
>5 |
 4 | .   F
 3 |
 2 | c
 1 |
`
*/

var singleValueData = `
   | a b c d e f g h i j
---+----------------------
 9 | 
 8 |
 7 |
 6 |     C
>5 |   B
 4 | A
 3 |
 2 |
 1 |
`

var multipleValuesNewerData = `
   | a b c d e f g h i j
---+----------------------
 9 | 
 8 | A C E
 7 |
 6 |     F
>5 |   D
 4 | B
 3 |
 2 |
 1 |
`

var multipleValuesOlderData = `
   | a b c d e f g h i j
---+----------------------
 9 | 
 8 |
 7 |
 6 |     E
>5 |   C
 4 | A
 3 |
 2 | b d F
 1 |
`

var deleteData = `
   | a b c d e f g h i j
---+----------------------
 9 | 
 8 |
 7 |
 6 |     *
>5 |   .
 4 | .
 3 |
 2 | a b C
 1 |
`

var deleteWithNewerData = `
   | a b c d e f g h i j
---+----------------------
 9 | 
 8 | A C E
 7 |
 6 |     *
>5 |   .
 4 | .
 3 |
 2 | b d F
 1 |
`

var multipleValuesData = `
   | a b c d e f g h i j
---+----------------------
 9 |
 8 | A F *
 7 |
 6 | B * J
>5 |
 4 | C G K
 3 | d h .
 2 | e i m
 1 |
`

var intents = `
   | a  b  c  d e f g h i j
---+----------------------
 9 | 
 8 |
 7 |
 6 |       !C
>5 |    !B
 4 | !A
 3 |
 2 |
 1 |
`

var intentsAfterData = `
   | a  b  c  d e f g h i j
---+----------------------
 9 | 
 8 | !A !C !E
 7 |
 6 |       F
>5 |    D
 4 | B
 3 |
 2 |
 1 |
`

var intentsAfterDelete = `
   | a  b  c  d e f g h i j
---+----------------------
 9 | 
 8 | !A !C !E
 7 |
 6 |       *
>5 |    .
 4 | .
 3 |
 2 | b  d  F
 1 |
`

var deleteRangeData = `
   | a b c d e f g h i j
---+----------------------
 9 | 
 8 |
 7 |
 6 |     *-
>5 |   .-
 4 | .-
 3 |
 2 | a b C
 1 |
`

var deleteRangeDataWithNewerValues = `
   | a b c d e f g h i j
---+----------------------
 9 |
 8 | A C E *---
 7 |
 6 |     *-G
>5 |   .-
 4 | .-      I
 3 |
 2 | b d F H i
 1 |
`

var deleteRangeMultipleValues = `
   | a b c d e f g h i j
---+----------------------
 9 | 
 8 |
 7 |
 6 |   *---
>5 |   
 4 | .-
 3 |
 2 | a B C
 1 |
`

var deleteRangeDataWithIntents = `
   | a  b  c  d e f g h i j
---+----------------------
 9 | 
 8 | !A !C !E
 7 |
 6 |       *--
>5 |    .--
 4 | .--
 3 |
 2 | b  d  F
 1 |
`

// This case verifies that if range tombstone stack caching is working correctly
// when different keys have different removal thresholds.
var differentRangeStacksPerPoint = `
   | a  bb ccc  d
---+---------------
 9 |
>8 |    B3
 7 | .----------
 6 |    b2
 5 | .----------
 4 | a2 b1
 3 | .----------
 2 | a1
 1 |
`

var deleteFragmentedRanges = `
   | a  b  c  d e f g h i j
---+----------------------
 9 | 
 8 | A  C  F
 7 |
 6 |       
>5 |    .--
 4 |    d
 3 | .--------
 2 | b  f  g
 1 |
`

var deleteMergesRanges = `
   | a  bb ccc  d
---+---------------
 9 | 
 8 | A  B  F
 7 | *----------
 6 | *----------
>5 |    .--
 4 |
 3 |    c
 2 |
 1 |
`

var avoidMergingDifferentTs = `
   | a  bb ccc  d e
---+---------------
 9 | 
 8 | *--
 7 |    *--
 6 | 
>5 | .-----
 4 |
 3 |
 2 |
 1 |
`

type testRunData struct {
	data                 string
	deleteRangeThreshold int64
	keyBytesThreshold    int64
	disableClearRange    bool
	maxPendingKeySize    int64
}

func TestGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, d := range []struct {
		name string
		data string
	}{
		{name: "single", data: singleValueData},
		{name: "multiple_newer", data: multipleValuesNewerData},
		{name: "multiple_older", data: multipleValuesOlderData},
		{name: "delete", data: deleteData},
		{name: "delete_with_newer", data: deleteWithNewerData},
		{name: "multiple_values", data: multipleValuesData},
		{name: "intents", data: intents},
		{name: "intents_after_data", data: intentsAfterData},
		{name: "intents_after_delete", data: intentsAfterDelete},
		{name: "delete_range_data", data: deleteRangeData},
		{name: "delete_range_data_newer", data: deleteRangeDataWithNewerValues},
		{name: "delete_range_multiple_points", data: deleteRangeMultipleValues},
		{name: "delete_range_with_intents", data: deleteRangeDataWithIntents},
		{name: "delete_with_different_range_stacks", data: differentRangeStacksPerPoint},
		{name: "delete_fragments_ranges", data: deleteFragmentedRanges},
		{name: "delete_merges_rages", data: deleteMergesRanges},
		{name: "avoid_merging_different_ts", data: avoidMergingDifferentTs},
	} {
		t.Run(d.name, func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "clearRange", func(t *testing.T, clearRange bool) {
				runTest(t, testRunData{
					data:                 d.data,
					deleteRangeThreshold: 2,
					disableClearRange:    !clearRange,
				}, nil)
			})
		})
	}
}

type gCR kvpb.GCRequest_GCClearRange

// Format implements the fmt.Formatter interface.
func (k gCR) Format(f fmt.State, r rune) {
	k.StartKey.Format(f, r)
	fmt.Fprintf(f, "@%d-", k.StartKeyTimestamp.WallTime/1e9)
	k.EndKey.Format(f, r)
}

type gCRS []kvpb.GCRequest_GCClearRange

func (k gCRS) toTestData() (spans []gCR) {
	if len(k) == 0 {
		return nil
	}
	spans = make([]gCR, len(k))
	for i, c := range k {
		spans[i] = gCR{
			StartKey:          c.StartKey,
			StartKeyTimestamp: c.StartKeyTimestamp,
			EndKey:            c.EndKey,
		}
	}
	return spans
}

type clearPointsKey kvpb.GCRequest_GCKey

// Format implements the fmt.Formatter interface.
func (k clearPointsKey) Format(f fmt.State, c rune) {
	storage.MVCCKey{
		Key:       k.Key,
		Timestamp: k.Timestamp,
	}.Format(f, c)
}

type gcPointsBatches [][]kvpb.GCRequest_GCKey

// toTestData converts batches to test data structs that are easier to create
// and assert. This method also ignores nil convention that allows tests to
// use nil to ignore asserts when not interesting and use empty slice when we
// want to ensure that there are no requests.
func (b gcPointsBatches) toTestData() (keys [][]clearPointsKey) {
	keys = make([][]clearPointsKey, len(b))
	for i, b := range b {
		keys[i] = make([]clearPointsKey, len(b))
		for j, k := range b {
			keys[i][j] = clearPointsKey{
				Key:       k.Key,
				Timestamp: k.Timestamp,
			}
		}
	}
	return keys
}

// For testing clear range we perform normal checks, but also explicitly verify
// generated clear range spans as normal point key deletions would do the same
// job but less efficiently.
type clearRangeTestData struct {
	name        string
	data        testRunData
	clearSpans  []gCR
	clearPoints [][]clearPointsKey
}

// TestGCUseClearRange tests garbage collection correctness from scanning engine
// data to applying MVCC operations based on GC run results.
// Test doesn't cover GC all internals extensively (batching limits, selecting
// between point and range deletions etc.), but it provides a test that executes
// collection as a whole ensuring GC components work well together.
// There are separate tests for GC iterator, batcher, intent batcher that verify
// corner cases in each component.
func TestGCUseClearRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	first := keys.SystemSQLCodec.TablePrefix(42)
	// mkKey creates a key in a table from a string value. If key ends with a +
	// then Next() key is returned.
	mkKey := func(key string) roachpb.Key {
		if l := len(key); l > 1 && key[l-1] == '+' {
			return append(first[:len(first):len(first)], key[:l-1]...).Next()
		}
		return append(first[:len(first):len(first)], key...)
	}
	last := first.PrefixEnd()

	mkKeyPair := func(start string, startTs int64, end string) gCR {
		return gCR{
			StartKey:          mkKey(start),
			StartKeyTimestamp: hlc.Timestamp{WallTime: startTs * time.Second.Nanoseconds()},
			EndKey:            mkKey(end),
		}
	}
	mkKeyPairToLast := func(start string, startTs int64) gCR {
		return gCR{
			StartKey:          mkKey(start),
			StartKeyTimestamp: hlc.Timestamp{WallTime: startTs * time.Second.Nanoseconds()},
			EndKey:            last,
		}
	}
	mkRawKeyPair := func(start, end roachpb.Key) gCR {
		return gCR{
			StartKey: start,
			EndKey:   end,
		}
	}
	_, _, _ = mkKeyPair, mkKeyPairToLast, mkRawKeyPair
	clearRangeTestDefaults := func(d testRunData) testRunData {
		if d.deleteRangeThreshold == 0 {
			d.deleteRangeThreshold = 1
		}
		if d.keyBytesThreshold == 0 {
			d.keyBytesThreshold = 1
		}
		return d
	}

	for _, d := range []clearRangeTestData{
		{
			name: "clear multiple keys",
			data: testRunData{
				data: `
   | a  bb ccc  d e
---+---------------
 6 |
>5 |
 4 |    C  .    F
 3 | A     ee
 2 |    dd
 1 |
`,
			},
			clearSpans: []gCR{
				mkKeyPair("bb", 2, "ccc+"),
			},
		},
		{
			name: "clear range limits",
			data: testRunData{
				data: `
   | a  bb ccc  d e
---+---------------
 6 |
>5 |            X
 4 |    C  .    y
 3 | A     ee
 2 |    dd      z
 1 |
`,
				deleteRangeThreshold: 3,
			},
			clearSpans: []gCR{
				mkKeyPair("bb", 2, "d"),
			},
		},
		{
			name: "clear multiple keys till end range",
			data: testRunData{
				data: `
   | a  bb ccc  d e
---+---------------
 6 |
>5 |            .
 4 |    C  .    f
 3 | A     ee
 2 |    dd
 1 |
`,
			},
			clearSpans: []gCR{
				mkKeyPair("bb", 2, "d+"),
			},
		},
		{
			name: "clear multiple keys from start range",
			data: testRunData{
				data: `
   | a  bb ccc  d e
---+---------------
 6 |
>5 |            
 4 | .  .  .    F
 3 | a     ee
 2 |    dd
 1 |
`,
			},
			clearSpans: []gCR{
				mkKeyPair("a", 4, "ccc+"),
			},
		},
		{
			name: "clear range restarts on live data",
			data: testRunData{
				data: `
   | a  bb ccc  d e
---+---------------
 7 |
 6 |            X 
>5 |       .     
 4 |    .  e    
 3 | A          . 
 2 |    dd        .
 1 |       ff   y z
`,
				deleteRangeThreshold: 2,
			},
			clearSpans: []gCR{
				mkKeyPair("d", 3, "e+"),
				mkKeyPair("bb", 4, "ccc+"),
			},
		},
		{
			name: "clear range restart on intents",
			data: testRunData{
				data: `
   | a  bb ccc  d e
---+---------------
 7 |
 6 |            !X 
>5 |       .     
 4 |    .  e    .
 3 | A            .
 2 |    dd        x
 1 |       ff   y
`,
			},
			clearSpans: []gCR{
				mkKeyPair("d", 4, "e+"),
				mkKeyPair("bb", 4, "ccc+"),
			},
		},
		{
			name: "clear range when covered by range tombstone",
			data: testRunData{
				data: `
   | a  bb ccc  d e f
---+------------------
 6 |                Z
>5 |    .----------
 4 |    c  f    . .
 3 | A          h i
 2 | b  dd ggg
 1 |      
`,
			},
			clearSpans: []gCR{
				mkKeyPair("a", 2, "e+"),
			},
		},
		{
			name: "clear range with live data above range tombstone",
			data: testRunData{
				data: `
   | a  bb ccc  d e f
---+------------------
 6 |       E         
>5 |    .-----------
 4 |    c  f    . .
 3 | A          h i
 2 | b  dd ggg
 1 |      
`,
			},
			clearSpans: []gCR{
				mkKeyPair("ccc", 4, "e+"),
				mkKeyPair("a", 2, "bb+"),
			},
		},
		{
			name: "clear full range optimization",
			data: testRunData{
				data: `
   | a  bb ccc  d e f
---+------------------
 6 |           
>5 | .--------------
 4 |    c  f    . .
 3 | a          h i
 2 | b  dd ggg
 1 |      
`,
			},
			clearSpans: []gCR{
				{StartKey: first, EndKey: last},
			},
			clearPoints: [][]clearPointsKey{},
		},
	} {
		t.Run(d.name, func(t *testing.T) {
			runTest(t, clearRangeTestDefaults(d.data), func(t *testing.T, gcer *fakeGCer) {
				require.EqualValues(t, d.clearSpans, gCRS(gcer.gcClearRanges).toTestData(),
					"clear range requests")
				if d.clearPoints != nil {
					require.EqualValues(t, d.clearPoints, gcPointsBatches(gcer.gcPointsBatches).toTestData())
				}
			})
		})
	}
}

type gcVerifier func(t *testing.T, gcer *fakeGCer)

func runTest(t *testing.T, data testRunData, verify gcVerifier) {
	ctx := context.Background()
	tablePrefix := keys.SystemSQLCodec.TablePrefix(42)
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey(tablePrefix),
		EndKey:   roachpb.RKey(tablePrefix.PrefixEnd()),
	}

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	dataItems, gcTS, now := readTableData(t, desc.StartKey.AsRawKey(), data.data)
	ds := dataItems.fullDistribution()
	stats := ds.setupTest(t, eng, desc)
	snap := eng.NewSnapshot()
	defer snap.Close()

	if data.disableClearRange {
		data.deleteRangeThreshold = 0
	}
	if data.maxPendingKeySize == 0 {
		data.maxPendingKeySize = math.MaxInt64
	}

	gcer := makeFakeGCer()
	_, err := Run(ctx, &desc, snap, now, gcTS,
		RunOptions{
			LockAgeThreshold:        time.Nanosecond * time.Duration(now.WallTime),
			TxnCleanupThreshold:     txnCleanupThreshold,
			MaxKeyVersionChunkBytes: data.keyBytesThreshold,
			ClearRangeMinKeys:       data.deleteRangeThreshold,
			MaxPendingKeysSize:      data.maxPendingKeySize,
		}, time.Second,
		&gcer,
		gcer.resolveIntents, gcer.resolveIntentsAsync)

	if verify != nil {
		verify(t, &gcer)
	}

	require.NoError(t, err)
	require.Empty(t, gcer.locks, "expecting no intents")
	require.NoError(t,
		storage.MVCCGarbageCollect(ctx, eng, &stats, gcer.pointKeys(), gcTS))

	for _, r := range gcer.clearRanges() {
		if r.StartKeyTimestamp.IsEmpty() {
			require.NoError(t,
				storage.MVCCGarbageCollectWholeRange(ctx, eng, &stats, r.StartKey, r.EndKey, gcTS, stats))
		}
		require.NoError(t,
			storage.MVCCGarbageCollectPointsWithClearRange(ctx, eng, &stats, r.StartKey, r.EndKey,
				r.StartKeyTimestamp, gcTS))
	}

	for _, batch := range gcer.rangeKeyBatches() {
		rangeKeys := makeCollectableGCRangesFromGCRequests(desc.StartKey.AsRawKey(),
			desc.EndKey.AsRawKey(), batch)
		require.NoError(t,
			storage.MVCCGarbageCollectRangeKeys(ctx, eng, &stats, rangeKeys))
	}

	ctrlEng := storage.NewDefaultInMemForTesting()
	defer ctrlEng.Close()
	expectedStats := dataItems.liveDistribution().setupTest(t, ctrlEng, desc)

	if log.V(1) {
		log.Info(ctx, "Expected data:")
		for _, l := range formatTable(engineData(t, ctrlEng, desc), tablePrefix) {
			log.Infof(ctx, "%s", l)
		}

		log.Info(ctx, "Actual data:")
		for _, l := range formatTable(engineData(t, eng, desc), tablePrefix) {
			log.Infof(ctx, "%s", l)
		}
	}

	requireEqualReaders(t, ctrlEng, eng, desc)

	// Age stats to the same TS before comparing.
	expectedStats.AgeTo(now.WallTime)
	stats.AgeTo(now.WallTime)
	require.Equal(t, expectedStats, stats, "mvcc stats don't match the data")
}

// requireEqualReaders compares data in two readers
func requireEqualReaders(
	t *testing.T, exected storage.Reader, actual storage.Reader, desc roachpb.RangeDescriptor,
) {
	// First compare only points. We assert points and ranges separately for
	// simplicity.
	itExp, err := exected.NewMVCCIterator(context.Background(), storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound:           desc.StartKey.AsRawKey(),
		UpperBound:           desc.EndKey.AsRawKey(),
		KeyTypes:             storage.IterKeyTypePointsOnly,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	require.NoError(t, err)
	defer itExp.Close()
	itExp.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})

	itActual, err := actual.NewMVCCIterator(context.Background(), storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound:           desc.StartKey.AsRawKey(),
		UpperBound:           desc.EndKey.AsRawKey(),
		KeyTypes:             storage.IterKeyTypePointsOnly,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	require.NoError(t, err)
	defer itActual.Close()
	itActual.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})

	for {
		okExp, err := itExp.Valid()
		require.NoError(t, err, "failed to iterate values")
		okAct, err := itActual.Valid()
		require.NoError(t, err, "failed to iterate values")
		if !okExp && !okAct {
			break
		}

		if okExp && !okAct {
			t.Errorf("expected data not found in actual: %s", itExp.UnsafeKey().String())
		}
		if !okExp && okAct {
			t.Errorf("unexpected data found in actual: %s", itActual.UnsafeKey().String())
		}
		require.True(t, itExp.UnsafeKey().Equal(itActual.UnsafeKey()),
			"expected key not equal to actual (expected %s, found %s)", itExp.UnsafeKey(),
			itActual.UnsafeKey())
		checkValErr := func(v []byte, err error) []byte {
			require.NoError(t, err)
			return v
		}
		require.True(t, bytes.Equal(checkValErr(itExp.UnsafeValue()),
			checkValErr(itActual.UnsafeValue())),
			"expected value not equal to actual for key %s", itExp.UnsafeKey())
		itExp.Next()
		itActual.Next()
	}

	// Compare only ranges.
	itExpRanges, err := exected.NewMVCCIterator(context.Background(), storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound:           desc.StartKey.AsRawKey(),
		UpperBound:           desc.EndKey.AsRawKey(),
		KeyTypes:             storage.IterKeyTypeRangesOnly,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	require.NoError(t, err)
	defer itExpRanges.Close()
	itExpRanges.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})

	itActualRanges, err := actual.NewMVCCIterator(context.Background(), storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound:           desc.StartKey.AsRawKey(),
		UpperBound:           desc.EndKey.AsRawKey(),
		KeyTypes:             storage.IterKeyTypeRangesOnly,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	require.NoError(t, err)
	defer itActualRanges.Close()
	itActualRanges.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})

	for {
		okExp, err := itExpRanges.Valid()
		require.NoError(t, err, "failed to iterate ranges")
		okAct, err := itActualRanges.Valid()
		require.NoError(t, err, "failed to iterate ranges")
		if !okExp && !okAct {
			break
		}

		require.Equal(t, okExp, okAct, "range iterators have different number of elements")
		require.EqualValues(t, itExpRanges.RangeKeys(), itActualRanges.RangeKeys(), "range keys")
		itExpRanges.Next()
		itActualRanges.Next()
	}
}

// dataItem is element read from test table containing mvcc key value along with
// metadata needed for filtering.
type dataItem struct {
	value         storage.MVCCKeyValue
	txn           *roachpb.Transaction
	rangeKeyValue storage.MVCCRangeKeyValue
	live          bool
}

func (d dataItem) timestamp() hlc.Timestamp {
	if !d.value.Key.Timestamp.IsEmpty() {
		return d.value.Key.Timestamp
	}
	return d.rangeKeyValue.RangeKey.Timestamp
}

type tableData []dataItem

// readTableData reads all table data and returns data slice of items to
// initialize engine, gc timestamp, and now timestamp which is above any
// timestamp in items. items are sorted in ascending order.
func readTableData(
	t *testing.T, prefix roachpb.Key, data string,
) (tableData, hlc.Timestamp, hlc.Timestamp) {
	lines := strings.Split(data, "\n")
	var items []dataItem

	var columnPositions []int
	var columnKeys []roachpb.Key
	var minLen int

	parseHeader := func(l string) {
		// Find keys and their positions from the first line.
		fs := strings.Fields(l)
		lastPos := 0
		for _, key := range fs {
			pos := lastPos + strings.Index(l[lastPos:], key)
			lastPos = pos + len(key)
			if key == "|" && len(columnKeys) == 0 {
				continue
			}
			columnPositions = append(columnPositions, pos)
			var mvccKey roachpb.Key
			mvccKey = append(mvccKey, prefix...)
			mvccKey = append(mvccKey, key...)
			columnKeys = append(columnKeys, mvccKey)
		}
		minLen = columnPositions[len(columnPositions)-1] + 1
		columnPositions = append(columnPositions, 0)
	}

	parsePoint := func(val string, i int, ts hlc.Timestamp) int {
		val = strings.TrimSpace(val)
		if len(val) > 0 {
			var txn *roachpb.Transaction
			if val[0] == '!' {
				val = val[1:]
				txn = &roachpb.Transaction{
					Status:                 roachpb.PENDING,
					ReadTimestamp:          ts,
					GlobalUncertaintyLimit: ts.Next().Next(),
				}
				txn.ID = uuid.MakeV4()
				txn.WriteTimestamp = ts
				txn.Key = txn.ID.GetBytes()
			}
			var v roachpb.Value
			// Special meaning for deletions.
			if val != "*" && val != "." {
				v.SetString(val)
			}
			kv := storage.MVCCKeyValue{
				Key: storage.MVCCKey{
					Key:       columnKeys[i],
					Timestamp: ts,
				},
				Value: v.RawBytes,
			}
			live := strings.ToLower(val) != val || val == "*"
			items = append(items, dataItem{value: kv, txn: txn, live: live})
		}
		return i + 1
	}

	parseRange := func(val, l string, i int, ts hlc.Timestamp) int {
		startKey := columnKeys[i]
		// Handle range key. We are modifying outer loop index here.
		p := columnPositions[i+1] - 1
		// Find where range definition ends.
		for ; p < columnPositions[len(columnPositions)-1] && l[p] == '-'; p++ {
		}
		// Find key following the end of range.
		for i++; i < len(columnKeys) && columnPositions[i] < p; i++ {
		}
		// Extract value from the first element.
		value := val[0]
		if value != '*' && value != '.' {
			panic("test data only supports range deletions")
		}
		live := value == '*'
		// Add range to data.
		endKey := columnKeys[i]
		items = append(items, dataItem{
			rangeKeyValue: storage.MVCCRangeKeyValue{
				RangeKey: storage.MVCCRangeKey{
					StartKey:  startKey,
					EndKey:    endKey,
					Timestamp: ts,
				},
			}, live: live,
		})
		return i
	}

	var gcTS hlc.Timestamp
	var lastTs int64 = math.MaxInt64
	parseTS := func(l string) (ts hlc.Timestamp) {
		fs := strings.Fields(l)
		tss := fs[0]
		// Timestamp starting with '>' is a gc marker.
		if tss[0] == '>' {
			tss = tss[1:]
			defer func() {
				gcTS = ts
			}()
		}
		tsInt, err := strconv.ParseInt(tss, 10, 64)
		require.NoError(t, err, "Failed to parse timestamp from %s", l)
		require.Less(t, tsInt, lastTs, "Timestamps should be decreasing")
		lastTs = tsInt
		return hlc.Timestamp{WallTime: tsInt * time.Second.Nanoseconds()}
	}

	for _, l := range lines {
		if len(l) == 0 || l[0] == '-' {
			// Ignore empty lines and table separators.
			continue
		}
		if len(columnPositions) == 0 {
			parseHeader(l)
			continue
		}

		// We extend a line to always have at least characters to read values by
		// index without caring to get beyond slice.
		if shortBy := minLen - len(l); shortBy > 0 {
			l = l + strings.Repeat(" ", shortBy)
		}
		// Add length to the end of keys to eliminate extra boundary checks.
		columnPositions[len(columnPositions)-1] = len(l)

		ts := parseTS(l)
		for i := 0; i < len(columnKeys); {
			val := l[columnPositions[i]:columnPositions[i+1]]
			if val[len(val)-1] == '-' {
				i = parseRange(val, l, i, ts)
			} else {
				i = parsePoint(val, i, ts)
			}
		}
	}

	// Reverse from oldest to newest.
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}

	return items, gcTS, items[len(items)-1].timestamp().Add(1, 0)
}

// fullDistribution creates a data distribution that contains all data read from
// table.
func (d tableData) fullDistribution() dataDistribution {
	items := d
	return func() (storage.MVCCKeyValue, storage.MVCCRangeKeyValue, *roachpb.Transaction, bool) {
		if len(items) == 0 {
			return storage.MVCCKeyValue{}, storage.MVCCRangeKeyValue{}, nil, false
		}
		defer func() { items = items[1:] }()
		return items[0].value, items[0].rangeKeyValue, items[0].txn, true
	}
}

// liveDistribution creates a data distribution from the table data is was only
// marked as live (see table data format above).
func (d tableData) liveDistribution() dataDistribution {
	items := d
	return func() (storage.MVCCKeyValue, storage.MVCCRangeKeyValue, *roachpb.Transaction, bool) {
		for {
			if len(items) == 0 {
				return storage.MVCCKeyValue{}, storage.MVCCRangeKeyValue{}, nil, false
			}
			if items[0].live {
				break
			}
			items = items[1:]
		}
		defer func() { items = items[1:] }()
		return items[0].value, items[0].rangeKeyValue, items[0].txn, true
	}
}

// engineData reads all engine data as tableCells in no particular order.
func engineData(t *testing.T, r storage.Reader, desc roachpb.RangeDescriptor) []tableCell {
	var result []tableCell

	rangeIt, err := r.NewMVCCIterator(context.Background(), storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound:           desc.StartKey.AsRawKey(),
		UpperBound:           desc.EndKey.AsRawKey(),
		KeyTypes:             storage.IterKeyTypeRangesOnly,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	require.NoError(t, err)
	defer rangeIt.Close()
	rangeIt.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})
	makeRangeCells := func(rks []storage.MVCCRangeKey) (tc []tableCell) {
		for _, rk := range rks {
			tc = append(tc, tableCell{
				key: storage.MVCCKey{
					Key:       rk.StartKey,
					Timestamp: rk.Timestamp,
				},
				endKey: rk.EndKey,
				value:  ".",
			})
		}
		return tc
	}
	var partialRangeKeys []storage.MVCCRangeKey
	var lastEnd roachpb.Key
	for {
		ok, err := rangeIt.Valid()
		require.NoError(t, err, "failed to iterate range keys")
		if !ok {
			break
		}
		_, r := rangeIt.HasPointAndRange()
		if r {
			span := rangeIt.RangeBounds()
			newKeys := rangeIt.RangeKeys().AsRangeKeys()
			if lastEnd.Equal(span.Key) {
				// Try merging keys by timestamp.
				var newPartial []storage.MVCCRangeKey
				i, j := 0, 0
				for i < len(newKeys) && j < len(partialRangeKeys) {
					switch newKeys[i].Timestamp.Compare(partialRangeKeys[j].Timestamp) {
					case 1:
						newPartial = append(newPartial, newKeys[i].Clone())
						i++
					case 0:
						newPartial = append(newPartial, storage.MVCCRangeKey{
							StartKey:               partialRangeKeys[j].StartKey,
							EndKey:                 newKeys[i].EndKey.Clone(),
							Timestamp:              partialRangeKeys[j].Timestamp,
							EncodedTimestampSuffix: partialRangeKeys[j].EncodedTimestampSuffix,
						})
						i++
						j++
					case -1:
						newPartial = append(newPartial, partialRangeKeys[j].Clone())
						j++
					}
				}
				for ; i < len(newKeys); i++ {
					newPartial = append(newPartial, newKeys[i].Clone())
				}
				for ; j < len(partialRangeKeys); j++ {
					newPartial = append(newPartial, partialRangeKeys[j].Clone())
				}
				partialRangeKeys = newPartial
			} else {
				result = append(result, makeRangeCells(partialRangeKeys)...)
				partialRangeKeys = make([]storage.MVCCRangeKey, len(newKeys))
				for i, rk := range newKeys {
					partialRangeKeys[i] = rk.Clone()
				}
			}
			lastEnd = span.EndKey.Clone()
		}
		rangeIt.NextKey()
	}
	result = append(result, makeRangeCells(partialRangeKeys)...)

	it, err := r.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		LowerBound:           desc.StartKey.AsRawKey(),
		UpperBound:           desc.EndKey.AsRawKey(),
		KeyTypes:             storage.IterKeyTypePointsOnly,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	require.NoError(t, err)
	defer it.Close()
	it.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})
	prefix := ""
	for {
		okAct, err := it.Valid()
		require.NoError(t, err, "failed to iterate values")
		if !okAct {
			break
		}
		if !it.UnsafeKey().IsValue() {
			prefix = "!"
		} else {
			v := "."
			value, err := it.UnsafeValue()
			require.NoError(t, err)
			if len(value) > 0 {
				val := roachpb.Value{
					RawBytes: value,
				}
				b, err := val.GetBytes()
				require.NoError(t, err, "failed to read byte value for cell")
				v = prefix + string(b)
			}
			result = append(result, tableCell{
				key:   it.UnsafeKey().Clone(),
				value: v,
			})
			prefix = ""
		}
		it.Next()
	}
	return result
}

type tableCell struct {
	key storage.MVCCKey
	// Optional endKey for range keys.
	endKey roachpb.Key
	// Formatted key value (could be value, deletion or intent).
	value string
}

type columnInfo struct {
	key            string
	maxValueLength int
	position       int
	formatStr      string
}

type cellValue struct {
	value  string
	endKey string
}

// formatTable renders table with data. expecting data to be sorted naturally:
// keys ascending, timestamps descending.
// prefix if provided defines start of the key, that would be stripped from the
// keys to avoid clutter.
func formatTable(data []tableCell, prefix roachpb.Key) []string {
	// Table with no data is a special case.
	if len(data) == 0 {
		return nil
	}

	keyPrefixStr := ""
	if prefix != nil {
		keyPrefixStr = prefix.String()
	}
	keyRe := regexp.MustCompile(`^/"(.*)"$`)
	columnName := func(key roachpb.Key) string {
		keyStr := key.String()
		if strings.Index(keyStr, keyPrefixStr) == 0 {
			keyStr = keyStr[len(keyPrefixStr):]
			if keyRe.FindSubmatch([]byte(keyStr)) != nil {
				keyStr = keyStr[2 : len(keyStr)-1]
			}
		}
		return keyStr
	}

	// Table data indexed by ts, key.
	keyData := make(map[string]columnInfo)

	addKeyColumn := func(key roachpb.Key, columnLen int) {
		keyStr := key.String()
		if column, ok := keyData[keyStr]; !ok {
			title := columnName(key)
			if titleLen := len(title); columnLen < len(title) {
				columnLen = titleLen
			}
			keyData[keyStr] = columnInfo{
				key:            title,
				maxValueLength: columnLen,
			}
		} else {
			if columnLen > column.maxValueLength {
				column.maxValueLength = columnLen
				keyData[keyStr] = column
			}
		}
	}

	sparseTable := make(map[int64]map[string]cellValue)
	for _, c := range data {
		ts := c.key.Timestamp.WallTime
		key := c.key.Key.String()
		addKeyColumn(c.key.Key, len(c.value))
		if _, ok := sparseTable[ts]; !ok {
			sparseTable[ts] = make(map[string]cellValue)
		}
		endKey := ""
		if len(c.endKey) > 0 {
			endKey = c.endKey.String()
		}
		sparseTable[ts][key] = cellValue{c.value, endKey}
		if len(c.endKey) > 0 {
			addKeyColumn(c.endKey, 0)
		}
	}

	// Build table key positions for ease of iterations.
	var keySeq []string
	for k := range keyData {
		keySeq = append(keySeq, k)
	}
	sort.Strings(keySeq)
	base := 0
	for _, k := range keySeq {
		kd := keyData[k]
		kd.position = base
		kd.formatStr = fmt.Sprintf("%%-%ds", kd.maxValueLength)
		base += kd.maxValueLength + 1
		keyData[k] = kd
	}
	// Build key sequence.
	var tsSeq []int64
	for ts := range sparseTable {
		tsSeq = append(tsSeq, ts)
	}
	sort.Slice(tsSeq, func(i, j int) bool {
		return tsSeq[i] > tsSeq[j]
	})

	var result []string

	lsLen := len(fmt.Sprintf("%d", tsSeq[0]/time.Second.Nanoseconds()))
	rowPrefixFmt := fmt.Sprintf(" %%%dd | ", lsLen)
	firstRow := fmt.Sprintf(" %s | ", strings.Repeat(" ", lsLen))
	for _, key := range keySeq {
		colInfo := keyData[key]
		firstRow += fmt.Sprintf(colInfo.formatStr, colInfo.key)
		firstRow += " "
	}
	result = append(result, firstRow)
	result = append(result, strings.Repeat("-", len(firstRow)))

	for _, ts := range tsSeq {
		row := sparseTable[ts]
		rowStr := fmt.Sprintf(rowPrefixFmt, ts/time.Second.Nanoseconds())
		curLen := 0
		for _, key := range keySeq {
			if v, ok := row[key]; ok {
				colInfo := keyData[key]
				pos := colInfo.position
				gap := pos - curLen
				valStr := strings.Repeat(" ", gap)
				if len(v.endKey) > 0 {
					rangeEnd := keyData[v.endKey].position
					rangeLen := rangeEnd - pos - len(v.value)
					valStr += v.value + strings.Repeat("-", rangeLen)
				} else {
					valStr += fmt.Sprintf(colInfo.formatStr, v.value)
				}
				rowStr += valStr
				curLen += len(valStr)
			}
		}
		result = append(result, rowStr)
	}
	return result
}

func TestRangeKeyBatching(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	mkKey := func(key string) roachpb.Key {
		var k roachpb.Key
		k = append(k, keys.SystemSQLCodec.TablePrefix(42)...)
		k = append(k, key...)
		return k
	}

	mkKvs := func(start, end string, tss ...int) storage.MVCCRangeKeyStack {
		rangeKeys := storage.MVCCRangeKeyStack{
			Bounds: roachpb.Span{Key: mkKey(start), EndKey: mkKey(end)},
		}
		for _, ts := range tss {
			rangeKeys.Versions = append(rangeKeys.Versions, storage.MVCCRangeKeyVersion{
				Timestamp: hlc.Timestamp{WallTime: int64(ts) * time.Second.Nanoseconds()},
			})
		}
		return rangeKeys
	}

	mkGCr := func(start, end string, ts int) kvpb.GCRequest_GCRangeKey {
		return kvpb.GCRequest_GCRangeKey{
			StartKey: mkKey(start),
			EndKey:   mkKey(end),
			Timestamp: hlc.Timestamp{
				WallTime: int64(ts) * time.Second.Nanoseconds(),
			},
		}
	}

	for _, data := range []struct {
		name      string
		data      []storage.MVCCRangeKeyStack
		batchSize int64
		expect    []kvpb.GCRequest_GCRangeKey
	}{
		{
			name: "single batch",
			data: []storage.MVCCRangeKeyStack{
				mkKvs("a", "b", 5, 3, 1),
				mkKvs("c", "d", 5, 2, 1),
			},
			batchSize: 99999,
			expect: []kvpb.GCRequest_GCRangeKey{
				mkGCr("a", "b", 5),
				mkGCr("c", "d", 5),
			},
		},
		{
			name: "merge adjacent",
			data: []storage.MVCCRangeKeyStack{
				mkKvs("a", "b", 5, 3, 1),
				mkKvs("b", "c", 5, 2),
				mkKvs("c", "d", 3, 2),
			},
			batchSize: 99999,
			expect: []kvpb.GCRequest_GCRangeKey{
				mkGCr("a", "c", 5),
				mkGCr("c", "d", 3),
			},
		},
		{
			name: "batch split stack",
			data: []storage.MVCCRangeKeyStack{
				mkKvs("a", "b", 5, 3, 1),
				mkKvs("b", "c", 5, 2),
				mkKvs("c", "d", 3, 2),
			},
			batchSize: 40, // We could only fit 2 keys in a batch.
			expect: []kvpb.GCRequest_GCRangeKey{
				mkGCr("a", "b", 3),
				mkGCr("a", "b", 5),
				mkGCr("b", "c", 2),
				mkGCr("b", "c", 5),
				mkGCr("c", "d", 2),
				mkGCr("c", "d", 3),
			},
		},
		{
			name: "batch split keys",
			data: []storage.MVCCRangeKeyStack{
				mkKvs("a", "b", 5, 3, 1),
				mkKvs("b", "c", 5, 2, 1),
				mkKvs("c", "d", 3, 2),
			},
			batchSize: 50, // We could only fit 3 keys in a batch.
			expect: []kvpb.GCRequest_GCRangeKey{
				mkGCr("a", "b", 5),
				mkGCr("b", "c", 5),
				mkGCr("c", "d", 3),
			},
		},
		{
			name: "batch split and merge",
			data: []storage.MVCCRangeKeyStack{
				mkKvs("a", "b", 5, 3),
				mkKvs("b", "c", 5, 2),
				mkKvs("c", "d", 5, 1),
			},
			batchSize: 85, // We could only fit 5 keys in a batch.
			expect: []kvpb.GCRequest_GCRangeKey{
				mkGCr("a", "c", 5),
				mkGCr("c", "d", 1),
				mkGCr("c", "d", 5),
			},
		},
	} {
		t.Run(data.name, func(t *testing.T) {
			gcer := makeFakeGCer()
			b := rangeKeyBatcher{
				gcer:      &gcer,
				batchSize: data.batchSize,
			}
			for _, d := range data.data {
				require.NoError(t, b.addAndMaybeFlushRangeKeys(ctx, d), "failed to gc ranges")
			}
			require.NoError(t, b.flushPendingFragments(ctx), "failed to gc ranges")
			require.EqualValues(t, data.expect, gcer.rangeKeys())
		})
	}
}

// Union of GC args.
type gcReq struct {
	// Note that we just reuse the type, but semantic is different because EndKey
	// is not used, we only use that to have consistent request formatting in
	// test results.
	gcKeys          []gCR
	gcClearRangeKey gCR
}

func (o gcReq) Format(f fmt.State, r rune) {
	if len(o.gcKeys) > 0 {
		fmt.Fprintf(f, "%s", o.gcKeys)
	} else {
		o.gcClearRangeKey.Format(f, r)
	}
}

type capturingGCer struct {
	t   *testing.T
	ops []gcReq
}

func (c *capturingGCer) GC(
	_ context.Context,
	k []kvpb.GCRequest_GCKey,
	_ []kvpb.GCRequest_GCRangeKey,
	cr *kvpb.GCRequest_GCClearRange,
) error {
	if len(k) > 0 {
		kk := make([]gCR, len(k))
		for i, k := range k {
			kk[i] = gCR{StartKey: k.Key.Clone(), StartKeyTimestamp: k.Timestamp}
		}
		c.ops = append(c.ops, gcReq{gcKeys: kk})
		return nil
	}
	if cr != nil {
		c.ops = append(c.ops, gcReq{gcClearRangeKey: gCR{
			StartKey:          cr.StartKey.Clone(),
			StartKeyTimestamp: cr.StartKeyTimestamp,
			EndKey:            cr.EndKey.Clone(),
		}})
		return nil
	}
	c.t.Fatal("unexpected or empty GC request")
	return nil
}

type gcData struct {
	kv      mvccKeyValue
	garbage bool
}

func TestGcKeyBatcher(t *testing.T) {
	ctx := context.Background()

	// Size is a GC size including 1 extra byte and MVCCVersionTimestampSize
	keyOfSize := func(seq int, size int) roachpb.Key {
		t.Helper()
		var k roachpb.Key
		k = append(k, keys.SystemSQLCodec.IndexPrefix(42, 1)...)
		kt := encoding.EncodeStringAscending(k, fmt.Sprintf("%06d", seq))
		baseKeySize := storage.MVCCKey{Key: kt, Timestamp: hlc.Timestamp{WallTime: 1}}.EncodedSize()
		if padding := size - baseKeySize; padding < 0 {
			t.Fatalf("invalid test data: test key size is too small. must be >= %d", baseKeySize)
		} else if padding > 0 {
			k = encoding.EncodeStringAscending(k, fmt.Sprintf("%06d-%s", seq, strings.Repeat("a", padding-1)))
		} else {
			k = kt
		}
		require.Equal(t, size,
			storage.MVCCKey{Key: k, Timestamp: hlc.Timestamp{WallTime: 1}}.EncodedSize(),
			"test infra bug: generated key size mismatched requested")
		return k
	}

	hist := func(key roachpb.Key, versions, garbage int) []gcData {
		require.LessOrEqual(t, garbage, versions, "invalid test data: can not have more garbage than versions")
		var res []gcData
		for i := 0; i < versions; i++ {
			res = append(res, gcData{
				kv: mvccKeyValue{
					key: storage.MVCCKey{
						Key: key,
						Timestamp: hlc.Timestamp{
							WallTime: int64(i+1) * time.Second.Nanoseconds(),
						},
					},
					mvccValueLen: 10,
				},
				garbage: i < garbage,
			})
		}
		return res
	}

	point := func(key roachpb.Key, ts int) gCR {
		return gCR{
			StartKey:          key,
			StartKeyTimestamp: hlc.Timestamp{WallTime: int64(ts) * time.Second.Nanoseconds()},
		}
	}

	points := func(keys ...gCR) gcReq {
		return gcReq{gcKeys: keys}
	}

	clearRange := func(key roachpb.Key, ts int, endKey roachpb.Key) gcReq {
		return gcReq{
			gcClearRangeKey: gCR{
				StartKey:          key,
				StartKeyTimestamp: hlc.Timestamp{WallTime: int64(ts) * time.Second.Nanoseconds()},
				EndKey:            endKey,
			},
		}
	}

	const minKeySize = 24
	const largeKeySize = 100

	for _, d := range []struct {
		name     string
		data     []gcData
		reqs     []gcReq
		batchMax int64
		memMax   int
		rangeMin int
	}{
		{
			// Test verifies that min key limit is not reached with 4 versions of the
			// first key, but reached on the second and third. We have two consecutive
			// keys specifically to avoid single key range optimizations.
			name: "minimum range limit",
			data: keySeq(
				hist(keyOfSize(4, minKeySize), 6 /* versions */, 5 /* garbage */),
				hist(keyOfSize(3, minKeySize), 1 /* versions */, 1 /* garbage */),
				hist(keyOfSize(2, minKeySize), 10 /* versions */, 9 /* garbage */),
				hist(keyOfSize(1, minKeySize), 2 /* versions */, 1 /* garbage */),
			),
			reqs: []gcReq{
				points(point(keyOfSize(4, minKeySize), 5)),
				clearRange(keyOfSize(2, minKeySize), 9, keyOfSize(4, minKeySize)),
				points(point(keyOfSize(1, minKeySize), 1)),
			},
			rangeMin: 6,
			batchMax: minKeySize * 5,
		},
		{
			// Test verifies that a batch could be split correctly by the start of
			// range key. We have two consecutive keys specifically to avoid single
			// key range optimizations.
			name: "clear range mid batch",
			data: keySeq(
				hist(keyOfSize(4, minKeySize), 5 /* versions */, 2 /* garbage */),
				hist(keyOfSize(3, minKeySize), 1 /* versions */, 1 /* garbage */),
				hist(keyOfSize(2, minKeySize), 10 /* versions */, 9 /* garbage */),
				hist(keyOfSize(1, minKeySize), 2 /* versions */, 1 /* garbage */),
			),
			reqs: []gcReq{
				points(point(keyOfSize(4, minKeySize), 2)),
				clearRange(keyOfSize(2, minKeySize), 9, keyOfSize(4, minKeySize)),
				points(point(keyOfSize(1, minKeySize), 1)),
			},
			rangeMin: 6,
			batchMax: minKeySize * 4,
		},
		{
			// Test verifies that a batch could be split correctly by the start of
			// range key. Batch spans multiple keys with non gc data.
			name: "clear range mid batch 2",
			data: keySeq(
				hist(keyOfSize(5, minKeySize), 2 /* versions */, 1 /* garbage */),
				hist(keyOfSize(4, minKeySize), 3 /* versions */, 2 /* garbage */),
				hist(keyOfSize(3, minKeySize), 1 /* versions */, 1 /* garbage */),
				hist(keyOfSize(2, minKeySize), 10 /* versions */, 8 /* garbage */),
				hist(keyOfSize(1, minKeySize), 2 /* versions */, 1 /* garbage */),
			),
			reqs: []gcReq{
				points(
					point(keyOfSize(5, minKeySize), 1),
					point(keyOfSize(4, minKeySize), 2),
				),
				clearRange(keyOfSize(2, minKeySize), 8, keyOfSize(4, minKeySize)),
				points(point(keyOfSize(1, minKeySize), 1)),
			},
			rangeMin: 6,
			batchMax: minKeySize * 4,
		},
		{
			name: "clear range at batch boundary",
			data: keySeq(
				hist(keyOfSize(4, minKeySize), 3 /* versions */, 2 /* garbage */),
				hist(keyOfSize(3, minKeySize), 1 /* versions */, 1 /* garbage */),
				hist(keyOfSize(2, minKeySize), 10 /* versions */, 9 /* garbage */),
				hist(keyOfSize(1, minKeySize), 2 /* versions */, 1 /* garbage */),
			),
			reqs: []gcReq{
				points(point(keyOfSize(4, minKeySize), 2)),
				clearRange(keyOfSize(2, minKeySize), 9, keyOfSize(4, minKeySize)),
				points(point(keyOfSize(1, minKeySize), 1)),
			},
			rangeMin: 6,
			batchMax: minKeySize * 2,
		},
		{
			// Test verifies that point batch history is not lost when non-garbage
			// data is found. Batch spans multiple versions of a key.
			name: "clear multiple points batches",
			data: keySeq(
				hist(keyOfSize(3, minKeySize), 10 /* versions */, 8 /* garbage */),
				hist(keyOfSize(2, minKeySize), 10 /* versions */, 10 /* garbage */),
				hist(keyOfSize(1, minKeySize), 2 /* versions */, 1 /* garbage */),
			),
			reqs: []gcReq{
				points(point(keyOfSize(3, minKeySize), 5)),
				points(point(keyOfSize(3, minKeySize), 8)),
				clearRange(keyOfSize(1, minKeySize), 1, keyOfSize(3, minKeySize)),
			},
			rangeMin: 10,
			batchMax: minKeySize*4 + 1,
		},
		{
			// Test verifies that point batch history is not lost when non-garbage
			// data is found. Batch spans multiple keys.
			name: "clear multiple points batches 2",
			data: keySeq(
				hist(keyOfSize(6, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(5, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(4, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(3, minKeySize), 2 /* versions */, 1 /* garbage */),
				hist(keyOfSize(2, minKeySize), 10 /* versions */, 10 /* garbage */),
				hist(keyOfSize(1, minKeySize), 2 /* versions */, 1 /* garbage */),
			),
			reqs: []gcReq{
				points(
					point(keyOfSize(6, minKeySize), 2),
					point(keyOfSize(5, minKeySize), 2),
					point(keyOfSize(4, minKeySize), 1),
				),
				points(
					point(keyOfSize(4, minKeySize), 2),
					point(keyOfSize(3, minKeySize), 1),
				),
				clearRange(keyOfSize(1, minKeySize), 1, keyOfSize(3, minKeySize)),
			},
			rangeMin: 10,
			batchMax: 100,
		},
		{
			// Test clear range reaches start of the range.
			name: "clear range to first key",
			data: keySeq(
				hist(keyOfSize(3, minKeySize), 2 /* versions */, 1 /* garbage */),
				hist(keyOfSize(2, minKeySize), 10 /* versions */, 10 /* garbage */),
				hist(keyOfSize(1, minKeySize), 2 /* versions */, 2 /* garbage */),
			),
			reqs: []gcReq{
				points(point(keyOfSize(3, minKeySize), 1)),
				clearRange(keyOfSize(1, minKeySize), 2, keyOfSize(3, minKeySize)),
			},
			rangeMin: 10,
			batchMax: 100,
		},
		{
			// Test clear range reaches start of the range.
			name: "clear points to first key",
			data: keySeq(
				hist(keyOfSize(3, minKeySize), 2 /* versions */, 1 /* garbage */),
				hist(keyOfSize(2, minKeySize), 8 /* versions */, 8 /* garbage */),
				hist(keyOfSize(1, minKeySize), 3 /* versions */, 3 /* garbage */),
			),
			reqs: []gcReq{
				points(
					point(keyOfSize(3, minKeySize), 1),
					point(keyOfSize(2, minKeySize), 3),
				),
				points(
					point(keyOfSize(2, minKeySize), 7),
				),
				points(
					point(keyOfSize(2, minKeySize), 8),
					point(keyOfSize(1, minKeySize), 3),
				),
			},
			rangeMin: 100,
			batchMax: 96,
		},
		{
			// Test clear range ends at the end of the range.
			name: "clear range to last key",
			data: keySeq(
				hist(keyOfSize(3, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(2, minKeySize), 10 /* versions */, 10 /* garbage */),
				hist(keyOfSize(1, minKeySize), 2 /* versions */, 1 /* garbage */),
			),
			reqs: []gcReq{
				clearRange(keyOfSize(1, minKeySize), 1, keyOfSize(3, minKeySize).Next()),
			},
			rangeMin: 10,
		},
		{
			// Test memory limit is reached before min range limit.
			// For memory limits we need multiple keys (not versions) since we only
			// keep latest version of the key per batch.
			name: "prevent clear range when reaching memory limit",
			data: keySeq(
				hist(keyOfSize(6, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(5, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(4, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(3, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(2, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(1, minKeySize), 2 /* versions */, 2 /* garbage */),
			),
			reqs: []gcReq{
				points(
					point(keyOfSize(6, minKeySize), 2),
					point(keyOfSize(5, minKeySize), 2),
				),
				points(
					point(keyOfSize(4, minKeySize), 2),
					point(keyOfSize(3, minKeySize), 2),
				),
				points(
					point(keyOfSize(2, minKeySize), 2),
					point(keyOfSize(1, minKeySize), 2),
				),
			},
			rangeMin: 6,
			batchMax: 96,
			memMax:   1,
		},
		{
			// Test memory limit is reached before min range limit.
			// For memory limits we need multiple keys (not versions) since we only
			// keep latest version of the key per batch.
			name: "delay clear range when reaching memory limit",
			data: keySeq(
				hist(keyOfSize(6, largeKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(5, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(4, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(3, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(2, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(1, minKeySize), 2 /* versions */, 2 /* garbage */),
			),
			reqs: []gcReq{
				points(
					point(keyOfSize(6, largeKeySize), 1),
				),
				points(
					point(keyOfSize(6, largeKeySize), 2),
				),
				// Next() is iffy, but we don't have a better option when we flush the
				// batch as we don't always know if there are more versions of the same
				// key or not that should be covered by clear range.
				clearRange(keyOfSize(1, minKeySize), 2, keyOfSize(5, minKeySize).Next()),
			},
			rangeMin: 5,
			batchMax: 100,
			// When calculating mem size we don't use encoded length, but raw key and
			// timestamp length which adds 3 extra bytes.
			memMax: 100,
		},
		{
			// Test memory limit is reached before min range limit. Range starts in
			// the middle of the points batch. Is it even possible?
			name: "delay clear range to mid batch",
			data: keySeq(
				hist(keyOfSize(6, largeKeySize), 2 /* versions */, 1 /* garbage */),
				hist(keyOfSize(5, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(4, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(3, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(2, minKeySize), 2 /* versions */, 2 /* garbage */),
				hist(keyOfSize(1, minKeySize), 2 /* versions */, 2 /* garbage */),
			),
			reqs: []gcReq{
				points(
					point(keyOfSize(6, largeKeySize), 1),
					point(keyOfSize(5, minKeySize), 1),
				),
				clearRange(keyOfSize(1, minKeySize), 2, keyOfSize(5, minKeySize).Next()),
			},
			rangeMin: 6,
			batchMax: 101,
			// When calculating mem size we don't use encoded length, but raw key and
			// timestamp length which adds 3 extra bytes.
			memMax: 120,
		},
		{
			// Verify that when we delete multiple versions of the same key, then we
			// don't send the end range key to signal that we can avoid locking.
			name: "clear range optimize single key deletions for several versions",
			data: keySeq(
				hist(keyOfSize(3, minKeySize), 2 /* versions */, 1 /* garbage */),
				hist(keyOfSize(2, minKeySize), 10 /* versions */, 10 /* garbage */),
				hist(keyOfSize(1, minKeySize), 2 /* versions */, 0 /* garbage */),
			),
			reqs: []gcReq{
				points(point(keyOfSize(3, minKeySize), 1)),
				clearRange(keyOfSize(2, minKeySize), 10, keyOfSize(2, minKeySize).Next()),
			},
			rangeMin: 6,
		},
		{
			// Verify that when we delete multiple versions of the same key, then we
			// don't send the end range key to signal that we can avoid locking.
			name: "clear range optimize single key deletions for all versions",
			data: keySeq(
				hist(keyOfSize(3, minKeySize), 2 /* versions */, 1 /* garbage */),
				hist(keyOfSize(2, minKeySize), 10 /* versions */, 9 /* garbage */),
				hist(keyOfSize(1, minKeySize), 2 /* versions */, 0 /* garbage */),
			),
			reqs: []gcReq{
				points(point(keyOfSize(3, minKeySize), 1)),
				clearRange(keyOfSize(2, minKeySize), 9, keyOfSize(2, minKeySize).Next()),
			},
			rangeMin: 6,
		},
	} {
		t.Run(d.name, func(t *testing.T) {
			g := capturingGCer{}
			enabled := true
			if d.rangeMin == 0 {
				enabled = false
			}
			memMax := d.memMax
			if memMax == 0 {
				memMax = math.MaxInt
			}
			batchMax := d.batchMax
			if batchMax == 0 {
				batchMax = math.MaxInt64
			}
			b := gcKeyBatcher{
				gcKeyBatcherThresholds: gcKeyBatcherThresholds{
					batchGCKeysBytesThreshold: batchMax,
					clearRangeMinKeys:         d.rangeMin,
					clearRangeEnabled:         enabled,
					maxPendingKeysSize:        memMax,
				},
				gcer:             &g,
				info:             &Info{},
				pointsBatches:    make([]pointsBatch, 1),
				clearRangeEndKey: keys.MaxKey,
				prevWasNewest:    true,
			}

			assertBatcher := func() {
				// Assert used memory invariants.
				totalMem := 0
				for _, b := range b.pointsBatches {
					batchMem := 0
					for _, k := range b.batchGCKeys {
						batchMem += len(k.Key) + hlcTimestampSize
					}
					require.Equal(t, batchMem, b.memUsed, "batch memory usage")
					totalMem += batchMem
				}
				require.Equal(t, totalMem, b.totalMemUsed, "batcher memory usage")
			}

			process := func(k gcData, newest bool) {
				var (
					err error
				)
				if k.garbage {
					err = b.foundGarbage(ctx, &k.kv, newest)
				} else {
					err = b.foundNonGCableData(ctx, &k.kv, newest)
				}
				require.NoError(t, err, "failed to update batcher")
				assertBatcher()
			}

			// Run simulated GC
			var prev gcData
			for _, k := range d.data {
				if prev.kv.key.Key != nil {
					newest := prev.kv.key.Key.Compare(k.kv.key.Key) != 0
					process(prev, newest)
				}
				prev = k
			}
			process(prev, true)
			err := b.flushLastBatch(ctx)
			require.NoError(t, err, "failed last batch")
			assertBatcher()

			// Assert batch history
			require.EqualValues(t, d.reqs, g.ops, "Produced batches")
		})
	}
}

func keySeq(keyHistory ...[]gcData) (res []gcData) {
	for _, h := range keyHistory {
		res = append(res, h...)
	}
	return res
}
