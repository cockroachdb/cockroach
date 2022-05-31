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
	"math"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
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
	txn := roachpb.MakeTransaction("txn", key, roachpb.NormalUserPriority, intentHlc, 1000, 0)
	require.NoError(t,
		storage.MVCCPut(ctx, eng, nil, key, intentHlc, hlc.ClockTimestamp{}, value, &txn))
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
	info, err := Run(ctx, &desc, snap, nowTs, nowTs,
		RunOptions{IntentAgeThreshold: intentLongThreshold}, gcTTL, &fakeGCer, fakeGCer.resolveIntents,
		fakeGCer.resolveIntentsAsync)
	require.NoError(t, err, "GC Run shouldn't fail")
	assert.Zero(t, info.IntentsConsidered,
		"Expected no intents considered by GC with default threshold")

	info, err = Run(ctx, &desc, snap, nowTs, nowTs,
		RunOptions{IntentAgeThreshold: intentShortThreshold}, gcTTL, &fakeGCer, fakeGCer.resolveIntents,
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
		txn := roachpb.MakeTransaction("txn", key, roachpb.NormalUserPriority, intentHlc, 1000, 0)
		for _, suffix := range objectKeys {
			key := []byte{prefix, suffix}
			require.NoError(t,
				storage.MVCCPut(ctx, eng, nil, key, intentHlc, hlc.ClockTimestamp{}, value, &txn))
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
	_, err := Run(ctx, &desc, snap, nowTs, nowTs, RunOptions{IntentAgeThreshold: intentAgeThreshold},
		gcTTL, &baseGCer, baseGCer.resolveIntents,
		baseGCer.resolveIntentsAsync)
	if err != nil {
		t.Fatal("Can't prepare test fixture. Non batched GC run fails.")
	}
	baseGCer.normalize()

	var batchSize int64 = 7
	fakeGCer := makeFakeGCer()
	info, err := Run(ctx, &desc, snap, nowTs, nowTs,
		RunOptions{IntentAgeThreshold: intentAgeThreshold, MaxIntentsPerIntentCleanupBatch: batchSize},
		gcTTL,
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
			require.Less(t, int64(totalKeyBytes-len(batch[len(batch)-1].Key)),
				opts.maxIntentKeyBytesPerIntentCleanupBatch,
				fmt.Sprintf("Byte limit was exceeded for more than the last key in batch %d/%d", i+1,
					len(*r)))
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
			txnUUID = uuid.FastMakeV4()
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

func TestGC(t *testing.T) {
	for _, d := range []struct {
		name string
		data string
	}{
		{"single", singleValueData},
		{"multiple_newer", multipleValuesNewerData},
		{"multiple_older", multipleValuesOlderData},
		{"delete", deleteData},
		{"delete_with_newer", deleteWithNewerData},
		{"multiple_values", multipleValuesData},
		{"intents", intents},
		{"intents_after_data", intentsAfterData},
		{"intents_after_delete", intentsAfterDelete},
	} {
		t.Run(d.name, func(t *testing.T) {
			runTest(t, d.data)
		})
	}
}

func runTest(t *testing.T, data string) {
	ctx := context.Background()
	tablePrefix := keys.SystemSQLCodec.TablePrefix(42)
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey(tablePrefix),
		EndKey:   roachpb.RKey(tablePrefix.PrefixEnd()),
	}

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	dataItems, gcTS, now := readTableData(t, desc.StartKey.AsRawKey(), data)
	ds := dataItems.fullDistribution()
	stats := ds.setupTest(t, eng, desc)
	snap := eng.NewSnapshot()
	defer snap.Close()

	gcer := makeFakeGCer()
	_, err := Run(ctx, &desc, snap, now, gcTS,
		RunOptions{IntentAgeThreshold: time.Nanosecond * time.Duration(now.WallTime)}, time.Second,
		&gcer,
		gcer.resolveIntents, gcer.resolveIntentsAsync)
	require.NoError(t, err)
	require.Empty(t, gcer.intents, "expecting no intents")
	require.NoError(t, storage.MVCCGarbageCollect(ctx, eng, &stats, gcer.requests(), gcTS))

	ctrlEng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	expectedStats := dataItems.liveDistribution().setupTest(t, ctrlEng, desc)

	//fmt.Println("Dump of expected:")
	//for _, l := range formatTable(engineData(t, ctrlEng, desc), tablePrefix) {
	//	fmt.Println(l)
	//}
	//
	//fmt.Println("Dump of result:")
	//for _, l := range formatTable(engineData(t, eng, desc), tablePrefix) {
	//	fmt.Println(l)
	//}

	requireEqualReaders(t, ctrlEng, eng, desc)
	require.Equal(t, expectedStats, stats, "mvcc stats don't match the data")
}

// requireEqualReaders compares data in two readers
func requireEqualReaders(
	t *testing.T, exected storage.Reader, actual storage.Reader, desc roachpb.RangeDescriptor,
) {
	itExp := exected.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound:           desc.StartKey.AsRawKey(),
		UpperBound:           desc.EndKey.AsRawKey(),
		KeyTypes:             storage.IterKeyTypePointsOnly,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	defer itExp.Close()

	itActual := actual.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound:           desc.StartKey.AsRawKey(),
		UpperBound:           desc.EndKey.AsRawKey(),
		KeyTypes:             storage.IterKeyTypePointsOnly,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
	defer itActual.Close()
	itExp.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})
	itActual.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})
	for {
		okExp, err := itExp.Valid()
		require.NoError(t, err, "failed to iterate values")
		okAct, err := itActual.Valid()
		require.NoError(t, err, "failed to iterate values")
		if !okExp && !okAct {
			break
		}

		require.Equal(t, okExp, okAct, "iterators have different number of elements")
		require.True(t, itExp.UnsafeKey().Equal(itActual.UnsafeKey()),
			"expected key not equal to actual (expected %s, found %s)", itExp.UnsafeKey(),
			itActual.UnsafeKey())
		require.True(t, bytes.Compare(itExp.UnsafeValue(), itActual.UnsafeValue()) == 0,
			"expected value not equal to actual for key %s", itExp.UnsafeKey())

		itExp.Next()
		itActual.Next()
	}
}

// dataItem is element read from test table containing mvcc key value along with
// metadata needed for filtering.
type dataItem struct {
	value storage.MVCCKeyValue
	txn   *roachpb.Transaction
	live  bool
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
			live := strings.ToLower(val) != val || val == "*"
			var txn *roachpb.Transaction
			// Handle special delete characters.
			if val == "*" || val == "." {
				val = ""
			} else if val[0] == '!' {
				// Handle intents.
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
			// Handle normal values.
			var value []byte
			if len(val) > 0 {
				var v roachpb.Value
				v.SetString(val)
				value = v.RawBytes
			}
			kv := storage.MVCCKeyValue{
				Key: storage.MVCCKey{
					Key:       columnKeys[i],
					Timestamp: ts,
				},
				Value: value,
			}
			items = append(items, dataItem{value: kv, txn: txn, live: live})
		}
		return i + 1
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
		return hlc.Timestamp{WallTime: tsInt}
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
			i = parsePoint(val, i, ts)
		}
	}

	// Reverse from oldest to newest.
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}

	return items, gcTS, items[len(items)-1].value.Key.Timestamp.Add(1, 0)
}

// fullDistribution creates a data distribution that contains all data read from
// table.
func (d tableData) fullDistribution() dataDistribution {
	items := d
	return func() (storage.MVCCKeyValue, *roachpb.Transaction, bool) {
		if len(items) == 0 {
			return storage.MVCCKeyValue{}, nil, false
		}
		defer func() { items = items[1:] }()
		return items[0].value, items[0].txn, true
	}
}

// liveDistribution creates a data distribution from the table data is was only
// marked as live (see table data format above).
func (d tableData) liveDistribution() dataDistribution {
	items := d
	return func() (storage.MVCCKeyValue, *roachpb.Transaction, bool) {
		for {
			if len(items) == 0 {
				return storage.MVCCKeyValue{}, nil, false
			}
			if items[0].live {
				break
			}
			items = items[1:]
		}
		defer func() { items = items[1:] }()
		return items[0].value, items[0].txn, true
	}
}

func engineData(t *testing.T, r storage.Reader, desc roachpb.RangeDescriptor) []tableCell {
	var result []tableCell
	it := r.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		LowerBound:           desc.StartKey.AsRawKey(),
		UpperBound:           desc.EndKey.AsRawKey(),
		KeyTypes:             storage.IterKeyTypePointsOnly,
		RangeKeyMaskingBelow: hlc.Timestamp{},
	})
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
			if len(it.UnsafeValue()) > 0 {
				mvccVal, err := storage.DecodeMVCCValue(it.UnsafeValue())
				require.NoError(t, err, "can't read mvcc value from engine")
				b, err := mvccVal.Value.GetBytes()
				require.NoError(t, err, "can't read bytes from mvcc value")
				v = prefix + string(b)
			}
			result = append(result, tableCell{
				key:   it.Key(),
				value: v,
			})
			prefix = ""
		}
		it.Next()
	}
	return result
}

type tableCell struct {
	key   storage.MVCCKey
	value string
}

type columnInfo struct {
	key            string
	maxValueLength int
}

// formatTable renders table with data. expecting data to be sorted naturally:
// keys ascending, timestamps descending.
// prefix if provided defines start of the key, that would be stripped from the
// keys to avoid clutter.
func formatTable(data []tableCell, prefix roachpb.Key) []string {
	prefixStr := ""
	if prefix != nil {
		prefixStr = prefix.String()
	}
	keyRe := regexp.MustCompile(`^/"(.*)"$`)
	var foundKeys []columnInfo
	var lastKey roachpb.Key
	rowData := make(map[int64][]string)
	for _, c := range data {
		ts := c.key.Timestamp.WallTime
		key := c.key.Key.String()
		if strings.Index(key, prefixStr) == 0 {
			key = key[len(prefixStr):]
			if keyRe.FindSubmatch([]byte(key)) != nil {
				key = key[2 : len(key)-1]
			}
		}
		if !c.key.Key.Equal(lastKey) {
			foundKeys = append(foundKeys, columnInfo{
				key:            key,
				maxValueLength: len(key),
			})
			lastKey = c.key.Key
		}
		row, _ := rowData[ts]
		for len(row) < len(foundKeys)-1 {
			row = append(row, "")
		}
		rowData[ts] = append(row, c.value)
		valueLen := len(c.value)
		if i := len(foundKeys) - 1; valueLen > foundKeys[i].maxValueLength {
			foundKeys[i].maxValueLength = valueLen
		}
	}
	var tss []int64
	for ts := range rowData {
		tss = append(tss, ts)
	}
	sort.Slice(tss, func(i, j int) bool {
		return tss[i] > tss[j]
	})

	lsLen := len(fmt.Sprintf("%d", tss[0]))
	rowPrefixFmt := fmt.Sprintf(" %%%dd | ", lsLen)

	var result []string

	firstRow := fmt.Sprintf(" %s | ", strings.Repeat(" ", lsLen))
	for _, colInfo := range foundKeys {
		firstRow += fmt.Sprintf(fmt.Sprintf("%%%ds ", colInfo.maxValueLength), colInfo.key)
	}
	result = append(result, firstRow)
	result = append(result, strings.Repeat("-", len(firstRow)))
	for _, ts := range tss {
		row := rowData[ts]
		rowStr := fmt.Sprintf(rowPrefixFmt, ts)
		for i, v := range row {
			rowStr += fmt.Sprintf(fmt.Sprintf("%%%ds ", foundKeys[i].maxValueLength), v)
		}
		result = append(result, rowStr)
	}
	return result
}
