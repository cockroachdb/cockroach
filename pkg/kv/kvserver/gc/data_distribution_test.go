// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gc

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// dataDistribution is an abstraction for testing that represents a stream of
// MVCCKeyValues and MVCCRangeKeyValues. Each call would return either point
// value or range tombstone, but not both.
// The stream may indicate that a point value is an intent by returning
// a non-nil transaction. If an intent is returned it must have a higher
// timestamp than any other version written for the key.
// Range key values could only be tombstones and can't have transaction or
// intent placed on them.
type dataDistribution func() (
	storage.MVCCKeyValue, storage.MVCCRangeKeyValue, *roachpb.Transaction, bool,
)

// setupTest writes the data from this distribution into eng. All data should
// be a part of the range represented by desc.
func (ds dataDistribution) setupTest(
	t testing.TB, eng storage.Engine, desc roachpb.RangeDescriptor,
) enginepb.MVCCStats {
	ctx := context.Background()
	var maxTs hlc.Timestamp
	var ms enginepb.MVCCStats
	for {
		kv, rangeKV, txn, ok := ds()
		if !ok {
			break
		}
		if rangeKey := rangeKV.RangeKey; len(rangeKey.StartKey) > 0 {
			require.Nil(t, txn, "invalid test data, range can't use transaction")
			require.Zero(t, len(kv.Key.Key),
				"invalid test data, range can't be used together with value: key=%s, rangeKey=%s",
				kv.Key.String(), rangeKey.String())
			err := storage.MVCCDeleteRangeUsingTombstone(ctx, eng, &ms, rangeKey.StartKey,
				rangeKey.EndKey, rangeKey.Timestamp, hlc.ClockTimestamp{}, nil, nil, false, 1, 0, nil)
			require.NoError(t, err, "failed to put delete range")
		} else if txn == nil {
			if kv.Key.Timestamp.IsEmpty() {
				require.NoError(t, eng.PutUnversioned(kv.Key.Key, kv.Value))
			} else {
				require.NoError(t, eng.PutMVCC(kv.Key, storage.MVCCValue{Value: roachpb.Value{RawBytes: kv.Value}}))
			}
		} else {
			// TODO(ajwerner): Decide if using MVCCPut is worth it.
			ts := kv.Key.Timestamp
			if txn.ReadTimestamp.IsEmpty() {
				txn.ReadTimestamp = ts
			}
			if txn.WriteTimestamp.IsEmpty() {
				txn.WriteTimestamp = ts
			}
			_, err := storage.MVCCPut(ctx, eng, kv.Key.Key, ts,
				roachpb.Value{RawBytes: kv.Value}, storage.MVCCWriteOptions{Txn: txn, Stats: &ms})
			require.NoError(t, err, "failed to insert value for key %s, value length=%d",
				kv.Key.String(), len(kv.Value))
		}
		if !kv.Key.Timestamp.Less(maxTs) {
			maxTs = kv.Key.Timestamp
		}
		if ts := rangeKV.RangeKey.Timestamp; !ts.Less(maxTs) {
			maxTs = ts
		}
	}
	require.NoError(t, eng.Flush())
	snap := eng.NewSnapshot()
	defer snap.Close()
	ms, err := rditer.ComputeStatsForRange(ctx, &desc, snap, maxTs.WallTime)
	require.NoError(t, err)
	return ms
}

type dataFeedItem struct {
	kv  storage.MVCCKeyValue
	rkv storage.MVCCRangeKeyValue
	txn *roachpb.Transaction
}

func (i *dataFeedItem) String() string {
	if i.txn != nil {
		return fmt.Sprintf("%s ! %s", i.kv.Key.String(), i.txn.ID.String())
	}
	if len(i.kv.Key.Key) > 0 {
		return i.kv.Key.String()
	}
	return i.rkv.RangeKey.String()
}

// sortedDistribution consume provided distribution fully and produce a
// distribution with data ordered by timestamp. This distribution is helpful
// for range key tombstones as they must be placed on top of multiple existing
// point keys.
func sortedDistribution(dist dataDistribution) dataDistribution {
	var allData []dataFeedItem
	for {
		kv, rkv, txn, ok := dist()
		if !ok {
			break
		}
		allData = append(allData, dataFeedItem{kv: kv, rkv: rkv, txn: txn})
	}
	isPoint := func(d dataFeedItem) bool {
		return len(d.kv.Key.Key) > 0
	}
	meta := func(i int) (roachpb.Key, hlc.Timestamp, bool) {
		if !isPoint(allData[i]) {
			return allData[i].rkv.RangeKey.StartKey, allData[i].rkv.RangeKey.Timestamp, false
		}
		return allData[i].kv.Key.Key, allData[i].kv.Key.Timestamp, true
	}
	sort.Slice(allData, func(i, j int) bool {
		ki, ti, pi := meta(i)
		kj, tj, _ := meta(j)
		switch ti.Compare(tj) {
		case -1:
			return true
		case 1:
			return false
		}
		switch ki.Compare(kj) {
		case -1:
			return true
		case 1:
			return false
		}
		return pi
	})

	var lastTs hlc.Timestamp
	var lastIsPoint = true
	for i, v := range allData {
		switch {
		case isPoint(v) && !lastIsPoint && v.kv.Key.Timestamp.LessEq(lastTs):
			lastTs.WallTime++
			allData[i].kv.Key.Timestamp = lastTs
			lastIsPoint = true
		case isPoint(v) && lastIsPoint && v.kv.Key.Timestamp.Less(lastTs):
			allData[i].kv.Key.Timestamp = lastTs
		case !isPoint(v) && !lastIsPoint && v.rkv.RangeKey.Timestamp.LessEq(lastTs):
			lastTs.WallTime++
			allData[i].rkv.RangeKey.Timestamp = lastTs
		case !isPoint(v) && lastIsPoint && v.rkv.RangeKey.Timestamp.LessEq(lastTs):
			lastTs.WallTime++
			allData[i].rkv.RangeKey.Timestamp = lastTs
			lastIsPoint = false
		default:
			lastIsPoint = isPoint(v)
			if lastIsPoint {
				lastTs = v.kv.Key.Timestamp
			} else {
				lastTs = v.rkv.RangeKey.Timestamp
			}
		}
	}

	return func() (storage.MVCCKeyValue, storage.MVCCRangeKeyValue, *roachpb.Transaction, bool) {
		if len(allData) == 0 {
			return storage.MVCCKeyValue{}, storage.MVCCRangeKeyValue{}, nil, false
		}
		defer func() {
			allData = allData[1:]
		}()
		return allData[0].kv, allData[0].rkv, allData[0].txn, true
	}
}

// maxRetriesAllowed is limiting how many times we could retry when generating
// keys and timestamps for objects that are restricted by some criteria (e.g.
// keys are unique, timestamps shouldn't be duplicate in history, intents
// should be newer than range tombstones). If distribution spec is too
// restrictive it may limit maximum permissive objects and generation would loop
// infinitely. Once this threshold is reached, generator will panic and stop
// test or benchmark with meaningful message instead of timeout.
const maxRetriesAllowed = 1000

// newDataDistribution constructs a dataDistribution from various underlying
// distributions.
func newDataDistribution(
	tsDist func() hlc.Timestamp,
	minIntentTs, maxOldIntentTs hlc.Timestamp,
	keyDist func() roachpb.Key,
	valueDist func() roachpb.Value,
	versionsPerKey func() int,
	intentFrac float64,
	oldIntentFrac float64, // within intents(!)
	rangeKeyFrac float64,
	totalKeys int,
	rng *rand.Rand,
) dataDistribution {
	rangeKeyDist := rangeKeyDistribution(keyDist)
	var (
		// Remaining values (all versions of all keys together with intents).
		remaining = totalKeys
		// Key for the objects currently emitted (if versions are not empty).
		key, endKey roachpb.Key
		// Set of key.String() to avoid generating data for the same key multiple
		// times.
		seen = map[string]struct{}{}
		// Pending timestamps for current key sorted in ascending order.
		timestamps []hlc.Timestamp
		// If we should have an intent at the start of history.
		hasIntent bool
	)

	generatePointKey := func() (nextKey, unusedEndKey roachpb.Key, keyTimestamps []hlc.Timestamp, hasIntent bool) {
		hasIntent = rng.Float64() < intentFrac
		oldIntent := hasIntent && rng.Float64() < oldIntentFrac
		for retries := 0; len(keyTimestamps) == 0; retries++ {
			if retries > maxRetriesAllowed {
				panic("generation rules are too restrictive, can't generate more data")
			}
			versions := versionsPerKey()
			if versions == 0 {
				continue
			}
			if versions > remaining {
				versions = remaining
			}
			keyTimestamps = make([]hlc.Timestamp, 0, versions)
			for i := 0; i < versions; i++ {
				keyTimestamps = append(keyTimestamps, tsDist())
			}
			sort.Slice(keyTimestamps, func(i, j int) bool {
				return keyTimestamps[i].Less(keyTimestamps[j])
			})
			prevTs := hlc.Timestamp{WallTime: math.MaxInt64}
			duplicate := false
			for _, ts := range keyTimestamps {
				if ts.Equal(prevTs) {
					duplicate = true
					break
				}
				prevTs = ts
			}
			if duplicate {
				keyTimestamps = nil
				continue
			}
			lastTs := keyTimestamps[len(keyTimestamps)-1]
			if hasIntent {
				// Last value (intent) is older than min intent threshold.
				if lastTs.LessEq(minIntentTs) {
					keyTimestamps = nil
					continue
				}
				// Intent ts is higher than max allowed old intent.
				if oldIntent && maxOldIntentTs.Less(lastTs) {
					keyTimestamps = nil
					continue
				}
				// Intent ts is lower than min allowed for non-pushed intents.
				if !oldIntent && lastTs.LessEq(maxOldIntentTs) {
					keyTimestamps = nil
					continue
				}
			}
			for ; ; retries++ {
				if retries > maxRetriesAllowed {
					panic("generation rules are too restrictive, can't generate more data")
				}
				nextKey = keyDist()
				if _, ok := seen[string(nextKey)]; ok {
					continue
				}
				break
			}
			retries = 0
		}
		return nextKey, unusedEndKey, keyTimestamps, hasIntent
	}

	generateRangeKey := func() (startKey, endKey roachpb.Key, timestamps []hlc.Timestamp, hasIntent bool) {
		var ts hlc.Timestamp
		for {
			ts = tsDist()
			if ts.Less(minIntentTs) {
				break
			}
		}
		timestamps = []hlc.Timestamp{ts}
		startKey, endKey = rangeKeyDist()
		return startKey, endKey, timestamps, false
	}

	return func() (storage.MVCCKeyValue, storage.MVCCRangeKeyValue, *roachpb.Transaction, bool) {
		if remaining == 0 {
			// Throw away temp key data, because we reached the end of sequence.
			seen = nil
			return storage.MVCCKeyValue{}, storage.MVCCRangeKeyValue{}, nil, false
		}
		defer func() { remaining-- }()

		if len(timestamps) == 0 {
			// Loop because we can have duplicate keys or unacceptable values, in that
			// case we retry key from scratch.
			for len(timestamps) == 0 {
				if rng.Float64() < rangeKeyFrac {
					key, endKey, timestamps, hasIntent = generateRangeKey()
				} else {
					key, endKey, timestamps, hasIntent = generatePointKey()
				}
			}
			seen[string(key)] = struct{}{}
		}
		ts := timestamps[0]
		timestamps = timestamps[1:]

		if len(endKey) > 0 {
			return storage.MVCCKeyValue{},
				storage.MVCCRangeKeyValue{
					RangeKey: storage.MVCCRangeKey{
						StartKey:  key,
						EndKey:    endKey,
						Timestamp: ts,
					},
				}, nil, true
		}

		var txn *roachpb.Transaction
		// On the last version, we generate a transaction as needed.
		if len(timestamps) == 0 && hasIntent {
			txn = &roachpb.Transaction{
				Status:                 roachpb.PENDING,
				ReadTimestamp:          ts,
				GlobalUncertaintyLimit: ts.Next().Next(),
			}
			txn.ID = uuid.MakeV4()
			txn.WriteTimestamp = ts
			txn.Key = keyDist()
		}
		return storage.MVCCKeyValue{
			Key:   storage.MVCCKey{Key: key, Timestamp: ts},
			Value: valueDist().RawBytes,
		}, storage.MVCCRangeKeyValue{}, txn, true
	}
}

// distSpec abstractly represents a distribution.
type distSpec interface {
	dist(maxRows int, rng *rand.Rand) dataDistribution
	desc() *roachpb.RangeDescriptor
	String() string
}

// uniformDistSpec is a distSpec which represents uniform distributions over its
// various dimensions.
type uniformDistSpec struct {
	tsSecFrom, tsSecTo int64
	// Intents are split into two categories with distinct time ranges.
	// All intents have lower timestamp bound to ensure they don't overlap with
	// range tombstones since we will not be able to put a range tombstone over
	// intent.
	// Additionally, we have two time thresholds for intents. This is needed to
	// ensure that we have certain fraction of intents GC'able since they lay
	// below certain threshold.
	tsSecMinIntent, tsSecOldIntentTo int64
	keySuffixMin, keySuffixMax       int
	valueLenMin, valueLenMax         int
	deleteFrac                       float64
	// keysPerValue parameters determine number of versions for a key. This number
	// includes tombstones and intents which may be present on top of the history.
	versionsPerKeyMin, versionsPerKeyMax int
	// Fractions define how likely is that a key will belong to one of categories.
	// If we only had a single version for each key, then that would be fraction
	// of total number of objects, but if we have many versions, this value would
	// roughly be total objects/avg(versionsPerKeyMin, versionsPerKeyMax) * frac.
	intentFrac, oldIntentFrac float64
	rangeKeyFrac              float64
}

var _ distSpec = uniformDistSpec{}

func (ds uniformDistSpec) dist(maxRows int, rng *rand.Rand) dataDistribution {
	if ds.tsSecMinIntent <= ds.tsSecFrom && ds.rangeKeyFrac > 0 {
		panic("min intent ts should be set if range key generation is needed")
	}
	if ds.tsSecOldIntentTo <= ds.tsSecMinIntent && ds.oldIntentFrac > 0 {
		panic("old intent ts must be lower than min intent ts if old intents are enabled")
	}
	return newDataDistribution(
		uniformTimestampDistribution(ds.tsSecFrom*time.Second.Nanoseconds(),
			ds.tsSecTo*time.Second.Nanoseconds(), rng),
		hlc.Timestamp{WallTime: ds.tsSecMinIntent * time.Second.Nanoseconds()},
		hlc.Timestamp{WallTime: ds.tsSecOldIntentTo * time.Second.Nanoseconds()},
		uniformTableStringKeyDistribution(ds.desc().StartKey.AsRawKey(), ds.keySuffixMin,
			ds.keySuffixMax, rng),
		uniformValueStringDistribution(ds.valueLenMin, ds.valueLenMax, ds.deleteFrac, rng),
		uniformVersionsPerKey(ds.versionsPerKeyMin, ds.versionsPerKeyMax, rng),
		ds.intentFrac,
		ds.oldIntentFrac,
		ds.rangeKeyFrac,
		maxRows,
		rng,
	)
}

func (ds uniformDistSpec) desc() *roachpb.RangeDescriptor {
	tablePrefix := keys.SystemSQLCodec.TablePrefix(42)
	return &roachpb.RangeDescriptor{
		StartKey: roachpb.RKey(tablePrefix),
		EndKey:   roachpb.RKey(tablePrefix.PrefixEnd()),
	}
}

func (ds uniformDistSpec) String() string {
	return fmt.Sprintf(
		"ts=[%d,%d],"+
			"keySuffix=[%d,%d],"+
			"valueLen=[%d,%d],"+
			"versionsPerKey=[%d,%d],"+
			"deleteFrac=%f,intentFrac=%f,oldIntentFrac=%f,rangeFrac=%f",
		ds.tsSecFrom, ds.tsSecTo,
		ds.keySuffixMin, ds.keySuffixMax,
		ds.valueLenMin, ds.valueLenMax,
		ds.versionsPerKeyMin, ds.versionsPerKeyMax,
		ds.deleteFrac, ds.intentFrac, ds.oldIntentFrac, ds.rangeKeyFrac)
}

// uniformTimestamp returns an hlc timestamp distribution with a wall time
// uniform over [from, to] and a zero logical timestamp.
func uniformTimestampDistribution(from, to int64, rng *rand.Rand) func() hlc.Timestamp {
	if from >= to {
		panic(fmt.Errorf("from (%d) >= to (%d)", from, to))
	}
	n := int(to-from) + 1
	return func() hlc.Timestamp {
		return hlc.Timestamp{WallTime: from + int64(rng.Intn(n))}
	}
}

// returns a uniform length random value distribution.
func uniformValueDistribution(
	min, max int, deleteFrac float64, rng *rand.Rand,
) func() roachpb.Value {
	if min > max {
		panic(fmt.Errorf("min (%d) > max (%d)", min, max))
	}
	n := (max - min) + 1
	return func() roachpb.Value {
		if rng.Float64() < deleteFrac {
			return roachpb.Value{}
		}
		b := make([]byte, min+rng.Intn(n))
		if _, err := rng.Read(b); err != nil {
			panic(err)
		}
		var v roachpb.Value
		v.SetBytes(b)
		return v
	}
}

// returns a uniform length random value distribution.
func uniformValueStringDistribution(
	min, max int, deleteFrac float64, rng *rand.Rand,
) func() roachpb.Value {
	if min > max {
		panic(fmt.Errorf("min (%d) > max (%d)", min, max))
	}
	n := (max - min) + 1
	return func() roachpb.Value {
		if rng.Float64() < deleteFrac {
			return roachpb.Value{}
		}
		var v roachpb.Value
		v.SetString(randutil.RandString(rng, min+rng.Intn(n), randutil.PrintableKeyAlphabet))
		return v
	}
}

func uniformVersionsPerKey(valuesPerKeyMin, valuesPerKeyMax int, rng *rand.Rand) func() int {
	if valuesPerKeyMin > valuesPerKeyMax {
		panic(fmt.Errorf("min (%d) > max (%d)", valuesPerKeyMin, valuesPerKeyMax))
	}
	n := (valuesPerKeyMax - valuesPerKeyMin) + 1
	return func() int { return valuesPerKeyMin + rng.Intn(n) }
}

func uniformTableKeyDistribution(
	prefix roachpb.Key, suffixMin, suffixMax int, rng *rand.Rand,
) func() roachpb.Key {
	if suffixMin > suffixMax {
		panic(fmt.Errorf("suffixMin (%d) > suffixMax (%d)", suffixMin, suffixMax))
	}
	n := (suffixMax - suffixMin) + 1
	return func() roachpb.Key {
		randData := make([]byte, suffixMin+rng.Intn(n))
		_, _ = rng.Read(randData)
		return encoding.EncodeBytesAscending(prefix[0:len(prefix):len(prefix)], randData)
	}
}

// TODO(oleg): Suppress lint for now, check how reduced byte choice affects
// performance of tests.
var _ = uniformTableKeyDistribution

func uniformTableStringKeyDistribution(
	prefix roachpb.Key, suffixMin, suffixMax int, rng *rand.Rand,
) func() roachpb.Key {
	if suffixMin > suffixMax {
		panic(fmt.Errorf("suffixMin (%d) > suffixMax (%d)", suffixMin, suffixMax))
	}
	n := (suffixMax - suffixMin) + 1
	return func() roachpb.Key {
		lenSuffix := suffixMin + rng.Intn(n)
		key := randutil.RandString(rng, lenSuffix, randutil.PrintableKeyAlphabet)
		return encoding.EncodeBytesAscending(prefix[0:len(prefix):len(prefix)], []byte(key))
	}
}

func rangeKeyDistribution(keyDist func() roachpb.Key) func() (roachpb.Key, roachpb.Key) {
	return func() (roachpb.Key, roachpb.Key) {
		k1 := keyDist()
		k2 := keyDist()
		for ; k1.Equal(k2); k2 = keyDist() {
		}
		if k1.Compare(k2) > 0 {
			return k2, k1
		}
		return k1, k2
	}
}
