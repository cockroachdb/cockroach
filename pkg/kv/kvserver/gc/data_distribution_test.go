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
// MVCCKeyValues. The stream may indicate that a value is an intent by returning
// a non-nil transaction. If an intent is returned it must have a higher
// timestamp than any other version written for the key.
type dataDistribution func() (storage.MVCCKeyValue, *roachpb.Transaction, bool)

// setupTest writes the data from this distribution into eng. All data should
// be a part of the range represented by desc.
func (ds dataDistribution) setupTest(
	t testing.TB, eng storage.Engine, desc roachpb.RangeDescriptor,
) enginepb.MVCCStats {
	ctx := context.Background()
	var maxTs hlc.Timestamp
	var ms enginepb.MVCCStats
	for {
		kv, txn, ok := ds()
		if !ok {
			break
		}
		if txn == nil {
			if kv.Key.Timestamp.IsEmpty() {
				require.NoError(t, eng.PutUnversioned(kv.Key.Key, kv.Value))
			} else {
				require.NoError(t, eng.PutRawMVCC(kv.Key, kv.Value))
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
			err := storage.MVCCPut(ctx, eng, &ms, kv.Key.Key, ts,
				hlc.ClockTimestamp{}, roachpb.Value{RawBytes: kv.Value}, txn)
			require.NoError(t, err, "failed to insert value for key %s, value length=%d",
				kv.Key.String(), len(kv.Value))
		}
		if !kv.Key.Timestamp.Less(maxTs) {
			maxTs = kv.Key.Timestamp
		}
	}
	require.NoError(t, eng.Flush())
	snap := eng.NewSnapshot()
	defer snap.Close()
	ms, err := rditer.ComputeStatsForRange(&desc, snap, maxTs.WallTime)
	require.NoError(t, err)
	return ms
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
	totalKeys int,
	rng *rand.Rand,
) dataDistribution {
	// TODO(ajwerner): provide a mechanism to control the rate of expired intents
	// or the intent age. Such a knob would likely require decoupling intents from
	// other keys.
	var (
		// Remaining values (all versions of all keys together with intents).
		remaining = totalKeys
		// Key for the objects currently emitted (if versions are not empty).
		key roachpb.Key
		// Set of key.String() to avoid generating data for the same key multiple
		// times.
		seen = map[string]struct{}{}
		// Pending timestamps for current key sorted in ascending order.
		timestamps []hlc.Timestamp
		// If we should have an intent at the start of history.
		hasIntent bool
	)

	generatePointKey := func() (nextKey roachpb.Key, keyTimestamps []hlc.Timestamp, hasIntent bool) {
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
				sk := string(nextKey)
				if _, ok := seen[sk]; ok {
					continue
				}
				break
			}
			retries = 0
		}
		return nextKey, keyTimestamps, hasIntent
	}

	return func() (storage.MVCCKeyValue, *roachpb.Transaction, bool) {
		if remaining == 0 {
			// Throw away temp key data, because we reached the end of sequence.
			seen = nil
			return storage.MVCCKeyValue{}, nil, false
		}
		defer func() { remaining-- }()

		if len(timestamps) == 0 {
			// Loop because we can have duplicate keys or unacceptable values, in that
			// case we retry key from scratch.
			for len(timestamps) == 0 {
				key, timestamps, hasIntent = generatePointKey()
			}
			seen[string(key)] = struct{}{}
		}
		ts := timestamps[0]
		timestamps = timestamps[1:]
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
		}, txn, true
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
	tsSecFrom, tsSecTo               int64 // seconds
	tsSecMinIntent, tsSecOldIntentTo int64
	keySuffixMin, keySuffixMax       int
	valueLenMin, valueLenMax         int
	deleteFrac                       float64
	keysPerValueMin, keysPerValueMax int
	intentFrac, oldIntentFrac        float64
}

var _ distSpec = uniformDistSpec{}

func (ds uniformDistSpec) dist(maxRows int, rng *rand.Rand) dataDistribution {
	return newDataDistribution(
		uniformTimestampDistribution(ds.tsSecFrom*time.Second.Nanoseconds(), ds.tsSecTo*time.Second.Nanoseconds(), rng),
		hlc.Timestamp{WallTime: ds.tsSecMinIntent * time.Second.Nanoseconds()},
		hlc.Timestamp{WallTime: ds.tsSecOldIntentTo * time.Second.Nanoseconds()},
		uniformTableStringKeyDistribution(ds.desc().StartKey.AsRawKey(), ds.keySuffixMin, ds.keySuffixMax, rng),
		uniformValueStringDistribution(ds.valueLenMin, ds.valueLenMax, ds.deleteFrac, rng),
		uniformValuesPerKey(ds.keysPerValueMin, ds.keysPerValueMax, rng),
		ds.intentFrac, ds.oldIntentFrac,
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
			"keysPerValue=[%d,%d],"+
			"deleteFrac=%f,intentFrac=%f",
		ds.tsSecFrom, ds.tsSecTo,
		ds.keySuffixMin, ds.keySuffixMax,
		ds.valueLenMin, ds.valueLenMax,
		ds.keysPerValueMin, ds.keysPerValueMax,
		ds.deleteFrac, ds.intentFrac)
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

func uniformValuesPerKey(valuesPerKeyMin, valuesPerKeyMax int, rng *rand.Rand) func() int {
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
