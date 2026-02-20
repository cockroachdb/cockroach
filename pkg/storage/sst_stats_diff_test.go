// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

// pFixed is like p, but simply sets the value to the key. This is useful for
// test cases that process duplicates between the eng and the sst, since we
// cannot handle two identical roachpb keys + timestamps with different values.
func pFixed(stringifiedKVs string) storageutils.KVs {
	kvs := storageutils.KVs{}
	for i := 0; i < len(stringifiedKVs); i += 2 {
		key := string(stringifiedKVs[i])
		ts := int64(stringifiedKVs[i+1]) * 1e9
		value := key
		kv := storageutils.PointKV(key, int(ts), value)
		kvs = append(kvs, kv)
	}
	return kvs
}

func TestMVCCComputeSSTStatsDiff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()

	rng, _ := randutil.NewPseudoRand()

	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()

	randString := func(n int) string {
		const letters = "abcdefghijklmnopqrstuvwxyz"
		b := make([]byte, n)
		for i := range b {
			b[i] = letters[rng.Intn(len(letters))]
		}
		return string(b)
	}

	// p parses a string of point KVs like "b1a1", and sets the value to a random
	// string.
	p := func(stringifiedKVs string) storageutils.KVs {
		kvs := storageutils.KVs{}
		for i := 0; i < len(stringifiedKVs); i += 2 {
			key := string(stringifiedKVs[i])
			// Multiply by 1e9 so each ts is 1 second apart to actually test
			// GCBytesAge computation.
			ts := int64(stringifiedKVs[i+1]) * 1e9
			value := randString(rng.Intn(10) + 1)
			kv := storageutils.PointKV(key, int(ts), value)
			kvs = append(kvs, kv)
		}
		return kvs
	}

	testCases := []struct {
		name string
		// sst describes an sst that the test will run ComputeStatsDiff on
		sst storageutils.KVs

		// eng describes the keys in the existing key space
		eng storageutils.KVs
	}{
		{
			name: "emptyKeyspace",
			sst:  p("a1"),
			eng:  p(""),
		},
		{
			name: "emptyKeySpaceHistory",
			sst:  p("a2a1"),
			eng:  p(""),
		},
		{
			name: "insert",
			sst:  p("a1"),
			eng:  p("b1"),
		},
		{
			name: "update",
			sst:  p("a2"),
			eng:  p("a1"),
		},
		{
			name: "delete",
			sst:  storageutils.KVs{storageutils.PointKV("a", 2, "")},
			eng:  storageutils.KVs{storageutils.PointKV("a", 1, "a1")},
		},
		{
			name: "exhaustEng",
			sst:  p("b1"),
			eng:  p("a1"),
		},
		{
			name: "exhaustSST",
			sst:  p("a1"),
			eng:  p("b1"),
		},
		{
			name: "dupe",
			sst:  pFixed("a2"),
			eng:  pFixed("a2"),
		},
		{
			name: "sstHistoryGreaterThanEng",
			sst:  p("a3a2"),
			eng:  p("a1"),
		},
		{
			name: "sstHistoryThreeVersionsGreaterThanEng",
			sst:  p("a4a3a2"),
			eng:  p("a1"),
		},
		{
			name: "engHistoryLessThanSST",
			sst:  p("a3"),
			eng:  p("a2a1"),
		},
		{
			name: "sstBehind",
			sst:  pFixed("a2a1b1"),
			eng:  pFixed("a4a3a2a1b1"),
		},
		{
			name: "engBehind",
			sst:  pFixed("a4a3a2a1b1"),
			eng:  pFixed("a2a1b1"),
		},
		{
			name: "multipleKeys",
			sst:  p("a2a1c2"),
			eng:  p("b1c1d2"),
		},
		{
			name: "dupeThenSSTAhead",
			sst:  pFixed("a2c1"),
			eng:  pFixed("a2b1"),
		},
		{
			name: "dupeThenSSTBehind",
			sst:  pFixed("a2b1"),
			eng:  pFixed("a2c1"),
		},
		{
			name: "doubleDupe",
			sst:  pFixed("a2b1"),
			eng:  pFixed("a2b1"),
		},
		{
			name: "processDupesFallBackToSeek",
			sst:  pFixed("a1"),
			eng:  pFixed("a7a6a5a4a3a2a1"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "randomDeletes", func(t *testing.T, randomDeletes bool) {

				// Clear the engine before each test, so stats collection for each test
				// is independent.
				require.NoError(t, engine.Excise(ctx, roachpb.Span{Key: keys.LocalMax, EndKey: roachpb.KeyMax}))

				eng := tc.eng
				sst := tc.sst
				if randomDeletes {
					eng, sst = addRandomDeletes(t, rng, eng, sst)
				}

				local, _, _ := storageutils.MakeSST(t, st, eng)
				require.NoError(t, fs.WriteFile(engine.Env(), "local", local, fs.UnspecifiedWriteCategory))
				require.NoError(t, engine.IngestLocalFiles(ctx, []string{"local"}))

				now := int64(timeutil.Now().Nanosecond())

				baseStats, err := storage.ComputeStats(ctx, engine, fs.UnknownReadCategory, keys.LocalMax, roachpb.KeyMax, now)
				require.NoError(t, err)

				sstEncoded, startUnversioned, endUnversioned := storageutils.MakeSST(t, st, sst)
				start := storage.MVCCKey{Key: startUnversioned}
				end := storage.MVCCKey{Key: endUnversioned}
				updateTime := now + 1

				statsDelta, err := storage.ComputeSSTStatsDiff(
					ctx, sstEncoded, engine, updateTime, start, end)
				require.NoError(t, err)

				require.NoError(t, fs.WriteFile(engine.Env(), "sst", sstEncoded, fs.UnspecifiedWriteCategory))
				require.NoError(t, engine.IngestLocalFiles(ctx, []string{"sst"}))

				expStats, err := storage.ComputeStats(ctx, engine, fs.UnknownReadCategory, keys.LocalMax, roachpb.KeyMax, updateTime)
				require.NoError(t, err)

				baseStats.Add(statsDelta)

				require.Equal(t, baseStats.LastUpdateNanos, updateTime)

				t.Logf("sst %s, eng %s", sst, eng)
				if !baseStats.Equal(expStats) {
					t.Log("test, expected")
					pretty.Ldiff(t, baseStats, expStats)
					t.Errorf("%s: diff(ms, expMS) nontrivial", tc.name)
				}
			})
		})
	}
}

func addRandomDeletes(
	t require.TestingT, rng *rand.Rand, engKVs, sstKVs storageutils.KVs,
) (storageutils.KVs, storageutils.KVs) {

	// deleteSwap converts some kvs to tombstones and returns the modified kvs and the kvs deleted.
	deleteSwap := func(kvs storageutils.KVs) (storageutils.KVs, map[string]struct{}) {
		deleted := make(map[string]struct{})
		for i := range kvs {
			if rng.Intn(2) == 0 {
				kv := kvs[i].(storage.MVCCKeyValue)
				deleted[kv.Key.String()] = struct{}{}
				encodedTombstoneValue, err := storage.EncodeMVCCValue(storage.MVCCValue{})
				require.NoError(t, err)
				kv.Value = encodedTombstoneValue
				kvs[i] = kv
			}
		}
		return kvs, deleted
	}

	reconcileDeletedDupes := func(kvs storageutils.KVs, deleted map[string]struct{}) storageutils.KVs {
		for i := range kvs {
			kv := kvs[i].(storage.MVCCKeyValue)
			if _, ok := deleted[kv.Key.String()]; ok {
				encodedTombstoneValue, err := storage.EncodeMVCCValue(storage.MVCCValue{})
				require.NoError(t, err)
				kv.Value = encodedTombstoneValue
				kvs[i] = kv
			}
		}
		return kvs
	}

	engKVs, deletedLocal := deleteSwap(engKVs)
	sstKVs, deletedSST := deleteSwap(sstKVs)

	// Ensures that duplicate MVCC keys are either both deleted or both not
	// deleted.
	engKVs = reconcileDeletedDupes(engKVs, deletedSST)
	sstKVs = reconcileDeletedDupes(sstKVs, deletedLocal)

	return engKVs, sstKVs
}

func TestMVCCComputeStatsDiffEstimates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()

	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()

	testCases := []struct {
		name string
		// sst describes an sst that the test will run ComputeStatsDiff on
		sst storageutils.KVs

		// eng describes the keys in the existing key space
		eng storageutils.KVs
	}{
		{
			name: "extraVersions",
			sst:  pFixed("a2a1"),
			eng:  pFixed("a2"),
		},
		{
			name: "extraVersionsMoreData",
			sst:  pFixed("a2a1"),
			eng:  pFixed("a2b1"),
		},
		{
			name: "missingNewVersion",
			sst:  pFixed("a1"),
			eng:  pFixed("a2"),
		},
		{
			name: "missingNewVersionMoreData",
			sst:  pFixed("a1"),
			eng:  pFixed("a2b1"),
		},
		{
			name: "seekToExtraVersions",
			sst:  pFixed("a2a1"),
			eng:  pFixed("a4a3a2"),
		},
		{
			name: "hole",
			sst:  pFixed("a2"),
			eng:  pFixed("a3a1"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			// Clear the engine before each test, so stats collection for each test
			// is independent.
			require.NoError(t, engine.Excise(ctx, roachpb.Span{Key: keys.LocalMax, EndKey: roachpb.KeyMax}))

			eng := tc.eng
			sst := tc.sst

			local, _, _ := storageutils.MakeSST(t, st, eng)
			require.NoError(t, fs.WriteFile(engine.Env(), "local", local, fs.UnspecifiedWriteCategory))
			require.NoError(t, engine.IngestLocalFiles(ctx, []string{"local"}))

			now := int64(timeutil.Now().Nanosecond())

			sstEncoded, startUnversioned, endUnversioned := storageutils.MakeSST(t, st, sst)
			start := storage.MVCCKey{Key: startUnversioned}
			end := storage.MVCCKey{Key: endUnversioned}
			updateTime := now + 1

			_, err := storage.ComputeSSTStatsDiff(
				ctx, sstEncoded, engine, updateTime, start, end)
			require.ErrorContains(t, err, storage.ComputeStatsDiffViolation.Error())
		})
	}
}
