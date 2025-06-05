// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_test

import (
	"context"
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

	// p parses a string of point KVs like "b1a1".
	p := func(stringifiedKVs string) storageutils.KVs {
		kvs := storageutils.KVs{}
		for i := 0; i < len(stringifiedKVs); i += 2 {
			key := string(stringifiedKVs[i])
			ts := int64(stringifiedKVs[i+1])
			value := randString(rng.Intn(10) + 1)
			kv := storageutils.PointKV(key, int(ts), value)
			kvs = append(kvs, kv)
		}
		return kvs
	}

	testCases := []struct {
		name  string
		sst   storageutils.KVs
		local storageutils.KVs
	}{
		{
			name:  "emptyKeyspace",
			sst:   p("a1"),
			local: p(""),
		},
		{
			name:  "emptyKeySpaceHistory",
			sst:   p("a2a1"),
			local: p(""),
		},
		{
			name:  "insert",
			sst:   p("a1"),
			local: p("b1"),
		},
		{
			name:  "update",
			sst:   p("a2"),
			local: p("a1"),
		},
		{
			name:  "delete",
			sst:   storageutils.KVs{storageutils.PointKV("a", 2, "")},
			local: p("a1"),
		},
		{
			name:  "exhaustLocal",
			sst:   p("b1"),
			local: p("a1"),
		},
		{
			name:  "exhaustSST",
			sst:   p("a1"),
			local: p("b1"),
		},
		{
			name:  "shadow",
			sst:   p("a1"),
			local: p("a2"),
		},
		{
			name:  "dupe",
			sst:   storageutils.KVs{storageutils.PointKV("a", 2, "a2")},
			local: storageutils.KVs{storageutils.PointKV("a", 2, "a2")},
		},
		{
			name:  "sstHistoryGreaterThanLocal",
			sst:   p("a3a2"),
			local: p("a1"),
		},
		{
			name:  "sstHistoryThreeVersionsGreaterThanLocal",
			sst:   p("a4a3a2"),
			local: p("a1"),
		},
		{
			name:  "sstHistoryLessThanLocal",
			sst:   p("a2a1"),
			local: p("a3"),
		},
		{
			name:  "sstHistoryStraddlesLocal",
			sst:   p("a3a1"),
			local: p("a2"),
		},
		{
			name:  "localHistoryGreaterThanSST",
			sst:   p("a1"),
			local: p("a3a2"),
		},
		{
			name:  "localHistoryLessThanSST",
			sst:   p("a3"),
			local: p("a2a1"),
		},
		{
			name:  "localStraddlesSST",
			sst:   p("a2"),
			local: p("a3a1"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "randomDeletes", func(t *testing.T, randomDeletes bool) {

				// Clear the engine before each test, so stats collection for each test
				// is independent.
				require.NoError(t, engine.Excise(ctx, roachpb.Span{Key: keys.LocalMax, EndKey: roachpb.KeyMax}))

				if tc.name == "dupe" && randomDeletes {
					t.Log("skipping dupe test with random deletes as it is nonsenical to write a key with the same timestamp but different value")
					return
				}
				localT := tc.local
				sstT := tc.sst
				if randomDeletes {
					deleteSwap := func(kvs storageutils.KVs) storageutils.KVs {
						for i := range kvs {
							if rng.Intn(2) == 0 {
								kv := kvs[i].(storage.MVCCKeyValue)
								encodedTombstoneValue, err := storage.EncodeMVCCValue(storage.MVCCValue{})
								require.NoError(t, err)
								kv.Value = encodedTombstoneValue
								kvs[i] = kv
							}
						}
						return kvs
					}
					// Randomly delete some keys in the local history.
					localT = deleteSwap(tc.local)
					sstT = deleteSwap(tc.sst)
				}

				local, _, _ := storageutils.MakeSST(t, st, localT)
				require.NoError(t, fs.WriteFile(engine.Env(), "local", local, fs.UnspecifiedWriteCategory))
				require.NoError(t, engine.IngestLocalFiles(ctx, []string{"local"}))

				now := int64(timeutil.Now().Nanosecond())

				baseStats, err := storage.ComputeStats(ctx, engine, keys.LocalMax, roachpb.KeyMax, now)
				require.NoError(t, err)

				sst, startUnversioned, endUnversioned := storageutils.MakeSST(t, st, sstT)
				start := storage.MVCCKey{Key: startUnversioned}
				end := storage.MVCCKey{Key: endUnversioned}

				statsDelta, err := storage.ComputeSSTStatsDiff(
					ctx, sst, engine, now, start, end)
				require.NoError(t, err)

				require.NoError(t, fs.WriteFile(engine.Env(), "sst", sst, fs.UnspecifiedWriteCategory))
				require.NoError(t, engine.IngestLocalFiles(ctx, []string{"sst"}))

				expStats, err := storage.ComputeStats(ctx, engine, keys.LocalMax, roachpb.KeyMax, now)
				require.NoError(t, err)

				baseStats.Add(statsDelta)

				t.Logf("sst %s, local %s", sstT, localT)
				if !baseStats.Equal(expStats) {
					t.Log("test, expected")
					pretty.Ldiff(t, baseStats, expStats)
					t.Errorf("%s: diff(ms, expMS) nontrivial", tc.name)
				}
			})
		})
	}
}
