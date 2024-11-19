// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigstore

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestRandomized randomly sets/deletes span configs for arbitrary keyspans
// within some alphabet. For a test span, it then asserts that the config we
// retrieve is what we expect to find from the store. It also ensures that all
// ranges are non-overlapping, and that coalesced split-keys works as expected
// (adjacent configs, if identical, don't induce split points).
func TestRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()

	randutil.SeedForTests()
	ctx := context.Background()
	alphabet := "abcdefghijklmnopqrstuvwxyz"
	configs := "ABCDEF"
	ops := []string{"set", "del"}

	getRandomSpan := func() roachpb.Span {
		startIdx, endIdx := rand.Intn(len(alphabet)-1), 1+rand.Intn(len(alphabet)-1)
		if startIdx == endIdx {
			endIdx = (endIdx + 1) % len(alphabet)
		}
		if endIdx < startIdx {
			startIdx, endIdx = endIdx, startIdx
		}
		spanStr := fmt.Sprintf("[%s, %s)", string(alphabet[startIdx]), string(alphabet[endIdx]))
		sp := spanconfigtestutils.ParseSpan(t, spanStr)
		require.True(t, sp.Valid())
		return sp
	}

	getRandomConf := func() roachpb.SpanConfig {
		confStr := string(configs[rand.Intn(len(configs))])
		return spanconfigtestutils.ParseConfig(t, confStr)
	}

	getRandomOp := func() string {
		return ops[rand.Intn(2)]
	}

	getRandomUpdate := func() spanconfig.Update {
		sp, conf, op := getRandomSpan(), getRandomConf(), getRandomOp()
		switch op {
		case "set":
			addition, err := spanconfig.Addition(spanconfig.MakeTargetFromSpan(sp), conf)
			require.NoError(t, err)
			return addition
		case "del":
			del, err := spanconfig.Deletion(spanconfig.MakeTargetFromSpan(sp))
			require.NoError(t, err)
			return del
		default:
		}
		t.Fatalf("unexpected op: %s", op)
		return spanconfig.Update{}
	}

	getRandomUpdates := func() []spanconfig.Update {
		numUpdates := 1 + rand.Intn(3)
		updates := make([]spanconfig.Update, numUpdates)
		for {
			for i := 0; i < numUpdates; i++ {
				updates[i] = getRandomUpdate()
			}
			sort.Slice(updates, func(i, j int) bool {
				return updates[i].GetTarget().Less(updates[j].GetTarget())
			})
			invalid := false
			for i := 1; i < numUpdates; i++ {
				if updates[i].GetTarget().GetSpan().Overlaps(updates[i-1].GetTarget().GetSpan()) {
					invalid = true
				}
			}

			if invalid {
				continue // try again
			}

			rand.Shuffle(len(updates), func(i, j int) {
				updates[i], updates[j] = updates[j], updates[i]
			})
			return updates
		}
	}

	testSpan := spanconfigtestutils.ParseSpan(t, "[f,g)") // pin a single character span to test with
	var expConfig roachpb.SpanConfig
	var expFound bool

	const numOps = 5000
	store := newSpanConfigStore(cluster.MakeTestingClusterSettings(), &spanconfig.TestingKnobs{
		StoreIgnoreCoalesceAdjacentExceptions: true,
	})
	for i := 0; i < numOps; i++ {
		updates := getRandomUpdates()
		_, _, err := store.apply(ctx, updates...)
		require.NoError(t, err)
		for _, update := range updates {
			if testSpan.Overlaps(update.GetTarget().GetSpan()) {
				if update.Addition() {
					expConfig, expFound = update.GetConfig(), true
				} else {
					expConfig, expFound = roachpb.SpanConfig{}, false
				}
			}
		}
	}

	if !expFound {
		t.Run("deleted-entry-should-not-appear", func(t *testing.T) {
			_ = store.forEachOverlapping(testSpan,
				func(sp roachpb.Span, conf roachpb.SpanConfig) error {
					record, err := spanconfig.MakeRecord(spanconfig.MakeTargetFromSpan(sp), conf)
					require.NoError(t, err)
					t.Fatalf("found unexpected entry: %s",
						spanconfigtestutils.PrintSpanConfigRecord(t, record))
					return nil
				},
			)
		})
	} else {
		t.Run("should-observe-last-write", func(t *testing.T) {
			var foundSpanConfigPair spanConfigPair
			_ = store.forEachOverlapping(testSpan,
				func(sp roachpb.Span, conf roachpb.SpanConfig) error {
					if !foundSpanConfigPair.isEmpty() {
						record, err := spanconfig.MakeRecord(spanconfig.MakeTargetFromSpan(sp), conf)
						require.NoError(t, err)
						t.Fatalf("expected single overlapping entry, found second: %s",
							spanconfigtestutils.PrintSpanConfigRecord(t, record))
					}
					foundSpanConfigPair = spanConfigPair{
						span:   sp,
						config: conf,
					}

					// Check that the entry is exactly what we'd expect.
					gotSpan, gotConfig := sp, conf
					require.Truef(t, gotSpan.Contains(testSpan),
						"improper result: expected retrieved span (%s) to contain test span (%s)",
						spanconfigtestutils.PrintSpan(gotSpan), spanconfigtestutils.PrintSpan(testSpan))

					require.Truef(t, expConfig.Equal(gotConfig),
						"mismatched configs: expected %s, got %s",
						spanconfigtestutils.PrintSpanConfig(expConfig), spanconfigtestutils.PrintSpanConfig(gotConfig))

					return nil
				},
			)

			// Ensure that the config accessed through the StoreReader interface is
			// the same as above.
			storeReaderConfig, _, found := store.getSpanConfigForKey(ctx, roachpb.RKey(testSpan.Key))
			require.True(t, found)
			require.True(t, foundSpanConfigPair.config.Equal(storeReaderConfig))
		})
	}

	t.Run("entries-are-valid-and-non-overlapping", func(t *testing.T) {
		everythingSpan := spanconfigtestutils.ParseSpan(t, fmt.Sprintf("[%s,%s)",
			string(alphabet[0]), string(alphabet[len(alphabet)-1])))
		var lastOverlapping spanConfigPair
		require.NoError(t, store.forEachOverlapping(everythingSpan,
			func(sp roachpb.Span, conf roachpb.SpanConfig) error {
				// All spans are expected to be valid.
				require.True(t, sp.Valid(),
					"expected to only find valid spans, found %s",
					spanconfigtestutils.PrintSpan(sp),
				)

				if !lastOverlapping.isEmpty() {
					// Span configs are returned in strictly sorted order.
					require.True(t, lastOverlapping.span.Key.Compare(sp.Key) < 0,
						"expected to find spans in strictly sorted order, found %s then %s",
						spanconfigtestutils.PrintSpan(lastOverlapping.span), spanconfigtestutils.PrintSpan(sp))

					// Span configs must also be non-overlapping.
					require.Falsef(t, lastOverlapping.span.Overlaps(sp),
						"expected non-overlapping spans, found %s and %s",
						spanconfigtestutils.PrintSpan(lastOverlapping.span), spanconfigtestutils.PrintSpan(sp))
				}

				lastOverlapping = spanConfigPair{
					span:   sp,
					config: conf,
				}
				return nil
			},
		))
	})

	t.Run("split-key-properties", func(t *testing.T) {
		// Properties for computed split keys:
		//
		// (1) sp.ProperlyContains(ComputeSplitKey(sp))
		//     (i) When computing the set of all split keys over a given span, they
		//         should be sorted
		// (2) Configs at adjacent split keys must be non-identical
		// (3) Span config entries between two split keys 'a' and 'b' should have a
		//     config identical to the config at 'a'
		//     (i) The config at the last split key should be identical the last
		//         config that overlaps with the query span
		//     (ii) Properties (3) and (1) imply that the first split key should
		//          have a different config than the first config that overlaps with
		//         the query span if the first entry start key == query span start
		//         key.
		//
		// This subtest computes the set of all split keys over a randomly generated
		// span and asserts on all the properties above.
		querySpan := getRandomSpan()
		splitKeys := store.TestingSplitKeys(t,
			ctx,
			roachpb.RKey(querySpan.Key),
			roachpb.RKey(querySpan.EndKey),
		)

		numOverlappingWithQuerySp := 0
		var firstOverlappingWithQuerySp, lastOverlappingWithQuerySp spanConfigPair
		require.NoError(t, store.forEachOverlapping(querySpan,
			func(sp roachpb.Span, conf roachpb.SpanConfig) error {
				if numOverlappingWithQuerySp == 0 {
					firstOverlappingWithQuerySp = spanConfigPair{
						span:   sp,
						config: conf,
					}
				}
				numOverlappingWithQuerySp++
				lastOverlappingWithQuerySp = spanConfigPair{
					span:   sp,
					config: conf,
				}
				return nil
			},
		))

		var lastSplitKey roachpb.RKey
		var confAtLastSplitKey roachpb.SpanConfig
		for i, curSplitKey := range splitKeys {
			// Property (1).
			require.Truef(t, querySpan.ProperlyContainsKey(curSplitKey.AsRawKey()),
				"invalid split key %s (over span %s)", curSplitKey, querySpan)

			confAtCurSplitKey, _, found := store.getSpanConfigForKey(ctx, curSplitKey)
			require.True(t, found)

			if i == 0 {
				// Property (1.i).
				require.True(t, firstOverlappingWithQuerySp.span.Key.Compare(curSplitKey.AsRawKey()) <= 0,
					"expected to find %s sorted before %s",
					firstOverlappingWithQuerySp.span.Key, curSplitKey)

				// The config at the first split key must not be identical to the first
				// overlapping span's if the first overlapping span key is the same as the
				// query start key, property (3.ii).
				if firstOverlappingWithQuerySp.span.Key.Equal(querySpan.Key) {
					require.Falsef(t, confAtCurSplitKey.Equal(firstOverlappingWithQuerySp.config),
						"expected non-identical configs, found %s:%s and %s:%s",
						curSplitKey, spanconfigtestutils.PrintSpanConfig(confAtCurSplitKey),
						firstOverlappingWithQuerySp.span.Key, spanconfigtestutils.PrintSpanConfig(firstOverlappingWithQuerySp.config),
					)
				}
			} else {
				// Split keys are returned in strictly sorted order, property (1.i).
				require.True(t, lastSplitKey.Compare(curSplitKey) < 0,
					"expected to find split keys in strictly sorted order, found %s then %s",
					lastSplitKey, curSplitKey)

				// Adjacent split key configs must have non-identical configs, property
				// (2).
				require.Falsef(t, confAtLastSplitKey.Equal(confAtCurSplitKey),
					"expected non-identical configs, found %s:%s and %s:%s",
					lastSplitKey, spanconfigtestutils.PrintSpanConfig(confAtLastSplitKey),
					curSplitKey, spanconfigtestutils.PrintSpanConfig(confAtCurSplitKey),
				)

				// Span config entries between the split keys should be identical to the
				// config at the last split key, property (3).
				require.NoError(t, store.forEachOverlapping(roachpb.Span{
					Key:    lastSplitKey.AsRawKey(),
					EndKey: curSplitKey.AsRawKey(),
				}, func(sp roachpb.Span, conf roachpb.SpanConfig) error {
					require.Truef(t, confAtLastSplitKey.Equal(conf),
						"expected identical configs, found %s:%s and %s:%s",
						lastSplitKey, spanconfigtestutils.PrintSpanConfig(confAtLastSplitKey),
						sp.Key, spanconfigtestutils.PrintSpanConfig(conf),
					)
					return nil
				}))
			}

			lastSplitKey, confAtLastSplitKey = curSplitKey, confAtCurSplitKey
		}

		if len(splitKeys) != 0 {
			require.True(t, lastOverlappingWithQuerySp.span.Key.Compare(lastSplitKey.AsRawKey()) >= 0,
				"expected to find %s sorted after %s",
				lastOverlappingWithQuerySp.span.Key, lastSplitKey)

			// The config at the last split key must match the last overlapping
			// config, property (3.i).
			require.Truef(t, confAtLastSplitKey.Equal(lastOverlappingWithQuerySp.config),
				"expected identical configs, found %s:%s and %s:%s",
				lastSplitKey, spanconfigtestutils.PrintSpanConfig(confAtLastSplitKey),
				lastOverlappingWithQuerySp.span.Key, spanconfigtestutils.PrintSpanConfig(lastOverlappingWithQuerySp.config),
			)
		}
	})
}
