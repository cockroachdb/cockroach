// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigstore

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestRandomized randomly sets/deletes span configs for arbitrary keyspans
// within some alphabet. For a test span, it then asserts that the config we
// retrieve is what we expect to find from the store. It also ensures that all
// ranges are non-overlapping.
func TestRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()

	randutil.SeedForTests()
	ctx := context.Background()
	alphabet := "abcdefghijklmnopqrstuvwxyz"
	configs := "ABCDEF"
	ops := []string{"set", "del"}

	genRandomSpan := func() roachpb.Span {
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
		sp, conf, op := genRandomSpan(), getRandomConf(), getRandomOp()
		switch op {
		case "set":
			return spanconfig.Addition(spanconfig.MakeTargetFromSpan(sp), conf)
		case "del":
			return spanconfig.Deletion(spanconfig.MakeTargetFromSpan(sp))
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
				return updates[i].Target.Less(updates[j].Target)
			})
			invalid := false
			for i := 1; i < numUpdates; i++ {
				if updates[i].Target.GetSpan().Overlaps(updates[i-1].Target.GetSpan()) {
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
	store := newSpanConfigStore()
	for i := 0; i < numOps; i++ {
		updates := getRandomUpdates()
		_, _, err := store.apply(false /* dryrun */, updates...)
		require.NoError(t, err)
		for _, update := range updates {
			if testSpan.Overlaps(update.Target.GetSpan()) {
				if update.Addition() {
					expConfig, expFound = update.Config, true
				} else {
					expConfig, expFound = roachpb.SpanConfig{}, false
				}
			}
		}
	}

	if !expFound {
		_ = store.forEachOverlapping(testSpan,
			func(entry spanConfigEntry) error {
				t.Fatalf("found unexpected entry: %s",
					spanconfigtestutils.PrintSpanConfigRecord(t, spanconfig.Record{
						Target: spanconfig.MakeTargetFromSpan(entry.span),
						Config: entry.config,
					}))
				return nil
			},
		)
	} else {
		var foundEntry spanConfigEntry
		_ = store.forEachOverlapping(testSpan,
			func(entry spanConfigEntry) error {
				if !foundEntry.isEmpty() {
					t.Fatalf("expected single overlapping entry, found second: %s",
						spanconfigtestutils.PrintSpanConfigRecord(t, spanconfig.Record{
							Target: spanconfig.MakeTargetFromSpan(entry.span),
							Config: entry.config,
						}))
				}
				foundEntry = entry

				// Check that the entry is exactly what we'd expect.
				gotSpan, gotConfig := entry.span, entry.config
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
		storeReaderConfig, found, err := store.getSpanConfigForKey(ctx, roachpb.RKey(testSpan.Key))
		require.NoError(t, err)
		require.True(t, found)
		require.True(t, foundEntry.config.Equal(storeReaderConfig))
	}

	everythingSpan := spanconfigtestutils.ParseSpan(t, fmt.Sprintf("[%s,%s)",
		string(alphabet[0]), string(alphabet[len(alphabet)-1])))

	var last spanConfigEntry
	_ = store.forEachOverlapping(everythingSpan,
		func(cur spanConfigEntry) error {
			// All spans are expected to be valid.
			require.True(t, cur.span.Valid(),
				"expected to only find valid spans, found %s",
				spanconfigtestutils.PrintSpan(cur.span),
			)

			if last.isEmpty() {
				last = cur
				return nil
			}

			// Span configs are returned in strictly sorted order.
			require.True(t, last.span.Key.Compare(cur.span.Key) < 0,
				"expected to find spans in strictly sorted order, found %s then %s",
				spanconfigtestutils.PrintSpan(last.span), spanconfigtestutils.PrintSpan(cur.span))

			// Span configs must also be non-overlapping.
			require.Falsef(t, last.span.Overlaps(cur.span),
				"expected non-overlapping spans, found %s and %s",
				spanconfigtestutils.PrintSpan(last.span), spanconfigtestutils.PrintSpan(cur.span))

			return nil
		},
	)
}
