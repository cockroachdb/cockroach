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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestingApplyInternal exports an internal method for testing purposes.
func (s *Store) TestingApplyInternal(
	_ context.Context, dryrun bool, updates ...spanconfig.Update,
) (deleted []roachpb.Span, added []roachpb.SpanConfigEntry, err error) {
	return s.applyInternal(dryrun, updates...)
}

// TestDataDriven runs datadriven tests against the Store interface.
// The syntax is as follows:
//
// 		apply
// 		delete [a,c)
// 		set [c,h):X
// 		----
// 		deleted [b,d)
// 		deleted [e,g)
// 		added [c,h):X
//
// 		get key=b
// 		----
// 		conf=A # or conf=FALLBACK if the key is not present
//
// 		needs-split span=[b,h)
// 		----
// 		true
//
// 		compute-split span=[b,h)
// 		----
// 		key=c
//
// 		overlapping span=[b,h)
// 		----
// 		[b,d):A
// 		[d,f):B
// 		[f,h):A
//
//
// Text of the form [a,b) and [a,b):C correspond to spans and span config
// entries; see spanconfigtestutils.Parse{Span,Config,SpanConfigEntry} for more
// details.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		store := New(spanconfigtestutils.ParseConfig(t, "FALLBACK"))
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var spanStr, keyStr string
			switch d.Cmd {
			case "apply":
				updates := spanconfigtestutils.ParseStoreApplyArguments(t, d.Input)
				dryrun := d.HasArg("dryrun")
				deleted, added, err := store.TestingApplyInternal(ctx, dryrun, updates...)
				if err != nil {
					return fmt.Sprintf("err: %v", err)
				}

				sort.Sort(roachpb.Spans(deleted))
				sort.Slice(added, func(i, j int) bool {
					return added[i].Span.Key.Compare(added[j].Span.Key) < 0
				})

				var b strings.Builder
				for _, sp := range deleted {
					b.WriteString(fmt.Sprintf("deleted %s\n", spanconfigtestutils.PrintSpan(sp)))
				}
				for _, ent := range added {
					b.WriteString(fmt.Sprintf("added %s\n", spanconfigtestutils.PrintSpanConfigEntry(ent)))
				}
				return b.String()

			case "get":
				d.ScanArgs(t, "key", &keyStr)
				config, err := store.GetSpanConfigForKey(ctx, roachpb.RKey(keyStr))
				require.NoError(t, err)
				return fmt.Sprintf("conf=%s", spanconfigtestutils.PrintSpanConfig(config))

			case "needs-split":
				d.ScanArgs(t, "span", &spanStr)
				span := spanconfigtestutils.ParseSpan(t, spanStr)
				start, end := roachpb.RKey(span.Key), roachpb.RKey(span.EndKey)
				result := store.NeedsSplit(ctx, start, end)
				return fmt.Sprintf("%t", result)

			case "compute-split":
				d.ScanArgs(t, "span", &spanStr)
				span := spanconfigtestutils.ParseSpan(t, spanStr)
				start, end := roachpb.RKey(span.Key), roachpb.RKey(span.EndKey)
				splitKey := store.ComputeSplitKey(ctx, start, end)
				return fmt.Sprintf("key=%s", string(splitKey))

			case "overlapping":
				d.ScanArgs(t, "span", &spanStr)
				span := spanconfigtestutils.ParseSpan(t, spanStr)

				var results []string
				_ = store.ForEachOverlapping(ctx, span,
					func(entry roachpb.SpanConfigEntry) error {
						results = append(results, spanconfigtestutils.PrintSpanConfigEntry(entry))
						return nil
					},
				)
				return strings.Join(results, "\n")

			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}

			return ""
		})
	})
}

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
		confStr := fmt.Sprintf("conf_%s", string(configs[rand.Intn(len(configs))]))
		return spanconfigtestutils.ParseConfig(t, confStr)
	}

	getRandomOp := func() string {
		return ops[rand.Intn(2)]
	}

	getRandomUpdate := func() spanconfig.Update {
		sp, conf, op := genRandomSpan(), getRandomConf(), getRandomOp()
		switch op {
		case "set":
			return spanconfig.Addition(sp, conf)
		case "del":
			return spanconfig.Deletion(sp)
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
				return updates[i].Span.Key.Compare(updates[j].Span.Key) < 0
			})
			invalid := false
			for i := 1; i < numUpdates; i++ {
				if updates[i].Span.Overlaps(updates[i-1].Span) {
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
	store := New(roachpb.TestingDefaultSpanConfig())
	for i := 0; i < numOps; i++ {
		updates := getRandomUpdates()
		store.Apply(ctx, false /* dryrun */, updates...)
		for _, update := range updates {
			if testSpan.Overlaps(update.Span) {
				if update.Addition() {
					expConfig, expFound = update.Config, true
				} else {
					expConfig, expFound = roachpb.SpanConfig{}, false
				}
			}
		}
	}

	if !expFound {
		_ = store.ForEachOverlapping(ctx, testSpan,
			func(entry roachpb.SpanConfigEntry) error {
				t.Fatalf("found unexpected entry: %s",
					spanconfigtestutils.PrintSpanConfigEntry(entry))
				return nil
			},
		)
	} else {
		var foundEntry roachpb.SpanConfigEntry
		_ = store.ForEachOverlapping(ctx, testSpan,
			func(entry roachpb.SpanConfigEntry) error {
				if !foundEntry.Empty() {
					t.Fatalf("expected single overlapping entry, found second: %s",
						spanconfigtestutils.PrintSpanConfigEntry(entry))
				}
				foundEntry = entry

				// Check that the entry is exactly what we'd expect.
				gotSpan, gotConfig := entry.Span, entry.Config
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
		storeReaderConfig, err := store.GetSpanConfigForKey(ctx, roachpb.RKey(testSpan.Key))
		require.NoError(t, err)
		require.True(t, foundEntry.Config.Equal(storeReaderConfig))
	}

	everythingSpan := spanconfigtestutils.ParseSpan(t, fmt.Sprintf("[%s,%s)",
		string(alphabet[0]), string(alphabet[len(alphabet)-1])))

	var last roachpb.SpanConfigEntry
	_ = store.ForEachOverlapping(ctx, everythingSpan,
		func(cur roachpb.SpanConfigEntry) error {
			// All spans are expected to be valid.
			require.True(t, cur.Span.Valid(),
				"expected to only find valid spans, found %s",
				spanconfigtestutils.PrintSpan(cur.Span),
			)

			if last.Empty() {
				last = cur
				return nil
			}

			// Span configs are returned in strictly sorted order.
			require.True(t, last.Span.Key.Compare(cur.Span.Key) < 0,
				"expected to find spans in strictly sorted order, found %s then %s",
				spanconfigtestutils.PrintSpan(last.Span), spanconfigtestutils.PrintSpan(cur.Span))

			// Span configs must also be non-overlapping.
			require.Falsef(t, last.Span.Overlaps(cur.Span),
				"expected non-overlapping spans, found %s and %s",
				spanconfigtestutils.PrintSpan(last.Span), spanconfigtestutils.PrintSpan(cur.Span))

			return nil
		},
	)
}

// TestStoreClone verifies that a cloned store contains the same contents as the
// original.
func TestStoreClone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	everything := spanconfigtestutils.ParseSpan(t, "[a, z)")
	updates := []spanconfig.Update{
		spanconfig.Addition(spanconfigtestutils.ParseSpan(t, "[a, b)"), spanconfigtestutils.ParseConfig(t, "A")),
		spanconfig.Addition(spanconfigtestutils.ParseSpan(t, "[c, d)"), spanconfigtestutils.ParseConfig(t, "C")),
		spanconfig.Addition(spanconfigtestutils.ParseSpan(t, "[e, f)"), spanconfigtestutils.ParseConfig(t, "E")),
	}

	original := New(roachpb.TestingDefaultSpanConfig())
	original.Apply(ctx, false, updates...)
	clone := original.Copy(ctx)

	var originalEntries, clonedEntries []roachpb.SpanConfigEntry
	_ = original.ForEachOverlapping(ctx, everything,
		func(entry roachpb.SpanConfigEntry) error {
			originalEntries = append(originalEntries, entry)
			return nil
		},
	)

	_ = clone.ForEachOverlapping(ctx, everything,
		func(entry roachpb.SpanConfigEntry) error {
			clonedEntries = append(clonedEntries, entry)
			return nil
		},
	)

	require.Equal(t, len(originalEntries), len(clonedEntries))
	for i := 0; i < len(originalEntries); i++ {
		require.True(t, originalEntries[i].Equal(clonedEntries[i]))
	}
}
