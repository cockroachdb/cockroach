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

// TestingGetAllOverlapping is a testing only helper to retrieve the set of
// overlapping entries in sorted order.
func (s *Store) TestingGetAllOverlapping(
	_ context.Context, sp roachpb.Span,
) []roachpb.SpanConfigEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Iterate over all overlapping ranges and return corresponding span config
	// entries.
	var res []roachpb.SpanConfigEntry
	for _, overlapping := range s.mu.tree.Get(sp.AsRange()) {
		res = append(res, overlapping.(*storeEntry).SpanConfigEntry)
	}
	return res
}

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		store := New(spanconfigtestutils.ParseConfig(t, "FALLBACK"))
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var (
				spanStr, confStr, keyStr string
			)
			switch d.Cmd {
			case "set":
				d.ScanArgs(t, "span", &spanStr)
				d.ScanArgs(t, "conf", &confStr)
				span, config := spanconfigtestutils.ParseSpan(t, spanStr), spanconfigtestutils.ParseConfig(t, confStr)

				dryrun := d.HasArg("dryrun")
				deleted, added := store.Apply(ctx, spanconfig.Update{Span: span, Config: config}, dryrun)

				var b strings.Builder
				for _, sp := range deleted {
					b.WriteString(fmt.Sprintf("deleted %s\n", spanconfigtestutils.PrintSpan(sp)))
				}
				for _, ent := range added {
					b.WriteString(fmt.Sprintf("added %s\n", spanconfigtestutils.PrintSpanConfigEntry(ent)))
				}
				return b.String()

			case "delete":
				d.ScanArgs(t, "span", &spanStr)
				span := spanconfigtestutils.ParseSpan(t, spanStr)

				dryrun := d.HasArg("dryrun")
				deleted, added := store.Apply(ctx, spanconfig.Update{Span: span}, dryrun)

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
				entries := store.TestingGetAllOverlapping(ctx, span)
				var results []string
				for _, entry := range entries {
					results = append(results, spanconfigtestutils.PrintSpanConfigEntry(entry))
				}
				return strings.Join(results, "\n")

			default:
			}

			t.Fatalf("unknown command: %s", d.Cmd)
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

	testSpan := spanconfigtestutils.ParseSpan(t, "[f,g)") // pin a single character span to test with
	var expConfig roachpb.SpanConfig
	var expFound bool

	const numOps = 5000
	store := New(roachpb.TestingDefaultSpanConfig())
	for i := 0; i < numOps; i++ {
		sp, conf, op := genRandomSpan(), getRandomConf(), getRandomOp()
		switch op {
		case "set":
			store.Apply(ctx, spanconfig.Update{Span: sp, Config: conf}, false)
			if testSpan.Overlaps(sp) {
				expConfig, expFound = conf, true
			}
		case "del":
			store.Apply(ctx, spanconfig.Update{Span: sp}, false)
			if testSpan.Overlaps(sp) {
				expConfig, expFound = roachpb.SpanConfig{}, false
			}
		default:
			t.Fatalf("unexpected op: %s", op)
		}
	}

	overlappingConfigs := store.TestingGetAllOverlapping(ctx, testSpan)
	if !expFound {
		require.Len(t, overlappingConfigs, 0)
	} else {
		// Check to see that the set of overlapping span configs is exactly what
		// we expect.
		require.Len(t, overlappingConfigs, 1)
		gotSpan, gotConfig := overlappingConfigs[0].Span, overlappingConfigs[0].Config

		require.Truef(t, gotSpan.Contains(testSpan),
			"improper result: expected retrieved span (%s) to contain test span (%s)",
			spanconfigtestutils.PrintSpan(gotSpan), spanconfigtestutils.PrintSpan(testSpan))

		require.Truef(t, expConfig.Equal(gotConfig),
			"mismatched configs: expected %s, got %s",
			spanconfigtestutils.PrintSpanConfig(expConfig), spanconfigtestutils.PrintSpanConfig(gotConfig))

		// Ensure that the config accessed through the StoreReader interface is
		// the same as above.
		storeReaderConfig, err := store.GetSpanConfigForKey(ctx, roachpb.RKey(testSpan.Key))
		require.NoError(t, err)
		require.True(t, gotConfig.Equal(storeReaderConfig))
	}

	var last roachpb.SpanConfigEntry
	everythingSpan := spanconfigtestutils.ParseSpan(t, fmt.Sprintf("[%s,%s)",
		string(alphabet[0]), string(alphabet[len(alphabet)-1])))
	for i, cur := range store.TestingGetAllOverlapping(ctx, everythingSpan) {
		if i == 0 {
			last = cur
			continue
		}

		// Span configs are returned in strictly sorted order.
		require.True(t, last.Span.Key.Compare(cur.Span.Key) < 0,
			"expected to find spans in strictly sorted order, found %s then %s",
			spanconfigtestutils.PrintSpan(last.Span), spanconfigtestutils.PrintSpan(cur.Span))

		// Span configs must also be non-overlapping.
		require.Falsef(t, last.Span.Overlaps(cur.Span),
			"expected non-overlapping spans, found %s and %s",
			spanconfigtestutils.PrintSpan(last.Span), spanconfigtestutils.PrintSpan(cur.Span))
	}
}
