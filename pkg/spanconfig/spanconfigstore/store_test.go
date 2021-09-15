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
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// spanRe matches strings of the form "[start, end)", capturing both the "start"
// and "end" keys.
var spanRe = regexp.MustCompile(`^\[(\w+),\s??(\w+)\)$`)

// configRe matches a single word. It's a shorthand for declaring a unique
// config.
var configRe = regexp.MustCompile(`^(\w+)$`)

func TestSpanRe(t *testing.T) {
	for _, tc := range []struct {
		input            string
		expMatch         bool
		expStart, expEnd string
	}{
		{"[a, b)", true, "a", "b"},
		{"[acd, bfg)", true, "acd", "bfg"}, // multi character keys allowed
		{"[a,b)", true, "a", "b"},          // separating space is optional
		{"[ a,b) ", false, "", ""},         // extraneous spaces disallowed
		{"[a,b ) ", false, "", ""},         // extraneous spaces disallowed
		{"[a,, b)", false, "", ""},         // only single comma allowed
		{" [a, b)", false, "", ""},         // need to start with '['
		{"[a,b)x", false, "", ""},          // need to end with ')'
	} {
		require.Equalf(t, tc.expMatch, spanRe.MatchString(tc.input), "input = %s", tc.input)
		if !tc.expMatch {
			continue
		}

		matches := spanRe.FindStringSubmatch(tc.input)
		require.Len(t, matches, 3)
		start, end := matches[1], matches[2]
		require.Equal(t, tc.expStart, start)
		require.Equal(t, tc.expEnd, end)
	}
}

// parseSpan is helper function that constructs a roachpb.Span from a string of
// the form "[start, end)".
func parseSpan(t *testing.T, sp string) roachpb.Span {
	if !spanRe.MatchString(sp) {
		t.Fatalf("expected %s to match span regex", sp)
	}

	matches := spanRe.FindStringSubmatch(sp)
	start, end := matches[1], matches[2]
	return roachpb.Span{
		Key:    roachpb.Key(start),
		EndKey: roachpb.Key(end),
	}
}

// parseConfig is helper function that constructs a roachpb.SpanConfig that's
// "tagged" with the given string (i.e. a constraint with the given string a
// required key).
func parseConfig(t *testing.T, conf string) roachpb.SpanConfig {
	if !configRe.MatchString(conf) {
		t.Fatalf("expected %s to match config regex", conf)
	}
	return roachpb.SpanConfig{
		Constraints: []roachpb.ConstraintsConjunction{
			{
				Constraints: []roachpb.Constraint{
					{
						Key: conf,
					},
				},
			},
		},
	}
}

// printSpan is a helper function that transforms roachpb.Span into a string of
// the form "[start,end)". The span is assumed to have been constructed by the
// parseSpan helper above.
func printSpan(sp roachpb.Span) string {
	return fmt.Sprintf("[%s,%s)", string(sp.Key), string(sp.EndKey))
}

// printSpanConfig is a helper function that transforms roachpb.SpanConfig into
// a readable string. The span config is assumed to have been constructed by the
// parseSpanConfig helper above.
func printSpanConfig(conf roachpb.SpanConfig) string {
	return conf.Constraints[0].Constraints[0].Key // see parseConfig for what a "tagged" roachpb.SpanConfig translates to
}

// printSpanConfigEntry is a helper function that transforms
// roachpb.SpanConfigEntry into a string of the form "[start, end):config". The
// span and config are expected to have been constructed using the
// parse{Span,Config} helpers above.
func printSpanConfigEntry(entry roachpb.SpanConfigEntry) string {
	return fmt.Sprintf("%s:%s", printSpan(entry.Span), printSpanConfig(entry.Config))
}

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

func TestDatadriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		store := New(parseConfig(t, "FALLBACK"))
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var (
				spanStr, confStr, keyStr string
			)
			switch d.Cmd {
			case "set":
				d.ScanArgs(t, "span", &spanStr)
				d.ScanArgs(t, "conf", &confStr)
				span, config := parseSpan(t, spanStr), parseConfig(t, confStr)

				dryrun := d.HasArg("dryrun")
				deleted, added := store.Apply(ctx, spanconfig.Update{Span: span, Config: config}, dryrun)

				var b strings.Builder
				for _, sp := range deleted {
					b.WriteString(fmt.Sprintf("deleted %s\n", printSpan(sp)))
				}
				for _, ent := range added {
					b.WriteString(fmt.Sprintf("added %s\n", printSpanConfigEntry(ent)))
				}
				return b.String()

			case "delete":
				d.ScanArgs(t, "span", &spanStr)
				span := parseSpan(t, spanStr)

				dryrun := d.HasArg("dryrun")
				deleted, added := store.Apply(ctx, spanconfig.Update{Span: span}, dryrun)

				var b strings.Builder
				for _, sp := range deleted {
					b.WriteString(fmt.Sprintf("deleted %s\n", printSpan(sp)))
				}
				for _, ent := range added {
					b.WriteString(fmt.Sprintf("added %s\n", printSpanConfigEntry(ent)))
				}
				return b.String()

			case "get":
				d.ScanArgs(t, "key", &keyStr)
				config, err := store.GetSpanConfigForKey(ctx, roachpb.RKey(keyStr))
				require.NoError(t, err)
				return fmt.Sprintf("conf=%s", printSpanConfig(config))

			case "needs-split":
				d.ScanArgs(t, "span", &spanStr)
				span := parseSpan(t, spanStr)
				start, end := roachpb.RKey(span.Key), roachpb.RKey(span.EndKey)
				result := store.NeedsSplit(ctx, start, end)
				return fmt.Sprintf("%t", result)

			case "compute-split":
				d.ScanArgs(t, "span", &spanStr)
				span := parseSpan(t, spanStr)
				start, end := roachpb.RKey(span.Key), roachpb.RKey(span.EndKey)
				splitKey := store.ComputeSplitKey(ctx, start, end)
				return fmt.Sprintf("key=%s", string(splitKey))

			case "overlapping":
				d.ScanArgs(t, "span", &spanStr)
				span := parseSpan(t, spanStr)
				entries := store.TestingGetAllOverlapping(ctx, span)
				var results []string
				for _, entry := range entries {
					results = append(results, printSpanConfigEntry(entry))
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
		sp := parseSpan(t, spanStr)
		require.True(t, sp.Valid())
		return sp
	}

	getRandomConf := func() roachpb.SpanConfig {
		confStr := fmt.Sprintf("conf_%s", string(configs[rand.Intn(len(configs))]))
		return parseConfig(t, confStr)
	}

	getRandomOp := func() string {
		return ops[rand.Intn(2)]
	}

	testSpan := parseSpan(t, "[f,g)") // pin a single character span to test with
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
			printSpan(gotSpan), printSpan(testSpan))

		require.Truef(t, expConfig.Equal(gotConfig),
			"mismatched configs: expected %s, got %s",
			printSpanConfig(expConfig), printSpanConfig(gotConfig))

		// Ensure that the config accessed through the StoreReader interface is
		// the same as above.
		storeReaderConfig, err := store.GetSpanConfigForKey(ctx, roachpb.RKey(testSpan.Key))
		require.NoError(t, err)
		require.True(t, gotConfig.Equal(storeReaderConfig))
	}

	var last roachpb.SpanConfigEntry
	everythingSpan := parseSpan(t, fmt.Sprintf("[%s,%s)",
		string(alphabet[0]), string(alphabet[len(alphabet)-1])))
	for i, cur := range store.TestingGetAllOverlapping(ctx, everythingSpan) {
		if i == 0 {
			last = cur
			continue
		}

		// Span configs are returned in strictly sorted order.
		require.True(t, last.Span.Key.Compare(cur.Span.Key) < 0,
			"expected to find spans in strictly sorted order, found %s then %s",
			printSpan(last.Span), printSpan(cur.Span))

		// Span configs must also be non-overlapping.
		require.Falsef(t, last.Span.Overlaps(cur.Span),
			"expected non-overlapping spans, found %s and %s",
			printSpan(last.Span), printSpan(cur.Span))
	}
}
