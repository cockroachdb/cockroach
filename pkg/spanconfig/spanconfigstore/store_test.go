// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigstore_test

import (
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// spanRe matches strings of the form "[start, end)", capturing both "start" and
// "end" key.
var spanRe = regexp.MustCompile(`^\[(\w+),\s??(\w+)\)$`)

// confRe matches a single word.
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

// parseSpan is helper function that constructs a roachpb.Span from a string of the
// form "[start, end)".
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

// parseConfig is helper function that constructs a roachpb.SpanConfig with
// "tagged" with the given string.
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
// roachpb.SpanConfigEntry into a string of the form "[start,end):config". The
// embedded span config is assumed to have been constructed by the parseConfig
// helper above.
func printSpanConfigEntry(entry roachpb.SpanConfigEntry) string {
	return fmt.Sprintf("%s:%s", printSpan(entry.Span), printSpanConfig(entry.Config))
}

func TestDatadriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		storage := spanconfigstore.New()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var spanStr, confStr string
			switch d.Cmd {
			case "get":
				d.ScanArgs(t, "span", &spanStr)
				span := parseSpan(t, spanStr)
				entries := storage.GetConfigsForSpan(span)
				var results []string
				for _, entry := range entries {
					results = append(results, printSpanConfigEntry(entry))
				}
				return strings.Join(results, "\n")

			case "set":
				d.ScanArgs(t, "span", &spanStr)
				d.ScanArgs(t, "conf", &confStr)
				span, config := parseSpan(t, spanStr), parseConfig(t, confStr)
				storage.SetSpanConfig(span, config)
				return ""

			case "del":
				d.ScanArgs(t, "span", &spanStr)
				span := parseSpan(t, spanStr)
				storage.SetSpanConfig(span, roachpb.SpanConfig{})
				return ""

			default:
				return "unknown command"
			}
		})
	})
}

// TestRandomized randomly sets/deletes span configs for arbitrary keyspans
// within some alphabet. For a test span, it then asserts that the config we
// retrieve is what we expect to find from the store. It also ensures that all
// ranges are non-overlapping, and that adjacent ranges have different span
// configs.
func TestRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()

	randutil.SeedForTests()

	storage := spanconfigstore.New()
	alphabet := "abcdefghijklmnopqrstuvwxyz"
	configs := "ABCDEF"
	ops := []string{"set", "del"}

	var expConfig roachpb.SpanConfig
	var expFound bool

	getRandomSpan := func() roachpb.Span {
		for {
			startIdx, endIdx := rand.Intn(len(alphabet)-1), 1+rand.Intn(len(alphabet)-1)
			if endIdx < startIdx {
				startIdx, endIdx = endIdx, startIdx
			}
			spanStr := fmt.Sprintf("[%s, %s)", string(alphabet[startIdx]), string(alphabet[endIdx]))
			if sp := parseSpan(t, spanStr); sp.Valid() {
				return sp
			}
		}
	}

	getRandomConf := func() roachpb.SpanConfig {
		confStr := fmt.Sprintf("conf%s", string(configs[rand.Intn(len(configs))]))
		return parseConfig(t, confStr)
	}

	const numOps = 5000
	testSpan := parseSpan(t, "[f,g)") // pin a single character span to test with.
	for i := 0; i < numOps; i++ {
		sp, conf := getRandomSpan(), getRandomConf()
		op := ops[rand.Intn(2)]

		switch op {
		case "set":
			t.Logf("set sp=%s conf=%s", printSpan(sp), printSpanConfig(conf))
			storage.SetSpanConfig(sp, conf)
			if testSpan.Overlaps(sp) {
				expConfig, expFound = conf, true
			}
		case "del":
			t.Logf("del sp=%s", printSpan(sp))
			storage.SetSpanConfig(sp, roachpb.SpanConfig{})
			if testSpan.Overlaps(sp) {
				expConfig, expFound = roachpb.SpanConfig{}, false
			}
		default:
			t.Fatalf("unexpected op: %s", op)
		}
	}

	gotConfigs := storage.GetConfigsForSpan(testSpan)
	if !expFound {
		require.Len(t, gotConfigs, 0)
	} else {
		require.Len(t, gotConfigs, 1)
		gotSpan, gotConfig := gotConfigs[0].Span, gotConfigs[0].Config
		require.Truef(t, gotSpan.Contains(testSpan), "improper result: expected got-sp=%s to contain test-sp=%s",
			printSpan(gotSpan), printSpan(testSpan))
		require.Truef(t, expConfig.Equal(gotConfig), "mismatched configs: expected=%s got=%s",
			printSpanConfig(expConfig), printSpanConfig(gotConfig))
	}

	var last roachpb.SpanConfigEntry
	for i, cur := range storage.GetConfigsForSpan(parseSpan(t, "[a,z)")) {
		if i == 0 {
			last = cur
			continue
		}

		// Span configs are returned in strictly sorted order.
		require.True(t, last.Span.Key.Compare(cur.Span.Key) < 0,
			"expected to find spans in strictly sorted order, found %s then %s",
			printSpan(last.Span), printSpan(cur.Span))

		// All span configs must also be non-overlapping.
		require.Falsef(t, last.Span.Overlaps(cur.Span),
			"expected non-overlapping spans, found %s and %s",
			printSpan(last.Span), printSpan(cur.Span))

		// If two span configs are found to adjacent to one another, they must
		// have differing configs.
		if last.Span.EndKey.Equal(cur.Span.Key) {
			require.Falsef(t, last.Config.Equal(cur.Config),
				"expected adjacent spans %s and %s to have different configs, found %s for both",
				printSpan(last.Span), printSpan(cur.Span), printSpanConfig(cur.Config))
		}
	}
}
