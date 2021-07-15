// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigstorage_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstorage"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// spanRe matches strings of the form "[start, end)", capturing both "start" and
// "end" key.
var spanRe = regexp.MustCompile("^\\[(\\w+),\\s??(\\w+)\\)$")

// confRe matches a single word.
var configRe = regexp.MustCompile("^(\\w+)$")

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
	return fmt.Sprintf("%s",
		conf.Constraints[0].Constraints[0].Key) // see parseConfig for what a "tagged" roachpb.SpanConfig translates to
}

// printSpanConfigEntry is a helper function that transforms
// roachpb.SpanConfigEntry into a string of the form "[start,end):config". The
// embedded span config is assumed to have been constructed by the parseConfig
// helper above.
func printSpanConfigEntry(entry roachpb.SpanConfigEntry) string {
	return fmt.Sprintf("%s:%s", printSpan(entry.Span), printSpanConfig(entry.Config))
}

func TestDatadriven(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		var storage spanconfig.Storage = spanconfigstorage.New()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var spanStr, confStr string
			switch d.Cmd {
			case "get":
				d.ScanArgs(t, "span", &spanStr)
				span := parseSpan(t, spanStr)
				entries := storage.Get(span)
				var results []string
				for _, entry := range entries {
					results = append(results, printSpanConfigEntry(entry))
				}
				return strings.Join(results, "\n")

			case "set":
				d.ScanArgs(t, "span", &spanStr)
				d.ScanArgs(t, "conf", &confStr)
				span, config := parseSpan(t, spanStr), parseConfig(t, confStr)
				storage.Set(span, config)
				return ""

			case "del":
				d.ScanArgs(t, "span", &spanStr)
				span := parseSpan(t, spanStr)
				storage.Delete(span)
				return ""

			default:
				return "unknown command"
			}
		})
	})
}

// XXX: Add a randomized testing variant. Randomly set configs for arbitrary
// key spans within some alphabet, capturing the operations in some in-memory op
// log, and then for all alphabets, ensure that the config we expect from the
// log is the same as what we find from the store. We would then also ensure
// that all ranges are non-overlapping.
