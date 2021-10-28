// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigtestutils

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// spanRe matches strings of the form "[start, end)", capturing both the "start"
// and "end" keys.
var spanRe = regexp.MustCompile(`^\[(\w+),\s??(\w+)\)$`)

// configRe matches a single word. It's a shorthand for declaring a unique
// config.
var configRe = regexp.MustCompile(`^(\w+)$`)

// ParseSpan is helper function that constructs a roachpb.Span from a string of
// the form "[start, end)".
func ParseSpan(t *testing.T, sp string) roachpb.Span {
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

// ParseConfig is helper function that constructs a roachpb.SpanConfig that's
// "tagged" with the given string (i.e. a constraint with the given string a
// required key).
func ParseConfig(t *testing.T, conf string) roachpb.SpanConfig {
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

// ParseSpanConfigEntry is helper function that constructs a
// roachpb.SpanConfigEntry from a string of the form [start,end]:config. See
// ParseSpan and ParseConfig above.
func ParseSpanConfigEntry(t *testing.T, conf string) roachpb.SpanConfigEntry {
	parts := strings.Split(conf, ":")
	if len(parts) != 2 {
		t.Fatalf("expected single %q separator", ":")
	}
	return roachpb.SpanConfigEntry{
		Span:   ParseSpan(t, parts[0]),
		Config: ParseConfig(t, parts[1]),
	}
}

// ParseKVAccessorGetArguments is a helper function that parses datadriven
// kvaccessor-get arguments into the relevant spans. The input is of the
// following form:
//
// 		span [a,e)
// 		span [a,b)
// 		span [b,c)
//
func ParseKVAccessorGetArguments(t *testing.T, input string) []roachpb.Span {
	var spans []roachpb.Span
	for _, line := range strings.Split(input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		const spanPrefix = "span "
		if !strings.HasPrefix(line, spanPrefix) {
			t.Fatalf("malformed line %q, expected to find spanPrefix %q", line, spanPrefix)
		}
		line = strings.TrimPrefix(line, spanPrefix)
		spans = append(spans, ParseSpan(t, line))
	}
	return spans
}

// ParseKVAccessorUpdateArguments is a helper function that parses datadriven
// kvaccessor-update arguments into the relevant spans. The input is of the
// following form:
//
// 		delete [c,e)
// 		upsert [c,d):C
// 		upsert [d,e):D
//
func ParseKVAccessorUpdateArguments(
	t *testing.T, input string,
) ([]roachpb.Span, []roachpb.SpanConfigEntry) {
	var toDelete []roachpb.Span
	var toUpsert []roachpb.SpanConfigEntry
	for _, line := range strings.Split(input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		const upsertPrefix, deletePrefix = "upsert ", "delete "
		switch {
		case strings.HasPrefix(line, deletePrefix):
			line = strings.TrimPrefix(line, line[:len(deletePrefix)])
			toDelete = append(toDelete, ParseSpan(t, line))
		case strings.HasPrefix(line, upsertPrefix):
			line = strings.TrimPrefix(line, line[:len(upsertPrefix)])
			toUpsert = append(toUpsert, ParseSpanConfigEntry(t, line))
		default:
			t.Fatalf("malformed line %q, expected to find prefix %q or %q",
				line, upsertPrefix, deletePrefix)
		}
	}
	return toDelete, toUpsert
}

// PrintSpan is a helper function that transforms roachpb.Span into a string of
// the form "[start,end)". The span is assumed to have been constructed by the
// ParseSpan helper above.
func PrintSpan(sp roachpb.Span) string {
	return fmt.Sprintf("[%s,%s)", string(sp.Key), string(sp.EndKey))
}

// PrintSpanConfig is a helper function that transforms roachpb.SpanConfig into
// a readable string. The span config is assumed to have been constructed by the
// ParseSpanConfig helper above.
func PrintSpanConfig(conf roachpb.SpanConfig) string {
	return conf.Constraints[0].Constraints[0].Key // see ParseConfig for what a "tagged" roachpb.SpanConfig translates to
}

// PrintSpanConfigEntry is a helper function that transforms
// roachpb.SpanConfigEntry into a string of the form "[start, end):config". The
// entry is assumed to either have been constructed using ParseSpanConfigEntry
// above, or the constituen span and config to have been constructed using the
// Parse{Span,Config} helpers above.
func PrintSpanConfigEntry(entry roachpb.SpanConfigEntry) string {
	return fmt.Sprintf("%s:%s", PrintSpan(entry.Span), PrintSpanConfig(entry.Config))
}
