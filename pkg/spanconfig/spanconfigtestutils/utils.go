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
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
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

// ParseStoreApplyArguments is a helper function that parses datadriven
// store update arguments. The input is of the following form:
//
//      delete [c,e)
//      set [c,d):C
//      set [d,e):D
//
func ParseStoreApplyArguments(t *testing.T, input string) (updates []spanconfig.Update) {
	for _, line := range strings.Split(input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		const setPrefix, deletePrefix = "set ", "delete "
		switch {
		case strings.HasPrefix(line, deletePrefix):
			line = strings.TrimPrefix(line, line[:len(deletePrefix)])
			updates = append(updates, spanconfig.Update{Span: ParseSpan(t, line)})
		case strings.HasPrefix(line, setPrefix):
			line = strings.TrimPrefix(line, line[:len(setPrefix)])
			entry := ParseSpanConfigEntry(t, line)
			updates = append(updates, spanconfig.Update{Span: entry.Span, Config: entry.Config})
		default:
			t.Fatalf("malformed line %q, expected to find prefix %q or %q",
				line, setPrefix, deletePrefix)
		}
	}
	return updates
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

// PrintSpanConfigDiffedAgainstDefaults is a helper function that diffs the given
// config against RANGE {DEFAULT, SYSTEM} and returns a string for the
// mismatched fields. If there are none, "range {default,system}" is returned.
func PrintSpanConfigDiffedAgainstDefaults(conf roachpb.SpanConfig) string {
	if conf.Equal(roachpb.TestingDefaultSpanConfig()) {
		return "range default"
	}
	if conf.Equal(roachpb.TestingSystemSpanConfig()) {
		return "range system"
	}

	defaultConf := roachpb.TestingDefaultSpanConfig()
	var diffs []string
	if conf.RangeMaxBytes != defaultConf.RangeMaxBytes {
		diffs = append(diffs, fmt.Sprintf("range_max_bytes=%d", conf.RangeMaxBytes))
	}
	if conf.RangeMinBytes != defaultConf.RangeMinBytes {
		diffs = append(diffs, fmt.Sprintf("range_min_bytes=%d", conf.RangeMinBytes))
	}
	if conf.GCPolicy.TTLSeconds != defaultConf.GCPolicy.TTLSeconds {
		diffs = append(diffs, fmt.Sprintf("ttl_seconds=%d", conf.GCPolicy.TTLSeconds))
	}
	if conf.GlobalReads != defaultConf.GlobalReads {
		diffs = append(diffs, fmt.Sprintf("global_reads=%v", conf.GlobalReads))
	}
	if conf.NumReplicas != defaultConf.NumReplicas {
		diffs = append(diffs, fmt.Sprintf("num_replicas=%d", conf.NumReplicas))
	}
	if conf.NumVoters != defaultConf.NumVoters {
		diffs = append(diffs, fmt.Sprintf("num_voters=%d", conf.NumVoters))
	}
	if !reflect.DeepEqual(conf.Constraints, defaultConf.Constraints) {
		diffs = append(diffs, fmt.Sprintf("constraints=%v", conf.Constraints))
	}
	if !reflect.DeepEqual(conf.VoterConstraints, defaultConf.VoterConstraints) {
		diffs = append(diffs, fmt.Sprintf("voter_constraints=%v", conf.VoterConstraints))
	}
	if !reflect.DeepEqual(conf.LeasePreferences, defaultConf.LeasePreferences) {
		diffs = append(diffs, fmt.Sprintf("lease_preferences=%v", conf.VoterConstraints))
	}

	return strings.Join(diffs, " ")
}
