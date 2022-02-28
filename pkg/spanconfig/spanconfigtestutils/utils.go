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
	"context"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// spanRe matches strings of the form "[start, end)", capturing both the "start"
// and "end" keys.
var spanRe = regexp.MustCompile(`^\[(\w+),\s??(\w+)\)$`)

// systemTargetRe matches strings of the form
// "{entire-keyspace|source=<id>,(target=<id>|all-tenant-keyspace-targets-set)}".
var systemTargetRe = regexp.MustCompile(
	`^{(entire-keyspace)|(source=(\d*),\s??((target=(\d*))|all-tenant-keyspace-targets-set))}$`,
)

// configRe matches either FALLBACK (for readability) or a single letter. It's a
// shorthand for declaring a unique tagged config.
var configRe = regexp.MustCompile(`^(FALLBACK)|(^\w)$`)

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

// parseSystemTarget is a helepr function that constructs a
// spanconfig.SystemTarget from a string of the form {source=<id>,target=<id>}
func parseSystemTarget(t *testing.T, systemTarget string) spanconfig.SystemTarget {
	if !systemTargetRe.MatchString(systemTarget) {
		t.Fatalf("expected %s to match system target regex", systemTargetRe)
	}
	matches := systemTargetRe.FindStringSubmatch(systemTarget)

	if matches[1] == "entire-keyspace" {
		return spanconfig.MakeEntireKeyspaceTarget()
	}

	sourceID, err := strconv.Atoi(matches[3])
	require.NoError(t, err)
	if matches[4] == "all-tenant-keyspace-targets-set" {
		return spanconfig.MakeAllTenantKeyspaceTargetsSet(roachpb.MakeTenantID(uint64(sourceID)))
	}
	targetID, err := strconv.Atoi(matches[6])
	require.NoError(t, err)
	target, err := spanconfig.MakeTenantKeyspaceTarget(
		roachpb.MakeTenantID(uint64(sourceID)), roachpb.MakeTenantID(uint64(targetID)),
	)
	require.NoError(t, err)
	return target
}

// ParseTarget is a helper function that constructs a spanconfig.Target from a
// string that conforms to spanRe.
func ParseTarget(t *testing.T, target string) spanconfig.Target {
	switch {
	case spanRe.MatchString(target):
		return spanconfig.MakeTargetFromSpan(ParseSpan(t, target))
	case systemTargetRe.MatchString(target):
		return spanconfig.MakeTargetFromSystemTarget(parseSystemTarget(t, target))
	default:
		t.Fatalf("expected %s to match span or system target regex", target)
	}
	panic("unreachable")
}

// ParseConfig is helper function that constructs a roachpb.SpanConfig that's
// "tagged" with the given string (i.e. a constraint with the given string a
// required key).
func ParseConfig(t *testing.T, conf string) roachpb.SpanConfig {
	if !configRe.MatchString(conf) {
		t.Fatalf("expected %s to match config regex", conf)
	}
	matches := configRe.FindStringSubmatch(conf)

	var ts int64
	if matches[1] == "FALLBACK" {
		ts = -1
	} else {
		ts = int64(matches[2][0])
	}
	return roachpb.SpanConfig{
		GCPolicy: roachpb.GCPolicy{
			ProtectionPolicies: []roachpb.ProtectionPolicy{
				{
					ProtectedTimestamp: hlc.Timestamp{
						WallTime: ts,
					},
				},
			},
		},
	}
}

// ParseSpanConfigRecord is helper function that constructs a
// spanconfig.Target from a string of the form target:config. See
// ParseTarget and ParseConfig above.
func ParseSpanConfigRecord(t *testing.T, conf string) spanconfig.Record {
	parts := strings.Split(conf, ":")
	if len(parts) != 2 {
		t.Fatalf("expected single %q separator", ":")
	}
	return spanconfig.Record{
		Target: ParseTarget(t, parts[0]),
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
//		system-target {source=1,target=1}
//		system-target {source=20,target=20}
//		system-target {source=1,target=20}
//
func ParseKVAccessorGetArguments(t *testing.T, input string) []spanconfig.Target {
	var targets []spanconfig.Target
	for _, line := range strings.Split(input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		const spanPrefix = "span "
		const systemTargetPrefix = "system-target "
		switch {
		case strings.HasPrefix(line, spanPrefix):
			line = strings.TrimPrefix(line, spanPrefix)
		case strings.HasPrefix(line, systemTargetPrefix):
			line = strings.TrimPrefix(line, systemTargetPrefix)
		default:
			t.Fatalf(
				"malformed line %q, expected to find %q or %q prefix",
				line,
				spanPrefix,
				systemTargetPrefix,
			)
		}
		targets = append(targets, ParseTarget(t, line))
	}
	return targets
}

// ParseKVAccessorUpdateArguments is a helper function that parses datadriven
// kvaccessor-update arguments into the relevant targets and records. The input
// is of the following form:
//
// 		delete [c,e)
// 		upsert [c,d):C
// 		upsert [d,e):D
// 		delete {source=1,target=1}
// 		delete {source=1,target=20}
// 		upsert {source=1,target=1}:A
// 		delete {source=1,target=20}:D
//
func ParseKVAccessorUpdateArguments(
	t *testing.T, input string,
) ([]spanconfig.Target, []spanconfig.Record) {
	var toDelete []spanconfig.Target
	var toUpsert []spanconfig.Record
	for _, line := range strings.Split(input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		const upsertPrefix, deletePrefix = "upsert ", "delete "
		switch {
		case strings.HasPrefix(line, deletePrefix):
			line = strings.TrimPrefix(line, line[:len(deletePrefix)])
			toDelete = append(toDelete, ParseTarget(t, line))
		case strings.HasPrefix(line, upsertPrefix):
			line = strings.TrimPrefix(line, line[:len(upsertPrefix)])
			toUpsert = append(toUpsert, ParseSpanConfigRecord(t, line))
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
			updates = append(updates, spanconfig.Deletion(ParseTarget(t, line)))
		case strings.HasPrefix(line, setPrefix):
			line = strings.TrimPrefix(line, line[:len(setPrefix)])
			entry := ParseSpanConfigRecord(t, line)
			updates = append(updates, spanconfig.Update(entry))
		default:
			t.Fatalf("malformed line %q, expected to find prefix %q or %q",
				line, setPrefix, deletePrefix)
		}
	}
	return updates
}

// PrintSpan is a helper function that transforms roachpb.Span into a string of
// the form "[start,end)". Spans constructed by the ParseSpan helper above
// roundtrip; spans containing special keys that translate to pretty-printed
// keys are printed as such.
func PrintSpan(sp roachpb.Span) string {
	s := []string{
		sp.Key.String(),
		sp.EndKey.String(),
	}
	for i := range s {
		// Raw keys are quoted, so we unquote them.
		if strings.Contains(s[i], "\"") {
			var err error
			s[i], err = strconv.Unquote(s[i])
			if err != nil {
				panic(err)
			}
		}
	}
	return fmt.Sprintf("[%s,%s)", s[0], s[1])
}

// PrintTarget is a helper function that prints a spanconfig.Target.
func PrintTarget(t *testing.T, target spanconfig.Target) string {
	switch {
	case target.IsSpanTarget():
		return PrintSpan(target.GetSpan())
	case target.IsSystemTarget():
		return target.GetSystemTarget().String()
	default:
		t.Fatalf("unknown target type")
	}
	panic("unreachable")
}

// PrintSpanConfig is a helper function that transforms roachpb.SpanConfig into
// a readable string. The span config is assumed to have been constructed by the
// ParseSpanConfig helper above.
func PrintSpanConfig(config roachpb.SpanConfig) string {
	// See ParseConfig for what a "tagged" roachpb.SpanConfig translates to.
	conf := make([]string, 0, len(config.GCPolicy.ProtectionPolicies)*2)
	for i, policy := range config.GCPolicy.ProtectionPolicies {
		if i > 0 {
			conf = append(conf, "+")
		}
		// Special case handling for "FALLBACK" config for readability.
		if policy.ProtectedTimestamp.WallTime == -1 {
			conf = append(conf, "FALLBACK")
		} else {
			conf = append(conf, fmt.Sprintf("%c", policy.ProtectedTimestamp.WallTime))
		}
	}
	return strings.Join(conf, "")
}

// PrintSpanConfigRecord is a helper function that transforms
// spanconfig.Record into a string of the form "target:config". The
// entry is assumed to either have been constructed using ParseSpanConfigRecord
// above, or the constituent span and config to have been constructed using the
// Parse{Span,Config} helpers above.
func PrintSpanConfigRecord(t *testing.T, record spanconfig.Record) string {
	return fmt.Sprintf("%s:%s", PrintTarget(t, record.Target), PrintSpanConfig(record.Config))
}

// PrintSystemSpanConfigDiffedAgainstDefault is a helper function that diffs the
// given config against the default system span config that applies to
// spanconfig.SystemTargets, and returns a string for the mismatched fields.
func PrintSystemSpanConfigDiffedAgainstDefault(conf roachpb.SpanConfig) string {
	if conf.Equal(roachpb.TestingDefaultSystemSpanConfiguration()) {
		return "default system span config"
	}

	var diffs []string
	defaultSystemTargetConf := roachpb.TestingDefaultSystemSpanConfiguration()
	if !reflect.DeepEqual(conf.GCPolicy.ProtectionPolicies,
		defaultSystemTargetConf.GCPolicy.ProtectionPolicies) {
		sort.Slice(conf.GCPolicy.ProtectionPolicies, func(i, j int) bool {
			lhs := conf.GCPolicy.ProtectionPolicies[i].ProtectedTimestamp
			rhs := conf.GCPolicy.ProtectionPolicies[j].ProtectedTimestamp
			return lhs.Less(rhs)
		})
		protectionPolicies := make([]string, 0, len(conf.GCPolicy.ProtectionPolicies))
		for _, pp := range conf.GCPolicy.ProtectionPolicies {
			protectionPolicies = append(protectionPolicies, pp.String())
		}
		diffs = append(diffs, fmt.Sprintf("protection_policies=[%s]", strings.Join(protectionPolicies, " ")))
	}
	return strings.Join(diffs, " ")
}

// PrintSpanConfigDiffedAgainstDefaults is a helper function that diffs the given
// config against RANGE {DEFAULT, SYSTEM} and the config for the system database
// (as expected on both kinds of tenants), and returns a string for the
// mismatched fields. If it matches one of the standard templates, "range
// {default,system}" or "database system ({host,tenant})" is returned.
func PrintSpanConfigDiffedAgainstDefaults(conf roachpb.SpanConfig) string {
	if conf.Equal(roachpb.TestingDefaultSpanConfig()) {
		return "range default"
	}
	if conf.Equal(roachpb.TestingSystemSpanConfig()) {
		return "range system"
	}
	if conf.Equal(roachpb.TestingDatabaseSystemSpanConfig(true /* host */)) {
		return "database system (host)"
	}
	if conf.Equal(roachpb.TestingDatabaseSystemSpanConfig(false /* host */)) {
		return "database system (tenant)"
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
	if conf.GCPolicy.IgnoreStrictEnforcement != defaultConf.GCPolicy.IgnoreStrictEnforcement {
		diffs = append(diffs, fmt.Sprintf("ignore_strict_gc=%t", conf.GCPolicy.IgnoreStrictEnforcement))
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
	if conf.RangefeedEnabled != defaultConf.RangefeedEnabled {
		diffs = append(diffs, fmt.Sprintf("rangefeed_enabled=%t", conf.RangefeedEnabled))
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
	if !reflect.DeepEqual(conf.GCPolicy.ProtectionPolicies, defaultConf.GCPolicy.ProtectionPolicies) {
		sort.Slice(conf.GCPolicy.ProtectionPolicies, func(i, j int) bool {
			lhs := conf.GCPolicy.ProtectionPolicies[i].ProtectedTimestamp
			rhs := conf.GCPolicy.ProtectionPolicies[j].ProtectedTimestamp
			return lhs.Less(rhs)
		})
		protectionPolicies := make([]string, 0, len(conf.GCPolicy.ProtectionPolicies))
		for _, pp := range conf.GCPolicy.ProtectionPolicies {
			protectionPolicies = append(protectionPolicies, pp.String())
		}
		diffs = append(diffs, fmt.Sprintf("protection_policies=[%s]", strings.Join(protectionPolicies, " ")))
	}
	if conf.ExcludeDataFromBackup != defaultConf.ExcludeDataFromBackup {
		diffs = append(diffs, fmt.Sprintf("exclude_data_from_backup=%v", conf.ExcludeDataFromBackup))
	}

	return strings.Join(diffs, " ")
}

// MaybeLimitAndOffset checks if "offset" and "limit" arguments are provided in
// the datadriven test, and if so, returns a minification of the given input
// after having dropped an offset number of lines and limiting the results as
// need. If lines are dropped on either end, the given separator is used to
// indicate the omission.
func MaybeLimitAndOffset(
	t *testing.T, d *datadriven.TestData, separator string, lines []string,
) string {
	var offset, limit int
	if d.HasArg("offset") {
		d.ScanArgs(t, "offset", &offset)
		require.True(t, offset >= 0)
		require.Truef(t, offset <= len(lines),
			"offset (%d) larger than number of lines (%d)", offset, len(lines))
	}
	if d.HasArg("limit") {
		d.ScanArgs(t, "limit", &limit)
		require.True(t, limit >= 0)
	} else {
		limit = len(lines)
	}

	var output strings.Builder
	if offset > 0 && len(lines) > 0 && separator != "" {
		output.WriteString(fmt.Sprintf("%s\n", separator)) // print leading separator
	}
	lines = lines[offset:]
	for i, line := range lines {
		if i == limit {
			if separator != "" {
				output.WriteString(fmt.Sprintf("%s\n", separator)) // print trailing separator
			}
			break
		}
		output.WriteString(fmt.Sprintf("%s\n", line))
	}

	return strings.TrimSpace(output.String())
}

// SplitPoint is a unit of what's retrievable from a spanconfig.StoreReader. It
// captures a single split point, and the config to be applied over the
// following key span (or at least until the next such SplitPoint).
//
// TODO(irfansharif): Find a better name?
type SplitPoint struct {
	RKey   roachpb.RKey
	Config roachpb.SpanConfig
}

// SplitPoints is a collection of split points.
type SplitPoints []SplitPoint

func (rs SplitPoints) String() string {
	var output strings.Builder
	for _, c := range rs {
		output.WriteString(fmt.Sprintf("%-42s %s\n", c.RKey.String(),
			PrintSpanConfigDiffedAgainstDefaults(c.Config)))
	}
	return output.String()
}

// GetSplitPoints returns a list of range split points as suggested by the given
// StoreReader.
func GetSplitPoints(ctx context.Context, t *testing.T, reader spanconfig.StoreReader) SplitPoints {
	var splitPoints []SplitPoint
	splitKey := roachpb.RKeyMin
	for {
		splitKeyConf, err := reader.GetSpanConfigForKey(ctx, splitKey)
		require.NoError(t, err)

		splitPoints = append(splitPoints, SplitPoint{
			RKey:   splitKey,
			Config: splitKeyConf,
		})

		if !reader.NeedsSplit(ctx, splitKey, roachpb.RKeyMax) {
			break
		}
		splitKey = reader.ComputeSplitKey(ctx, splitKey, roachpb.RKeyMax)
	}

	return splitPoints
}

// ParseProtectionTarget returns a ptpb.Target based on the input. This target
// could either refer to a Cluster, list of Tenants or SchemaObjects.
func ParseProtectionTarget(t *testing.T, input string) *ptpb.Target {
	line := strings.Split(input, "\n")
	if len(line) != 1 {
		t.Fatal("only one target must be specified per protectedts operation")
	}
	target := line[0]

	const clusterPrefix, tenantPrefix, schemaObjectPrefix = "cluster", "tenants", "descs"
	switch {
	case strings.HasPrefix(target, clusterPrefix):
		return ptpb.MakeClusterTarget()
	case strings.HasPrefix(target, tenantPrefix):
		target = strings.TrimPrefix(target, target[:len(tenantPrefix)+1])
		tenantIDs := strings.Split(target, ",")
		ids := make([]roachpb.TenantID, 0, len(tenantIDs))
		for _, tenID := range tenantIDs {
			id, err := strconv.Atoi(tenID)
			require.NoError(t, err)
			ids = append(ids, roachpb.MakeTenantID(uint64(id)))
		}
		return ptpb.MakeTenantsTarget(ids)
	case strings.HasPrefix(target, schemaObjectPrefix):
		target = strings.TrimPrefix(target, target[:len(schemaObjectPrefix)+1])
		schemaObjectIDs := strings.Split(target, ",")
		ids := make([]descpb.ID, 0, len(schemaObjectIDs))
		for _, tenID := range schemaObjectIDs {
			id, err := strconv.Atoi(tenID)
			require.NoError(t, err)
			ids = append(ids, descpb.ID(id))
		}
		return ptpb.MakeSchemaObjectsTarget(ids)
	default:
		t.Fatalf("malformed line %q, expected to find prefix %q, %q or %q", target, tenantPrefix,
			schemaObjectPrefix, clusterPrefix)
	}
	return nil
}
