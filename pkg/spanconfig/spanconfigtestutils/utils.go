// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigbounds"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// spanRe matches strings of the form "[start, end)", capturing both the "start"
// and "end" keys.
var spanRe = regexp.MustCompile(`^\[((/Tenant/\d*/)?\w+),\s??((/Tenant/\d*/)?\w+)\)$`)

// systemTargetRe matches strings of the form
// "{entire-keyspace|source=<id>,(target=<id>|all-tenant-keyspace-targets-set)}".
var systemTargetRe = regexp.MustCompile(
	`^{(entire-keyspace)|(source=(\d*),\s??((target=(\d*))|all-tenant-keyspace-targets-set))}$`,
)

// configRe matches either FALLBACK (for readability), a single letter, or a
// specified GC TTL value. It's a shorthand for declaring a unique tagged config.
var configRe = regexp.MustCompile(`^(FALLBACK)|(^GC\.ttl=(\d*))|(^\w)$`)

// boundsRe matches a string of the form "{GC.ttl_start=<int>,GC.ttl_end=<int>}".
// It translates to an upper/lower bound expressed through SpanConfigBounds.
var boundsRe = regexp.MustCompile(`{GC\.ttl_start=(\d*),\s??GC.ttl_end=(\d*)}`)

// tenantRe matches a string of the form "/Tenant/<id>".
var tenantRe = regexp.MustCompile(`/Tenant/(\d*)`)

// keyRe matches a string of the form "a", "Tenant/10/a". An optional tenant
// prefix may be used to specify a key inside a secondary tenant's keyspace;
// otherwise, the key is within the system tenant's keyspace.
var keyRe = regexp.MustCompile(`(Tenant/\d*/)?(\w+)`)

// ParseRangeID is helper function that constructs a roachpb.RangeID from a
// string of the form "r<int>".
func ParseRangeID(t testing.TB, s string) roachpb.RangeID {
	rangeID, err := strconv.Atoi(strings.TrimPrefix(s, "r"))
	require.NoError(t, err)
	return roachpb.RangeID(rangeID)
}

// ParseNodeID is helper function that constructs a roachpb.NodeID from a string
// of the form "n<int>".
func ParseNodeID(t testing.TB, s string) roachpb.NodeID {
	nodeID, err := strconv.Atoi(strings.TrimPrefix(s, "n"))
	require.NoError(t, err)
	return roachpb.NodeID(nodeID)
}

// ParseReplicaSet is helper function that constructs a roachpb.ReplicaSet from
// a string of the form "voters=[n1,n2,...] non-voters=[n3,...]". The
// {store,replica} IDs for each replica is set to be equal to the corresponding
// node ID.
func ParseReplicaSet(t testing.TB, s string) roachpb.ReplicaSet {
	replSet := roachpb.ReplicaSet{}
	rtypes := map[string]roachpb.ReplicaType{
		"voters":                     roachpb.VOTER_FULL,
		"voters-incoming":            roachpb.VOTER_INCOMING,
		"voters-outgoing":            roachpb.VOTER_OUTGOING,
		"voters-demoting-learners":   roachpb.VOTER_DEMOTING_LEARNER,
		"voters-demoting-non-voters": roachpb.VOTER_DEMOTING_NON_VOTER,
		"learners":                   roachpb.LEARNER,
		"non-voters":                 roachpb.NON_VOTER,
	}
	for _, part := range strings.Split(s, " ") {
		inner := strings.Split(part, "=")
		require.Len(t, inner, 2)
		rtype, found := rtypes[inner[0]]
		require.Truef(t, found, "unexpected replica type: %s", inner[0])
		nodes := strings.TrimSuffix(strings.TrimPrefix(inner[1], "["), "]")

		for _, n := range strings.Split(nodes, ",") {
			n = strings.TrimSpace(n)
			if n == "" {
				continue
			}
			nodeID := ParseNodeID(t, n)
			replSet.AddReplica(roachpb.ReplicaDescriptor{
				NodeID:    nodeID,
				StoreID:   roachpb.StoreID(nodeID),
				ReplicaID: roachpb.ReplicaID(nodeID),
				Type:      rtype,
			})
		}
	}
	return replSet
}

// ParseZoneConfig is helper function that constructs a zonepb.ZoneConfig from a
// string of the form "num_replicas=<int> num_voters=<int> constraints='..'
// voter_constraints='..'".
func ParseZoneConfig(t testing.TB, s string) zonepb.ZoneConfig {
	config := zonepb.DefaultZoneConfig()
	parts := strings.Split(s, " ")
	for _, part := range parts {
		switch {
		case strings.HasPrefix(part, "num_replicas="):
			part = strings.TrimPrefix(part, "num_replicas=")
			n, err := strconv.Atoi(part)
			require.NoError(t, err)
			n32 := int32(n)
			config.NumReplicas = &n32
		case strings.HasPrefix(part, "num_voters="):
			part = strings.TrimPrefix(part, "num_voters=")
			n, err := strconv.Atoi(part)
			require.NoError(t, err)
			n32 := int32(n)
			config.NumVoters = &n32
		case strings.HasPrefix(part, "constraints="):
			cl := zonepb.ConstraintsList{}
			part = strings.TrimPrefix(part, "constraints=")
			require.NoError(t, yaml.UnmarshalStrict([]byte(part), &cl))
			config.Constraints = cl.Constraints
		case strings.HasPrefix(part, "voter_constraints="):
			cl := zonepb.ConstraintsList{}
			part = strings.TrimPrefix(part, "voter_constraints=")
			require.NoError(t, yaml.UnmarshalStrict([]byte(part), &cl))
			config.VoterConstraints = cl.Constraints
		case strings.HasPrefix(part, "lease_preferences="):
			cl := []zonepb.LeasePreference{}
			part = strings.TrimPrefix(part, "lease_preferences=")
			require.NoError(t, yaml.UnmarshalStrict([]byte(part), &cl))
			config.LeasePreferences = cl
		default:
			t.Fatalf("unrecognized suffix for %s, expected 'num_replicas=', 'num_voters=', 'constraints=', or 'voter_constraints='", part)
		}
	}
	return config
}

// ParseSpan is helper function that constructs a roachpb.Span from a string of
// the form "[start, end)".
func ParseSpan(t testing.TB, sp string) roachpb.Span {
	if !spanRe.MatchString(sp) {
		t.Fatalf("expected %s to match span regex", sp)
	}

	matches := spanRe.FindStringSubmatch(sp)
	startStr, endStr := matches[1], matches[3]
	start, tenStart := ParseKey(t, startStr)
	end, tenEnd := ParseKey(t, endStr)

	// Sanity check keys don't straddle tenant boundaries.
	require.Equal(t, tenStart, tenEnd)

	return roachpb.Span{
		Key:    start,
		EndKey: end,
	}
}

// ParseKey constructs a roachpb.Key from the supplied input. The key may be
// optionally prefixed with a "/Tenant/ID/" prefix; doing so ensures the key
// belongs to the specified tenant's keyspace. Otherwise, the key lies in the
// system tenant's keyspace.
//
// In addition to the key, the tenant ID is also returned.
func ParseKey(t testing.TB, key string) (roachpb.Key, roachpb.TenantID) {
	require.True(t, keyRe.MatchString(key))

	matches := keyRe.FindStringSubmatch(key)
	tenantID := roachpb.SystemTenantID
	if matches[1] != "" {
		tenantID = parseTenant(t, key)
	}

	codec := keys.MakeSQLCodec(tenantID)
	return append(codec.TenantPrefix(), roachpb.Key(matches[2])...), tenantID
}

// parseTenant expects a string of the form "ten-<tenantID>" and returns the
// tenant ID.
func parseTenant(t testing.TB, input string) roachpb.TenantID {
	require.True(t, tenantRe.MatchString(input), input)

	matches := tenantRe.FindStringSubmatch(input)
	tenID := matches[1]
	tenIDRaw, err := strconv.Atoi(tenID)
	require.NoError(t, err)
	return roachpb.MustMakeTenantID(uint64(tenIDRaw))
}

// parseSystemTarget is a helepr function that constructs a
// spanconfig.SystemTarget from a string of the form {source=<id>,target=<id>}
func parseSystemTarget(t testing.TB, systemTarget string) spanconfig.SystemTarget {
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
		return spanconfig.MakeAllTenantKeyspaceTargetsSet(roachpb.MustMakeTenantID(uint64(sourceID)))
	}
	targetID, err := strconv.Atoi(matches[6])
	require.NoError(t, err)
	target, err := spanconfig.MakeTenantKeyspaceTarget(
		roachpb.MustMakeTenantID(uint64(sourceID)), roachpb.MustMakeTenantID(uint64(targetID)),
	)
	require.NoError(t, err)
	return target
}

// ParseTarget is a helper function that constructs a spanconfig.Target from a
// string that conforms to spanRe.
func ParseTarget(t testing.TB, target string) spanconfig.Target {
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
// "tagged" with the given string. See configRe for specifics.
func ParseConfig(t testing.TB, conf string) roachpb.SpanConfig {
	if !configRe.MatchString(conf) {
		t.Fatalf("expected %s to match config regex", conf)
	}
	matches := configRe.FindStringSubmatch(conf)

	var ts int64
	var gcTTL int
	if matches[1] == "FALLBACK" {
		ts = -1
	} else if matches[4] != "" {
		ts = int64(matches[4][0])
	} else {
		var err error
		gcTTL, err = strconv.Atoi(matches[3])
		require.NoError(t, err)
	}

	var protectionPolicies []roachpb.ProtectionPolicy
	if ts != 0 {
		protectionPolicies = []roachpb.ProtectionPolicy{
			{
				ProtectedTimestamp: hlc.Timestamp{
					WallTime: ts,
				},
			},
		}
	}
	return roachpb.SpanConfig{
		GCPolicy: roachpb.GCPolicy{
			ProtectionPolicies: protectionPolicies,
			TTLSeconds:         int32(gcTTL),
		},
	}
}

// BoundsUpdate ..
type BoundsUpdate struct {
	TenantID roachpb.TenantID
	Bounds   *spanconfigbounds.Bounds
	Deleted  bool
}

// ParseDeclareBoundsArguments parses datadriven test input to a list of
// tenantcapabilities.Updates that can be applied to a
// tenantcapabilities.Reader. The input is of the following form:
//
// delete ten-10
// set ten-10:{GC.ttl_start=5, GC.ttl_end=30}
func ParseDeclareBoundsArguments(t *testing.T, input string) (updates []BoundsUpdate) {
	for _, line := range strings.Split(input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		const setPrefix, deletePrefix = "set ", "delete "

		switch {
		case strings.HasPrefix(line, setPrefix):
			line = strings.TrimPrefix(line, line[:len(setPrefix)])
			parts := strings.Split(line, ":")
			require.Len(t, parts, 2)
			tenantID := parseTenant(t, parts[0])
			bounds := parseBounds(t, parts[1])
			updates = append(updates, BoundsUpdate{
				TenantID: tenantID,
				Bounds:   bounds,
			})

		case strings.HasPrefix(line, deletePrefix):
			line = strings.TrimPrefix(line, line[:len(deletePrefix)])
			tenantID := parseTenant(t, line)
			updates = append(updates,
				BoundsUpdate{
					TenantID: tenantID,
					Bounds:   nil,
					Deleted:  true,
				},
			)
		default:
			t.Fatalf("malformed line %q, expected to find prefix %q or %q",
				line, setPrefix, deletePrefix)
		}
	}
	return updates
}

// parseBounds parses a string that looks like {GC.ttl_start=5, GC.ttl_end=40}
// into a spanconfigbounds.Bounds struct.
func parseBounds(t *testing.T, input string) *spanconfigbounds.Bounds {
	require.True(t, boundsRe.MatchString(input))

	matches := boundsRe.FindStringSubmatch(input)
	gcTTLStart, err := strconv.Atoi(matches[1])
	require.NoError(t, err)
	gcTTLEnd, err := strconv.Atoi(matches[2])
	require.NoError(t, err)

	return spanconfigbounds.New(&tenantcapabilitiespb.SpanConfigBounds{
		GCTTLSeconds: &tenantcapabilitiespb.SpanConfigBounds_Int32Range{
			Start: int32(gcTTLStart),
			End:   int32(gcTTLEnd),
		},
	})
}

// ParseSpanConfigRecord is helper function that constructs a
// spanconfig.Target from a string of the form target:config. See
// ParseTarget and ParseConfig above.
func ParseSpanConfigRecord(t testing.TB, conf string) spanconfig.Record {
	parts := strings.Split(conf, ":")
	if len(parts) != 2 {
		t.Fatalf("expected single %q separator", ":")
	}
	record, err := spanconfig.MakeRecord(ParseTarget(t, parts[0]),
		ParseConfig(t, parts[1]))
	require.NoError(t, err)
	return record
}

// ParseKVAccessorGetArguments is a helper function that parses datadriven
// kvaccessor-get arguments into the relevant spans. The input is of the
// following form:
//
//	span [a,e)
//	span [a,b)
//	span [b,c)
//	system-target {source=1,target=1}
//	system-target {source=20,target=20}
//	system-target {source=1,target=20}
func ParseKVAccessorGetArguments(t testing.TB, input string) []spanconfig.Target {
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
//	delete [c,e)
//	upsert [c,d):C
//	upsert [d,e):D
//	delete {source=1,target=1}
//	delete {source=1,target=20}
//	upsert {source=1,target=1}:A
//	delete {source=1,target=20}:D
func ParseKVAccessorUpdateArguments(
	t testing.TB, input string,
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
//	delete [c,e)
//	set [c,d):C
//	set [d,e):D
func ParseStoreApplyArguments(t testing.TB, input string) (updates []spanconfig.Update) {
	for _, line := range strings.Split(input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		const setPrefix, deletePrefix = "set ", "delete "
		switch {
		case strings.HasPrefix(line, deletePrefix):
			line = strings.TrimPrefix(line, line[:len(deletePrefix)])
			del, err := spanconfig.Deletion(ParseTarget(t, line))
			require.NoError(t, err)
			updates = append(updates, del)
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
	var res []string
	for _, key := range []roachpb.Key{sp.Key, sp.EndKey} {
		var tenantPrefixStr string
		rest, tenID, err := keys.DecodeTenantPrefix(key)
		if err != nil {
			panic(err)
		}

		if !tenID.IsSystem() {
			tenantPrefixStr = fmt.Sprintf("%s", keys.MakeSQLCodec(tenID).TenantPrefix())
		}

		// Raw keys are quoted, so we unquote them.
		restStr := roachpb.Key(rest).String()
		if strings.Contains(restStr, "\"") {
			var err error
			restStr, err = strconv.Unquote(restStr)
			if err != nil {
				panic(err)
			}
		}

		// For keys inside a secondary tenant, we don't print the "/Min" suffix if
		// the key is at the start of their keyspace. Also, we add a "/" delimiter
		// after the tenant prefix.
		if !tenID.IsSystem() {
			if roachpb.Key(rest).Compare(keys.MinKey) == 0 {
				restStr = ""
			}

			if restStr != "" {
				restStr = fmt.Sprintf("/%s", restStr)
			}
		}

		res = append(res, fmt.Sprintf("%s%s", tenantPrefixStr, restStr))
	}
	return fmt.Sprintf("[%s,%s)", res[0], res[1])
}

// PrintTarget is a helper function that prints a spanconfig.Target.
func PrintTarget(t testing.TB, target spanconfig.Target) string {
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
	if config.GCPolicy.TTLSeconds != 0 && len(config.GCPolicy.ProtectionPolicies) != 0 {
		panic("cannot have both TTL and protection policies set for tagged configs") // sanity check
	}
	if config.GCPolicy.TTLSeconds != 0 {
		return fmt.Sprintf("GC.ttl=%d", config.GCPolicy.TTLSeconds)
	}
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
func PrintSpanConfigRecord(t testing.TB, record spanconfig.Record) string {
	return fmt.Sprintf("%s:%s", PrintTarget(t, record.GetTarget()), PrintSpanConfig(record.GetConfig()))
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
		diffs = append(diffs, fmt.Sprintf("lease_preferences=%v", conf.LeasePreferences))
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

// ParseProtectionTarget returns a ptpb.Target based on the input. This target
// could either refer to a Cluster, list of Tenants or SchemaObjects.
func ParseProtectionTarget(t testing.TB, input string) *ptpb.Target {
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
			ids = append(ids, roachpb.MustMakeTenantID(uint64(id)))
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

// configOverride is used to override span configs for specific ranges.
type configOverride struct {
	key    roachpb.Key
	config roachpb.SpanConfig
}

// WrappedKVSubscriber is a KVSubscriber which wraps another KVSubscriber and
// overrides specific SpanConfigs. This test struct should be used in
// StoreKVSubscriberOverride as a more generic and powerful SpanConfig injection
// alternative to change the behavior for specific ranges instead of using any
// of DefaultZoneConfigOverride, DefaultSystemZoneConfigOverride,
// OverrideFallbackConf, ConfReaderInterceptor, UseSystemConfigSpanForQueues,
// SpanConfigUpdateInterceptor or SetSpanConfigInterceptor. By using this struct
// as an alternative to the others it mainly avoids any need to depend on
// rangefeed timing to propagate the config change.
type WrappedKVSubscriber struct {
	wrapped spanconfig.KVSubscriber
	// Overrides are list of tuples of roachpb.Key and spanconfig.
	overrides []configOverride
}

// NewWrappedKVSubscriber creates a new WrappedKVSubscriber.
func NewWrappedKVSubscriber(wrapped spanconfig.KVSubscriber) *WrappedKVSubscriber {
	return &WrappedKVSubscriber{wrapped: wrapped}
}

// AddOverride adds a new override to the WrappedKVSubscriber. This should only
// be used during construction.
func (w *WrappedKVSubscriber) AddOverride(key roachpb.Key, config roachpb.SpanConfig) {
	w.overrides = append(w.overrides, configOverride{key: key, config: config})
}

// GetProtectionTimestamps implements spanconfig.KVSubscriber.
func (w *WrappedKVSubscriber) GetProtectionTimestamps(
	ctx context.Context, sp roachpb.Span,
) ([]hlc.Timestamp, hlc.Timestamp, error) {
	return w.wrapped.GetProtectionTimestamps(ctx, sp)
}

// LastUpdated always reports that it has been updated to allow
// GetSpanConfigForKey to be used immediately.
func (w *WrappedKVSubscriber) LastUpdated() hlc.Timestamp {
	return hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
}

// Subscribe implements spanconfig.KVSubscriber.
func (w *WrappedKVSubscriber) Subscribe(f func(context.Context, roachpb.Span)) {
	w.wrapped.Subscribe(f)
}

// ComputeSplitKey implements spanconfig.StoreReader.
func (w *WrappedKVSubscriber) ComputeSplitKey(
	ctx context.Context, start roachpb.RKey, end roachpb.RKey,
) (roachpb.RKey, error) {
	return w.wrapped.ComputeSplitKey(ctx, start, end)
}

// GetSpanConfigForKey implements spanconfig.StoreReader.
func (w *WrappedKVSubscriber) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, roachpb.Span, error) {
	spanConfig, span, err := w.wrapped.GetSpanConfigForKey(ctx, key)
	for _, o := range w.overrides {
		if key.Equal(o.key) {
			return o.config, span, nil
		}
	}
	return spanConfig, span, err
}

// NeedsSplit implements spanconfig.StoreReader.
func (w *WrappedKVSubscriber) NeedsSplit(
	ctx context.Context, start roachpb.RKey, end roachpb.RKey,
) (bool, error) {
	return w.wrapped.NeedsSplit(ctx, start, end)
}

var _ spanconfig.KVSubscriber = &WrappedKVSubscriber{}
