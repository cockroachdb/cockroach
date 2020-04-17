// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package zonepb

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	proto "github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestZoneConfigValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		cfg      ZoneConfig
		expected string
	}{
		{
			ZoneConfig{
				NumReplicas: proto.Int32(0),
			},
			"at least one replica is required",
		},
		{
			ZoneConfig{
				NumReplicas: proto.Int32(-1),
			},
			"at least one replica is required",
		},
		{
			ZoneConfig{
				NumReplicas: proto.Int32(2),
			},
			"at least 3 replicas are required for multi-replica configurations",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(1),
				RangeMaxBytes: proto.Int64(0),
			},
			"RangeMaxBytes 0 less than minimum allowed",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(1),
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 0},
			},
			"GC.TTLSeconds 0 less than minimum allowed",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(1),
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 1},
			},
			"",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(1),
				RangeMinBytes: proto.Int64(-1),
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 1},
			},
			"RangeMinBytes -1 less than minimum allowed",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(1),
				RangeMinBytes: DefaultZoneConfig().RangeMaxBytes,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 1},
			},
			"is greater than or equal to RangeMaxBytes",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(1),
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 1},
				Constraints: []ConstraintsConjunction{
					{Constraints: []Constraint{{Value: "a", Type: Constraint_DEPRECATED_POSITIVE}}},
				},
			},
			"constraints must either be required .+ or prohibited .+",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(1),
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 1},
				Constraints: []ConstraintsConjunction{
					{Constraints: []Constraint{{Value: "a", Type: Constraint_PROHIBITED}}},
				},
			},
			"",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(1),
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 1},
				Constraints: []ConstraintsConjunction{
					{
						Constraints: []Constraint{{Value: "a", Type: Constraint_PROHIBITED}},
						NumReplicas: 1,
					},
				},
			},
			"",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(1),
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 1},
				Constraints: []ConstraintsConjunction{
					{
						Constraints: []Constraint{{Value: "a", Type: Constraint_REQUIRED}},
						NumReplicas: 2,
					},
				},
			},
			"the number of replicas specified in constraints .+ cannot be greater than",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(3),
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 1},
				Constraints: []ConstraintsConjunction{
					{
						Constraints: []Constraint{{Value: "a", Type: Constraint_REQUIRED}},
						NumReplicas: 2,
					},
				},
			},
			"",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(1),
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 1},
				Constraints: []ConstraintsConjunction{
					{
						Constraints: []Constraint{{Value: "a", Type: Constraint_REQUIRED}},
						NumReplicas: 0,
					},
					{
						Constraints: []Constraint{{Value: "b", Type: Constraint_REQUIRED}},
						NumReplicas: 1,
					},
				},
			},
			"constraints must apply to at least one replica",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(3),
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 1},
				Constraints: []ConstraintsConjunction{
					{
						Constraints: []Constraint{{Value: "a", Type: Constraint_REQUIRED}},
						NumReplicas: 2,
					},
					{
						Constraints: []Constraint{{Value: "b", Type: Constraint_PROHIBITED}},
						NumReplicas: 1,
					},
				},
			},
			"only required constraints .+ can be applied to a subset of replicas",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(1),
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 1},
				LeasePreferences: []LeasePreference{
					{
						Constraints: []Constraint{},
					},
				},
			},
			"every lease preference must include at least one constraint",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(1),
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 1},
				LeasePreferences: []LeasePreference{
					{
						Constraints: []Constraint{{Value: "a", Type: Constraint_DEPRECATED_POSITIVE}},
					},
				},
			},
			"lease preference constraints must either be required .+ or prohibited .+",
		},
		{
			ZoneConfig{
				NumReplicas:   proto.Int32(1),
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            &GCPolicy{TTLSeconds: 1},
				LeasePreferences: []LeasePreference{
					{
						Constraints: []Constraint{{Value: "a", Type: Constraint_REQUIRED}},
					},
					{
						Constraints: []Constraint{{Value: "b", Type: Constraint_PROHIBITED}},
					},
				},
			},
			"",
		},
	}

	for i, c := range testCases {
		err := c.cfg.Validate()
		if !testutils.IsError(err, c.expected) {
			t.Errorf("%d: expected %q, got %v", i, c.expected, err)
		}
	}
}

func TestZoneConfigValidateTandemFields(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		cfg      ZoneConfig
		expected string
	}{
		{
			ZoneConfig{
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
			},
			"range_min_bytes and range_max_bytes must be set together",
		},
		{
			ZoneConfig{
				RangeMinBytes: DefaultZoneConfig().RangeMinBytes,
			},
			"range_min_bytes and range_max_bytes must be set together",
		},
		{
			ZoneConfig{
				Constraints: []ConstraintsConjunction{
					{
						Constraints: []Constraint{{Value: "a", Type: Constraint_REQUIRED}},
						NumReplicas: 2,
					},
				},
			},
			"when per-replica constraints are set, num_replicas must be set as well",
		},
		{
			ZoneConfig{
				InheritedConstraints:      true,
				InheritedLeasePreferences: false,
				LeasePreferences: []LeasePreference{
					{
						Constraints: []Constraint{},
					},
				},
			},
			"lease preferences can not be set unless the constraints are explicitly set as well",
		},
	}

	for i, c := range testCases {
		err := c.cfg.ValidateTandemFields()
		if !testutils.IsError(err, c.expected) {
			t.Errorf("%d: expected %q, got %v", i, c.expected, err)
		}
	}
}

func TestZoneConfigSubzones(t *testing.T) {
	defer leaktest.AfterTest(t)()

	zone := DefaultZoneConfig()
	subzoneAInvalid := Subzone{IndexID: 1, PartitionName: "a", Config: ZoneConfig{NumReplicas: proto.Int32(-1)}}
	subzoneA := Subzone{IndexID: 1, PartitionName: "a", Config: zone}
	subzoneB := Subzone{IndexID: 1, PartitionName: "b", Config: zone}

	if zone.IsSubzonePlaceholder() {
		t.Errorf("default zone config should not be considered a subzone placeholder")
	}

	zone.SetSubzone(subzoneAInvalid)
	zone.SetSubzone(subzoneB)
	if err := zone.Validate(); !testutils.IsError(err, "at least one replica is required") {
		t.Errorf("expected 'at least one replica is required' validation error, but got %v", err)
	}

	zone.SetSubzone(subzoneA)
	if err := zone.Validate(); err != nil {
		t.Errorf("expected zone validation to succeed, but got %s", err)
	}
	if subzone := zone.GetSubzone(1, "a"); !subzoneA.Equal(subzone) {
		t.Errorf("expected subzone to equal %+v, but got %+v", &subzoneA, subzone)
	}
	if subzone := zone.GetSubzone(1, "b"); !subzoneB.Equal(subzone) {
		t.Errorf("expected subzone to equal %+v, but got %+v", &subzoneB, subzone)
	}
	if subzone := zone.GetSubzone(1, "c"); subzone != nil {
		t.Errorf("expected nil subzone, but got %+v", subzone)
	}
	if subzone := zone.GetSubzone(2, "a"); subzone != nil {
		t.Errorf("expected nil subzone, but got %+v", subzone)
	}
	if zone.IsSubzonePlaceholder() {
		t.Errorf("zone with its own config should not be considered a subzone placeholder")
	}

	zone.DeleteTableConfig()
	if e := (ZoneConfig{
		NumReplicas: proto.Int32(0),
		Subzones:    []Subzone{subzoneA, subzoneB},
	}); !e.Equal(zone) {
		t.Errorf("expected zone after clearing to equal %+v, but got %+v", e, zone)
	}
	if !zone.IsSubzonePlaceholder() {
		t.Errorf("expected cleared zone config to be considered a subzone placeholder")
	}

	if didDelete := zone.DeleteSubzone(1, "c"); didDelete {
		t.Errorf("deletion claims to have succeeded on non-existent subzone")
	}
	if didDelete := zone.DeleteSubzone(1, "b"); !didDelete {
		t.Errorf("valid deletion claims to have failed")
	}
	if subzone := zone.GetSubzone(1, "b"); subzone != nil {
		t.Errorf("expected deleted subzone to be nil when retrieved, but got %+v", subzone)
	}
	if subzone := zone.GetSubzone(1, "a"); !subzoneA.Equal(subzone) {
		t.Errorf("expected non-deleted subzone to equal %+v, but got %+v", &subzoneA, subzone)
	}

	zone.SetSubzone(Subzone{IndexID: 2, Config: DefaultZoneConfig()})
	zone.SetSubzone(Subzone{IndexID: 2, PartitionName: "a", Config: DefaultZoneConfig()})
	zone.SetSubzone(subzoneB) // interleave a subzone from a different index
	zone.SetSubzone(Subzone{IndexID: 2, PartitionName: "b", Config: DefaultZoneConfig()})
	if e, a := 5, len(zone.Subzones); e != a {
		t.Fatalf("expected %d subzones, but found %d", e, a)
	}
	if err := zone.Validate(); err != nil {
		t.Errorf("expected zone validation to succeed, but got %s", err)
	}

	zone.DeleteIndexSubzones(2)
	if e, a := 2, len(zone.Subzones); e != a {
		t.Fatalf("expected %d subzones, but found %d", e, a)
	}
	for _, subzone := range zone.Subzones {
		if subzone.IndexID == 2 {
			t.Fatalf("expected no subzones to refer to index 2, but found %+v", subzone)
		}
	}
	if err := zone.Validate(); err != nil {
		t.Errorf("expected zone validation to succeed, but got %s", err)
	}
}

// TestZoneConfigMarshalYAML makes sure that ZoneConfig is correctly marshaled
// to YAML and back.
func TestZoneConfigMarshalYAML(t *testing.T) {
	defer leaktest.AfterTest(t)()

	original := ZoneConfig{
		RangeMinBytes: proto.Int64(1),
		RangeMaxBytes: proto.Int64(1),
		GC: &GCPolicy{
			TTLSeconds: 1,
		},
		NumReplicas: proto.Int32(1),
	}

	testCases := []struct {
		constraints      []ConstraintsConjunction
		leasePreferences []LeasePreference
		expected         string
	}{
		{
			expected: `range_min_bytes: 1
range_max_bytes: 1
gc:
  ttlseconds: 1
num_replicas: 1
constraints: []
lease_preferences: []
`,
		},
		{
			constraints: []ConstraintsConjunction{
				{
					Constraints: []Constraint{
						{
							Type:  Constraint_REQUIRED,
							Key:   "duck",
							Value: "foo",
						},
					},
				},
			},
			expected: `range_min_bytes: 1
range_max_bytes: 1
gc:
  ttlseconds: 1
num_replicas: 1
constraints: [+duck=foo]
lease_preferences: []
`,
		},
		{
			constraints: []ConstraintsConjunction{
				{
					Constraints: []Constraint{
						{
							Type:  Constraint_DEPRECATED_POSITIVE,
							Value: "foo",
						},
						{
							Type:  Constraint_REQUIRED,
							Key:   "duck",
							Value: "foo",
						},
						{
							Type:  Constraint_PROHIBITED,
							Key:   "duck",
							Value: "foo",
						},
					},
				},
			},
			expected: `range_min_bytes: 1
range_max_bytes: 1
gc:
  ttlseconds: 1
num_replicas: 1
constraints: [foo, +duck=foo, -duck=foo]
lease_preferences: []
`,
		},
		{
			constraints: []ConstraintsConjunction{
				{
					NumReplicas: 3,
					Constraints: []Constraint{
						{
							Type:  Constraint_REQUIRED,
							Key:   "duck",
							Value: "foo",
						},
					},
				},
			},
			expected: `range_min_bytes: 1
range_max_bytes: 1
gc:
  ttlseconds: 1
num_replicas: 1
constraints: {+duck=foo: 3}
lease_preferences: []
`,
		},
		{
			constraints: []ConstraintsConjunction{
				{
					NumReplicas: 3,
					Constraints: []Constraint{
						{
							Type:  Constraint_DEPRECATED_POSITIVE,
							Value: "foo",
						},
						{
							Type:  Constraint_REQUIRED,
							Key:   "duck",
							Value: "foo",
						},
						{
							Type:  Constraint_PROHIBITED,
							Key:   "duck",
							Value: "foo",
						},
					},
				},
			},
			expected: `range_min_bytes: 1
range_max_bytes: 1
gc:
  ttlseconds: 1
num_replicas: 1
constraints: {'foo,+duck=foo,-duck=foo': 3}
lease_preferences: []
`,
		},
		{
			constraints: []ConstraintsConjunction{
				{
					NumReplicas: 1,
					Constraints: []Constraint{
						{
							Type:  Constraint_REQUIRED,
							Key:   "duck",
							Value: "bar1",
						},
						{
							Type:  Constraint_REQUIRED,
							Key:   "duck",
							Value: "bar2",
						},
					},
				},
				{
					NumReplicas: 2,
					Constraints: []Constraint{
						{
							Type:  Constraint_REQUIRED,
							Key:   "duck",
							Value: "foo",
						},
					},
				},
			},
			expected: `range_min_bytes: 1
range_max_bytes: 1
gc:
  ttlseconds: 1
num_replicas: 1
constraints: {'+duck=bar1,+duck=bar2': 1, +duck=foo: 2}
lease_preferences: []
`,
		},
		{
			leasePreferences: []LeasePreference{},
			expected: `range_min_bytes: 1
range_max_bytes: 1
gc:
  ttlseconds: 1
num_replicas: 1
constraints: []
lease_preferences: []
`,
		},
		{
			leasePreferences: []LeasePreference{
				{
					Constraints: []Constraint{
						{
							Type:  Constraint_REQUIRED,
							Key:   "duck",
							Value: "foo",
						},
					},
				},
			},
			expected: `range_min_bytes: 1
range_max_bytes: 1
gc:
  ttlseconds: 1
num_replicas: 1
constraints: []
lease_preferences: [[+duck=foo]]
`,
		},
		{
			constraints: []ConstraintsConjunction{
				{
					Constraints: []Constraint{
						{
							Type:  Constraint_REQUIRED,
							Key:   "duck",
							Value: "foo",
						},
					},
				},
			},
			leasePreferences: []LeasePreference{
				{
					Constraints: []Constraint{
						{
							Type:  Constraint_REQUIRED,
							Key:   "duck",
							Value: "bar1",
						},
						{
							Type:  Constraint_REQUIRED,
							Key:   "duck",
							Value: "bar2",
						},
					},
				},
				{
					Constraints: []Constraint{
						{
							Type:  Constraint_PROHIBITED,
							Key:   "duck",
							Value: "foo",
						},
					},
				},
			},
			expected: `range_min_bytes: 1
range_max_bytes: 1
gc:
  ttlseconds: 1
num_replicas: 1
constraints: [+duck=foo]
lease_preferences: [[+duck=bar1, +duck=bar2], [-duck=foo]]
`,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			original.Constraints = tc.constraints
			original.LeasePreferences = tc.leasePreferences
			body, err := yaml.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			if string(body) != tc.expected {
				t.Fatalf("yaml.Marshal(%+v)\ngot:\n%s\nwant:\n%s", original, body, tc.expected)
			}

			var unmarshaled ZoneConfig
			if err := yaml.UnmarshalStrict(body, &unmarshaled); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(&unmarshaled, &original) {
				t.Errorf("yaml.UnmarshalStrict(%q)\ngot:\n%+v\nwant:\n%+v", body, unmarshaled, original)
			}
		})
	}
}

// TestExperimentalLeasePreferencesYAML makes sure that we accept the
// lease_preferences YAML field both with and without the "experimental_"
// prefix.
func TestExperimentalLeasePreferencesYAML(t *testing.T) {
	defer leaktest.AfterTest(t)()

	originalPrefs := []LeasePreference{
		{Constraints: []Constraint{{Value: "original", Type: Constraint_REQUIRED}}},
	}
	originalZone := ZoneConfig{
		LeasePreferences: originalPrefs,
	}

	testCases := []struct {
		input    string
		expected []LeasePreference
	}{
		{
			input:    "",
			expected: originalPrefs,
		},
		{
			input:    "lease_preferences: []",
			expected: []LeasePreference{},
		},
		{
			input:    "experimental_lease_preferences: []",
			expected: []LeasePreference{},
		},
		{
			input: "lease_preferences: [[+a=b]]",
			expected: []LeasePreference{
				{Constraints: []Constraint{{Key: "a", Value: "b", Type: Constraint_REQUIRED}}},
			},
		},
		{
			input: "experimental_lease_preferences: [[+a=b]]",
			expected: []LeasePreference{
				{Constraints: []Constraint{{Key: "a", Value: "b", Type: Constraint_REQUIRED}}},
			},
		},
		{
			input: "lease_preferences: [[+a=b],[-c=d]]",
			expected: []LeasePreference{
				{Constraints: []Constraint{{Key: "a", Value: "b", Type: Constraint_REQUIRED}}},
				{Constraints: []Constraint{{Key: "c", Value: "d", Type: Constraint_PROHIBITED}}},
			},
		},
		{
			input: "experimental_lease_preferences: [[+a=b],[-c=d]]",
			expected: []LeasePreference{
				{Constraints: []Constraint{{Key: "a", Value: "b", Type: Constraint_REQUIRED}}},
				{Constraints: []Constraint{{Key: "c", Value: "d", Type: Constraint_PROHIBITED}}},
			},
		},
		{
			input: "lease_preferences: [[+c=d]]\nexperimental_lease_preferences: [[+a=b]]",
			expected: []LeasePreference{
				{Constraints: []Constraint{{Key: "a", Value: "b", Type: Constraint_REQUIRED}}},
			},
		},
		{
			input: "experimental_lease_preferences: [[+a=b]]\nlease_preferences: [[+c=d]]",
			expected: []LeasePreference{
				{Constraints: []Constraint{{Key: "a", Value: "b", Type: Constraint_REQUIRED}}},
			},
		},
	}

	for _, tc := range testCases {
		zone := originalZone
		if err := yaml.UnmarshalStrict([]byte(tc.input), &zone); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(zone.LeasePreferences, tc.expected) {
			t.Errorf("unmarshaling %q got %+v; want %+v", tc.input, zone.LeasePreferences, tc.expected)
		}
	}
}

func TestConstraintsListYAML(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		input     string
		expectErr bool
	}{
		{},
		{input: "[]"},
		{input: "[+a]"},
		{input: "[+a, -b=2, +c=d, e]"},
		{input: "{+a: 1}"},
		{input: "{+a: 1, '+a=1,+b,+c=d': 2}"},
		{input: "{'+a: 1'}"},  // unfortunately this parses just fine because yaml autoconverts it to a list...
		{input: "{+a,+b: 1}"}, // this also parses fine but will fail ZoneConfig.Validate()
		{input: "{+a: 1, '+a=1,+b,+c=d': b}", expectErr: true},
		{input: "[+a: 1]", expectErr: true},
		{input: "[+a: 1, '+a=1,+b,+c=d': 2]", expectErr: true},
		{input: "{\"+a=1,+b=2\": 1}"},    // this will work in SQL: constraints='{"+a=1,+b=2": 1}'
		{input: "{\"+a=1,+b=2,+c\": 1}"}, // won't work in SQL: constraints='{"+a=1,+b=2,+c": 1}'
		{input: "{'+a=1,+b=2,+c': 1}"},   // this will work in SQL: constraints=e'{\'+a=1,+b=2,+c\': 1}'
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			var constraints ConstraintsList
			err := yaml.UnmarshalStrict([]byte(tc.input), &constraints)
			if err == nil && tc.expectErr {
				t.Errorf("expected error, but got constraints %+v", constraints)
			}
			if err != nil && !tc.expectErr {
				t.Errorf("expected success, but got %v", err)
			}
		})
	}
}

func TestMarshalableZoneConfigRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	original := NewPopulatedZoneConfig(
		rand.New(rand.NewSource(timeutil.Now().UnixNano())), false /* easy */)
	marshalable := zoneConfigToMarshalable(*original)
	roundTripped := zoneConfigFromMarshalable(marshalable, *original)

	if !reflect.DeepEqual(roundTripped, *original) {
		t.Errorf("round-tripping a ZoneConfig through a marshalableZoneConfig failed:\noriginal:\n%+v\nmarshable:\n%+v\ngot:\n%+v", original, marshalable, roundTripped)
	}
}

func TestZoneSpecifiers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Simulate exactly two named zones: one named default and one named carl.
	// N.B. DefaultZoneName must always exist in the mapping; it is treated
	// specially so that it always appears first in the lookup path.
	defer func(old map[string]uint32) { NamedZones = old }(NamedZones)
	NamedZones = map[string]uint32{
		DefaultZoneName: 0,
		"carl":          42,
	}
	defer func(old map[uint32]string) { NamedZonesByID = old }(NamedZonesByID)
	NamedZonesByID = map[uint32]string{
		0:  DefaultZoneName,
		42: "carl",
	}

	// Simulate the following schema:
	//   CREATE DATABASE db;   CREATE TABLE db.tbl ...
	//   CREATE DATABASE carl; CREATE TABLE carl.toys ...
	type namespaceEntry struct {
		parentID uint32
		name     string
	}
	namespace := map[namespaceEntry]uint32{
		{0, "db"}:               50,
		{50, "tbl"}:             51,
		{0, "carl"}:             55,
		{55, "toys"}:            56,
		{9000, "broken_parent"}: 57,
	}
	resolveName := func(parentID uint32, name string) (uint32, error) {
		key := namespaceEntry{parentID, name}
		if id, ok := namespace[key]; ok {
			return id, nil
		}
		return 0, fmt.Errorf("%q not found", name)
	}
	resolveID := func(id uint32) (parentID uint32, name string, err error) {
		for entry, entryID := range namespace {
			if id == entryID {
				return entry.parentID, entry.name, nil
			}
		}
		return 0, "", fmt.Errorf("%d not found", id)
	}

	for _, tc := range []struct {
		specifier tree.ZoneSpecifier
		id        int
		err       string
	}{
		{tree.ZoneSpecifier{NamedZone: "default"}, 0, ""},
		{tree.ZoneSpecifier{NamedZone: "carl"}, 42, ""},
		{tree.ZoneSpecifier{NamedZone: "foo"}, -1, `"foo" is not a built-in zone`},
		{tree.ZoneSpecifier{Database: "db"}, 50, ""},
		{tree.ZoneSpecifier{NamedZone: "db"}, -1, `"db" is not a built-in zone`},
		{tableSpecifier("db", "tbl", "", ""), 51, ""},
		{tableSpecifier("db", "tbl", "", "prt"), 51, ""},
		{tableSpecifier("db", "tbl", "primary", ""), 51, ""},
		{tableSpecifier("db", "tbl", "idx", ""), 51, ""},
		{tableSpecifier("db", "tbl", "idx", "prt"), 51, ""},
		{tree.ZoneSpecifier{Database: "tbl"}, -1, `"tbl" not found`},
		{tree.ZoneSpecifier{Database: "carl"}, 55, ""},
		{tableSpecifier("carl", "toys", "", ""), 56, ""},
		{tableSpecifier("carl", "love", "", ""), -1, `"love" not found`},
	} {
		t.Run(fmt.Sprintf("resolve-specifier=%s", tc.specifier.String()), func(t *testing.T) {
			err := func() error {
				id, err := ResolveZoneSpecifier(&tc.specifier, resolveName)
				if err != nil {
					return err
				}
				if e, a := tc.id, int(id); a != e {
					t.Errorf("path %d did not match expected path %d", a, e)
				}
				return nil
			}()
			if !testutils.IsError(err, tc.err) {
				t.Errorf("expected error matching %q, but got %v", tc.err, err)
			}
		})
	}

	for _, tc := range []struct {
		id     uint32
		target string
		err    string
	}{
		{0, "RANGE default", ""},
		{41, "", "41 not found"},
		{42, "RANGE carl", ""},
		{50, "DATABASE db", ""},
		{51, "TABLE db.public.tbl", ""},
		{55, "DATABASE carl", ""},
		{56, "TABLE carl.public.toys", ""},
		{57, "", "9000 not found"},
		{58, "", "58 not found"},
	} {
		t.Run(fmt.Sprintf("resolve-id=%d", tc.id), func(t *testing.T) {
			zs, err := ZoneSpecifierFromID(tc.id, resolveID)
			if !testutils.IsError(err, tc.err) {
				t.Errorf("unable to lookup ID %d: %s", tc.id, err)
			}
			if tc.err != "" {
				return
			}
			if e, a := tc.target, zs.String(); e != a {
				t.Errorf("expected %q zone name for ID %d, but got %q", e, tc.id, a)
			}
		})
	}
}

func tableSpecifier(
	db tree.Name, tbl tree.Name, idx tree.UnrestrictedName, partition tree.Name,
) tree.ZoneSpecifier {
	return tree.ZoneSpecifier{
		TableOrIndex: tree.TableIndexName{
			Table: tree.MakeTableName(db, tbl),
			Index: idx,
		},
		Partition: partition,
	}
}

// TestDefaultRangeSizesAreSane is a sanity check test to ensure that the values
// in the default zone configs are what the author intended.
func TestDefaultRangeSizesAreSane(t *testing.T) {
	require.Regexp(t, "range_min_bytes:134217728 range_max_bytes:536870912",
		DefaultSystemZoneConfigRef().String())
	require.Regexp(t, "range_min_bytes:134217728 range_max_bytes:536870912",
		DefaultZoneConfigRef().String())
}
