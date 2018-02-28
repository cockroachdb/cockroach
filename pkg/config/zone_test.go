// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package config

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	proto "github.com/gogo/protobuf/proto"
	yaml "gopkg.in/yaml.v2"
)

func TestZoneConfigValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		cfg      ZoneConfig
		expected string
	}{
		{
			ZoneConfig{},
			"at least one replica is required",
		},
		{
			ZoneConfig{
				NumReplicas: -1,
			},
			"at least one replica is required",
		},
		{
			ZoneConfig{
				NumReplicas: 2,
			},
			"at least 3 replicas are required for multi-replica configurations",
		},
		{
			ZoneConfig{
				NumReplicas: 1,
			},
			"RangeMaxBytes 0 less than minimum allowed",
		},
		{
			ZoneConfig{
				NumReplicas:   1,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
			},
			"GC.TTLSeconds 0 less than minimum allowed",
		},
		{
			ZoneConfig{
				NumReplicas:   1,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            GCPolicy{TTLSeconds: 1},
			},
			"",
		},
		{
			ZoneConfig{
				NumReplicas:   1,
				RangeMinBytes: -1,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            GCPolicy{TTLSeconds: 1},
			},
			"RangeMinBytes -1 less than minimum allowed",
		},
		{
			ZoneConfig{
				NumReplicas:   1,
				RangeMinBytes: DefaultZoneConfig().RangeMaxBytes,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            GCPolicy{TTLSeconds: 1},
			},
			"is greater than or equal to RangeMaxBytes",
		},
		{
			ZoneConfig{
				NumReplicas:   1,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            GCPolicy{TTLSeconds: 1},
				Constraints: []Constraints{
					{Constraints: []Constraint{{Value: "a", Type: Constraint_DEPRECATED_POSITIVE}}},
				},
			},
			"constraints must either be required .+ or prohibited .+",
		},
		{
			ZoneConfig{
				NumReplicas:   1,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            GCPolicy{TTLSeconds: 1},
				Constraints: []Constraints{
					{Constraints: []Constraint{{Value: "a", Type: Constraint_PROHIBITED}}},
				},
			},
			"",
		},
		{
			ZoneConfig{
				NumReplicas:   1,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            GCPolicy{TTLSeconds: 1},
				Constraints: []Constraints{
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
				NumReplicas:   1,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            GCPolicy{TTLSeconds: 1},
				Constraints: []Constraints{
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
				NumReplicas:   3,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            GCPolicy{TTLSeconds: 1},
				Constraints: []Constraints{
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
				NumReplicas:   1,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            GCPolicy{TTLSeconds: 1},
				Constraints: []Constraints{
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
				NumReplicas:   3,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            GCPolicy{TTLSeconds: 1},
				Constraints: []Constraints{
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
				NumReplicas:   1,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            GCPolicy{TTLSeconds: 1},
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
				NumReplicas:   1,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            GCPolicy{TTLSeconds: 1},
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
				NumReplicas:   1,
				RangeMaxBytes: DefaultZoneConfig().RangeMaxBytes,
				GC:            GCPolicy{TTLSeconds: 1},
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

func TestZoneConfigSubzones(t *testing.T) {
	defer leaktest.AfterTest(t)()

	zone := DefaultZoneConfig()
	subzoneAInvalid := Subzone{IndexID: 1, PartitionName: "a", Config: ZoneConfig{}}
	subzoneA := Subzone{IndexID: 1, PartitionName: "a", Config: DefaultZoneConfig()}
	subzoneB := Subzone{IndexID: 1, PartitionName: "b", Config: DefaultZoneConfig()}

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
		Subzones: []Subzone{subzoneA, subzoneB},
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
		RangeMinBytes: 1,
		RangeMaxBytes: 1,
		GC: GCPolicy{
			TTLSeconds: 1,
		},
		NumReplicas: 1,
	}

	testCases := []struct {
		constraints      []Constraints
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
`,
		},
		{
			constraints: []Constraints{
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
`,
		},
		{
			constraints: []Constraints{
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
`,
		},
		{
			constraints: []Constraints{
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
`,
		},
		{
			constraints: []Constraints{
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
`,
		},
		{
			constraints: []Constraints{
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
experimental_lease_preferences: [[+duck=foo]]
`,
		},
		{
			constraints: []Constraints{
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
experimental_lease_preferences: [[+duck=bar1, +duck=bar2], [-duck=foo]]
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
	roundTripped := zoneConfigFromMarshalable(marshalable)

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
	//   CREATE DATABASE db;   CREATE TABLE db.table ...
	//   CREATE DATABASE ".";  CREATE TABLE ".".".table." ...
	//   CREATE DATABASE carl; CREATE TABLE carl.toys ...
	type namespaceEntry struct {
		parentID uint32
		name     string
	}
	namespace := map[namespaceEntry]uint32{
		{0, "db"}:               50,
		{50, "tbl"}:             51,
		{0, "user"}:             52,
		{0, "."}:                53,
		{53, ".table."}:         54,
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
		cliSpecifier string
		id           int
		err          string
	}{
		{".default", 0, ""},
		{".carl", 42, ""},
		{".foo", -1, `"foo" is not a built-in zone`},
		{"db", 50, ""},
		{".db", -1, `"db" is not a built-in zone`},
		{"db.tbl", 51, ""},
		{"db.tbl.prt", 51, ""},
		{`db.tbl@idx`, 51, ""},
		{`db.tbl.prt@idx`, -1, "index and partition cannot be specified simultaneously"},
		{"db.tbl.too.many.dots", 0, `malformed name: "db.tbl.too.many.dots"`},
		{`db.tbl@primary`, 51, ""},
		{"tbl", -1, `"tbl" not found`},
		{"table", -1, `malformed name: "table"`}, // SQL keyword; requires quotes
		{`"table"`, -1, `"table" not found`},
		{"user", -1, "malformed name: \"user\""}, // SQL keyword; requires quotes
		{`"user"`, 52, ""},
		{`"."`, 53, ""},
		{`.`, -1, `missing zone name`},
		{`".table."`, -1, `".table." not found`},
		{`".".".table."`, 54, ""},
		{`.table.`, -1, `"table." is not a built-in zone`},
		{"carl", 55, ""},
		{"carl.toys", 56, ""},
		{"carl.love", -1, `"love" not found`},
		{"; DROP DATABASE system", -1, `malformed name`},
	} {
		t.Run(fmt.Sprintf("parse-cli=%s", tc.cliSpecifier), func(t *testing.T) {
			err := func() error {
				zs, err := ParseCLIZoneSpecifier(tc.cliSpecifier)
				if err != nil {
					return err
				}
				id, err := ResolveZoneSpecifier(&zs, resolveName)
				if err != nil {
					return err
				}
				if e, a := tc.id, int(id); a != e {
					t.Errorf("path %d did not match expected path %d", a, e)
				}
				if e, a := tc.cliSpecifier, CLIZoneSpecifier(&zs); e != a {
					t.Errorf("expected %q to roundtrip, but got %q", e, a)
				}
				return nil
			}()
			if !testutils.IsError(err, tc.err) {
				t.Errorf("expected error matching %q, but got %v", tc.err, err)
			}
		})
	}

	for _, tc := range []struct {
		id           uint32
		cliSpecifier string
		err          string
	}{
		{0, ".default", ""},
		{41, "", "41 not found"},
		{42, ".carl", ""},
		{50, "db", ""},
		{51, "db.tbl", ""},
		{52, `"user"`, ""},
		{53, `"."`, ""},
		{54, `".".".table."`, ""},
		{55, "carl", ""},
		{56, "carl.toys", ""},
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
			if e, a := tc.cliSpecifier, CLIZoneSpecifier(&zs); e != a {
				t.Errorf("expected %q specifier for ID %d, but got %q", e, tc.id, a)
			}
		})
	}
}
