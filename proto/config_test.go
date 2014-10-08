// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package proto

import (
	"bytes"
	"reflect"
	"testing"
)

func TestAttributesIsSubset(t *testing.T) {
	a := Attributes{Attrs: []string{"a", "b", "c"}}
	b := Attributes{Attrs: []string{"a", "b"}}
	c := Attributes{Attrs: []string{"a"}}
	if !b.IsSubset(a) {
		t.Errorf("expected %+v to be a subset of %+v", b, a)
	}
	if !c.IsSubset(a) {
		t.Errorf("expected %+v to be a subset of %+v", c, a)
	}
	if !c.IsSubset(b) {
		t.Errorf("expected %+v to be a subset of %+v", c, b)
	}
	if a.IsSubset(b) {
		t.Errorf("%+v should not be a subset of %+v", a, b)
	}
	if a.IsSubset(c) {
		t.Errorf("%+v should not be a subset of %+v", a, c)
	}
	if b.IsSubset(c) {
		t.Errorf("%+v should not be a subset of %+v", b, c)
	}
}

func TestAttributesSortedString(t *testing.T) {
	a := Attributes{Attrs: []string{"a", "b", "c"}}
	if a.SortedString() != "a,b,c" {
		t.Errorf("sorted string of %+v (%s) != \"a,b,c\"", a, a.SortedString())
	}
	b := Attributes{Attrs: []string{"c", "a", "b"}}
	if b.SortedString() != "a,b,c" {
		t.Errorf("sorted string of %+v (%s) != \"a,b,c\"", b, b.SortedString())
	}
	// Duplicates.
	c := Attributes{Attrs: []string{"c", "c", "a", "a", "b", "b"}}
	if c.SortedString() != "a,b,c" {
		t.Errorf("sorted string of %+v (%s) != \"a,b,c\"", c, c.SortedString())
	}
}

// TestRangeDescriptorContains verifies methods to check whether a key
// or key range is contained within the range.
func TestRangeDescriptorContains(t *testing.T) {
	desc := RangeDescriptor{}
	desc.StartKey = []byte("a")
	desc.EndKey = []byte("b")

	testData := []struct {
		start, end []byte
		contains   bool
	}{
		// Single keys.
		{[]byte("a"), []byte("a"), true},
		{[]byte("aa"), []byte("aa"), true},
		{[]byte("`"), []byte("`"), false},
		{[]byte("b"), []byte("b"), false},
		{[]byte("c"), []byte("c"), false},
		// Key ranges.
		{[]byte("a"), []byte("b"), true},
		{[]byte("a"), []byte("aa"), true},
		{[]byte("aa"), []byte("b"), true},
		{[]byte("0"), []byte("9"), false},
		{[]byte("`"), []byte("a"), false},
		{[]byte("b"), []byte("bb"), false},
		{[]byte("0"), []byte("bb"), false},
		{[]byte("aa"), []byte("bb"), false},
	}
	for _, test := range testData {
		if bytes.Compare(test.start, test.end) == 0 {
			if desc.ContainsKey(test.start) != test.contains {
				t.Errorf("expected key %q within range", test.start)
			}
		} else {
			if desc.ContainsKeyRange(test.start, test.end) != test.contains {
				t.Errorf("expected key range %q-%q within range", test.start, test.end)
			}
		}
	}
}

var testConfig = ZoneConfig{
	ReplicaAttrs: []Attributes{
		Attributes{Attrs: []string{"a", "ssd"}},
		Attributes{Attrs: []string{"a", "hdd"}},
		Attributes{Attrs: []string{"b", "ssd"}},
		Attributes{Attrs: []string{"b", "hdd"}},
	},
	RangeMinBytes: 1 << 20,
	RangeMaxBytes: 64 << 20,
}

var yamlConfig = `
replicas:
  - attrs: [a, ssd]
  - attrs: [a, hdd]
  - attrs: [b, ssd]
  - attrs: [b, hdd]
range_min_bytes: 1048576
range_max_bytes: 67108864
`

func TestZoneConfigJSONRoundTrip(t *testing.T) {
	js, err := testConfig.ToJSON()
	if err != nil {
		t.Fatalf("failed converting to json: %v", err)
	}
	parsedZoneConfig, err := ZoneConfigFromJSON(json)
	if err != nil {
		t.Errorf("failed parsing config: %v", err)
	}
	if !reflect.DeepEqual(testConfig, *parsedZoneConfig) {
		t.Errorf("json round trip configs differ.\nOriginal: %+v\nParse: %+v\n", testConfig, parsedZoneConfig)
	}
}

func TestZoneConfigYAMLRoundTrip(t *testing.T) {
	yaml, err := testConfig.ToYAML()
	if err != nil {
		t.Fatalf("failed converting to yaml: %v", err)
	}
	parsedZoneConfig, err := ZoneConfigFromYAML(yaml)
	if err != nil {
		t.Errorf("failed parsing config: %v", err)
	}
	if !reflect.DeepEqual(testConfig, *parsedZoneConfig) {
		t.Errorf("yaml round trip configs differ.\nOriginal: %+v\nParse: %+v\n", testConfig, parsedZoneConfig)
	}
}

func TestZoneConfigYAMLParsing(t *testing.T) {
	parsedZoneConfig, err := ZoneConfigFromYAML([]byte(yamlConfig))
	if err != nil {
		t.Fatalf("failed parsing config: %v", err)
	}
	if !reflect.DeepEqual(testConfig, *parsedZoneConfig) {
		t.Errorf("yaml round trip configs differ.\nOriginal: %+v\nParse: %+v\n", testConfig, parsedZoneConfig)
	}
}

func TestPermConfig(t *testing.T) {
	p := &PermConfig{
		Read:  []string{"foo", "bar", "baz"},
		Write: []string{"foo", "baz"},
	}
	for _, u := range p.Read {
		if !p.CanRead(u) {
			t.Errorf("expected read permission for %q", u)
		}
	}
	if p.CanRead("bad") {
		t.Errorf("unexpected read access for user \"bad\"")
	}
	for _, u := range p.Write {
		if !p.CanWrite(u) {
			t.Errorf("expected read permission for %q", u)
		}
	}
	if p.CanWrite("bar") {
		t.Errorf("unexpected read access for user \"bar\"")
	}
}
