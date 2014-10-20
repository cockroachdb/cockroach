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
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package proto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	yaml "gopkg.in/yaml.v1"
)

// IsSubset returns whether attributes list b is a subset of
// attributes list a.
func (a Attributes) IsSubset(b Attributes) bool {
	m := map[string]struct{}{}
	for _, s := range b.Attrs {
		m[s] = struct{}{}
	}
	for _, s := range a.Attrs {
		if _, ok := m[s]; !ok {
			return false
		}
	}
	return true
}

// SortedString returns a sorted, de-duplicated, comma-separated list
// of the attributes.
func (a Attributes) SortedString() string {
	m := map[string]struct{}{}
	for _, s := range a.Attrs {
		m[s] = struct{}{}
	}
	var attrs []string
	for a := range m {
		attrs = append(attrs, a)
	}
	sort.Strings(attrs)
	return strings.Join(attrs, ",")
}

// ContainsKey returns whether this RangeDescriptor contains the specified key.
func (r *RangeDescriptor) ContainsKey(key []byte) bool {
	return bytes.Compare(key, r.StartKey) >= 0 && bytes.Compare(key, r.EndKey) < 0
}

// ContainsKeyRange returns whether this RangeDescriptor contains the specified
// key range from start to end.
func (r *RangeDescriptor) ContainsKeyRange(start, end []byte) bool {
	if len(end) == 0 {
		end = append(append([]byte(nil), start...), byte(0))
	}
	if bytes.Compare(end, start) < 0 {
		panic(fmt.Sprintf("start key is larger than end key %q > %q", string(start), string(end)))
	}
	return bytes.Compare(start, r.StartKey) >= 0 && bytes.Compare(r.EndKey, end) >= 0
}

// FindReplica returns the replica which matches the specified store
// ID. Panic in the event that no replica matches.
func (r *RangeDescriptor) FindReplica(storeID int32) *Replica {
	for i := range r.Replicas {
		if r.Replicas[i].StoreID == storeID {
			return &r.Replicas[i]
		}
	}
	panic(fmt.Sprintf("unable to find matching replica for store %d: %v", storeID, r.Replicas))
}

// CanRead does a linear search for user to verify read permission.
func (p *PermConfig) CanRead(user string) bool {
	for _, u := range p.Read {
		if u == user {
			return true
		}
	}
	return false
}

// CanWrite does a linear search for user to verify write permission.
func (p *PermConfig) CanWrite(user string) bool {
	for _, u := range p.Write {
		if u == user {
			return true
		}
	}
	return false
}

// AcctConfigFromJSON parses a JSON serialized AcctConfig.
func AcctConfigFromJSON(in []byte) (*AcctConfig, error) {
	a := &AcctConfig{}
	err := json.Unmarshal(in, a)
	return a, err
}

// PermConfigFromJSON parses a JSON serialized PermConfig.
func PermConfigFromJSON(in []byte) (*PermConfig, error) {
	p := &PermConfig{}
	err := json.Unmarshal(in, p)
	return p, err
}

// ZoneConfigFromJSON parses a JSON serialized ZoneConfig.
func ZoneConfigFromJSON(in []byte) (*ZoneConfig, error) {
	z := &ZoneConfig{}
	err := json.Unmarshal(in, z)
	return z, err
}

// ToJSON serializes a AcctConfig as "pretty", indented JSON.
func (a *AcctConfig) ToJSON() ([]byte, error) {
	return json.MarshalIndent(a, "", "  ")
}

// ToJSON serializes a PermConfig as "pretty", indented JSON.
func (p *PermConfig) ToJSON() ([]byte, error) {
	return json.MarshalIndent(p, "", "  ")
}

// ToJSON serializes a ZoneConfig as "pretty", indented JSON.
func (z *ZoneConfig) ToJSON() ([]byte, error) {
	return json.MarshalIndent(z, "", "  ")
}

// AcctConfigFromYAML parses a YAML serialized AcctConfig.
func AcctConfigFromYAML(in []byte) (*AcctConfig, error) {
	a := &AcctConfig{}
	err := yaml.Unmarshal(in, a)
	return a, err
}

// PermConfigFromYAML parses a YAML serialized PermConfig.
func PermConfigFromYAML(in []byte) (*PermConfig, error) {
	p := &PermConfig{}
	err := yaml.Unmarshal(in, p)
	return p, err
}

// ZoneConfigFromYAML parses a YAML serialized ZoneConfig.
func ZoneConfigFromYAML(in []byte) (*ZoneConfig, error) {
	z := &ZoneConfig{}
	err := yaml.Unmarshal(in, z)
	return z, err
}

var yamlXXXUnrecognizedRE = regexp.MustCompile(` *xxx_unrecognized: \[\]\n?`)

// sanitizeYAML filters lines in the input which match xxx_unrecognized, a
// truly-annoying public member of proto Message structs, which we
// cannot specify yaml output tags for.
// TODO(spencer): there's got to be a better way to do this.
func sanitizeYAML(b []byte) []byte {
	return yamlXXXUnrecognizedRE.ReplaceAll(b, []byte{})
}

// ToYAML serializes a AcctConfig as YAML.
func (a *AcctConfig) ToYAML() ([]byte, error) {
	b, err := yaml.Marshal(a)
	if err != nil {
		return b, err
	}
	return sanitizeYAML(b), nil
}

// ToYAML serializes a PermConfig as YAML.
func (p *PermConfig) ToYAML() ([]byte, error) {
	b, err := yaml.Marshal(p)
	if err != nil {
		return b, err
	}
	return sanitizeYAML(b), nil
}

// ToYAML serializes a ZoneConfig as YAML.
func (z *ZoneConfig) ToYAML() ([]byte, error) {
	b, err := yaml.Marshal(z)
	if err != nil {
		return b, err
	}
	return sanitizeYAML(b), nil
}
