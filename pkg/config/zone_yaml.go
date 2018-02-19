// Copyright 2018 The Cockroach Authors.
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
	fmt "fmt"
	"strconv"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

var _ yaml.Marshaler = Constraints{}
var _ yaml.Unmarshaler = &Constraints{}

// MarshalYAML implements yaml.Marshaler.
//
// We use two different formats here:
// 1. A legacy format when NumReplicas is zero:
//    [c1, c2, c3]
// 2. A per-replica format when NumReplicas is non-zero:
//    (c1,c2,c3):numReplicas
//
// There can be more than one of the latter form in a slice of Constraints,
// but not of the former.
func (c Constraints) MarshalYAML() (interface{}, error) {
	short := make([]string, len(c.Constraints))
	for i, c := range c.Constraints {
		short[i] = c.String()
	}

	// Print in the new format (i.e. "(c1,c2,c3):numReplicas") if NumReplicas is
	// non-zero, otherwise match the legacy format ([c1, c2, c3]) to avoid
	// confusing users. Note that the caller of this function has to be careful
	// to avoid creating double brackets around the legacy format, which would
	// break compatability.
	if c.NumReplicas != 0 {
		return fmt.Sprintf("(%s):%d", strings.Join(short, ","), c.NumReplicas), nil
	}
	return short, nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (c *Constraints) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	splitStr := strings.Split(str, ":")
	if len(splitStr) != 2 {
		return fmt.Errorf("no more than one separator ':' is allowed; got %q", str)
	}

	if !strings.HasPrefix(splitStr[0], "(") || !strings.HasSuffix(splitStr[0], ")") {
		return fmt.Errorf("constraints before separator ':' must be wrapped in parentheses; got %q", str)
	}
	constraintList := splitStr[0][1 : len(splitStr[0])-1]

	numReplicas, err := strconv.Atoi(splitStr[1])
	if err != nil {
		return fmt.Errorf("number of replicas for constraints %q must be an integer: %v", str, err)
	}

	shortConstraints := strings.Split(constraintList, ",")
	constraints := make([]Constraint, len(shortConstraints))
	for i, short := range shortConstraints {
		if err := constraints[i].FromString(short); err != nil {
			return err
		}
	}

	c.NumReplicas = int32(numReplicas)
	c.Constraints = constraints
	return nil
}

type ConstraintsList []Constraints

var _ yaml.Marshaler = ConstraintsList{}
var _ yaml.Unmarshaler = &ConstraintsList{}

// MarshalYAML implements yaml.Marshaler.
func (c ConstraintsList) MarshalYAML() (interface{}, error) {
	if len(c) != 1 || c[0].NumReplicas != 0 {
		return []Constraints(c), nil
	}
	return c[0].MarshalYAML()
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (c *ConstraintsList) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var strs []string
	if err := unmarshal(&strs); err != nil {
		return err
	}

	if len(strs) == 0 {
		return nil
	}

	// Check how many of the strings include a NumReplicas suffix.
	var includeNumReplicas int
	for _, str := range strs {
		splitStr := strings.Split(str, ":")
		switch len(splitStr) {
		case 1:
		case 2:
			includeNumReplicas++
		default:
			return fmt.Errorf("no more than one separator ':' is allowed; got %q", str)
		}
	}
	switch includeNumReplicas {
	case 0:
		constraints := make([]Constraint, len(strs))
		for i, short := range strs {
			if err := constraints[i].FromString(short); err != nil {
				return err
			}
		}
		*c = []Constraints{
			{
				Constraints: constraints,
				NumReplicas: 0,
			},
		}
	case len(strs):
		constraintsList := make([]Constraints, len(strs))
		for i, str := range strs {
			var constraints Constraints
			if err := yaml.Unmarshal([]byte(str), &constraints); err != nil {
				return err
			}
			constraintsList[i] = constraints
		}
		*c = constraintsList
	default:
		return fmt.Errorf(
			"either all constraints must specify a number of replicas, or none of them; got %d/%d: %v",
			includeNumReplicas, len(strs), strs)
	}
	return nil
}

// marshalableZoneConfig should be kept up-to-date with the real,
// auto-generated ZoneConfig type, but with []Constraints changed to
// ConstraintsList for backwards-compatible yaml marshaling and unmarshaling.
type marshalableZoneConfig struct {
	RangeMinBytes int64           `protobuf:"varint,2,opt,name=range_min_bytes,json=rangeMinBytes" json:"range_min_bytes" yaml:"range_min_bytes"`
	RangeMaxBytes int64           `protobuf:"varint,3,opt,name=range_max_bytes,json=rangeMaxBytes" json:"range_max_bytes" yaml:"range_max_bytes"`
	GC            GCPolicy        `protobuf:"bytes,4,opt,name=gc" json:"gc"`
	NumReplicas   int32           `protobuf:"varint,5,opt,name=num_replicas,json=numReplicas" json:"num_replicas" yaml:"num_replicas"`
	Constraints   ConstraintsList `protobuf:"bytes,6,rep,name=constraints" json:"constraints" yaml:"constraints,flow"`
	Subzones      []Subzone       `protobuf:"bytes,8,rep,name=subzones" json:"subzones" yaml:"-"`
	SubzoneSpans  []SubzoneSpan   `protobuf:"bytes,7,rep,name=subzone_spans,json=subzoneSpans" json:"subzone_spans" yaml:"-"`
}

func zoneConfigToMarshalable(c ZoneConfig) marshalableZoneConfig {
	var m marshalableZoneConfig
	m.RangeMinBytes = c.RangeMinBytes
	m.RangeMaxBytes = c.RangeMaxBytes
	m.GC = c.GC
	if c.NumReplicas != 0 {
		m.NumReplicas = c.NumReplicas
	}
	m.Constraints = ConstraintsList(c.Constraints)
	m.Subzones = c.Subzones
	m.SubzoneSpans = c.SubzoneSpans
	return m
}

func zoneConfigFromMarshalable(m marshalableZoneConfig) ZoneConfig {
	var c ZoneConfig
	c.RangeMinBytes = m.RangeMinBytes
	c.RangeMaxBytes = m.RangeMaxBytes
	c.GC = m.GC
	c.NumReplicas = m.NumReplicas
	c.Constraints = []Constraints(m.Constraints)
	c.Subzones = m.Subzones
	c.SubzoneSpans = m.SubzoneSpans
	return c
}

var _ yaml.Marshaler = ZoneConfig{}
var _ yaml.Unmarshaler = &ZoneConfig{}

// MarshalYAML implements yaml.Marshaler.
func (c ZoneConfig) MarshalYAML() (interface{}, error) {
	return zoneConfigToMarshalable(c), nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (c *ZoneConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// Pre-initialize aux with the contents of c. This is important for
	// maintaining the behavior of not overwriting existing fields unless the
	// user provided new values for them.
	aux := zoneConfigToMarshalable(*c)
	if err := unmarshal(&aux); err != nil {
		return err
	}
	*c = zoneConfigFromMarshalable(aux)
	return nil
}
