// Copyright 2018 The Cockroach Authors.
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
	"runtime/debug"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"gopkg.in/yaml.v2"
)

var _ yaml.Marshaler = LeasePreference{}
var _ yaml.Unmarshaler = &LeasePreference{}

// MarshalYAML implements yaml.Marshaler.
func (l LeasePreference) MarshalYAML() (interface{}, error) {
	short := make([]string, len(l.Constraints))
	for i, c := range l.Constraints {
		short[i] = c.String()
	}
	return short, nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (l *LeasePreference) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var shortConstraints []string
	if err := unmarshal(&shortConstraints); err != nil {
		return err
	}
	constraints := make([]Constraint, len(shortConstraints))
	for i, short := range shortConstraints {
		if err := constraints[i].FromString(short); err != nil {
			return err
		}
	}
	l.Constraints = constraints
	return nil
}

var _ yaml.Marshaler = ConstraintsConjunction{}
var _ yaml.Unmarshaler = &ConstraintsConjunction{}

// MarshalYAML implements yaml.Marshaler.
func (c ConstraintsConjunction) MarshalYAML() (interface{}, error) {
	return nil, fmt.Errorf(
		"MarshalYAML should never be called directly on Constraints (%v): %v", c, debug.Stack())
}

// UnmarshalYAML implements yaml.Marshaler.
func (c *ConstraintsConjunction) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return fmt.Errorf(
		"UnmarshalYAML should never be called directly on Constraints: %v", debug.Stack())
}

// ConstraintsList is an alias for a slice of Constraints that can be
// properly marshaled to/from YAML.
type ConstraintsList struct {
	Constraints []ConstraintsConjunction
	Inherited   bool
}

var _ yaml.Marshaler = ConstraintsList{}
var _ yaml.Unmarshaler = &ConstraintsList{}

// MarshalYAML implements yaml.Marshaler.
//
// We use two different formats here, dependent on whether per-replica
// constraints are being used in ConstraintsList:
// 1. A legacy format when there are 0 or 1 Constraints and NumReplicas is
//    zero:
//    [c1, c2, c3]
// 2. A per-replica format when NumReplicas is non-zero:
//    {"c1,c2,c3": numReplicas1, "c4,c5": numReplicas2}
func (c ConstraintsList) MarshalYAML() (interface{}, error) {
	// If per-replica Constraints aren't in use, marshal everything into a list
	// for compatibility with pre-2.0-style configs.
	if c.Inherited || len(c.Constraints) == 0 {
		return []string{}, nil
	}
	if len(c.Constraints) == 1 && c.Constraints[0].NumReplicas == 0 {
		short := make([]string, len(c.Constraints[0].Constraints))
		for i, constraint := range c.Constraints[0].Constraints {
			short[i] = constraint.String()
		}
		return short, nil
	}

	// Otherwise, convert into a map from Constraints to NumReplicas.
	constraintsMap := make(map[string]int32)
	for _, constraints := range c.Constraints {
		short := make([]string, len(constraints.Constraints))
		for i, constraint := range constraints.Constraints {
			short[i] = constraint.String()
		}
		constraintsMap[strings.Join(short, ",")] = constraints.NumReplicas
	}
	return constraintsMap, nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (c *ConstraintsList) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// Note that we're intentionally checking for err == nil here. This handles
	// unmarshaling the legacy Constraints format, which is just a list of
	// strings.
	var strs []string
	c.Inherited = true
	if err := unmarshal(&strs); err == nil {
		constraints := make([]Constraint, len(strs))
		for i, short := range strs {
			if err := constraints[i].FromString(short); err != nil {
				return err
			}
		}
		if len(constraints) == 0 {
			c.Constraints = []ConstraintsConjunction{}
			c.Inherited = false
		} else {
			c.Constraints = []ConstraintsConjunction{
				{
					Constraints: constraints,
					NumReplicas: 0,
				},
			}
			c.Inherited = false
		}
		return nil
	}

	// Otherwise, the input must be a map that can be converted to per-replica
	// constraints.
	constraintsMap := make(map[string]int32)
	if err := unmarshal(&constraintsMap); err != nil {
		return errors.New(
			"invalid constraints format. expected an array of strings or a map of strings to ints")
	}

	constraintsList := make([]ConstraintsConjunction, 0, len(constraintsMap))
	for constraintsStr, numReplicas := range constraintsMap {
		shortConstraints := strings.Split(constraintsStr, ",")
		constraints := make([]Constraint, len(shortConstraints))
		for i, short := range shortConstraints {
			if err := constraints[i].FromString(short); err != nil {
				return err
			}
		}
		constraintsList = append(constraintsList, ConstraintsConjunction{
			Constraints: constraints,
			NumReplicas: numReplicas,
		})
	}

	// Sort the resulting list for reproducible orderings in tests.
	sort.Slice(constraintsList, func(i, j int) bool {
		// First, try to find which Constraints sort first alphabetically in string
		// format, considering the shorter list lesser if they're otherwise equal.
		for k := range constraintsList[i].Constraints {
			if k >= len(constraintsList[j].Constraints) {
				return false
			}
			lStr := constraintsList[i].Constraints[k].String()
			rStr := constraintsList[j].Constraints[k].String()
			if lStr < rStr {
				return true
			}
			if lStr > rStr {
				return false
			}
		}
		if len(constraintsList[i].Constraints) < len(constraintsList[j].Constraints) {
			return true
		}
		// If they're completely equal and the same length, go by NumReplicas.
		return constraintsList[i].NumReplicas < constraintsList[j].NumReplicas
	})

	c.Constraints = constraintsList
	c.Inherited = false
	return nil
}

// marshalableZoneConfig should be kept up-to-date with the real,
// auto-generated ZoneConfig type, but with []Constraints changed to
// ConstraintsList for backwards-compatible yaml marshaling and unmarshaling.
// We also support parsing both lease_preferences (for v2.1+) and
// experimental_lease_preferences (for v2.0), copying both into the same proto
// field as needed.
//
// TODO(a-robinson,v2.2): Remove the experimental_lease_preferences field.
type marshalableZoneConfig struct {
	RangeMinBytes                *int64            `json:"range_min_bytes" yaml:"range_min_bytes"`
	RangeMaxBytes                *int64            `json:"range_max_bytes" yaml:"range_max_bytes"`
	GC                           *GCPolicy         `json:"gc"`
	GlobalReads                  *bool             `json:"global_reads" yaml:"global_reads"`
	NumReplicas                  *int32            `json:"num_replicas" yaml:"num_replicas"`
	NumVoters                    *int32            `json:"num_voters" yaml:"num_voters"`
	Constraints                  ConstraintsList   `json:"constraints" yaml:"constraints,flow"`
	VoterConstraints             ConstraintsList   `json:"voter_constraints" yaml:"voter_constraints,flow"`
	LeasePreferences             []LeasePreference `json:"lease_preferences" yaml:"lease_preferences,flow"`
	ExperimentalLeasePreferences []LeasePreference `json:"experimental_lease_preferences" yaml:"experimental_lease_preferences,flow,omitempty"`
	Subzones                     []Subzone         `json:"subzones" yaml:"-"`
	SubzoneSpans                 []SubzoneSpan     `json:"subzone_spans" yaml:"-"`
}

func zoneConfigToMarshalable(c ZoneConfig) marshalableZoneConfig {
	var m marshalableZoneConfig
	if c.RangeMinBytes != nil {
		m.RangeMinBytes = proto.Int64(*c.RangeMinBytes)
	}
	if c.RangeMaxBytes != nil {
		m.RangeMaxBytes = proto.Int64(*c.RangeMaxBytes)
	}
	if c.GC != nil {
		tempGC := *c.GC
		m.GC = &tempGC
	}
	if c.GlobalReads != nil {
		m.GlobalReads = proto.Bool(*c.GlobalReads)
	}
	if c.NumReplicas != nil && *c.NumReplicas != 0 {
		m.NumReplicas = proto.Int32(*c.NumReplicas)
	}
	m.Constraints = ConstraintsList{c.Constraints, c.InheritedConstraints}
	if c.NumVoters != nil && *c.NumVoters != 0 {
		m.NumVoters = proto.Int32(*c.NumVoters)
	}
	// NB: In order to preserve round-trippability, we're directly using
	// `NullVoterConstraintsIsEmpty` as opposed to calling
	// `c.InheritedVoterConstraints()`. This is copacetic as long as the value is
	// unmarshalled correctly in zoneConfigFromMarshalable().
	m.VoterConstraints = ConstraintsList{c.VoterConstraints, !c.NullVoterConstraintsIsEmpty}
	if !c.InheritedLeasePreferences {
		m.LeasePreferences = c.LeasePreferences
	}
	// We intentionally do not round-trip ExperimentalLeasePreferences. We never
	// want to return yaml containing it.
	m.Subzones = c.Subzones
	m.SubzoneSpans = c.SubzoneSpans
	return m
}

// zoneConfigFromMarshalable returns a ZoneConfig from the marshaled struct
// NOTE: The config passed in the parameter is used so we can determine keep
// the original value of the InheritedLeasePreferences field in the output.
func zoneConfigFromMarshalable(m marshalableZoneConfig, c ZoneConfig) ZoneConfig {
	if m.RangeMinBytes != nil {
		c.RangeMinBytes = proto.Int64(*m.RangeMinBytes)
	}
	if m.RangeMaxBytes != nil {
		c.RangeMaxBytes = proto.Int64(*m.RangeMaxBytes)
	}
	if m.GC != nil {
		tempGC := *m.GC
		c.GC = &tempGC
	}
	if m.GlobalReads != nil {
		c.GlobalReads = proto.Bool(*m.GlobalReads)
	}
	if m.NumReplicas != nil {
		c.NumReplicas = proto.Int32(*m.NumReplicas)
	}
	c.Constraints = m.Constraints.Constraints
	c.InheritedConstraints = m.Constraints.Inherited
	if m.NumVoters != nil {
		c.NumVoters = proto.Int32(*m.NumVoters)
	}
	c.VoterConstraints = m.VoterConstraints.Constraints
	c.NullVoterConstraintsIsEmpty = !m.VoterConstraints.Inherited
	if m.LeasePreferences != nil {
		c.LeasePreferences = m.LeasePreferences
	}

	// Prefer a provided m.ExperimentalLeasePreferences value over whatever is in
	// m.LeasePreferences, since we know that m.ExperimentalLeasePreferences can
	// only possibly come from the user-specified input, whereas
	// m.LeasePreferences could be the old value of the field retrieved from
	// internal storage that the user is now trying to overwrite.
	if m.ExperimentalLeasePreferences != nil {
		c.LeasePreferences = m.ExperimentalLeasePreferences
	}

	if m.LeasePreferences != nil || m.ExperimentalLeasePreferences != nil {
		c.InheritedLeasePreferences = false
	}
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
	*c = zoneConfigFromMarshalable(aux, *c)
	return nil
}
