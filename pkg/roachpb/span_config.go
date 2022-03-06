// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"fmt"
	"strings"
	"time"
)

// StoreMatchesConstraint returns whether a store's attributes or node's
// locality match the constraint's spec. It notably ignores whether the
// constraint is required, prohibited, positive, or otherwise.
func StoreMatchesConstraint(store StoreDescriptor, c Constraint) bool {
	if c.Key == "" {
		for _, attrs := range []Attributes{store.Attrs, store.Node.Attrs} {
			for _, attr := range attrs.Attrs {
				if attr == c.Value {
					return true
				}
			}
		}
		return false
	}
	for _, tier := range store.Node.Locality.Tiers {
		if c.Key == tier.Key && c.Value == tier.Value {
			return true
		}
	}
	return false
}

var emptySpanConfig = &SpanConfig{}

// IsEmpty returns true if s is an empty SpanConfig.
func (s *SpanConfig) IsEmpty() bool {
	return s.Equal(emptySpanConfig)
}

// TTL returns the implies TTL as a time.Duration.
func (s *SpanConfig) TTL() time.Duration {
	return time.Duration(s.GCPolicy.TTLSeconds) * time.Second
}

// GetNumVoters returns the number of voting replicas as defined in the
// span config.
// TODO(arul): We can get rid of this now that we're correctly populating
//  numVoters when going from ZoneConfigs -> SpanConfigs.
func (s *SpanConfig) GetNumVoters() int32 {
	if s.NumVoters != 0 {
		return s.NumVoters
	}
	return s.NumReplicas
}

// GetNumNonVoters returns the number of non-voting replicas as defined in the
// span config.
func (s *SpanConfig) GetNumNonVoters() int32 {
	return s.NumReplicas - s.GetNumVoters()
}

func (c Constraint) String() string {
	var str string
	switch c.Type {
	case Constraint_REQUIRED:
		str += "+"
	case Constraint_PROHIBITED:
		str += "-"
	}
	if len(c.Key) > 0 {
		str += c.Key + "="
	}
	str += c.Value
	return str
}

func (c ConstraintsConjunction) String() string {
	var sb strings.Builder
	for i, cons := range c.Constraints {
		if i > 0 {
			sb.WriteRune(',')
		}
		sb.WriteString(cons.String())
	}
	if c.NumReplicas != 0 {
		fmt.Fprintf(&sb, ":%d", c.NumReplicas)
	}
	return sb.String()
}

// TestingDefaultSpanConfig exports the default span config for testing purposes.
func TestingDefaultSpanConfig() SpanConfig {
	return SpanConfig{
		RangeMinBytes: 128 << 20, // 128 MB
		RangeMaxBytes: 512 << 20, // 512 MB
		GCPolicy: GCPolicy{
			TTLSeconds: 25 * 60 * 60,
		},
		NumReplicas: 3,
	}
}

// TestingSystemSpanConfig exports the system span config for testing purposes.
func TestingSystemSpanConfig() SpanConfig {
	config := TestingDefaultSpanConfig()
	config.NumReplicas = 5
	return config
}

// TestingDatabaseSystemSpanConfig exports the span config expected to be
// installed on system database for testing purposes. The provided bool switches
// between what's expected on the host vs. any secondary tenant.
func TestingDatabaseSystemSpanConfig(host bool) SpanConfig {
	config := TestingSystemSpanConfig()
	if !host {
		config = TestingDefaultSpanConfig()
	}
	config.RangefeedEnabled = true
	config.GCPolicy.IgnoreStrictEnforcement = true
	return config
}
