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

// Equal compares two span config entries.
func (e SpanConfigEntry) Equal(other SpanConfigEntry) bool {
	return e.Span.Equal(other.Span) && e.Config.Equal(other.Config)
}

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
