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

// String implements the stringer interface.
func (p ProtectionPolicy) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("{ts: %d", int(p.ProtectedTimestamp.WallTime)))
	if p.IgnoreIfExcludedFromBackup {
		sb.WriteString(fmt.Sprintf(",ignore_if_excluded_from_backup: %t",
			p.IgnoreIfExcludedFromBackup))
	}
	sb.WriteString("}")
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

// TestingDefaultSystemSpanConfiguration exports the default span config that
// applies to spanconfig.SystemTargets for testing purposes.
func TestingDefaultSystemSpanConfiguration() SpanConfig {
	return SpanConfig{}
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

// IsEntireKeyspaceTarget returns true if the receiver targets the entire
// keyspace.
func (st SystemSpanConfigTarget) IsEntireKeyspaceTarget() bool {
	return st.Type.GetEntireKeyspace() != nil
}

// IsSpecificTenantKeyspaceTarget returns true if the receiver targets a
// specific tenant's keyspace.
func (st SystemSpanConfigTarget) IsSpecificTenantKeyspaceTarget() bool {
	return st.Type.GetSpecificTenantKeyspace() != nil
}

// IsAllTenantKeyspaceTargetsSetTarget returns true if the receiver target
// encompasses all targets that have been set on specific tenant keyspaces
// by the system target source.
func (st SystemSpanConfigTarget) IsAllTenantKeyspaceTargetsSetTarget() bool {
	return st.Type.GetAllTenantKeyspaceTargetsSet() != nil
}

// NewEntireKeyspaceTargetType returns a system span config target type that
// targets the entire keyspace.
func NewEntireKeyspaceTargetType() *SystemSpanConfigTarget_Type {
	return &SystemSpanConfigTarget_Type{
		Type: &SystemSpanConfigTarget_Type_EntireKeyspace{
			EntireKeyspace: &SystemSpanConfigTarget_EntireKeyspace{},
		},
	}
}

// NewSpecificTenantKeyspaceTargetType returns a system span config target type
// that the given tenant ID's keyspace.
func NewSpecificTenantKeyspaceTargetType(tenantID TenantID) *SystemSpanConfigTarget_Type {
	return &SystemSpanConfigTarget_Type{
		Type: &SystemSpanConfigTarget_Type_SpecificTenantKeyspace{
			SpecificTenantKeyspace: &SystemSpanConfigTarget_TenantKeyspace{
				TenantID: tenantID,
			},
		},
	}
}

// NewAllTenantKeyspaceTargetsSetTargetType returns a read-only system span
// config target  type that encompasses all targets that have been set on
// specific tenant keyspaces.
func NewAllTenantKeyspaceTargetsSetTargetType() *SystemSpanConfigTarget_Type {
	return &SystemSpanConfigTarget_Type{
		Type: &SystemSpanConfigTarget_Type_AllTenantKeyspaceTargetsSet{
			AllTenantKeyspaceTargetsSet: &SystemSpanConfigTarget_AllTenantKeyspaceTargetsSet{},
		},
	}
}
