// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigcatalog

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/errors"
)

var _ spanconfig.Config = (*ZoneConfig)(nil)

// ZoneConfig wraps a zonepb.ZoneConfig proto and implements the Config
// interface.
type ZoneConfig struct {
	zonepb.ZoneConfig
}

// NewZoneConfigFromProto returns a new ZoneConfig.
func NewZoneConfigFromProto(zone *zonepb.ZoneConfig) *ZoneConfig {
	return &ZoneConfig{
		*zone,
	}
}

// ZoneConfig implements the Config interface.
func (*ZoneConfig) Config() {}

// GenerateSpanConfig implements the Config interface.
// GenerateSpanConfig maps the underlying ZoneConfig proto into a SpanConfig
// proto.
func (z *ZoneConfig) GenerateSpanConfig() (roachpb.SpanConfig, error) {
	var sc roachpb.SpanConfig
	// Copy over the values.
	sc.RangeMinBytes = *z.RangeMinBytes
	sc.RangeMaxBytes = *z.RangeMaxBytes
	sc.GCTTL = z.GC.TTLSeconds

	// GlobalReads is false by default.
	sc.GlobalReads = false
	if z.GlobalReads != nil {
		sc.GlobalReads = *z.GlobalReads
	}
	sc.NumReplicas = *z.NumReplicas

	// NumVoters = NumReplicas if NumVoters is unset.
	sc.NumVoters = *z.NumReplicas
	if z.NumVoters != nil {
		sc.NumVoters = *z.NumVoters
	}

	translateConstraints := func(src []zonepb.Constraint, dest []roachpb.Constraint) error {
		for i, c := range src {
			switch c.Type {
			case zonepb.Constraint_REQUIRED:
				dest[i].Type = roachpb.Constraint_REQUIRED
			case zonepb.Constraint_PROHIBITED:
				dest[i].Type = roachpb.Constraint_PROHIBITED
			default:
				return errors.AssertionFailedf("unknown constraint type: %v", c.Type)
			}
			dest[i].Key = c.Key
			dest[i].Value = c.Value
		}
		return nil
	}

	translateConstraintsConjunction := func(src []zonepb.ConstraintsConjunction, dest []roachpb.ConstraintsConjunction) error {
		for i, constraint := range src {
			dest[i].NumReplicas = constraint.NumReplicas
			dest[i].Constraints = make([]roachpb.Constraint, len(constraint.Constraints))
			if err := translateConstraints(constraint.Constraints, dest[i].Constraints); err != nil {
				return err
			}
		}
		return nil
	}

	sc.Constraints = make([]roachpb.ConstraintsConjunction, len(z.Constraints))
	if err := translateConstraintsConjunction(z.Constraints, sc.Constraints); err != nil {
		return roachpb.SpanConfig{}, err
	}
	sc.VoterConstraints = make([]roachpb.ConstraintsConjunction, len(z.VoterConstraints))
	if err := translateConstraintsConjunction(z.VoterConstraints, sc.VoterConstraints); err != nil {
		return roachpb.SpanConfig{}, err
	}

	sc.LeasePreferences = make([]roachpb.LeasePreference, len(z.LeasePreferences))
	for i, leasePreference := range z.LeasePreferences {
		sc.LeasePreferences[i].Constraints = make([]roachpb.Constraint, len(leasePreference.Constraints))
		if err := translateConstraints(
			leasePreference.Constraints,
			sc.LeasePreferences[i].Constraints,
		); err != nil {
			return roachpb.SpanConfig{}, err
		}
	}
	return sc, nil
}
