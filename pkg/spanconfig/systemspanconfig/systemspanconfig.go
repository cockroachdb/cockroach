// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package systemspanconfig

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// SystemSpanConfig is a system installed configuration that may apply to
// multiple spans.
type SystemSpanConfig struct {
	cfg roachpb.SpanConfig
}

// MakeSystemSpanConfig returns a SystemSpanConfig.
func MakeSystemSpanConfig(cfg roachpb.SpanConfig) error {
	s := SystemSpanConfig{cfg: cfg}
	return s.validate()
}

// validate ensures that only protection policies (GCPolicy.ProtectionPolicies)
// field is set on the underlying roachpb.SpanConfig.
func (s *SystemSpanConfig) validate() error {
	if s.cfg.RangeMinBytes != 0 {
		return errors.AssertionFailedf("RangeMinBytes set on system span config")
	}
	if s.cfg.RangeMaxBytes != 0 {
		return errors.AssertionFailedf("RangeMaxBytes set on system span config")
	}
	if s.cfg.GCPolicy.TTLSeconds != 0 {
		return errors.AssertionFailedf("TTLSeconds set on system span config")
	}
	if s.cfg.GCPolicy.IgnoreStrictEnforcement == true {
		return errors.AssertionFailedf("IgnoreStrictEnforcement set on system span config")
	}
	if s.cfg.GlobalReads == true {
		return errors.AssertionFailedf("GlobalReads set on system span config")
	}
	if s.cfg.NumReplicas != 0 {
		return errors.AssertionFailedf("NumReplicas set on system span config")
	}
	if s.cfg.NumVoters != 0 {
		return errors.AssertionFailedf("NumVoters set on system span config")
	}
	if len(s.cfg.Constraints) != 0 {
		return errors.AssertionFailedf("Constraints set on system span config")
	}
	if len(s.cfg.VoterConstraints) != 0 {
		return errors.AssertionFailedf("VoterConstraints set on system span config")
	}
	if len(s.cfg.LeasePreferences) != 0 {
		return errors.AssertionFailedf("LeasePreferences set on system span config")
	}
	if s.cfg.RangefeedEnabled == true {
		return errors.AssertionFailedf("RangefeedEnabled set on system span config")
	}
	return nil
}
