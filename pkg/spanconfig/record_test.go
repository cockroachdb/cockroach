// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfig

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

// TestRecordSystemTargetValidation checks that a Record with SystemTarget is
// validated on construction.
func TestRecordSystemTargetValidation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fn          func(scfg *roachpb.SpanConfig)
		expectedErr string
	}{
		{
			"range-min-bytes",
			func(scfg *roachpb.SpanConfig) {
				scfg.RangeMinBytes = 1
			},
			"RangeMinBytes set on system span config",
		},
		{
			"range-max-bytes",
			func(scfg *roachpb.SpanConfig) {
				scfg.RangeMaxBytes = 1
			},
			"RangeMaxBytes set on system span config",
		},
		{
			"gcttl",
			func(scfg *roachpb.SpanConfig) {
				scfg.GCPolicy.TTLSeconds = 1
			},
			"TTLSeconds set on system span config",
		},
		{
			"ignore-strict-gc",
			func(scfg *roachpb.SpanConfig) {
				scfg.GCPolicy.IgnoreStrictEnforcement = true
			},
			"IgnoreStrictEnforcement set on system span config",
		},
		{
			"global-reads",
			func(scfg *roachpb.SpanConfig) {
				scfg.GlobalReads = true
			},
			"GlobalReads set on system span config",
		},
		{
			"num-replicas",
			func(scfg *roachpb.SpanConfig) {
				scfg.NumReplicas = 1
			},
			"NumReplicas set on system span config",
		},
		{
			"num-voters",
			func(scfg *roachpb.SpanConfig) {
				scfg.NumVoters = 1
			},
			"NumVoters set on system span config",
		},
		{
			"constraints",
			func(scfg *roachpb.SpanConfig) {
				scfg.Constraints = append(scfg.Constraints, roachpb.ConstraintsConjunction{})
			},
			"Constraints set on system span config",
		},
		{
			"voter-constraints",
			func(scfg *roachpb.SpanConfig) {
				scfg.VoterConstraints = append(scfg.VoterConstraints, roachpb.ConstraintsConjunction{})
			},
			"VoterConstraints set on system span config",
		},
		{
			"lease-preferences",
			func(scfg *roachpb.SpanConfig) {
				scfg.LeasePreferences = append(scfg.LeasePreferences, roachpb.LeasePreference{})
			},
			"LeasePreferences set on system span config",
		},
		{
			"rangefeed-enabled",
			func(scfg *roachpb.SpanConfig) {
				scfg.RangefeedEnabled = true
			},
			"RangefeedEnabled set on system span config",
		},
		{
			"exclude-data-from-backup",
			func(scfg *roachpb.SpanConfig) {
				scfg.ExcludeDataFromBackup = true
			},
			"ExcludeDataFromBackup set on system span config",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			emptyScfg := roachpb.SpanConfig{}
			systemTarget := TestingMakeTenantKeyspaceTargetOrFatal(t, roachpb.MustMakeTenantID(2),
				roachpb.MustMakeTenantID(2))
			target := MakeTargetFromSystemTarget(systemTarget)
			_, err := MakeRecord(target, emptyScfg)
			testutils.IsError(err, tc.expectedErr)
		})
	}
}
