// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestSpanConfigHasConfigurationChange(t *testing.T) {
	spanConfig1 := SpanConfig{
		RangeMinBytes: 1,
		RangeMaxBytes: 2,
		GCPolicy: GCPolicy{
			TTLSeconds: 10,
			ProtectionPolicies: []ProtectionPolicy{{
				ProtectedTimestamp: hlc.Timestamp{
					WallTime: 1,
					Logical:  1,
				},
				IgnoreIfExcludedFromBackup: false,
			}},
			IgnoreStrictEnforcement: false,
		},
		GlobalReads: false,
		NumReplicas: 3,
		NumVoters:   4,
		Constraints: []ConstraintsConjunction{{
			NumReplicas: 1,
			Constraints: []Constraint{{
				Type:  Constraint_REQUIRED,
				Key:   "a",
				Value: "b",
			}},
		}},
		VoterConstraints:      nil,
		LeasePreferences:      nil,
		RangefeedEnabled:      false,
		ExcludeDataFromBackup: false,
	}
	spanConfig1Copy := spanConfig1
	spanConfig2 := SpanConfig{
		RangeMinBytes: 1,
		RangeMaxBytes: 2,
		GCPolicy: GCPolicy{
			TTLSeconds: 10,
			ProtectionPolicies: []ProtectionPolicy{{
				ProtectedTimestamp: hlc.Timestamp{
					WallTime: 2,
					Logical:  2,
				},
				IgnoreIfExcludedFromBackup: false,
			}},
			IgnoreStrictEnforcement: false,
		},
		GlobalReads:           false,
		NumReplicas:           3,
		NumVoters:             4,
		Constraints:           nil,
		VoterConstraints:      nil,
		LeasePreferences:      nil,
		RangefeedEnabled:      false,
		ExcludeDataFromBackup: false,
	}
	require.NotEqual(t, spanConfig1, spanConfig2)
	require.True(t, spanConfig1.HasConfigurationChange(spanConfig2))
	// Now they are the same other than the PTS
	spanConfig2.Constraints = spanConfig1.Constraints
	require.False(t, spanConfig1.HasConfigurationChange(spanConfig2))
	// Ensure that HasConfigurationChange didn't change the spanConfig.
	require.Equal(t, spanConfig1, spanConfig1Copy)

}
