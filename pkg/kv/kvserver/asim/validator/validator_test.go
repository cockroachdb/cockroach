// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package validator

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/stretchr/testify/require"
)

// TestValidator validates the correctness of span configuration satisfiability
// check in Validator.
func TestValidator(t *testing.T) {
	zoneToRegion, zone, region, total := processClusterInfo(state.ComplexConfig.Regions)
	// ComplexConfig Topology:
	// EU
	//  EU_1
	//  │ └── [19 20 21]
	//  EU_2
	//  │ └── [22 23 24]
	//  EU_3
	//  │ └── [25 26 27 28]
	// US_East
	//  US_East_1
	//  │ └── [1]
	//  US_East_2
	//  │ └── [2 3]
	//  US_East_3
	//  │ └── [4 5 6]
	//  US_East_4
	//  │ └── [7 8 9 10 11 12 13 14 15 16]
	// US_West
	//  US_West_1
	//    └── [17 18]
	testCases := []struct {
		description         string
		constraint          string
		expectedSuccess     bool
		expectedErrorMsgStr string
	}{
		{
			description:         "straightforward valid configuration",
			constraint:          "num_replicas=2 num_voters=1",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description: "straightforward valid configuration",
			constraint: "num_replicas=5 num_voters=5 " +
				"constraints={'+region=US_East':3,'+region=US_West':1,'+region=EU':1} " +
				"voter_constraints={'+region=US_East':3,'+region=US_West':1,'+region=EU':1}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description: "promotion to satisfy region voter constraint",
			constraint: "num_replicas=2 num_voters=2 " +
				"constraints={'+zone=US_West_1':2} voter_constraints={'+region=US_West':2}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description:         "promotion to satisfy cluster constraint",
			constraint:          "num_replicas=2 num_voters=2 constraints={'+zone=US_West_1':2}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description: "promoting some nonvoters to voters",
			constraint: "num_replicas=6 num_voters=3 constraints={'+zone=US_East_3':3} " +
				"voter_constraints={'+region=US_East':3,'+zone=US_East_2':2}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description: "promoting some nonvoters + add voters + add nonvoters",
			constraint: "num_replicas=15 num_voters=6 " +
				"constraints={'+zone=US_East_4':10,'+region=EU':3,'+region=US_East':11} " +
				"voter_constraints={'+region=US_East':3,'+zone=US_East_3':1,'+zone=US_West_1':1}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description:         "satisfying zone constraint can help satisfy region constraint",
			constraint:          "num_replicas=2 constraints={'+zone=US_West_1':2,'+region=US_West':2}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description: "cluster is fully assigned by region constraints",
			constraint: "num_replicas=28 num_voters=28 " +
				"constraints={'+region=US_East':16,'+region=US_West':2,'+region=EU':10}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description: "cluster is fully assigned by region and zone constraints",
			constraint: "num_replicas=28 num_voters=28 " +
				"constraints={'+region=US_East':16,'+region=US_West':2,'+region=EU':10," +
				"'+zone=US_East_1':1,'+zone=US_East_2':2,'+zone=US_East_3':3,'+zone=US_East_4':10,'+zone=US_West_1':2," +
				"'+zone=EU_1':3,'+zone=EU_2':3,'+zone=EU_3':4} " +
				"voter_constraints={'+region=US_East':16,'+region=US_West':2,'+region=EU':10," +
				"'+zone=US_East_1':1,'+zone=US_East_2':2,'+zone=US_East_3':3,'+zone=US_East_4':10,'+zone=US_West_1':2," +
				"'+zone=EU_1':3,'+zone=EU_2':3,'+zone=EU_3':4}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description: "having unconstrained replicas + unconstrained voters",
			constraint: "num_replicas=28 num_voters=25 " +
				"constraints={'+region=US_East':2} voter_constraints={'+region=US_East':2}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description:         "having unconstrained replicas + fully constrained voters",
			constraint:          "num_replicas=27 num_voters=16 voter_constraints={'+region=US_East':16}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description: "having fully constrained replicas + unconstrained voters",
			constraint: "num_replicas=16 num_voters=3 " +
				"constraints={'+region=US_East':16,'+zone=US_East_1':1,'+zone=US_East_2':2} " +
				"voter_constraints={'+zone=US_East_4':3}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description: "can promote any replicas to voters at cluster level",
			constraint: "num_replicas=28 num_voters=3 " +
				"constraints={'+region=US_East':16,'+region=US_West':2,'+region=EU':10} " +
				"voter_constraints={'+region=EU':2}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description: "configuration for issue #106559",
			constraint: "num_replicas=6 num_voters=5 " +
				"constraints={'+zone=US_West_1':1,'+zone=EU_1':1,'+zone=US_East_2':2,'+zone=US_East_3':2} " +
				"voter_constraints={'+zone=US_West_1':1,'+zone=EU_1':1,'+zone=US_East_2':2,'+zone=US_East_3':1}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description: "configuration for issue #106559",
			constraint: "num_replicas=6 num_voters=5 " +
				"constraints={'+zone=US_West_1':1,'+zone=EU_1':1,'+zone=US_East_2':1,'+zone=US_East_3':1} " +
				"voter_constraints={'+zone=US_West_1':2,'+zone=US_East_2':2}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description:         "no voters or replicas needed to add for constraints",
			constraint:          "num_replicas=0 constraints={'+zone=US_East_1':0}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			description: "insufficient replicas for region constraint",
			constraint: "num_replicas=28 num_voters=28 " +
				"constraints={'+region=US_East':17,'+region=US_West':2,'+region=EU':10}",
			expectedSuccess:     false,
			expectedErrorMsgStr: "failed to satisfy constraints for region US_East",
		},
		{
			description: "insufficient replicas for cluster constraints",
			constraint: "num_replicas=16 num_voters=3 " +
				"constraints={'+region=US_East':16} voter_constraints={'+region=EU':2}",
			expectedSuccess:     false,
			expectedErrorMsgStr: "failed to satisfy constraints for cluster",
		},
		{
			description:         "more voters than replicas",
			constraint:          "num_replicas=1 num_voters=2",
			expectedSuccess:     false,
			expectedErrorMsgStr: "failed to satisfy constraints for cluster",
		},
		{
			description:         "too many replicas for cluster constraint",
			constraint:          "num_replicas=6 num_voters=2 constraints={'+region=US_East':16}",
			expectedSuccess:     false,
			expectedErrorMsgStr: "failed to satisfy constraints for cluster",
		},
		{
			description:         "too many voters for cluster constraint",
			constraint:          "num_replicas=20 num_voters=2 voter_constraints={'+region=US_East':16}",
			expectedSuccess:     false,
			expectedErrorMsgStr: "failed to satisfy constraints for cluster",
		},
		{
			description: "zero NumReplicas should use total num_replicas, num_voters for constraints",
			constraint: "num_replicas=5 num_voters=3 " +
				"constraints={'+region=US_East'} voter_constraints={'+region=US_West'}",
			expectedSuccess:     false,
			expectedErrorMsgStr: "failed to satisfy constraints for region US_West",
		},
		{
			description:         "unsupported constraint key",
			constraint:          "num_replicas=5 constraints={'+az=US_East'}",
			expectedSuccess:     false,
			expectedErrorMsgStr: "only zone and region constraint keys are supported",
		},
		{
			description:         "unsupported constraint value",
			constraint:          "num_replicas=5 num_voters=1 voter_constraints={'+region=CA':1}",
			expectedSuccess:     false,
			expectedErrorMsgStr: "region constraint value CA is not found in the cluster set up",
		},
		{
			description:         "unsupported constraint value",
			constraint:          "num_replicas=5 constraints={'+zone=CA':1}",
			expectedSuccess:     false,
			expectedErrorMsgStr: "zone constraint value CA is not found in the cluster set up",
		},
		{
			description:         "unsupported constraint type",
			constraint:          "num_replicas=5 constraints={'-region=US_West':1}",
			expectedSuccess:     false,
			expectedErrorMsgStr: "constraints marked as Constraint_PROHIBITED are unsupported",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			ma := newMockAllocator(zoneToRegion, zone, region, total)
			config := spanconfigtestutils.ParseZoneConfig(t, tc.constraint).AsSpanConfig()
			success, actualError := ma.isSatisfiable(config)
			require.Equal(t, tc.expectedSuccess, success)
			if tc.expectedErrorMsgStr == "" {
				require.Nil(t, actualError)
			} else {
				require.EqualError(t, actualError, tc.expectedErrorMsgStr)
			}
		})
	}
}
