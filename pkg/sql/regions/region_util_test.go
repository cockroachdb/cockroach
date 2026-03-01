// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package regions

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/stretchr/testify/require"
)

func TestDistributeReplicasAcrossRegions(t *testing.T) {
	mkConstraint := func(region catpb.RegionName, n int32) zonepb.ConstraintsConjunction {
		return zonepb.ConstraintsConjunction{
			NumReplicas: n,
			Constraints: []zonepb.Constraint{MakeRequiredConstraintForRegion(region)},
		}
	}

	tests := []struct {
		name           string
		numReplicas    int32
		regions        catpb.RegionNames
		affinityRegion catpb.RegionName
		expected       []zonepb.ConstraintsConjunction
		expectErr      string
	}{{
		name:           "even distribution",
		numReplicas:    6,
		regions:        catpb.RegionNames{"us-east", "us-west", "eu-west"},
		affinityRegion: "us-east",
		expected: []zonepb.ConstraintsConjunction{
			mkConstraint("us-east", 2),
			mkConstraint("us-west", 2),
			mkConstraint("eu-west", 2),
		},
	}, {
		name:           "remainder goes to affinity region first",
		numReplicas:    5,
		regions:        catpb.RegionNames{"us-east", "us-west", "eu-west"},
		affinityRegion: "us-east",
		expected: []zonepb.ConstraintsConjunction{
			mkConstraint("us-east", 2),
			mkConstraint("us-west", 2),
			mkConstraint("eu-west", 1),
		},
	}, {
		name:           "two extras distributed to affinity then next",
		numReplicas:    5,
		regions:        catpb.RegionNames{"us-east", "us-west", "eu-west"},
		affinityRegion: "us-west",
		expected: []zonepb.ConstraintsConjunction{
			mkConstraint("us-east", 2),
			mkConstraint("us-west", 2),
			mkConstraint("eu-west", 1),
		},
	}, {
		name:           "single region",
		numReplicas:    5,
		regions:        catpb.RegionNames{"us-east"},
		affinityRegion: "us-east",
		expected: []zonepb.ConstraintsConjunction{
			mkConstraint("us-east", 5),
		},
	}, {
		name:           "more regions than replicas",
		numReplicas:    2,
		regions:        catpb.RegionNames{"us-east", "us-west", "eu-west", "ap-south"},
		affinityRegion: "us-west",
		expected: []zonepb.ConstraintsConjunction{
			mkConstraint("us-east", 1),
			mkConstraint("us-west", 1),
		},
	}, {
		name:           "zero replicas",
		numReplicas:    0,
		regions:        catpb.RegionNames{"us-east", "us-west"},
		affinityRegion: "us-east",
		expected:       nil,
	}, {
		name:           "empty regions returns error",
		numReplicas:    5,
		regions:        catpb.RegionNames{},
		affinityRegion: "us-east",
		expectErr:      "cannot distribute replicas across empty region set",
	}, {
		name:           "affinity region not in regions returns error",
		numReplicas:    5,
		regions:        catpb.RegionNames{"us-east", "us-west"},
		affinityRegion: "eu-west",
		expectErr:      "affinity region eu-west must be a member of the region set",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := distributeReplicasAcrossRegions(tt.numReplicas, tt.regions, tt.affinityRegion)
			if tt.expectErr != "" {
				require.ErrorContains(t, err, tt.expectErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}
