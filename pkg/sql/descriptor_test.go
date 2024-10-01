// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestFormatDefaultRegionNotice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		primary string
		regions []string
		expect  string
	}
	tests := []testCase{
		{
			primary: "us-east1",
			expect:  `defaulting to 'WITH PRIMARY REGION "us-east1"' as no primary region was specified`,
		},
		{
			primary: "us-east1",
			regions: []string{"us-west2"},
			expect:  `defaulting to 'WITH PRIMARY REGION "us-east1" REGIONS "us-west2"' as no primary region was specified`,
		},
		{
			primary: "us-east1",
			regions: []string{"us-west2", "us-central3"},
			expect:  `defaulting to 'WITH PRIMARY REGION "us-east1" REGIONS "us-west2", "us-central3"' as no primary region was specified`,
		},
	}
	for _, test := range tests {
		var regions []tree.Name
		for _, region := range test.regions {
			regions = append(regions, tree.Name(region))
		}
		require.Equal(t, test.expect, formatDefaultRegionNotice(tree.Name(test.primary), regions).Error())
	}
}
