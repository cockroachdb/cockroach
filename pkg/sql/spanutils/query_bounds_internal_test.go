// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanutils

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func normalizeClause(s string) string {
	lines := strings.Split(s, "\n")
	var trimmed []string
	for _, l := range lines {
		t := strings.TrimSpace(l)
		if t != "" {
			trimmed = append(trimmed, t)
		}
	}
	return strings.Join(trimmed, " ")
}

func TestRenderQueryBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testCase struct {
		name              string
		pkCols            []string
		pkDirs            []catenumpb.IndexColumn_Direction
		pkTypes           []*types.T
		numStart, numEnd  int
		startIncl         bool
		placeholderOffset int
		expectedClause    string
	}

	cases := []testCase{
		{
			name:              "simple ascending inclusive start",
			pkCols:            []string{"id"},
			pkDirs:            []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			pkTypes:           []*types.T{types.Int},
			numStart:          1,
			numEnd:            1,
			startIncl:         true,
			placeholderOffset: 2,
			expectedClause: `
				(
					(id >= $3::INT8)
				)
				AND (
					(id <= $2::INT8)
				)`,
		},
		{
			name:              "single-column descending inclusive",
			pkCols:            []string{"z"},
			pkDirs:            []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_DESC},
			pkTypes:           []*types.T{types.String},
			numStart:          1,
			numEnd:            1,
			startIncl:         true,
			placeholderOffset: 1,
			expectedClause: `
				(
					(z <= $2::STRING)
				)
				AND (
					(z >= $1::STRING)
				)
			`,
		},
		{
			name:   "multi-column descending",
			pkCols: []string{"a", "b"},
			pkDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
				catenumpb.IndexColumn_DESC,
			},
			pkTypes:           []*types.T{types.String, types.Int},
			numStart:          2,
			numEnd:            2,
			startIncl:         false,
			placeholderOffset: 4,
			expectedClause: `
				(
					(a < $6::STRING) OR
					(a = $6::STRING AND b < $7::INT8)
				)
				AND (
					(a > $4::STRING) OR
					(a = $4::STRING AND b >= $5::INT8)
				)`,
		},
		{
			name:              "no bounds",
			pkCols:            []string{"id"},
			pkDirs:            []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			pkTypes:           []*types.T{types.Int},
			numStart:          0,
			numEnd:            0,
			startIncl:         true,
			placeholderOffset: 1,
			expectedClause:    "",
		},
		{
			name:   "mixed pk directions",
			pkCols: []string{"a", "b"},
			pkDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_DESC,
			},
			pkTypes:           []*types.T{types.Int, types.String},
			numStart:          2,
			numEnd:            2,
			startIncl:         true,
			placeholderOffset: 10,
			expectedClause: `
				(
					(a > $12::INT8) OR
					(a = $12::INT8 AND b <= $13::STRING)
				)
				AND (
					(a < $10::INT8) OR
					(a = $10::INT8 AND b >= $11::STRING)
				)
			`,
		},
		{
			name:   "prefix bound only",
			pkCols: []string{"x", "y", "z"},
			pkDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_ASC,
			},
			pkTypes:           []*types.T{types.Int, types.Int, types.Int},
			numStart:          1,
			numEnd:            0,
			startIncl:         false,
			placeholderOffset: 3,
			expectedClause: `
				(
					(x > $3::INT8)
				)
			`,
		},
		{
			name:              "end bound only",
			pkCols:            []string{"k"},
			pkDirs:            []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			pkTypes:           []*types.T{types.Int},
			numStart:          0,
			numEnd:            1,
			startIncl:         false,
			placeholderOffset: 1,
			expectedClause: `
				(
					(k <= $1::INT8)
				)
			`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clause, err := RenderQueryBounds(
				tc.pkCols, tc.pkDirs, tc.pkTypes,
				tc.numStart, tc.numEnd, tc.startIncl, tc.placeholderOffset,
			)
			require.NoError(t, err)
			// Normalize so we don't have to compare with whitespace.
			require.Equal(t, normalizeClause(tc.expectedClause), normalizeClause(clause))
		})
	}
}
