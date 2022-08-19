// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distribution

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
)

func TestBuildProvided(t *testing.T) {
	tc := testcat.New()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.NewTestingEvalContext(st)
	var f norm.Factory
	f.Init(context.Background(), evalCtx, tc)

	testCases := []struct {
		leftDist  []string
		rightDist []string
		expected  []string
	}{
		{
			leftDist:  []string{},
			rightDist: []string{},
			expected:  []string{},
		},
		{
			leftDist: []string{},
			expected: []string{},
		},
		{
			leftDist:  []string{},
			rightDist: []string{"west"},
			expected:  []string{"west"},
		},
		{
			leftDist: []string{"east", "west"},
			expected: []string{"east", "west"},
		},
		{
			leftDist:  []string{"east"},
			rightDist: []string{"east"},
			expected:  []string{"east"},
		},
		{
			leftDist:  []string{"west"},
			rightDist: []string{"east"},
			expected:  []string{"east", "west"},
		},
		{
			leftDist:  []string{"central", "east", "west"},
			rightDist: []string{"central", "west"},
			expected:  []string{"central", "east", "west"},
		},
	}
	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			expected := physical.Distribution{Regions: tc.expected}

			leftInput := &testexpr.Instance{
				Rel: &props.Relational{},
				Provided: &physical.Provided{
					Distribution: physical.Distribution{Regions: tc.leftDist},
				},
			}

			// If there is only one input, build provided distribution for a Select.
			// Otherwise, build the distribution for a join (we use anti join to avoid
			// calling initJoinMultiplicity).
			var expr memo.RelExpr
			if tc.rightDist == nil {
				expr = f.Memo().MemoizeSelect(leftInput, memo.FiltersExpr{})
			} else {
				rightInput := &testexpr.Instance{
					Rel: &props.Relational{},
					Provided: &physical.Provided{
						Distribution: physical.Distribution{Regions: tc.rightDist},
					},
				}
				expr = f.Memo().MemoizeAntiJoin(
					leftInput, rightInput, memo.FiltersExpr{}, &memo.JoinPrivate{},
				)
			}

			res := BuildProvided(context.Background(), evalCtx, expr, &physical.Distribution{})
			if res.String() != expected.String() {
				t.Errorf("expected '%s', got '%s'", expected, res)
			}
		})
	}
}
