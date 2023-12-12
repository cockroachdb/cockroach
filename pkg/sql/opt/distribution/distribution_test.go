// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

			res := BuildProvided(context.Background(), evalCtx, f.Memo(), expr, &physical.Distribution{})
			if res.String() != expected.String() {
				t.Errorf("expected '%s', got '%s'", expected, res)
			}
		})
	}
}

// TestGetDistributions checks that the output of method GetDistributions is
// correct for several cases.
func TestGetDistributions(t *testing.T) {
	tc := testcat.New()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.NewTestingEvalContext(st)
	var f norm.Factory
	f.Init(context.Background(), evalCtx, tc)

	testCases := []struct {
		leftDist  []string
		rightDist []string
		expected  bool
	}{
		{
			leftDist:  []string{},
			rightDist: []string{},
			expected:  true,
		},
		{
			leftDist: []string{},
			expected: false,
		},
		{
			rightDist: []string{},
			expected:  false,
		},
		{
			leftDist:  []string{},
			rightDist: []string{"west"},
			expected:  true,
		},
		{
			leftDist: []string{"east", "west"},
			expected: false,
		},
		{
			leftDist:  []string{"east"},
			rightDist: []string{"east"},
			expected:  true,
		},
		{
			leftDist:  []string{"west"},
			rightDist: []string{"east"},
			expected:  true,
		},
		{
			leftDist:  []string{"central", "east", "west"},
			rightDist: []string{"central", "west"},
			expected:  true,
		},
	}
	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			var parentProvided, childProvided *physical.Provided

			if tc.leftDist != nil {
				parentProvided = &physical.Provided{}
				parentProvided.Distribution = physical.Distribution{Regions: tc.leftDist}
			}
			if tc.rightDist != nil {
				childProvided = &physical.Provided{}
				childProvided.Distribution = physical.Distribution{Regions: tc.rightDist}
			}
			childInput := &memo.SortExpr{}

			var distributeRel memo.RelExpr
			inputRel := childInput
			distributeExpr := &memo.DistributeExpr{Input: childInput}
			distributeRel = distributeExpr
			f.Memo().SetBestProps(distributeRel, &physical.Required{}, parentProvided, memo.Cost{C: 0})
			f.Memo().SetBestProps(inputRel, &physical.Required{}, childProvided, memo.Cost{C: 0})

			targetDist, sourceDist, ok := distributeExpr.GetDistributions()
			// Check if we got distributions when expected.
			if ok != tc.expected {
				t.Errorf("expected '%t', got '%t'", tc.expected, ok)
			}
			if ok {
				// Test that the returned distributions match those in the Distribute
				// expression and its input.
				if !targetDist.Equals(parentProvided.Distribution) {
					t.Errorf("expected target distribution '%v', got '%v'", targetDist, parentProvided.Distribution)
				}
				if !sourceDist.Equals(childProvided.Distribution) {
					t.Errorf("expected source distribution '%v', got '%v'", sourceDist, childProvided.Distribution)
				}
			}
		})
	}
}
