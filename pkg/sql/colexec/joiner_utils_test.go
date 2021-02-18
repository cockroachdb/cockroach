// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// joinTestCase is a helper struct shared by the hash and merge join unit
// tests. Not all fields have to be filled in, but init() method *must* be
// called.
type joinTestCase struct {
	description           string
	joinType              descpb.JoinType
	leftTuples            colexectestutils.Tuples
	leftTypes             []*types.T
	leftOutCols           []uint32
	leftEqCols            []uint32
	leftDirections        []execinfrapb.Ordering_Column_Direction
	rightTuples           colexectestutils.Tuples
	rightTypes            []*types.T
	rightOutCols          []uint32
	rightEqCols           []uint32
	rightDirections       []execinfrapb.Ordering_Column_Direction
	leftEqColsAreKey      bool
	rightEqColsAreKey     bool
	expected              colexectestutils.Tuples
	skipAllNullsInjection bool
	onExpr                execinfrapb.Expression
}

func (tc *joinTestCase) init() {
	if len(tc.leftDirections) == 0 {
		tc.leftDirections = make([]execinfrapb.Ordering_Column_Direction, len(tc.leftTypes))
		for i := range tc.leftDirections {
			tc.leftDirections[i] = execinfrapb.Ordering_Column_ASC
		}
	}

	if len(tc.rightDirections) == 0 {
		tc.rightDirections = make([]execinfrapb.Ordering_Column_Direction, len(tc.rightTypes))
		for i := range tc.rightDirections {
			tc.rightDirections[i] = execinfrapb.Ordering_Column_ASC
		}
	}
}

// mirror attempts to create a "mirror" test case of tc and returns nil if it
// can't (a "mirror" test case is derived from the original one by swapping the
// inputs and adjusting all of the corresponding fields accordingly). Currently
// it works only for LEFT SEMI and LEFT ANTI join types.
// TODO(yuzefovich): extend this to other join types when possible.
func (tc *joinTestCase) mirror() *joinTestCase {
	switch tc.joinType {
	case descpb.LeftSemiJoin, descpb.LeftAntiJoin:
	default:
		return nil
	}
	mirroringCase := *tc
	mirroringCase.description = strings.NewReplacer(
		"LEFT", "RIGHT",
		"RIGHT", "LEFT",
		"left", "right",
		"right", "left",
	).Replace(tc.description)
	if tc.joinType == descpb.LeftSemiJoin {
		mirroringCase.joinType = descpb.RightSemiJoin
	} else {
		mirroringCase.joinType = descpb.RightAntiJoin
	}
	mirroringCase.leftTuples, mirroringCase.rightTuples = mirroringCase.rightTuples, mirroringCase.leftTuples
	mirroringCase.leftTypes, mirroringCase.rightTypes = mirroringCase.rightTypes, mirroringCase.leftTypes
	mirroringCase.leftOutCols, mirroringCase.rightOutCols = mirroringCase.rightOutCols, mirroringCase.leftOutCols
	mirroringCase.leftEqCols, mirroringCase.rightEqCols = mirroringCase.rightEqCols, mirroringCase.leftEqCols
	mirroringCase.leftDirections, mirroringCase.rightDirections = mirroringCase.rightDirections, mirroringCase.leftDirections
	mirroringCase.leftEqColsAreKey, mirroringCase.rightEqColsAreKey = mirroringCase.rightEqColsAreKey, mirroringCase.leftEqColsAreKey
	// TODO(yuzefovich): once we support ON expression in more join types, this
	// method will need to update non-empty ON expressions as well.
	return &mirroringCase
}

// withMirrors will add all "mirror" test cases.
func withMirrors(testCases []*joinTestCase) []*joinTestCase {
	numOrigTestCases := len(testCases)
	for _, c := range testCases[:numOrigTestCases] {
		if mirror := c.mirror(); mirror != nil {
			testCases = append(testCases, mirror)
		}
	}
	return testCases
}

// mutateTypes returns a slice of joinTestCases with varied types. Assumes
// the input is made up of just int64s. Calling this
func (tc *joinTestCase) mutateTypes() []*joinTestCase {
	ret := []*joinTestCase{tc}

	for _, typ := range []*types.T{types.Decimal, types.Bytes} {
		if typ.Identical(types.Bytes) {
			// Skip test cases with ON conditions for now, since those expect
			// numeric inputs.
			if !tc.onExpr.Empty() {
				continue
			}
		}
		newTc := *tc
		newTc.leftTypes = make([]*types.T, len(tc.leftTypes))
		newTc.rightTypes = make([]*types.T, len(tc.rightTypes))
		copy(newTc.leftTypes, tc.leftTypes)
		copy(newTc.rightTypes, tc.rightTypes)
		for _, typs := range [][]*types.T{newTc.leftTypes, newTc.rightTypes} {
			for i := range typs {
				if !typ.Identical(types.Int) {
					// We currently can only mutate test cases that are made up of int64
					// only.
					return ret
				}
				typs[i] = typ
			}
		}
		newTc.leftTuples = tc.leftTuples.Clone()
		newTc.rightTuples = tc.rightTuples.Clone()
		newTc.expected = tc.expected.Clone()

		for _, tups := range []colexectestutils.Tuples{newTc.leftTuples, newTc.rightTuples, newTc.expected} {
			for i := range tups {
				for j := range tups[i] {
					if tups[i][j] == nil {
						continue
					}
					switch typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
					case types.DecimalFamily:
						var d apd.Decimal
						_, _ = d.SetFloat64(float64(tups[i][j].(int)))
						tups[i][j] = d
					case types.BytesFamily:
						tups[i][j] = fmt.Sprintf("%.10d", tups[i][j].(int))
					}
				}
			}
		}
		ret = append(ret, &newTc)
	}
	return ret
}
