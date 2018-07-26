// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package xfunc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// listSorter is a helper struct that implements the sort.Slice "less"
// comparison function.
type listSorter struct {
	cf   *CustomFuncs
	list []memo.GroupID
}

// less returns true if item i in the list compares less than item j.
// sort.Slice uses this method to sort the list.
// Constants sort before non-constants, and
// are sorted and uniquified according to Datum comparison rules. Non-constants
// are sorted and uniquified by GroupID (arbitrary, but stable).
func (s listSorter) less(i, j int) bool {
	// If both are constant values, then use datum comparison.
	isLeftConst := s.cf.mem.NormExpr(s.list[i]).IsConstValue()
	isRightConst := s.cf.mem.NormExpr(s.list[j]).IsConstValue()
	if isLeftConst {
		if !isRightConst {
			// Constant always sorts before non-constant
			return true
		}

		leftD := memo.ExtractConstDatum(memo.MakeNormExprView(s.cf.mem, s.list[i]))
		rightD := memo.ExtractConstDatum(memo.MakeNormExprView(s.cf.mem, s.list[j]))
		return tree.TotalOrderLess(s.cf.evalCtx, leftD, rightD)
	} else if isRightConst {
		// Non-constant always sorts after constant.
		return false
	}

	// Arbitrarily order by GroupID.
	if s.list[i] < s.list[j] {
		return true
	}
	return false
}
