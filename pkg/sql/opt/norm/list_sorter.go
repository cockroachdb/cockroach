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

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// listSorter is a helper struct that implements the sort.Slice "less"
// comparison function.
type listSorter struct {
	cf   *CustomFuncs
	list []memo.GroupID
}

// less returns true if item i in the list compares less than item j.
// sort.Slice uses this method to sort the list.
func (s listSorter) less(i, j int) bool {
	return s.compare(i, j) < 0
}

// compare returns -1 if item i compares less than item j, 0 if they are equal,
// and 1 if item i compares greater. Constants sort before non-constants, and
// are sorted and uniquified according to Datum comparison rules. Non-constants
// are sorted and uniquified by GroupID (arbitrary, but stable).
func (s listSorter) compare(i, j int) int {
	// If both are constant values, then use datum comparison.
	isLeftConst := s.cf.mem.NormExpr(s.list[i]).IsConstValue()
	isRightConst := s.cf.mem.NormExpr(s.list[j]).IsConstValue()
	if isLeftConst {
		if !isRightConst {
			// Constant always sorts before non-constant
			return -1
		}

		leftD := memo.ExtractConstDatum(memo.MakeNormExprView(s.cf.mem, s.list[i]))
		rightD := memo.ExtractConstDatum(memo.MakeNormExprView(s.cf.mem, s.list[j]))
		return leftD.Compare(s.cf.f.evalCtx, rightD)
	} else if isRightConst {
		// Non-constant always sorts after constant.
		return 1
	}

	// Arbitrarily order by GroupID.
	if s.list[i] < s.list[j] {
		return -1
	} else if s.list[i] > s.list[j] {
		return 1
	}
	return 0
}
