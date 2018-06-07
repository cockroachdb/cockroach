package xfunc

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
		return leftD.Compare(s.cf.evalCtx, rightD)
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
