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

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

func scanCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	ok, _ := ScanPrivateCanProvide(
		expr.Memo().Metadata(),
		&expr.(*memo.ScanExpr).ScanPrivate,
		required,
	)
	return ok
}

// ScanIsReverse returns true if the scan must be performed in reverse order
// in order to satisfy the required ordering. If either direction is ok (e.g. no
// required ordering), reutrns false. The scan must be able to satisfy the
// required ordering, according to ScanCanProvideOrdering.
func ScanIsReverse(scan *memo.ScanExpr, required *physical.OrderingChoice) bool {
	ok, reverse := ScanPrivateCanProvide(
		scan.Memo().Metadata(),
		&scan.ScanPrivate,
		required,
	)
	if !ok {
		panic("scan can't provide required ordering")
	}
	return reverse
}

// ScanPrivateCanProvide returns true if the scan operator returns rows
// that satisfy the given required ordering; it also returns whether the scan
// needs to be in reverse order to match the required ordering.
func ScanPrivateCanProvide(
	md *opt.Metadata, s *memo.ScanPrivate, required *physical.OrderingChoice,
) (ok bool, reverse bool) {
	// Scan naturally orders according to scanned index's key columns. A scan can
	// be executed either as a forward or as a reverse scan (unless it has a row
	// limit, in which case the direction is fixed).
	//
	// The code below follows the structure of OrderingChoice.Implies. We go
	// through the columns and determine if the ordering matches with either scan
	// direction.

	// We start off as accepting either a forward or a reverse scan. Until then,
	// the reverse variable is unset. Once the direction is known, reverseSet is
	// true and reverse indicates whether we need to do a reverse scan.
	const (
		either = 0
		fwd    = 1
		rev    = 2
	)
	direction := either
	if s.HardLimit.IsSet() {
		// When we have a limit, the limit forces a certain scan direction (because
		// it affects the results, not just their ordering).
		direction = fwd
		if s.HardLimit.Reverse() {
			direction = rev
		}
	}
	index := md.Table(s.Table).Index(s.Index)
	for left, right := 0, 0; right < len(required.Columns); {
		if left >= index.KeyColumnCount() {
			return false, false
		}
		indexCol := index.Column(left)
		indexColID := s.Table.ColumnID(indexCol.Ordinal)
		if required.Optional.Contains(int(indexColID)) {
			left++
			continue
		}
		reqCol := &required.Columns[right]
		if !reqCol.Group.Contains(int(indexColID)) {
			return false, false
		}
		// The directions of the index column and the required column impose either
		// a forward or a reverse scan.
		required := fwd
		if indexCol.Descending != reqCol.Descending {
			required = rev
		}
		if direction == either {
			direction = required
		} else if direction != required {
			// We already determined the direction, and according to it, this column
			// has the wrong direction.
			return false, false
		}
		left, right = left+1, right+1
	}
	// If direction is either, we prefer forward scan.
	return true, direction == rev
}

func scanBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	scan := expr.(*memo.ScanExpr)
	md := scan.Memo().Metadata()
	index := md.Table(scan.Table).Index(scan.Index)
	fds := &scan.Relational().FuncDeps

	// We need to know the direction of the scan.
	reverse := ScanIsReverse(scan, required)

	// We generate the longest ordering that this scan can provide, then we trim
	// it. This is the longest prefix of index columns that are output by the scan
	// (ignoring constant columns, in the case of constrained scans).
	constCols := fds.ComputeClosure(opt.ColSet{})
	numCols := index.KeyColumnCount()
	provided := make(opt.Ordering, 0, numCols)
	for i := 0; i < numCols; i++ {
		indexCol := index.Column(i)
		colID := scan.Table.ColumnID(indexCol.Ordinal)
		if !scan.Cols.Contains(int(colID)) {
			// Column not in output; we are done.
			break
		}
		if constCols.Contains(int(colID)) {
			// Column constrained to a constant, ignore.
			continue
		}
		direction := (indexCol.Descending != reverse) // != is bool XOR
		provided = append(provided, opt.MakeOrderingColumn(colID, direction))
	}

	return trimProvided(provided, required, fds)
}

func init() {
	memo.ScanIsReverseFn = func(
		md *opt.Metadata, s *memo.ScanPrivate, required *physical.OrderingChoice,
	) bool {
		ok, reverse := ScanPrivateCanProvide(md, s, required)
		if !ok {
			panic("scan can't provide required ordering")
		}
		return reverse
	}
}
