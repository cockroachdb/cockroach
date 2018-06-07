package xfunc

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// CustomFuncs contains all the custom match and replace functions that are
// used by both normalization and exploration rules.
type CustomFuncs struct {
	mem     *memo.Memo
	evalCtx *tree.EvalContext

	// scratchItems is a slice that is reused by ListBuilder to store temporary
	// results that are accumulated before passing them to memo.InternList.
	scratchItems []memo.GroupID
}

// MakeCustomFuncs returns a new CustomFuncs initialized with the given memo
// and evalContext.
func MakeCustomFuncs(mem *memo.Memo, evalCtx *tree.EvalContext) CustomFuncs {
	return CustomFuncs{
		mem:     mem,
		evalCtx: evalCtx,
	}
}

// IsSortedUniqueList returns true if the list is in sorted order, with no
// duplicates. See the comment for listSorter.compare for comparison rule
// details.
func (cf *CustomFuncs) IsSortedUniqueList(list memo.ListID) bool {
	ls := listSorter{cf: cf, list: cf.mem.LookupList(list)}
	for i := 0; i < int(list.Length-1); i++ {
		if !ls.less(i, i+1) {
			return false
		}
	}
	return true
}

// ConstructSortedUniqueList sorts the given list and removes duplicates, and
// returns the resulting list. See the comment for listSorter.compare for
// comparison rule details.
func (cf *CustomFuncs) ConstructSortedUniqueList(list memo.ListID) memo.ListID {
	// Make a copy of the list, since it needs to stay immutable.
	lb := MakeListBuilder(cf)
	lb.AddItems(cf.mem.LookupList(list))
	ls := listSorter{cf: cf, list: lb.items}

	// Sort the list.
	sort.Slice(ls.list, ls.less)

	// Remove duplicates from the list.
	n := 0
	for i := 0; i < int(list.Length); i++ {
		if i == 0 || ls.compare(i-1, i) < 0 {
			lb.items[n] = lb.items[i]
			n++
		}
	}
	lb.setLength(n)
	return lb.BuildList()
}
