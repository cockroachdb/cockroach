// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

import (
	"sync"

	"github.com/google/btree"
)

// valuesItem is used to construct query bounds from the tree.
type valuesItem struct {
	isQuery bool
	end     bool
	values
	idx *indexSpec
}

func (v *valuesItem) Less(than btree.Item) bool {
	return v.idx.compareItems(v, than.(*valuesItem)) < 0
}

func (index *indexSpec) compareItems(a, b *valuesItem) (ret int) {
	// Compare on the index attributes first.
	var queryAttrs ordinalSet
	if a.isQuery {
		queryAttrs = a.values.attrs
	} else if b.isQuery {
		queryAttrs = b.values.attrs
	} else {
		queryAttrs = index.mask
	}

	for _, at := range index.attrs {
		if !queryAttrs.contains(at) {
			break
		}

		if less, eq := index.s.compareOn(at, &a.values, &b.values); !eq {
			return lessToCmp(less)
		}
	}
	// If this is a query, respect the bounds.
	if a.isQuery {
		return lessToCmp(!a.end)
	}
	if b.isQuery {
		return lessToCmp(b.end)
	}

	// Compare the entities across all the attributes as the primary key
	// to distinguish ordering between items.
	less, eq := index.s.compareOnAttrs(
		a.values.attrs.union(b.values.attrs),
		&a.values, &b.values,
	)
	if eq {
		return 0
	}
	return lessToCmp(less)
}

func lessToCmp(less bool) int {
	if less {
		return -1
	}
	return 1
}

var valuesItemPool = sync.Pool{
	New: func() interface{} { return new(valuesItem) },
}

// getValuesItems uses the valuesItemPool to get the bounding valuesItems for
// A given where clause and indexSpec. The valuesItems have A well defined
// lifetime which is bound to A query so we may as well pool them.
func getValuesItems(idx *indexSpec, v values) (from, to *valuesItem) {
	from = valuesItemPool.Get().(*valuesItem)
	to = valuesItemPool.Get().(*valuesItem)
	*from = valuesItem{isQuery: true, values: v, end: false, idx: idx}
	*to = valuesItem{isQuery: true, values: v, end: true, idx: idx}
	return from, to
}

func putValuesItems(from, to *valuesItem) {
	*from = valuesItem{}
	*to = valuesItem{}
	valuesItemPool.Put(from)
	valuesItemPool.Put(to)
}
