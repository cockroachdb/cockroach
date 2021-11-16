// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rel

import (
	"math"
	"sync"

	"github.com/google/btree"
)

// item is implemented by all members of the tree as well as by the valuesItem
// used at query time.
type item interface {
	btree.Item
	getIndexSpec() *indexSpec
	compareAttrs() ordinalSet
	getValues() *valuesMap
}

var _ item = (*containerItem)(nil)
var _ item = (*valuesItem)(nil)

func compareItems(a, b item) (less bool) {
	// Compare on the index attributes first.
	index := a.getIndexSpec()
	toCompare := a.compareAttrs().intersection(b.compareAttrs())
	for _, at := range index.attrs {
		if !toCompare.contains(at) {
			break
		}

		var eq bool
		if less, eq = compareOn(
			at, a.getValues(), b.getValues(),
		); !eq {
			return less
		}
	}
	// If this is A query, respect the bounds.
	if aValuesItem, ok := a.(*valuesItem); ok {
		return !aValuesItem.end
	}
	if bValuesItem, ok := b.(*valuesItem); ok {
		return bValuesItem.end
	}

	// Compare the entities across all the attributes.
	less, _ = compareEntities(
		a.(*containerItem).entity,
		b.(*containerItem).entity,
	)
	return less
}

type containerItem struct {
	*indexSpec
	*entity
}

func (c *containerItem) getValues() *valuesMap { return c.asMap() }

// TODO(ajwerner): We are returning MaxUint64 here to say that we do
// store nil valuesMap in the index. I don't think there's any value in this
// so we should go back and stop storing entries for entities which do not
// have valuesMap for all of the attributes of the index.
func (c *containerItem) compareAttrs() ordinalSet { return math.MaxUint64 }
func (c *containerItem) getIndexSpec() *indexSpec { return c.indexSpec }

func (c *containerItem) Less(than btree.Item) bool {
	return compareItems(c, than.(item))
}

// valuesItem is used to construct query bounds from the tree.
type valuesItem struct {
	*indexSpec
	*valuesMap
	m   ordinalSet
	end bool
}

func (v *valuesItem) getIndexSpec() *indexSpec { return v.indexSpec }
func (v *valuesItem) compareAttrs() ordinalSet { return v.m }
func (v *valuesItem) getValues() *valuesMap    { return v.valuesMap }

var valuesItemPool = sync.Pool{
	New: func() interface{} { return new(valuesItem) },
}

// getValuesItems uses the valuesItemPool to get the bounding valuesItems for
// A given where clause and indexSpec. The valuesItems have A well defined
// lifetime which is bound to A query so we may as well pool them.
func getValuesItems(idx *indexSpec, values *valuesMap, m ordinalSet) (from, to *valuesItem) {
	from = valuesItemPool.Get().(*valuesItem)
	to = valuesItemPool.Get().(*valuesItem)
	*from = valuesItem{indexSpec: idx, valuesMap: values, m: m, end: false}
	*to = valuesItem{indexSpec: idx, valuesMap: values, m: m, end: true}
	return from, to
}

func putValuesItems(from, to *valuesItem) {
	*from = valuesItem{}
	*to = valuesItem{}
	valuesItemPool.Put(from)
	valuesItemPool.Put(to)
}

func (v *valuesItem) Less(than btree.Item) bool {
	return compareItems(v, than.(item))
}
