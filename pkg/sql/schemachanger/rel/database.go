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
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
	"github.com/google/btree"
)

// Database is a data structure for indexing entities.
type Database struct {
	schema *Schema

	// indexes store the entities, ordered by a specified set of attributes.
	// When an entity is inserted, it is inserted into each of the indexes. The
	// first entry in the list is the "primary index" which compares entities
	// based on all attributes.
	//
	// Note that the use of btree-tree backed indexes is far from fundamental.
	// One could easily envision a map-backed indexing structure which may well
	// perform much better.
	indexes []index
	// entities stores all the entities keyed on its pointer value.
	entities map[interface{}]*entity
}

// Schema returns the schema associated with the tree.
func (t *Database) Schema() *Schema {
	return t.schema
}

// NewDatabase constructs a new Database with the specified indexes.
// Note that the schema must not contain more than 64 attributes.
func NewDatabase(sc *Schema, indexes [][]Attr) (*Database, error) {
	t := &Database{
		schema:   sc,
		indexes:  make([]index, len(indexes)+1),
		entities: make(map[interface{}]*entity),
	}
	// Index everything by all the attributes. This serves as the "primary"
	// index.
	const degree = 8
	fl := btree.NewFreeList(len(indexes) + 1)
	{
		var primaryIndex index
		primaryIndex.s = sc
		primaryIndex.tree = btree.NewWithFreeList(8, fl)
		t.indexes[0] = primaryIndex
	}
	secondaryIndexes := t.indexes[1:]
	for i, attrs := range indexes {
		ords, set, err := sc.attributesToOrdinals(attrs)
		if err != nil {
			return nil, err
		}
		spec := indexSpec{mask: set, attrs: ords, s: sc}
		secondaryIndexes[i] = index{
			indexSpec: spec,
			tree:      btree.NewWithFreeList(degree, fl),
		}
	}
	return t, nil
}

// Insert inserts an entity. Note that entities are defined
// by their pointer value. If you want to avoid inserting an
// entity because a different entity exists with some of the
// same attribute values, this must be done above this call.
// Note also that entities may point to other entities. This
// call will recursively insert all entities referenced by the
// passed entity which do not already exist in the database.
//
// It is a no-op and not an error to insert an entity which
// already exists.
func (t *Database) Insert(v interface{}) error {
	e, err := toEntity(t.schema, v)
	if err != nil {
		return err
	}
	if err := t.insert(e); err != nil {
		return err
	}
	for _, v := range e.m {
		_, isEntity := t.schema.entityTypeSchemas[reflect.TypeOf(v)]
		_, alreadyDefined := t.entities[v]
		if isEntity && !alreadyDefined {
			if err := t.Insert(v); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Database) insert(e *entity) error {
	self := e.getComparableValue(t.schema, Self)
	if existing, exists := t.entities[self]; exists {
		// Sanity check that the entities really are equal.
		if _, eq := compareEntities(e, existing); !eq {
			return errors.AssertionFailedf(
				"expected entity %v to equal its already inserted value", self,
			)
		}
		return nil
	}
	t.entities[self] = e
	for i := range t.indexes {
		idx := &t.indexes[i]
		if g := idx.tree.ReplaceOrInsert(&containerItem{
			entity:    e,
			indexSpec: &idx.indexSpec,
		}); g != nil {
			return errors.AssertionFailedf(
				"expected entity %T(%v) to not exist", self, self,
			)
		}
	}
	return nil
}

type index struct {
	indexSpec
	tree *btree.BTree
}

type indexSpec struct {
	s     *Schema
	mask  ordinalSet
	attrs []ordinal
}

// entityIterator is used to iterate Entities.
type entityIterator interface {
	// Visit visits an entity. If iterutil.StopIteration
	// is returned, iteration will stop but no error is returned.
	visit(*entity) error
}

// Iterate will iterate the containers which match the specified valuesMap.
func (t *Database) iterate(where *valuesMap, f entityIterator) (err error) {
	idx, toCheck := t.chooseIndex(where.attrs)
	from, to := getValuesItems(&idx.indexSpec, where, where.attrs)
	defer putValuesItems(from, to)
	idx.tree.AscendRange(from, to, func(i btree.Item) (wantMore bool) {
		c := i.(*containerItem)
		// We want to skip items which do not have values set for
		// all members of the where clause.
		if where.attrs.without(c.entity.attrs) != 0 {
			return true
		}
		var failed bool
		toCheck.forEach(func(a ordinal) (wantMore bool) {
			_, eq := compareOn(a, (*valuesMap)(c.entity), where)
			failed = !eq
			return !failed
		})
		if !failed {
			err = f.visit(c.entity)
		}
		return err == nil
	})
	if iterutil.Done(err) {
		err = nil
	}
	return err
}

// chooseIndex chooses an index which has A prefix with the highest number of
// attributes which overlap with m. It also returns the ordinals of the
// attributes which are not covered by the index prefix.
//
// TODO(ajwerner): Consider something about selectivity by tracking the number
// of entries under each index (i.variable. which have non-NULL valuesMap)
// for the given dimension.
func (t *Database) chooseIndex(m ordinalSet) (_ *index, toCheck ordinalSet) {
	// Default to the "primary" index.
	best, bestOverlap := 0, ordinalSet(0)
	dims := t.indexes[1:]
	for i := range dims {
		if overlap := dims[i].overlap(m); overlap.len() > bestOverlap.len() {
			best, bestOverlap = i+1, overlap
		}
	}
	return &t.indexes[best], m.without(bestOverlap)
}

// overlap returns the ordinals from m which overlap with a prefix of
// attributes in s.
func (s *indexSpec) overlap(m ordinalSet) ordinalSet {
	var overlap ordinalSet
	for _, a := range s.attrs {
		if m.contains(a) {
			overlap = overlap.add(a)
		} else {
			break
		}
	}
	return overlap
}
