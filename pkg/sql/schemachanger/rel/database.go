// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
	"github.com/google/btree"
)

// Database is a data structure for indexing entities.
type Database struct {
	entitySet

	// indexes store the entities, ordered by a specified set of attributes.
	// When an entity is inserted, it is inserted into each of the indexes
	// for which it satisfies the relevant attribute existence and predicate
	// filters.
	//
	// Note that the use of btree-tree backed indexes is far from fundamental.
	// One could easily envision a map-backed indexing structure which may well
	// perform much better.
	indexes []index
}

// Schema returns the schema associated with the tree.
func (t *Database) Schema() *Schema {
	return t.schema
}

// IndexWhere is a partial index predicate for a rel index.
// It constrains the index to only contain entities which have a value
// equal to the provided value for the attribute in question.
type IndexWhere struct {
	Attr Attr
	Eq   interface{}
}

// Index defines an index over the stored entities.
type Index struct {
	// Attrs defines the ordering for the index. All entities will be sorted
	// first by these attributes and then by the canonical ordering of the
	// attributes as defined by the schema. Note that not all entities in the
	// index must have defined values for these attrs.
	Attrs []Attr

	// Where defines specific attribute-values which must be met for
	// entities to be included in this index. When queries are run, if the
	// predicate is searching for a different attribute value, this index
	// will be skipped. Where are one tool to prune the size of an
	// index.
	Where []IndexWhere

	// Exists defines requirements for the existence of attributes in entities
	// which are indexed. Note that for queries to take advantage of this index,
	// the fact that the entity being queried is expected to have an attribute
	// must be stated explicitly. This generally requires binding a variable to
	// the attribute, even if you have no use for it beyond to enable the index
	// choice.
	//
	// TODO(ajwerner): add a HasAttr method to Var to hide this internal
	// attribute existence from the query itself and to free the query-writer
	// from needing to define a bogus Var.
	Exists []Attr

	// Inverted is true if the index is an inverted index over slice membership.
	// If true, the attributes may only refer to a slice membership type.
	Inverted bool
}

// NewDatabase constructs a new Database with the specified indexes.
func NewDatabase(sc *Schema, indexes ...Index) (*Database, error) {
	t := &Database{
		entitySet: entitySet{
			schema:  sc,
			hash:    map[interface{}]int{},
			strings: map[string]int{},
		},
	}
	indexes, err := maybeExpandInvertedIndexes(sc, indexes)
	if err != nil {
		return nil, err
	}
	t.indexes = make([]index, len(indexes))
	for i, di := range indexes {
		var set, exists ordinalSet
		ords := make([]ordinal, len(di.Attrs))
		for i, a := range di.Attrs {
			ord, err := sc.getOrdinal(a)
			if err != nil {
				return nil, errors.Wrap(err, "invalid index attribute")
			}
			set = set.add(ord)
			ords[i] = ord
		}
		for _, a := range di.Exists {
			ord, err := sc.getOrdinal(a)
			if err != nil {
				return nil, errors.Wrap(err, "invalid index exists attribute")
			}
			exists = exists.add(ord)
		}
		var predicate values
		for _, w := range di.Where {
			ord, err := sc.getOrdinal(w.Attr)
			if err != nil {
				return nil, errors.Wrapf(err,
					"invalid index predicate attribute = %T(%v)", w.Attr, w.Attr)
			}
			if predicate.attrs.contains(ord) {
				return nil, errors.Errorf(
					"invalid index predicate: duplicate entries for attribute %v", w.Attr)
			}
			// Here we need to check whether this is a scalar or what, and then
			// deal with it accordingly.
			rv := reflect.ValueOf(w.Eq)
			if !rv.IsValid() || rv.Kind() == reflect.Ptr && rv.IsNil() {
				return nil, errors.Errorf(
					"invalid index predicate: nil entry for attribute %v", w.Attr)
			}
			if w.Attr == Self {
				return nil, errors.Errorf(
					"invalid index predicate: cannot populate self entry")
			}
			val, err := t.makeInlineValue(ord, rv)
			if err != nil {
				return nil, errors.Wrap(err, "invalid index predicate")
			}
			if !predicate.add(ord, val) {
				return nil, errors.Errorf(
					"invalid index predicate %v with more than %d attributes",
					di.Where, numAttrs,
				)
			}
		}
		spec := indexSpec{
			mask:       set,
			attrs:      ords,
			s:          &t.entitySet,
			exists:     exists,
			where:      predicate,
			isInverted: di.Inverted,
		}
		t.indexes[i] = index{
			indexSpec: spec,
			tree:      btree.New(32),
		}
	}
	return t, nil
}

// maybeExpandInvertedIndexes will replace inverted indexes defined by the
// user with two new indexes corresponding to the inverted attribute in
// order to enable efficient lookup by either source or value. This is
// done because generally in rel it is safe to assume that it is efficient
// to go from an entity to its attribute and that it is also efficient to go
// from an attribute to the set of entities so long as the attribute is
// indexed. Because the slice members are not directly a part of the entity
// in the rel model, we need to index from the entity to the slice member to
// make that expansion cheap.
func maybeExpandInvertedIndexes(sc *Schema, indexes []Index) ([]Index, error) {
	var expanded []Index
	for _, idx := range indexes {
		if !idx.Inverted {
			expanded = append(expanded, idx)
			continue
		}
		if err := validatedInvertedIndexProperties(sc, idx); err != nil {
			return nil, err
		}
		// We validated above that there is just one inverted attribute.
		attr := idx.Attrs[0]
		expanded = append(expanded, Index{
			Attrs:    []Attr{attr, sliceSource, sliceIndex},
			Exists:   []Attr{attr},
			Inverted: true,
		}, Index{
			Attrs:    []Attr{sliceSource, attr, sliceIndex},
			Exists:   []Attr{attr},
			Inverted: true,
		})
	}
	return expanded, nil
}

// validateInvertedIndexProperties ensures that a user-requested inverted
// index conforms to the supported properties. Namely, it has one attribute
// that is a slice attribute and no use of Where or Exists clauses.
func validatedInvertedIndexProperties(sc *Schema, idx Index) error {
	if len(idx.Attrs) > 1 {
		return errors.Errorf("inverted indexes may only reference a single slice attribute, got %v", idx.Attrs)
	}
	ord, err := sc.getOrdinal(idx.Attrs[0])
	switch {
	case err != nil:
		return errors.Wrap(err, "invalid index attribute")
	case !sc.sliceOrdinals.contains(ord):
		return errors.Errorf(
			"invalid non-slice index attribute %v for inverted index", idx.Attrs[0],
		)
	case len(idx.Where) > 0:
		return errors.Errorf(
			"inverted indexes are implicitly partial and cannot be further constrained",
		)
	case len(idx.Exists) > 0:
		return errors.Errorf(
			"inverted indexes may not have existence constraints",
		)
	default:
		return nil
	}
}

// entityStore is an abstraction to permit the relevant recursion of interning
// and indexing entities and their referenced children. The database delegates
// first to its entitySet, but the entitySet, when decomposing, may need to
// delegate back to the database. This interface enables that mutual delegation
// without needing to collect the intermediate elements into some data
// structure.
type entityStore interface {
	insert(v interface{}, set entityStore) (id int, err error)
}

// insert implements entityStore.
func (t *Database) insert(v interface{}, es entityStore) (id int, err error) {
	if existing, ok := t.entitySet.hash[v]; ok {
		return existing, nil
	}
	id, err = t.entitySet.insert(v, es)
	if err != nil {
		return 0, err
	}
	e := (*values)(&t.entitySet.entities[id])
	typIdx, ok := e.get(t.schema.typeOrdinal)
	if !ok {
		return 0, errors.AssertionFailedf("unknown type for entity %T: %v", v, v)
	}
	typ := t.schema.entityTypes[typIdx]
	for i := range t.indexes {
		idx := &t.indexes[i]
		if !idx.matchesPredicate(e) {
			continue
		}
		if idx.exists != 0 && !idx.exists.isContainedIn(e.attrs) {
			continue
		}
		// If this entity is a slice membership entity, it should not go
		if typ.isSliceMemberType && !idx.isInverted {
			continue
		}
		idx.tree.ReplaceOrInsert(&valuesItem{values: *e, idx: &idx.indexSpec})
	}
	return id, nil
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
	_, err := t.insert(v, t)
	return err
}

type index struct {
	indexSpec
	tree *btree.BTree
}

type indexSpec struct {
	s          *entitySet
	mask       ordinalSet
	attrs      []ordinal
	exists     ordinalSet
	where      values
	isInverted bool
}

// entityIterator is used to iterate Entities.
type entityIterator interface {
	// Visit visits an entity. If iterutil.StopIteration
	// is returned, iteration will stop but no error is returned.
	visit(entity) error
}

// Iterate will iterate the containers which match the specified valuesMap.
func (t *Database) iterate(where values, hasAttrs ordinalSet, f entityIterator) error {
	idx, toCheck, err := t.chooseIndex(where, hasAttrs)
	if err != nil {
		return err
	}
	from, to := getValuesItems(&idx.indexSpec, where)
	defer putValuesItems(from, to)
	idx.tree.AscendRange(from, to, func(i btree.Item) bool {
		cv := i.(*valuesItem)
		// We want to skip items which do not have values set for
		// all members of the where clause.
		if where.attrs.without(cv.attrs) != 0 {
			return true
		}
		var failed bool
		toCheck.forEach(func(a ordinal) (wantMore bool) {
			_, eq := t.compareOn(a, &cv.values, &where)
			failed = !eq
			return !failed
		})
		if !failed {
			if err = f.visit((entity)(cv.values)); err != nil {
				// Check if the error is errResultSetNotEmpty directly to avoid
				// the overhead of errors.Is which happens inside iterutil.Map.
				// nolint:errcmp
				if err != errResultSetNotEmpty {
					err = iterutil.Map(err)
				}
				return false
			}
		}
		return true
	})
	return err
}

// chooseIndex chooses an index which has A prefix with the highest number of
// attributes which overlap with m. It also returns the ordinals of the
// attributes which are not covered by the index prefix.
//
// TODO(ajwerner): Consider something about selectivity by tracking the number
// of entries under each index.
func (t *Database) chooseIndex(
	where values, hasAttrs ordinalSet,
) (_ *index, toCheck ordinalSet, _ error) {
	m := where.attrs
	best, bestOverlap := -1, ordinalSet(0)
	dims := t.indexes
	for i := range dims {
		overlap := dims[i].overlap(m)
		if best >= 0 && overlap.len() <= bestOverlap.len() {
			continue
		}
		// Only allow queries to proceed with no index overlap if this is the
		// zero-attribute index, which implies the database creator accepts bad
		// query plans. We'll also permit it in the rare case that this is
		// an inverted index join and we have some overlap in the attributes.
		hasSliceAttrs := !hasAttrs.intersection(t.schema.sliceOrdinals).empty()
		if overlap == 0 && len(dims[i].attrs) > 0 &&
			(!hasSliceAttrs || dims[i].mask.intersection(hasAttrs).empty()) {
			continue
		}
		if !dims[i].exists.isContainedIn(m.union(hasAttrs)) {
			continue
		}
		if !dims[i].matchesPredicate(&where) {
			continue
		}
		// Only inverted indexes can contain data which references slice ordinals.
		// If the query is for such an ordinal, it must search an inverted index.
		if hasSliceAttrs && !dims[i].isInverted {
			continue
		}
		best, bestOverlap = i, overlap
	}
	if best == -1 {
		return nil, 0, errors.AssertionFailedf(
			"failed to find index to satisfy query",
		)
	}
	return &t.indexes[best], m.without(bestOverlap), nil
}

func (s indexSpec) matchesPredicate(v *values) bool {
	if s.where.attrs.empty() {
		return true
	}
	matches := true
	s.where.attrs.forEach(func(a ordinal) (wantMore bool) {
		av, aOk := s.where.get(a)
		bv, bOk := v.get(a)
		matches = av == bv && aOk == bOk
		return matches
	})
	return matches
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
