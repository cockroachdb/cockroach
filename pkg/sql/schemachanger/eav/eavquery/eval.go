// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eavquery

import (
	"container/list"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/cockroachdb/errors"
)

// Result represents a setting of entities which fulfills the
// constraints of the query which generated it.
type Result interface {
	Entity(name string) eav.Entity
}

// ResultIterator is used to iterate results of a query.
// Iteration can be halted with the use of iterutils.StopIteration.
type ResultIterator func(r Result) error

// Evaluate will evaluate the query against the database.
func (q *Query) Evaluate(db eav.Database, f ResultIterator) error {
	return newEvalContext(q).iterate(db, f)
}

// evalContext implements Result and accumulates the state during the
// evaluation of a query.
type evalContext struct {
	q *Query

	// n is the index of the entity currently being populated.
	// When n is equal to the length of entities, then we have
	// a solution for all entities.
	n entity

	// values stores the current known setting of values for
	// entities.
	values []eav.Values
	// entities stores the currently populates entities during iteration.
	entities []eav.Entity
	// remaining is the set of facts which have not been fulfilled.
	remaining []fact

	// freeList is an optimization to avoid allocating too many extra
	// evalContexts for a given query. During evaluation we'll need to
	// create an evalContext for each depth of the query evaluation.
	// If we did not maintain a free-list, we'd end up allocating
	// evalContexts on the order of the number of results explored
	// as opposed to the depth of the query.
	freeList *list.List
}

func newEvalContext(q *Query) *evalContext {
	return &evalContext{
		q:        q,
		entities: make([]eav.Entity, len(q.nodes)),
		values: append(
			make([]eav.Values, 0, len(q.nodes)),
			q.prepared.nodeValues...),
		remaining: append(
			make([]fact, 0, len(q.facts)),
			q.prepared.remaining...),
		freeList: list.New(),
	}
}

func (ec *evalContext) Entity(name string) eav.Entity {
	n, ok := ec.q.nodes[name]
	if !ok {
		return nil
	}
	return ec.entities[n]
}

func (ec *evalContext) iterate(tree eav.Database, f ResultIterator) (err error) {
	// TODO(ajwerner): Allow client to return iterutil.StopIteration and have it
	// propagate all the way up through the call stack to Evaluate.
	if int(ec.n) == len(ec.entities) {
		for _, f := range ec.q.filters {
			if !f(ec) {
				return nil
			}
		}
		return f(ec)
	}
	return tree.Iterate(ec.values[ec.n], func(e eav.Entity) error {
		// Check for contradictions and return early if one is found.
		vals := ec.values[ec.n].Copy()

		sc := tree.Schema()
		var contradictionFound bool
		e.Attributes().ForEach(tree.Schema(), func(a eav.Attribute) (wantMore bool) {
			if got := e.Get(a); got != nil {
				contradictionFound = maybeSet(a, vals, got)
			}
			return !contradictionFound
		})
		if contradictionFound {
			vals.Release()
			return nil
		}
		child := ec.getChild()
		defer child.releaseChild()
		child.values[child.n] = vals
		child.entities[child.n] = e
		eav.SetAllFrom(sc, child.values[child.n], e)
		foundContradiction, remaining := propagateConstants(child.remaining, child.values)
		if foundContradiction {
			return nil
		}
		child.remaining = remaining
		child.n++
		return child.iterate(tree, f)
	})
}

func propagateConstants(
	facts []fact, nodeValues []eav.Values,
) (foundContradiction bool, remaining []fact) {
	// TODO(ajwerner): Get more sophisticated figuring out which facts can now
	// be fulfilled given new information.
	for len(facts) > 0 {
		truncated := facts[:0]
		for _, f := range facts {
			switch av := f.value.(type) {
			case *Reference:
				set, foundContradiction := maybeSetFromGetter(
					f.attr, av.attr, nodeValues[f.node], nodeValues[av.n],
				)
				if !set && !foundContradiction {
					set, foundContradiction = maybeSetFromGetter(
						av.attr, f.attr, nodeValues[av.n], nodeValues[f.node],
					)
				}
				if foundContradiction {
					return foundContradiction, nil
				}

				// Retain the fact.
				if !set {
					truncated = append(truncated, f)
				}

			case eav.Value:
				if maybeSet(f.attr, nodeValues[f.node], av) {
					return true, nil
				}
			default:
				panic(errors.AssertionFailedf("unknown fact type %T", av))
			}
		}
		if len(truncated) < len(facts) {
			facts = truncated
		} else {
			break
		}
	}
	return false, facts
}

func (ec *evalContext) getChild() *evalContext {
	var child *evalContext
	if got := ec.freeList.Front(); got != nil {
		ec.freeList.Remove(got)
		child = got.Value.(*evalContext)
	} else {
		child = &evalContext{
			q: ec.q,

			entities:  make([]eav.Entity, 0, len(ec.entities)),
			values:    make([]eav.Values, 0, len(ec.values)),
			remaining: make([]fact, 0, len(ec.remaining)),

			freeList: ec.freeList,
		}
	}
	child.n = ec.n
	child.entities = append(child.entities[:0], ec.entities...)
	child.values = child.values[:0]
	for _, m := range ec.values {
		child.values = append(child.values, m.Copy())
	}
	child.remaining = append(child.remaining[:0], ec.remaining...)
	return child
}

func (ec *evalContext) releaseChild() {
	for i := range ec.values {
		ec.values[i].Release()
	}
	ec.freeList.PushBack(ec)
}

func maybeSetFromGetter(
	setAttr, sourceAttr eav.Attribute, vals eav.Values, source eav.Entity,
) (set, foundContradiction bool) {
	got := source.Get(sourceAttr)
	if got == nil {
		return false, false
	}
	if foundContradiction = maybeSet(setAttr, vals, got); foundContradiction {
		return false, foundContradiction
	}
	return true, false
}

func maybeSet(a eav.Attribute, vals eav.Values, val eav.Value) (foundContradiction bool) {
	if have := vals.Get(a); have != nil {
		if _, _, eq := have.Compare(val); !eq {
			return true
		}
	}
	vals.Set(a, val)
	return false
}
