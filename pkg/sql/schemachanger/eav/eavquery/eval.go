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
	if len(q.nodes) == 0 {
		return nil
	}

	for i := range q.prepared.parts {
		ec := getEvalContext(q, &q.prepared.parts[i], db, f)
		if err := ec.db.Iterate(ec.frames[0].values[0], ec); err != nil {
			putEvalContext(ec)
			return err
		}
		putEvalContext(ec)
	}
	return nil
}

func getEvalContext(q *Query, d *queryDisjuncts, db eav.Database, f ResultIterator) *evalContext {
	ec := getCachedEvalContext(d)
	if ec == nil {
		ec = newEvalContext(q, d)
	}
	ec.db = db
	ec.ri = f
	ec.init(d)
	return ec
}

func putEvalContext(ec *evalContext) {
	ec.db, ec.ri = nil, nil
	for i := range ec.frames[0].values {
		ec.frames[0].values[i].Release()
	}
	ec.q.prepared.c.Lock()
	defer ec.q.prepared.c.Unlock()
	if ec.q.prepared.c.ec == nil {
		ec.q.prepared.c.ec = ec
	}
}

func getCachedEvalContext(q *queryDisjuncts) *evalContext {
	q.c.Lock()
	defer q.c.Unlock()
	if q.c.ec != nil {
		ec := q.c.ec
		q.c.ec = nil
		return ec
	}
	return nil
}

type evalFrame struct {
	frame     int
	values    []eav.Values
	remaining []fact
}

// evalContext implements Result and accumulates the state during the
// evaluation of a query.
type evalContext struct {
	q  *Query
	d  *queryDisjuncts
	db eav.Database
	ri ResultIterator

	depth, cur int
	entities   []eav.Entity
	frames     []evalFrame
}

func (ec *evalContext) getEntity(n entity) eav.Entity {
	if int(n) < ec.cur {
		return ec.entities[n]
	}
	return &ec.frames[ec.cur].values[n]
}

func newEvalContext(q *Query, d *queryDisjuncts) *evalContext {
	depth := len(q.nodes)
	frames := make([]evalFrame, depth)
	values := make([]eav.Values, depth*depth)
	facts := make([]fact, depth*len(q.facts))

	for i := range frames {
		frames[i] = evalFrame{
			frame:     i,
			values:    values[i*depth : (i+1)*depth],
			remaining: facts[i*len(q.facts) : (i+1)*len(q.facts)][:0],
		}
	}

	return &evalContext{
		q:        q,
		d:        d,
		depth:    depth,
		entities: make([]eav.Entity, depth),
		frames:   frames,
	}
}

func (ec *evalContext) init(d *queryDisjuncts) {
	ec.cur = 0
	frames := ec.frames
	frames[0].remaining = append(frames[0].remaining[:0], d.remaining...)
	for i := 0; i < len(d.nodeValues); i++ {
		frames[0].values[i] = d.nodeValues[i].Copy()
	}
}

func (ec *evalContext) Visit(e eav.Entity) error {
	defer func() {
		ec.cur--
		ec.entities[ec.cur] = nil
	}()
	ec.entities[ec.cur] = e
	ec.cur++

	if ec.cur == ec.depth {
		for _, f := range ec.q.filters {
			if !f(ec) {
				return nil
			}
		}
		return ec.ri(ec)
	}

	frame := &ec.frames[ec.cur]
	parent := &ec.frames[ec.cur-1]
	for i := ec.cur; i < ec.depth; i++ {
		frame.values[i] = parent.values[i].Copy()
	}
	frame.remaining = append(frame.remaining[:0], parent.remaining...)
	defer func() {
		for i := ec.cur; i < ec.depth; i++ {
			frame.values[i].Release()
		}
	}()
	// Check for contradictions and return early if one is found.
	foundContradiction, remaining := propagateConstants(frame.remaining, ec)
	if foundContradiction {
		return nil
	}
	frame.remaining = remaining
	return ec.db.Iterate(frame.values[ec.cur], ec)

}

func (ec *evalContext) Entity(name string) eav.Entity {
	n, ok := ec.q.nodes[name]
	if !ok {
		return nil
	}
	return ec.entities[n]
}

type entities interface {
	getEntity(entity) eav.Entity
}

func propagateConstants(facts []fact, e entities) (foundContradiction bool, remaining []fact) {
	// TODO(ajwerner): Get more sophisticated figuring out which facts can now
	// be fulfilled given new information.
	for len(facts) > 0 {
		truncated := facts[:0]
		for _, f := range facts {
			switch av := f.value.(type) {
			case *Reference:
				set, foundContradiction := maybeSetFromGetter(
					f.attr, av.attr, e.getEntity(f.node), e.getEntity(av.n),
				)
				if !set && !foundContradiction {
					set, foundContradiction = maybeSetFromGetter(
						av.attr, f.attr, e.getEntity(av.n), e.getEntity(f.node),
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
				if maybeSet(f.attr, e.getEntity(f.node), av) {
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

func maybeSetFromGetter(
	setAttr, sourceAttr eav.Attribute, vals eav.Entity, source eav.Entity,
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

func maybeSet(a eav.Attribute, vals eav.Entity, val eav.Value) (foundContradiction bool) {
	if have := vals.Get(a); have != nil {
		if _, _, eq := have.Compare(val); !eq {
			return true
		}
	}
	if vals, ok := vals.(*eav.Values); ok {
		vals.Set(a, val)
		return false
	}
	return true
}
