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
	"sort"

	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

type queryBuilder struct {
	sc            *Schema
	variables     []Var
	variableSlots map[Var]slotIdx
	facts         []fact
	slots         []slot
	filters       []filter

	// Track whether the slotIdx holds an entity separately. We want to
	// know this in planning, but it'll be implicit during execution.
	// This might be badly named. What we really mean here is that the
	// slotIdx is a join target.
	slotIsEntity []bool
}

// newQuery constructs a query. Errors are panicked and caught
// in the calling NewQuery function.
func newQuery(sc *Schema, clauses Clauses) *Query {
	p := &queryBuilder{
		sc:            sc,
		variableSlots: map[Var]slotIdx{},
	}
	// Flatten away nested and clauses. We may need them at some point
	// if we add something like or-join or not-join. At time of writing,
	// the and case in processClause is an assertion failure.
	forDisplay := flattened(clauses)
	for _, t := range expanded(clauses) {
		p.processClause(t)
	}

	// Order the facts for unification. The ordering is first by variable
	// variable and then by attribute.
	//
	// TODO(ajwerner): For disjunctions using Any, the code currently uses
	// the index to constrain the search for each value in the "first"
	// such fact for the variable. Maybe we should trust the user order of
	// facts for a given variable rather than sorting by attribute ordinal.
	// However, we do need all the facts with the same variable and attribute
	// to be adjacent for the unification fixed point evaluation to work.
	entities := p.findEntitySlots()
	sort.SliceStable(p.facts, func(i, j int) bool {
		if p.facts[i].variable == p.facts[j].variable {
			return p.facts[i].attr < p.facts[j].attr
		}
		return p.facts[i].variable < p.facts[j].variable
	})
	// Ensure that the query does not already contain a contradiction as that
	// is almost definitely a bug.
	if contradictionFound, contradiction := unifyReturningContradiction(
		p.facts, p.slots, nil,
	); contradictionFound {
		panic(errors.Errorf(
			"query contains contradiction on %v", sc.attrs[contradiction.attr],
		))
	}
	return &Query{
		schema:        sc,
		variables:     p.variables,
		variableSlots: p.variableSlots,
		clauses:       forDisplay,
		entities:      entities,
		facts:         p.facts,
		slots:         p.slots,
		filters:       p.filters,
	}
}

func (p *queryBuilder) processClause(t Clause) {
	defer func() {
		if r := recover(); r != nil {
			rErr, ok := r.(error)
			if !ok {
				rErr = errors.AssertionFailedf("processClause: panic: %v", r)
			}
			encoded, err := yaml.Marshal(t)
			if err != nil {
				panic(errors.CombineErrors(rErr, errors.Wrap(
					err, "failed to encode clause",
				)))
			}
			panic(errors.Wrapf(
				rErr, "failed to process invalid clause %s", encoded,
			))
		}
	}()
	switch t := t.(type) {
	case tripleDecl:
		p.processTripleDecl(t)
	case eqDecl:
		p.processEqDecl(t)
	case filterDecl:
		p.processFilterDecl(t)
	case and:
		panic(errors.AssertionFailedf("and clauses should be flattened away"))
	default:
		panic(errors.AssertionFailedf("unknown clause type %T", t))
	}
}

func (p *queryBuilder) processTripleDecl(fd tripleDecl) {
	f := fact{
		variable: p.maybeAddVar(fd.entity, true /* entity */),
		attr:     p.sc.mustGetOrdinal(fd.attribute),
	}
	f.value = p.processValueExpr(fd.value)
	p.typeCheck(f)
	p.facts = append(p.facts, f)
}

func (p *queryBuilder) processEqDecl(t eqDecl) {
	varIdx := p.maybeAddVar(t.v, false)
	valueIdx := p.processValueExpr(t.expr)
	// This is somewhat inefficient but what it does is it lets
	// us state that the variable is equal to itself and that it
	// is equal to the value. It should be obvious that a variable
	// is equal to itself, but we want to have the normal contradiction
	// discovery machinery run.
	//
	// Note that there's no need to typeCheck because Self accepts all types.
	p.facts = append(p.facts,
		fact{
			variable: varIdx,
			attr:     p.sc.mustGetOrdinal(Self),
			value:    valueIdx,
		},
		fact{
			variable: varIdx,
			attr:     p.sc.mustGetOrdinal(Self),
			value:    varIdx,
		})
}

func (p *queryBuilder) processFilterDecl(t filterDecl) {
	fv := reflect.ValueOf(t.predicateFunc)
	// Type check the function.
	if err := checkNotNil(fv); err != nil {
		panic(errors.Wrapf(err, "nil filter function for variables %s", t.vars))
	}
	if fv.Kind() != reflect.Func {
		panic(errors.Errorf(
			"non-function %T filter function for variables %s",
			t.predicateFunc, t.vars,
		))
	}
	ft := fv.Type()
	if ft.NumOut() != 1 || ft.Out(0) != boolType {
		panic(errors.Errorf(
			"invalid non-bool return from %T filter function for variables %s",
			t.predicateFunc, t.vars,
		))
	}
	if ft.NumIn() != len(t.vars) {
		panic(errors.Errorf(
			"invalid %T filter function for variables %s accepts %d inputs",
			t.predicateFunc, t.vars, ft.NumIn(),
		))
	}

	slots := make([]slotIdx, len(t.vars))
	for i, v := range t.vars {
		slots[i] = p.maybeAddVar(v, false)
		// TODO(ajwerner): This should end up constraining the slot type, but
		// it currently doesn't. In fact, we have no way of constraining the
		// type for a non-entity variable. Probably the way this should go is
		// that the slots should carry constraints like types and any values.
		// Then, when we go to populate them, we can enforce the constraints.
		//
		// Instead, as a hack, we've got a runtime check on the types to fail
		// out if any of the types are not right.
		checkSlotType(&p.slots[slots[i]], ft.In(i))
	}
	p.filters = append(p.filters, filter{
		input:     slots,
		predicate: fv,
	})
}

func (p *queryBuilder) processValueExpr(rawValue expr) slotIdx {
	switch v := rawValue.(type) {
	case Var:
		return p.maybeAddVar(v, false)
	case anyExpr:
		sd := slot{
			any: make([]typedValue, len(v)),
		}
		for i, vv := range v {
			tv, err := makeComparableValue(vv)
			if err != nil {
				panic(err)
			}
			sd.any[i] = tv
		}
		return p.fillSlot(sd, false)
	case valueExpr:
		tv, err := makeComparableValue(v.value)
		if err != nil {
			panic(err)
		}
		return p.fillSlot(slot{typedValue: tv}, false)
	default:
		panic(errors.AssertionFailedf("unknown expr type %T", rawValue))
	}
}

func (p *queryBuilder) maybeAddVar(v Var, entity bool) slotIdx {
	if v == Blank {
		if entity {
			panic(errors.AssertionFailedf("cannot use _ as an entity"))
		}
		return p.fillSlot(slot{}, entity)
	}
	id, exists := p.variableSlots[v]
	if exists {
		if entity && !p.slotIsEntity[id] {
			p.slotIsEntity[id] = entity
		}
		return id
	}
	id = p.fillSlot(slot{}, entity)
	p.variables = append(p.variables, v)
	p.variableSlots[v] = id
	return id
}

func (p *queryBuilder) fillSlot(sd slot, isEntity bool) slotIdx {
	s := slotIdx(len(p.slots))
	p.slots = append(p.slots, sd)
	p.slotIsEntity = append(p.slotIsEntity, isEntity)
	return s
}

// findEntitySlots finds the slots which correspond to entity variableSlots in
// the order in which they appear. This will imply the user-requested join
// order.
func (p *queryBuilder) findEntitySlots() (entitySlots []slotIdx) {
	for i := range p.slots {
		if p.slotIsEntity[i] {
			entitySlots = append(entitySlots, slotIdx(i))
		}
	}
	return entitySlots
}

// typeCheck asserts that the value types for the fact are sane given the
// attribute.
func (p *queryBuilder) typeCheck(f fact) {
	s := &p.slots[f.value]
	if s.empty() && s.any == nil {
		return
	}
	switch f.attr {
	case p.sc.mustGetOrdinal(Type):
		checkSlotType(s, reflectTypeType)
	default:
		checkSlotType(s, p.sc.attrTypes[f.attr])
	}
}

var boolType = reflect.TypeOf((*bool)(nil)).Elem()

func checkSlotType(s *slot, exp reflect.Type) {
	if !s.empty() {
		if err := checkType(s.typ, exp); err != nil {
			panic(err)
		}
	}
	for i := range s.any {
		if err := checkType(s.any[i].typ, exp); err != nil {
			panic(err)
		}
	}
}
