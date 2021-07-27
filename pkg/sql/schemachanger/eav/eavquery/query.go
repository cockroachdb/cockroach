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

// Query searches for sets of entities which uphold a set of constraints.
type Query struct {
	nodes   map[string]entity
	facts   []fact
	filters []Filter

	prepared preparedQuery
}

type preparedQuery struct {
	nodeValues []eav.Values
	remaining  []fact
}

// Builder is used to build a Query.
type Builder interface {

	// Entity is a named Entity which can constrain Query Results.
	Entity(name string) Entity

	// Filter allows the caller to pass a function which filters
	// the result set. The function must take exactly the number
	// of arguments as there are entities. The arguments must either
	// be scpb.Element or individual element types. If the function
	// signature contains an Element type then it will imply a constraint
	// on those entities.
	Filter(Filter)
}

// Filter is a predicate over a result set.
type Filter func(Result) bool

// Entity represents an element of a result which can be constrained either
// by setting properties using Is or by constraining other Nodes to relate
// to this Entity on a given Attribute.
type Entity interface {

	// Reference this entity's value of an Attribute.
	Reference(eav.Attribute) *Reference

	// Constrain this entity to have a certain value of this Attribute.
	// Con
	Constrain(attr eav.Attribute, val Value)
}

// MustBuild is used to construct a Query.
func MustBuild(f func(b Builder)) Query {
	var b builder
	b.q.nodes = make(map[string]entity)
	f(&b)
	b.q.prepared = prepare(b.q)
	return b.q
}

func prepare(q Query) preparedQuery {
	var pq preparedQuery
	var contradictionFound bool
	pq.nodeValues = make([]eav.Values, 0, len(q.nodes))
	for i := 0; i < len(q.nodes); i++ {
		pq.nodeValues = append(pq.nodeValues, eav.GetValues())
	}
	contradictionFound, pq.remaining = propagateConstants(q.facts, pq.nodeValues)
	if contradictionFound {
		panic(errors.AssertionFailedf("found contradiction"))
	}
	return pq
}

type builder struct {
	nodes []nodeBuilder
	q     Query
}

func (b *builder) Filter(f Filter) {
	b.q.filters = append(b.q.filters, f)
}

func (b *builder) Entity(name string) Entity {
	n, exists := b.q.nodes[name]
	if !exists {
		n = entity(len(b.q.nodes))
		b.q.nodes[name] = n
		b.nodes = append(b.nodes, nodeBuilder{b: b, n: n})
	}
	return &b.nodes[n]
}

type nodeBuilder struct {
	b *builder
	n entity
}

func (n *nodeBuilder) Constrain(a eav.Attribute, value Value) {
	switch value.(type) {
	case eav.Value, *Reference:
		if a.Type() != value.Type() {
			panic(errors.AssertionFailedf(
				"type mismatch for constraint Value of type %v != %v for attr %s",
				value.Type(), a.Type(), a.String(),
			))
		}
	default:
		panic(errors.AssertionFailedf("invalid Value of type %T", value))
	}
	n.b.q.facts = append(n.b.q.facts, fact{
		node:  n.n,
		attr:  a,
		value: value,
	})
}

func (n *nodeBuilder) Reference(attr eav.Attribute) *Reference {
	return &Reference{n: n.n, attr: attr}
}

type fact struct {
	node  entity
	attr  eav.Attribute
	value Value
}

// Reference refers to the attribute value of some entity.
type Reference struct {
	n    entity
	attr eav.Attribute
}

func (r *Reference) value() {}

// Type returns the type of the referenced attribute.
func (r *Reference) Type() eav.Type {
	return r.attr.Type()
}

type entity int

// Value is used to mark a value that a entity must take on for a given
// attribute.
type Value interface {
	Type() eav.Type
}

// EqOn is syntactic sugar to state that two entities match on the specified
// attrs.
func EqOn(a, b Entity, attrs ...eav.Attribute) {
	for _, attr := range attrs {
		a.Constrain(attr, b.Reference(attr))
	}
}
