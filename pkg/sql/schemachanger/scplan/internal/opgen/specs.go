// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"

type targetSpec struct {
	from, to        scpb.Status
	transitionSpecs []transitionSpec
}

type RevertibleFn func(scpb.Element, *opGenContext) bool

type transitionSpec struct {
	from       scpb.Status
	to         scpb.Status
	revertible RevertibleFn
	emitFns    []interface{}
}

func notRevertible(_ scpb.Element, _ *opGenContext) bool {
	return false
}

type transitionProperty interface {
	apply(spec *transitionSpec)
}

func to(to scpb.Status, properties ...transitionProperty) transitionSpec {
	ts := transitionSpec{
		to:         to,
		revertible: nil, /* revertible */
	}
	for _, p := range properties {
		p.apply(&ts)
	}
	return ts
}

func revertibleFunc(fn RevertibleFn) transitionProperty {
	return fn
}

func revertible(b bool) transitionProperty {
	return revertibleProperty(b)
}

func emit(fn interface{}) transitionProperty {
	return emitFnSpec{fn}
}

type revertibleProperty bool

func (r revertibleProperty) apply(spec *transitionSpec) {
	spec.revertible = nil /* revertible */
	if !r {
		spec.revertible = notRevertible
	}
}

func (r RevertibleFn) apply(spec *transitionSpec) {
	spec.revertible = r
}

var _ transitionProperty = revertibleProperty(true)

type emitFnSpec struct {
	fn interface{}
}

func (e emitFnSpec) apply(spec *transitionSpec) {
	spec.emitFns = append(spec.emitFns, e.fn)
}
