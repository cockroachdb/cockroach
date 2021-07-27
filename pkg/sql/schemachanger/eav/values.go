// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eav

import "sync"

// Values is used to store a select elements from a Tree.
type Values map[Ordinal]Value

// GetValues retrieves a Values instance from the sync pool. Use
// Release to put it back in the pool. Values are often used in
// contexts with well-defined lifecycles, hence the pooling.
func GetValues() Values {
	return valuesSyncPool.Get().(Values)
}

// Copy clones the Values into a newly allocated map.
func (vv Values) Copy() Values {
	cpy := GetValues()
	for k, v := range vv {
		cpy[k] = v
	}
	return cpy
}

// Release releases the Values back into the pool.
func (vv *Values) Release() {
	v := *vv
	for k := range v {
		delete(v, k)
	}
	valuesSyncPool.Put(v)
	*vv = nil
}

// Attributes returns the set of attributes defined on this Values.
func (vv Values) Attributes() OrdinalSet {
	var ret OrdinalSet
	for o := range vv {
		ret = ret.Add(o)
	}
	return ret
}

// Set setts the given attribute value. Note that v may be nil and it will
// still set mark this attribute as being set.
func (vv Values) Set(attr Attribute, v Value) {
	if v != nil {
		vv[attr.Ordinal()] = v
	}
}

// Get retrieves the given attribute value.
func (vv Values) Get(a Attribute) Value {
	return vv[a.Ordinal()]
}

// SetFrom sets the attributes supplied from the container.
func (vv Values) SetFrom(e Entity, attrs ...Attribute) {
	for _, a := range attrs {
		vv.Set(a, e.Get(a))
	}
}

// SetAllFrom sets all of the values in Values from the Entity.
func SetAllFrom(s Schema, v Values, e Entity) {
	e.Attributes().ForEach(s, func(a Attribute) (wantMore bool) {
		v.Set(a, e.Get(a))
		return true
	})
}

var valuesSyncPool = sync.Pool{
	New: func() interface{} { return make(Values) },
}
