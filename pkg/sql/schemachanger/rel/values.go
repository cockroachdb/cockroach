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

import "sync"

// valuesMap is a container for attributes.
//
// It stores the data in a format which is convenient for performing
// comparisons and lookups. If you want strongly typed data out of it,
// you need to use a Schema to retrieve that data. Note that the library
// expects all values to be stored in the map in the comparable, primitive
// form and not in the strongly typed format.
type valuesMap struct {
	attrs ordinalSet
	m     map[ordinal]interface{}
}

var valuesSyncPool = sync.Pool{
	New: func() interface{} {
		return &valuesMap{
			m: make(map[ordinal]interface{}),
		}
	},
}

func getValues() *valuesMap {
	return valuesSyncPool.Get().(*valuesMap)
}

func putValues(v *valuesMap) {
	v.clear()
	valuesSyncPool.Put(v)
}

func (vm *valuesMap) clear() {
	for k := range vm.m {
		delete(vm.m, k)
	}
	vm.attrs = 0
}

// get retrieves the primitive valuesMap stores in the valuesMap
// struct.
func (vm valuesMap) get(a ordinal) interface{} {
	return vm.m[a]
}

func (vm *valuesMap) add(ord ordinal, v interface{}) {
	vm.attrs = vm.attrs.add(ord)
	vm.m[ord] = v
}
