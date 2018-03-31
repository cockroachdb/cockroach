// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package memo

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// privateStorage stores private values for opt expressions. Each value is
// interned, which means that each unique value is stored at most once. If the
// same value is added twice to storage, the same storage is used, and the same
// private id is returned by the intern method.
//
// To use privateStorage, first call the init method to initialize storage.
// Call one of the intern method to add a private to storage and get back a
// unique private id. Call the lookup method with an id to retrieve a previously
// added private.
//
// Each different type of value needs a key derivation function, which maps
// from the private value to a key value. The key value must be a legal Go map
// key value, and equivalent private values must always map to the same key
// value. For example, the key cannot contain a slice (because it's not a legal
// Go key value) or contain a non-interned pointer (because pointers to
// equivalent values in different memory locations do not map to the same key
// value).
type privateStorage struct {
	// privatesMap maps from the interning key to the index of the private
	// value in the privates slice. Note that PrivateID 0 is invalid in order
	// to indicate an unknown private.
	privatesMap map[privateKey]PrivateID
	privates    []interface{}

	// datumCtx is used to get the string representation of datum values.
	datumCtx tree.FmtCtx

	// keyBuf is temporary "scratch" storage that's used to build keys.
	keyBuf keyBuffer
}

// privateKey is used as the key for the privates map. Different types of
// private values can use either or both fields to construct a unique key. For
// example, FuncOpDef values use only the iface field to store a pointer to the
// Builtin struct. Other types, like tree.Datum, use the iface field to store
// the reflect.Type of the value, and the str field to store its string
// representation. The key derived for every type/value combo must be guaranteed
// to never collide with any other.
type privateKey struct {
	iface interface{}
	str   string
}

// init must be called before privateStorage can be used.
func (ps *privateStorage) init() {
	ps.datumCtx = tree.MakeFmtCtx(&ps.keyBuf.Buffer, tree.FmtSimple)
	ps.privatesMap = make(map[privateKey]PrivateID)
	ps.privates = make([]interface{}, 1)
}

// lookup returns a private value previously interned by privateStorage.
func (ps *privateStorage) lookup(id PrivateID) interface{} {
	return ps.privates[id]
}

// internColumnID adds the given value to storage and returns an id that can
// later be used to retrieve the value by calling the lookup method. If the
// value has been previously added to storage, then internColumnID always
// returns the same private id that was returned from the previous call.
func (ps *privateStorage) internColumnID(colID opt.ColumnID) PrivateID {
	// The below code is carefully constructed to not allocate in the case
	// where the value is already in the map. Be careful when modifying.
	ps.keyBuf.Reset()
	ps.keyBuf.writeUvarint(uint64(colID))
	typ := (*opt.ColumnID)(nil)
	if id, ok := ps.privatesMap[privateKey{iface: typ, str: ps.keyBuf.String()}]; ok {
		return id
	}
	return ps.addValue(privateKey{iface: typ, str: ps.keyBuf.String()}, colID)
}

// internColSet adds the given value to storage and returns an id that can later
// be used to retrieve the value by calling the lookup method. If the value has
// been previously added to storage, then internColSet always returns the same
// private id that was returned from the previous call.
func (ps *privateStorage) internColSet(colSet opt.ColSet) PrivateID {
	// The below code is carefully constructed to not allocate in the case
	// where the value is already in the map. Be careful when modifying.
	ps.keyBuf.Reset()
	ps.keyBuf.writeColSet(colSet)
	typ := (*opt.ColSet)(nil)
	if id, ok := ps.privatesMap[privateKey{iface: typ, str: ps.keyBuf.String()}]; ok {
		return id
	}
	return ps.addValue(privateKey{iface: typ, str: ps.keyBuf.String()}, colSet)
}

// internColList adds the given value to storage and returns an id that can
// later be used to retrieve the value by calling the lookup method. If the
// value has been previously added to storage, then internColList always returns
// the same private id that was returned from the previous call.
func (ps *privateStorage) internColList(colList opt.ColList) PrivateID {
	// The below code is carefully constructed to not allocate in the case where
	// the value is already in the map. Be careful when modifying.
	ps.keyBuf.Reset()
	ps.keyBuf.writeColList(colList)
	typ := (*opt.ColList)(nil)
	if id, ok := ps.privatesMap[privateKey{iface: typ, str: ps.keyBuf.String()}]; ok {
		return id
	}
	return ps.addValue(privateKey{iface: typ, str: ps.keyBuf.String()}, colList)
}

// internOrdering adds the given value to storage and returns an id that can
// later be used to retrieve the value by calling the lookup method. If the
// value has been previously added to storage, then internOrdering always
// returns the same private id that was returned from the previous call.
func (ps *privateStorage) internOrdering(ordering Ordering) PrivateID {
	// The below code is carefully constructed to not allocate in the case where
	// the value is already in the map. Be careful when modifying.
	ps.keyBuf.Reset()
	for _, col := range ordering {
		ps.keyBuf.writeVarint(int64(col))
	}
	typ := (*Ordering)(nil)
	if id, ok := ps.privatesMap[privateKey{iface: typ, str: ps.keyBuf.String()}]; ok {
		return id
	}
	return ps.addValue(privateKey{iface: typ, str: ps.keyBuf.String()}, ordering)
}

// internFuncOpDef adds the given value to storage and returns an id that can
// later be used to retrieve the value by calling the lookup method. If the
// value has been previously added to storage, then internFuncOpDef always
// returns the same private id that was returned from the previous call.
func (ps *privateStorage) internFuncOpDef(def *FuncOpDef) PrivateID {
	// The below code is carefully constructed to not allocate in the case where
	// the value is already in the map. Be careful when modifying.
	// The Overload field is already interned, because it's the address of one
	// of the Builtin structs in the builtins package.
	if id, ok := ps.privatesMap[privateKey{iface: def.Overload}]; ok {
		return id
	}
	return ps.addValue(privateKey{iface: def.Overload}, def)
}

// internScanOpDef adds the given value to storage and returns an id that can
// later be used to retrieve the value by calling the lookup method. If the
// value has been previously added to storage, then internScanOpDef always
// returns the same private id that was returned from the previous call.
func (ps *privateStorage) internScanOpDef(def *ScanOpDef) PrivateID {
	// The below code is carefully constructed to not allocate in the case where
	// the value is already in the map. Be careful when modifying.
	ps.keyBuf.Reset()
	ps.keyBuf.writeUvarint(uint64(def.Table))
	ps.keyBuf.writeUvarint(uint64(def.Index))

	// TODO(radu): consider encoding the constraint rather than the pointer.
	// It's unclear if we have cases where we expect the same constraints to be
	// generated multiple times.
	ps.keyBuf.writeUvarint(uint64(uintptr(unsafe.Pointer(def.Constraint))))
	ps.keyBuf.writeVarint(def.HardLimit)
	ps.keyBuf.writeColSet(def.Cols)

	typ := (*ScanOpDef)(nil)
	if id, ok := ps.privatesMap[privateKey{iface: typ, str: ps.keyBuf.String()}]; ok {
		return id
	}
	return ps.addValue(privateKey{iface: typ, str: ps.keyBuf.String()}, def)
}

// internSetOpColMap adds the given value to storage and returns an id that can
// later be used to retrieve the value by calling the lookup method. If the
// value has been previously added to storage, then internSetOpColMap always
// returns the same private id that was returned from the previous call.
func (ps *privateStorage) internSetOpColMap(setOpColMap *SetOpColMap) PrivateID {
	// The below code is carefully constructed to not allocate in the case where
	// the value is already in the map. Be careful when modifying.
	// Write the values of each column list. This works with no length or
	// separator values because the lists are always the same length.
	ps.keyBuf.Reset()
	ps.keyBuf.writeColList(setOpColMap.Left)
	ps.keyBuf.writeColList(setOpColMap.Right)
	ps.keyBuf.writeColList(setOpColMap.Out)
	typ := (*SetOpColMap)(nil)
	if id, ok := ps.privatesMap[privateKey{iface: typ, str: ps.keyBuf.String()}]; ok {
		return id
	}
	return ps.addValue(privateKey{iface: typ, str: ps.keyBuf.String()}, setOpColMap)
}

// internDatum adds the given value to storage and returns an id that can later
// be used to retrieve the value by calling the lookup method. If the value has
// been previously added to storage, then internDatum always returns the same
// private id that was returned from the previous call.
func (ps *privateStorage) internDatum(datum tree.Datum) PrivateID {
	// The below code is carefully constructed to not allocate in the case where
	// the value is already in the map. Be careful when modifying.
	// Use the string representation of the datum value, and distinguish distinct
	// values with the same representation (i.e. "1" can be a Decimal or Int)
	// using the reflect.Type of the value.
	ps.keyBuf.Reset()
	datum.Format(&ps.datumCtx)
	typ := reflect.TypeOf(datum)
	id, ok := ps.privatesMap[privateKey{iface: typ, str: ps.keyBuf.String()}]
	if ok {
		return id
	}
	return ps.addValue(privateKey{iface: typ, str: ps.keyBuf.String()}, datum)
}

// internType adds the given value to storage and returns an id that can later
// be used to retrieve the value by calling the lookup method. If the value has
// been previously added to storage, then internType always returns the same
// private id that was returned from the previous call.
func (ps *privateStorage) internType(sqlType types.T) PrivateID {
	// The below code is carefully constructed to not allocate in the case where
	// the value is already in the map. Be careful when modifying.
	// While most types.T values are valid Go map keys, several are not, such as
	// types.TTuple. So use the string name of the type, and distinguish that
	// from other private types by using the reflect.Type of the types.T value.
	typ := reflect.TypeOf(sqlType)
	if id, ok := ps.privatesMap[privateKey{iface: typ, str: sqlType.String()}]; ok {
		return id
	}
	return ps.addValue(privateKey{iface: typ, str: sqlType.String()}, sqlType)
}

// internTypedExpr adds the given value to storage and returns an id that can
// later be used to retrieve the value by calling the lookup method. If the
// value has been previously added to storage, then internTypedExpr always
// returns the same private id that was returned from the previous call.
func (ps *privateStorage) internTypedExpr(expr tree.TypedExpr) PrivateID {
	// The below code is carefully constructed to not allocate in the case where
	// the value is already in the map. Be careful when modifying.
	if id, ok := ps.privatesMap[privateKey{iface: expr}]; ok {
		return id
	}
	return ps.addValue(privateKey{iface: expr}, expr)
}

func (ps *privateStorage) addValue(key privateKey, val interface{}) PrivateID {
	id := PrivateID(len(ps.privates))
	ps.privates = append(ps.privates, val)
	ps.privatesMap[key] = id
	return id
}

// keyBuffer wraps bytes.Buffer to provide several helper write methods.
type keyBuffer struct {
	bytes.Buffer
}

func (kb *keyBuffer) writeUvarint(val uint64) {
	var arr [10]byte
	cnt := binary.PutUvarint(arr[:], val)
	kb.Write(arr[:cnt])
}

func (kb *keyBuffer) writeVarint(val int64) {
	var arr [10]byte
	cnt := binary.PutVarint(arr[:], val)
	kb.Write(arr[:cnt])
}

// writeColSet writes a series of varints, one for each column in the set, in
// column id order.
func (kb *keyBuffer) writeColSet(colSet opt.ColSet) {
	var buf [10]byte
	colSet.ForEach(func(i int) {
		cnt := binary.PutUvarint(buf[:], uint64(i))
		kb.Write(buf[:cnt])
	})
}

// writeColSet writes a series of varints, one for each column in the list, in
// list order.
func (kb *keyBuffer) writeColList(colList opt.ColList) {
	var buf [10]byte
	for _, col := range colList {
		cnt := binary.PutUvarint(buf[:], uint64(col))
		kb.Write(buf[:cnt])
	}
}
