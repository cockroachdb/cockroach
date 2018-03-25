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
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type privateKeyFunc func(ps *privateStorage, private interface{}) interface{}

// privateKeyFuncMap contains private types for which there is a function to
// derive a key for that type.
var privateKeyFuncMap = map[reflect.Type]privateKeyFunc{
	// SQL types.T types.
	reflect.TypeOf(types.Unknown):     (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.Bool):        (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.Int):         (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.Float):       (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.Decimal):     (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.String):      (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.Bytes):       (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.Date):        (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.Time):        (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.Timestamp):   (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.TimestampTZ): (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.Interval):    (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.JSON):        (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.UUID):        (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.INet):        (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(types.Any):         (*privateStorage).usePrivateAsKey,

	// Datum types (almost all of them use pointers that aren't comparable).
	reflect.TypeOf(tree.DNull): (*privateStorage).usePrivateAsKey,

	// opt package types.
	reflect.TypeOf(opt.ColumnID(0)): (*privateStorage).usePrivateAsKey,
	reflect.TypeOf(opt.ColSet{}):    (*privateStorage).makeColSetKey,
	reflect.TypeOf(opt.ColList{}):   (*privateStorage).makeColListKey,

	// memo package types.
	reflect.TypeOf(&ScanOpDef{}):   (*privateStorage).makeScanOpDefKey,
	reflect.TypeOf(&FuncOpDef{}):   (*privateStorage).makeFuncOpDefKey,
	reflect.TypeOf(&SetOpColMap{}): (*privateStorage).makeSetOpColMapKey,
	reflect.TypeOf(Ordering{}):     (*privateStorage).makeOrderingKey,
}

// Declare custom string types for private value types that are not legal keys
// on their own. Even if the serialized string value is the same for private
// values of different types, the interface{} value will be different because
// it includes the type as well as the string value.
type sqlTypeKey string
type datumKey string
type colSetKey string
type colListKey string
type orderingKey string
type scanOpDefKey string

// privateStorage stores private values for opt expressions. Each value is
// interned, which means that each unique value is stored at most once. If the
// same value is added twice to storage, the same storage is used, and the same
// private id is returned by the intern method.
//
// To use privateStorage, first call the init method to initialize storage.
// Call the intern method to add privates to storage and get back a unique
// private id. Call the lookup method with an id to retrieve a previously added
// private.
//
// Each different type of value needs a key derivation function, which maps
// from the private value to a key value. The key value must be a legal Go map
// key value, and equivalent private values must always map to the same key
// value. For example, the key cannot contain be a slice (because it's not a
// legal Go key value) or contain a non-interned pointer (because pointers to
// equivalent values in different memory locations do not map to the same key
// value).
//
// If a private value is not a legal key on its own, then a key must be
// derived from the value. privateStorage does this by serializing the value as
// a slice of bytes, and converting that to a custom-typed string. The custom
// type ensures that two values of different types do not have the same key,
// even if their serialized value is the same sequence of bytes.
type privateStorage struct {
	// privatesMap maps from the interning key to the index of the private
	// value in the privates slice. Note that PrivateID 0 is invalid in order
	// to indicate an unknown private.
	privatesMap map[interface{}]PrivateID
	privates    []interface{}

	datumCtx tree.FmtCtx
	buf      bytes.Buffer
}

// init must be called before privateStorage can be used.
func (ps *privateStorage) init() {
	ps.datumCtx = tree.MakeFmtCtx(&ps.buf, tree.FmtCheckEquivalence)
	ps.privatesMap = make(map[interface{}]PrivateID)
	ps.privates = make([]interface{}, 1)
}

// intern adds the given private value to storage and returns an id that can
// later be used to retrieve the value by calling the lookup method. If the
// value has been previously added to storage, then intern always returns the
// same private id that was returned from the previous call.
func (ps *privateStorage) intern(private interface{}) PrivateID {
	var key interface{}
	typ := reflect.TypeOf(private)
	fn, ok := privateKeyFuncMap[typ]
	if ok {
		key = fn(ps, private)
	} else {
		// Type is not in map, so test for supported interfaces.
		switch t := private.(type) {
		case types.T:
			// Handle other SQL types that are not special-cased in map.
			key = sqlTypeKey(t.String())

		case tree.Datum:
			// Handle other datums that are not special-cased in map.
			ps.buf.Reset()
			ps.datumCtx.FormatNode(t)
			key = datumKey(ps.buf.String())

		case tree.TypedExpr:
			// TypedExprs are used with UnsupportedExpr, and can just be
			// interned as a pointer.
			key = t

		default:
			panic(fmt.Sprintf("unhandled private key type: %v", typ))
		}
	}

	id, ok := ps.privatesMap[key]
	if !ok {
		id = PrivateID(len(ps.privates))
		ps.privates = append(ps.privates, private)
		ps.privatesMap[key] = id
	}
	return id
}

// lookup returns a private value previously interned by privateStorage.
func (ps *privateStorage) lookup(id PrivateID) interface{} {
	return ps.privates[id]
}

// usePrivateAsKey returns the private value as its own key, since its a valid
// Go map key value.
func (ps *privateStorage) usePrivateAsKey(private interface{}) interface{} {
	return private
}

// makeColSetKey creates a key for a opt.ColSet private value by writing a
// sequence of variable-sized uint64 values in ascending order.
func (ps *privateStorage) makeColSetKey(private interface{}) interface{} {
	ps.buf.Reset()
	ps.writeColSetKey(private.(opt.ColSet))
	return colSetKey(ps.buf.String())
}

// makeColListKey creates a key for a opt.ColList private value by writing a
// sequence of variable-sized uint64 values in the same order as the list.
func (ps *privateStorage) makeColListKey(private interface{}) interface{} {
	ps.buf.Reset()
	ps.writeColListKey(private.(opt.ColList))
	return colListKey(ps.buf.String())
}

// makeOrderingKey creates a key for a memo.Ordering private value by writing a
// sequence of variable-sized int64 values in the same order as the ordering.
func (ps *privateStorage) makeOrderingKey(private interface{}) interface{} {
	ps.buf.Reset()
	var buf [10]byte
	for _, col := range private.(Ordering) {
		cnt := binary.PutVarint(buf[:], int64(col))
		ps.buf.Write(buf[:cnt])
	}
	return orderingKey(ps.buf.String())
}

// makeScanOpDefKey creates a key for a memo.ScanOpDef private value by writing
// its Table field as a variable-sized uint64 value, and its Cols field in the
// same way as makeColSetKey.
func (ps *privateStorage) makeScanOpDefKey(private interface{}) interface{} {
	def := private.(*ScanOpDef)
	ps.buf.Reset()
	ps.writeUvarint(uint64(def.Table))
	ps.writeColSetKey(def.Cols)
	return scanOpDefKey(ps.buf.String())
}

// makeFuncOpKey creates a key for a memo.FuncOpDef private value by writing
// its Overload field as a pointer. The Overload pointer is already "interned",
// because it points to the fixed address of an entry in the builtins.Builtins
// slice.
func (ps *privateStorage) makeFuncOpDefKey(private interface{}) interface{} {
	return private.(*FuncOpDef).Overload
}

// makeSetOpColMapKey creates a key for a memo.SetOpColMap private value by
// writing each of its fields in the same way as makeColListKey.
func (ps *privateStorage) makeSetOpColMapKey(private interface{}) interface{} {
	setOpColMap := private.(*SetOpColMap)
	ps.buf.Reset()
	ps.writeColListKey(setOpColMap.Left)
	ps.writeColListKey(setOpColMap.Right)
	ps.writeColListKey(setOpColMap.Out)
	return colListKey(ps.buf.String())
}

func (ps *privateStorage) writeUvarint(val uint64) {
	var buf [10]byte
	cnt := binary.PutUvarint(buf[:], val)
	ps.buf.Write(buf[:cnt])
}

func (ps *privateStorage) writeColSetKey(colSet opt.ColSet) {
	var buf [10]byte
	colSet.ForEach(func(i int) {
		cnt := binary.PutUvarint(buf[:], uint64(i))
		ps.buf.Write(buf[:cnt])
	})
}

func (ps *privateStorage) writeColListKey(colList opt.ColList) {
	var buf [9]byte
	for _, col := range colList {
		cnt := binary.PutUvarint(buf[:], uint64(col))
		ps.buf.Write(buf[:cnt])
	}
}
