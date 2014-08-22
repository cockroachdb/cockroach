// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package engine

// #include <stdlib.h>
import "C"
import (
	"encoding/gob"
	"reflect"
	"unsafe"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
)

func init() {
	gob.Register(Counter(0))
	gob.Register(Appender(nil))
}

// Mergable types specify two operations:
// 1) Init, used to generate a default value to use as an existing
// value for an update that needs to be carried out on a key that
// could not be decoded into a Mergable of the corresponding type,
// for instance when the base key is not initialized.
//
// 2) Merge, the operation which maps the existing value and the
// update to a new Mergable. If the merge operation signals an
// error, that error is propagated to the underlying engine.
type Mergable interface {
	Init([]byte) Mergable
	Merge(Mergable) (Mergable, error)
}

// A Counter is a mergable data type that stores an int64 and
// implements a merge operation that increments the counter.
// It is initially zero.
type Counter int64

// Init returns a Counter with value 0 as a Mergable.
func (n Counter) Init(s []byte) Mergable {
	return Counter(0)
}

// Merge updates the value of this Counter with the supplied
// update and returns an error in case of an integer overflow.
func (n Counter) Merge(o Mergable) (Mergable, error) {
	m, ok := o.(Counter)
	if !ok {
		return n, util.Error("parameter is of wrong type")
	}
	if encoding.WillOverflow(int64(n), int64(m)) {
		return n, util.Errorf("merge error: %d + %d overflows", n, m)
	}
	result := Counter(n + m)
	return result, nil
}

// An Appender is a mergable data type initializing to an empty
// byte slice and using concatenation as the merge operation.
type Appender []byte

// Init returns an Appender storing the empty string.
func (s Appender) Init(t []byte) Mergable {
	return Appender(nil)
}

// Merge appends the input value to the string and returns the result.
func (s Appender) Merge(t Mergable) (Mergable, error) {
	m, ok := t.(Appender)
	if !ok {
		return s, util.Error("parameter is of wrong type")
	}
	return append(s, m...), nil
}

// goMerge takes existing and update byte slices. It first attempts
// to gob-unmarshal the update string, returning an error on failure.
// The unmarshaled value must satisfy the Mergable interface.
// Next, it unmarshals the existing string, falling back to the init
// value supplied by the update value's Init() method if necessary.
// The two values obtained in this way are merged and the result, or
// an error, returned.
func goMerge(existing, update []byte) ([]byte, error) {
	u, err := encoding.GobDecode(update)
	if err != nil {
		return nil, util.Errorf("merge: %v", err)
	}
	if _, ok := u.(Mergable); !ok {
		return nil, util.Error("update is not Mergable")
	}
	e, err := encoding.GobDecode(existing)
	if err != nil {
		e = u.(Mergable).Init(existing)
	}
	if reflect.TypeOf(u) != reflect.TypeOf(e) {
		return nil, util.Error("cannot merge values of different type")
	}
	newValue, err := e.(Mergable).Merge(u.(Mergable))
	if err != nil {
		return nil, err
	}
	return encoding.GobEncode(newValue)
}

//export mergeInit
// mergeInit returns a C string that the C glue code will use as existing
// value if NULL is encountered.
// The return value is not garbage collected.
func mergeInit(a *C.char, aLen C.size_t) (*C.char, C.size_t) {
	// As it stands we don't even need to allocate an empty string.
	return nil, 0
}

//export merge
// merge is a wrapper around goMerge, returning the result
// as a non-garbage collected C string.
// It is used by C code in this package.
func merge(a, b *C.char, aLen, bLen C.size_t) (*C.char, C.size_t) {
	result, err := goMerge(C.GoBytes(unsafe.Pointer(a), C.int(aLen)),
		C.GoBytes(unsafe.Pointer(b), C.int(bLen)))
	if err != nil {
		return nil, 0
	}
	// As above, the first return value will have to be manually freed
	// by the receiver.
	return C.CString(string(result)), C.size_t(len(result))
}
