// Copyright 2014 Square Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"fmt"
	"reflect"
	"testing"
)

func fieldByName(m fieldMap, v reflect.Value, name string) reflect.Value {
	v = reflect.Indirect(v)
	f, ok := m[name]
	if !ok {
		return reflect.Value{}
	}
	return v.FieldByIndex(f.Index)
}

func TestMappingBasic(t *testing.T) {
	type Foo struct {
		A, B, C int64
	}

	f := Foo{1, 2, 3}
	fv := reflect.ValueOf(f)
	m, err := getMapping(reflect.TypeOf(f), "", nil)
	if err != nil {
		t.Fatal(err)
	}

	v := fieldByName(m, fv, "A")
	if v.Int() != f.A {
		t.Errorf("Expected %d, got got %d", f.A, v.Int())
	}
	v = fieldByName(m, fv, "B")
	if v.Int() != f.B {
		t.Errorf("Expected %d, got got %d", f.B, v.Int())
	}
	v = fieldByName(m, fv, "C")
	if v.Int() != f.C {
		t.Errorf("Expected %d, got got %d", f.C, v.Int())
	}
}

func TestMappingEmbedded(t *testing.T) {
	type Foo struct {
		A int64
	}

	type Bar struct {
		Foo
		B int64
	}

	type Baz struct {
		A int64
		Bar
	}

	z := Baz{}
	z.A = 1
	z.B = 2
	z.Bar.Foo.A = 3
	zv := reflect.ValueOf(z)
	m, err := getMapping(reflect.TypeOf(z), "", nil)
	if err != nil {
		t.Fatal(err)
	}

	v := fieldByName(m, zv, "A")
	if v.Int() != z.A {
		t.Errorf("Expecting %d, got %d", z.A, v.Int())
	}
	v = fieldByName(m, zv, "B")
	if v.Int() != z.B {
		t.Errorf("Expecting %d, got %d", z.B, v.Int())
	}

	type Blah struct {
		A int64
		B int64 `db:"a"`
	}
	m, err = getDBFields(reflect.TypeOf(Blah{}))
	if err == nil {
		t.Errorf("expected error, but found success")
	}
	fmt.Printf("%s\n", err)
}

func TestDBFields(t *testing.T) {
	type Foo struct {
		A int
		B string
		C bool `db:"foo"`
	}
	f := Foo{1, "two", true}
	m, err := getDBFields(reflect.TypeOf(f))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "foo"} {
		if _, ok := m[key]; !ok {
			t.Errorf("Expected to find key %s in mapping but did not", key)
		}
	}

	type Bar struct {
		D int
		E int
		Foo
	}
	b := Bar{D: 3, E: 4, Foo: f}
	m, err = getDBFields(reflect.TypeOf(b))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "foo", "d", "e"} {
		if _, ok := m[key]; !ok {
			t.Errorf("Expected to find key %s in mapping but did not", key)
		}
	}

	type Qux struct {
		Bar     `db:"baz"`
		F       int
		G       bool `db:"qux"`
		Ignored bool `db:"-"`
	}
	q := Qux{b, 5, true, false}
	m, err = getDBFields(reflect.TypeOf(q))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"baz", "f", "qux"} {
		if _, ok := m[key]; !ok {
			t.Errorf("Expected to find key %s in mapping but did not", key)
		}
	}

	if _, ok := m["ignored"]; ok {
		t.Errorf("Expected to ignore `Ignored` field")
	}
}
