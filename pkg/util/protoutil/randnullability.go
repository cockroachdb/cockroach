// Copyright 2017 The Cockroach Authors.
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

package protoutil

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"golang.org/x/sync/syncmap"
)

func hookVisitor(v reflect.Value, hook func(v reflect.Value, i int)) {
	if v.Kind() == reflect.Struct {
		for i, n := 0, v.NumField(); i < n; i++ {
			field := v.Type().Field(i)
			tag, ok := field.Tag.Lookup("cockroachdb")
			if !ok || !strings.Contains(tag, "randnullable") {
				continue
			}
			kind := field.Type.Kind()
			if kind != reflect.Ptr {
				panic(fmt.Sprintf("cannot fuzz a field of type %v", kind))
			}
			if !v.Field(i).IsNil() {
				// Don't overwrite existing pointers.
				continue
			}
			hook(v, i)
		}
	}
}

func hookInsertZero(v reflect.Value, i int) {
	field := v.Type().Field(i)
	ptrToZero := reflect.New(field.Type.Elem())
	v.Field(i).Set(ptrToZero)
}

// ZeroInsertingVisitor replaces all nil struct fields which have the `cockroachdb:"randnullable"`
// tag by pointers to a zero value.
func ZeroInsertingVisitor(v reflect.Value) {
	hookVisitor(v, hookInsertZero)
}

type k struct {
	fieldType reflect.Type
	index     int
}

var insertZero syncmap.Map

var r, _ = randutil.NewPseudoRand()

// RandomZeroInsertingVisitor inserts zero values randomly for fields that have the
// `cockroachdb:"randnullable"` struct tag set. "Randomly" here means the following: upon first
// encountering a given type (in a field with a tag) a coin is flipped, the result is stored until
// the process terminates, and the result of the flip is used to determine whether to insert zeroes
// for this type.
func RandomZeroInsertingVisitor(v reflect.Value) {
	hookVisitor(v, func(v reflect.Value, i int) {
		typ := v.Type()
		key := k{
			fieldType: typ,
			index:     i,
		}
		actual, loaded := insertZero.LoadOrStore(key, r.Intn(2) == 0)
		if !loaded {
			ctx := context.Background()
			buf := make([]byte, 8192)
			buf = buf[:runtime.Stack(buf, true)]
			if bytes.Contains(buf, []byte(`.Benchmark`)) {
				log.Fatalf(ctx, "must run with `-tags bench`")
			}

			log.Infof(ctx, "inserting null for (%v).%v: %t", typ, typ.Field(i).Name, actual)
		}
		if b := actual.(bool); b {
			hookInsertZero(v, i)
		}
	})
}
