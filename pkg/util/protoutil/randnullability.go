// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protoutil

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

var flipCoin = func() func() bool {
	r, _ := randutil.NewPseudoRand()
	var mu syncutil.Mutex
	return func() bool {
		mu.Lock()
		n := r.Intn(2)
		mu.Unlock()
		return n == 0
	}
}()

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
		actual, loaded := insertZero.LoadOrStore(key, flipCoin())
		if !loaded {
			log.Infof(context.Background(), "inserting null for (%v).%v: %t", typ, typ.Field(i).Name, actual)
		}
		if b := actual.(bool); b {
			hookInsertZero(v, i)
		}
	})
}
