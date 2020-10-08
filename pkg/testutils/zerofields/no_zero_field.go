// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package zerofields

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/errors"
)

type zeroFieldErr struct {
	field string
}

func (err zeroFieldErr) Error() string {
	return fmt.Sprintf("expected %s field to be non-zero", err.field)
}

// NoZeroField returns nil if none of the fields of the struct underlying the
// interface are equal to the zero value, and an error otherwise.
// It will panic if the struct has unexported fields and for any non-struct.
func NoZeroField(v interface{}) error {
	ele := reflect.Indirect(reflect.ValueOf(v))
	eleT := ele.Type()
	for i := 0; i < ele.NumField(); i++ {
		f := ele.Field(i)
		n := eleT.Field(i).Name
		switch f.Kind() {
		case reflect.Struct:
			if err := NoZeroField(f.Interface()); err != nil {
				var zfe zeroFieldErr
				_ = errors.As(err, &zfe)
				zfe.field = fmt.Sprintf("%s.%s", n, zfe.field)
				return zfe
			}
		default:
			zero := reflect.Zero(f.Type())
			if reflect.DeepEqual(f.Interface(), zero.Interface()) {
				switch field := eleT.Field(i).Name; field {
				case "XXX_NoUnkeyedLiteral", "XXX_DiscardUnknown", "XXX_sizecache":
					// Ignore these special protobuf fields.
				default:
					return zeroFieldErr{field: n}
				}
			}
		}
	}
	return nil
}
