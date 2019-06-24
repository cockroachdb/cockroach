// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import "reflect"

// EqualPtrFields uses reflection to check two "mirror" structures for matching pointer fields that
// point to the same object. Used to verify cloning/deep copy functions.
//
// Returns the names of equal pointer fields.
func EqualPtrFields(src, dst reflect.Value, prefix string) []string {
	t := dst.Type()
	if t.Kind() != reflect.Struct {
		return nil
	}
	if srcType := src.Type(); srcType != t {
		return nil
	}
	var res []string
	for i := 0; i < t.NumField(); i++ {
		srcF, dstF := src.Field(i), dst.Field(i)
		switch f := t.Field(i); f.Type.Kind() {
		case reflect.Ptr:
			if srcF.Interface() == dstF.Interface() {
				res = append(res, prefix+f.Name)
			}
		case reflect.Slice:
			if srcF.Pointer() == dstF.Pointer() {
				res = append(res, prefix+f.Name)
			}
			l := dstF.Len()
			if srcLen := srcF.Len(); srcLen < l {
				l = srcLen
			}
			for i := 0; i < l; i++ {
				res = append(res, EqualPtrFields(srcF.Index(i), dstF.Index(i), f.Name+".")...)
			}
		case reflect.Struct:
			res = append(res, EqualPtrFields(srcF, dstF, f.Name+".")...)
		}
	}
	return res
}
