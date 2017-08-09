// Copyright 2016 The Cockroach Authors.
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
