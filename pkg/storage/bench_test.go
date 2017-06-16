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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"reflect"
	"testing"
)

func BenchmarkEquality(b *testing.B) {
	type uint64x1 struct{ i uint64 }
	type uint64x16 struct {
		i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15, i16 uint64
	}
	type uint1024x1 struct {
		m [16]uint64
	}
	type byteslicex1 struct {
		s1 []byte
	}
	type byteslicex16 struct {
		s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16 []byte
	}

	{
		// Satisfy TestStyle/TestMetaCheck.
		_ = uint64x1{i: 1}
		_ = uint64x16{
			i1: 1, i2: 2, i3: 3, i4: 4, i5: 5, i6: 6, i7: 7, i8: 8, i9: 9, i10: 10, i11: 11, i12: 12, i13: 13, i14: 14, i15: 15, i16: 16,
		}
		_ = uint1024x1{m: [16]uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}
		c := []byte("0")
		_ = byteslicex1{s1: c}
		_ = byteslicex16{s1: c, s2: c, s3: c, s4: c, s5: c, s6: c, s7: c, s8: c, s9: c, s10: c, s11: c, s12: c, s13: c, s14: c, s15: c, s16: c}
	}

	b.Run("nil", func(b *testing.B) {
		var x, y *int64
		b.Run("builtin", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = x == y
			}
		})
		b.Run("DeepEqual", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = reflect.DeepEqual(x, y)
			}
		})
	})

	b.Run("uint64x1", func(b *testing.B) {
		var x, y uint64x1
		b.Run("builtin", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = x == y
			}
		})
		b.Run("DeepEqual", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = reflect.DeepEqual(x, y)
			}
		})
	})

	b.Run("uint64x16", func(b *testing.B) {
		var x, y uint64x16
		b.Run("builtin", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = x == y
			}
		})
		b.Run("DeepEqual", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = reflect.DeepEqual(x, y)
			}
		})
	})

	b.Run("uint1024x1", func(b *testing.B) {
		var x, y uint1024x1
		b.Run("builtin", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = x == y
			}
		})
		b.Run("DeepEqual", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = reflect.DeepEqual(x, y)
			}
		})
	})

	b.Run("byteslicex1", func(b *testing.B) {
		var x, y byteslicex1
		b.Run("DeepEqual", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = reflect.DeepEqual(x, y)
			}
		})
	})

	b.Run("byteslicex16", func(b *testing.B) {
		var x, y byteslicex16
		b.Run("DeepEqual", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = reflect.DeepEqual(x, y)
			}
		})
	})

	b.Run("EvalResult", func(b *testing.B) {
		var x, y EvalResult
		b.Run("DeepEqual", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = reflect.DeepEqual(x, y)
			}
		})
	})
}
