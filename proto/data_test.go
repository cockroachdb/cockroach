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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package proto

import (
	"math"
	"testing"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
)

func makeTS(walltime int64, logical int32) Timestamp {
	return Timestamp{
		WallTime: walltime,
		Logical:  logical,
	}
}

func TestLess(t *testing.T) {
	a := Timestamp{}
	b := Timestamp{}
	if a.Less(b) || b.Less(a) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	b = makeTS(1, 0)
	if !a.Less(b) {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = makeTS(1, 1)
	if !b.Less(a) {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

func TestEqual(t *testing.T) {
	a := Timestamp{}
	b := Timestamp{}
	if !a.Equal(b) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	b = makeTS(1, 0)
	if a.Equal(b) {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = makeTS(1, 1)
	if b.Equal(a) {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

func TestValueChecksumEmpty(t *testing.T) {
	k := []byte("key")
	v := Value{}
	// Before initializing checksum, always works.
	if err := v.VerifyChecksum(k); err != nil {
		t.Error(err)
	}
	if err := v.VerifyChecksum([]byte("key2")); err != nil {
		t.Error(err)
	}
	v.InitChecksum(k)
	if err := v.VerifyChecksum(k); err != nil {
		t.Error(err)
	}
}

func TestValueChecksumWithBytes(t *testing.T) {
	k := []byte("key")
	v := Value{Bytes: []byte("abc")}
	v.InitChecksum(k)
	if err := v.VerifyChecksum(k); err != nil {
		t.Error(err)
	}
	// Try a different key; should fail.
	if err := v.VerifyChecksum([]byte("key2")); err == nil {
		t.Error("expected checksum verification failure on different key")
	}
	// Mess with value.
	v.Bytes = []byte("abcd")
	if err := v.VerifyChecksum(k); err == nil {
		t.Error("expected checksum verification failure on different value")
	}
}

func TestValueChecksumWithInteger(t *testing.T) {
	k := []byte("key")
	testValues := []int64{0, 1, -1, math.MinInt64, math.MaxInt64}
	for _, i := range testValues {
		v := Value{Integer: gogoproto.Int64(i)}
		v.InitChecksum(k)
		if err := v.VerifyChecksum(k); err != nil {
			t.Error(err)
		}
		// Try a different key; should fail.
		if err := v.VerifyChecksum([]byte("key2")); err == nil {
			t.Error("expected checksum verification failure on different key")
		}
		// Mess with value.
		v.Integer = gogoproto.Int64(i + 1)
		if err := v.VerifyChecksum(k); err == nil {
			t.Error("expected checksum verification failure on different value")
		}
	}
}
