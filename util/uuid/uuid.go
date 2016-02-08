// Copyright 2015 The Cockroach Authors.
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
//
// Based on code from http://code.google.com/p/go-uuid/uuid

package uuid

import (
	"crypto/rand"
	"io"
	"unsafe"
)

const (
	// UUIDSize is the size in bytes of a UUID.
	UUIDSize = 16
)

var hexDigits = []byte("0123456789abcdef")

func fmtHex(in, out []byte) {
	for i, c := range in {
		out[i*2] = hexDigits[c>>4]
		out[i*2+1] = hexDigits[c&0xf]
	}
}

// UUID is a 16 byte UUID.
type UUID []byte

// NewUUID4 returns a new UUID (Version 4) using 16 random bytes or panics.
//
// The uniqueness depends on the strength of crypto/rand. Version 4
// UUIDs have 122 random bits.
func NewUUID4() UUID {
	uuid := make([]byte, UUIDSize)
	if _, err := io.ReadFull(rand.Reader, uuid); err != nil {
		panic(err) // rand should never fail
	}
	// UUID (Version 4) compliance.
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // Version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // Variant is 10
	return uuid
}

// String formats as hex xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx,
// or "" if u is invalid.
func (u UUID) String() string {
	if len(u) != UUIDSize {
		return ""
	}
	b := []byte(u)
	r := make([]byte, 36)
	fmtHex(b[:4], r[:8])
	r[8] = '-'
	fmtHex(b[4:6], r[9:13])
	r[13] = '-'
	fmtHex(b[6:8], r[14:18])
	r[18] = '-'
	fmtHex(b[8:10], r[19:23])
	r[23] = '-'
	fmtHex(b[10:], r[24:])
	// Transform our []byte into a string. This is actually safe because the
	// []byte never escapes this method.
	s := *(*string)(unsafe.Pointer(&r))
	return s
}

// Short formats the UUID using only the first four bytes for brevity.
func (u UUID) Short() string {
	if len(u) != UUIDSize {
		return ""
	}
	return u.String()[:8]
}
