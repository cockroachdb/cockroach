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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// A varint encoding scheme based on sqlite4's varint encoding:
// http://sqlite.org/src4/doc/trunk/www/varint.wiki
//
// A variable length integer is an encoding of 64-bit unsigned integers
// into between 1 and 9 bytes. The encoding has the following properties:
//
// Smaller (and more common) values use fewer bytes and take up less space
// than larger (and less common) values.
//
// The length of any varint can be determined by looking at just the first
// byte of the encoding.
//
// Lexicographical and numeric ordering for varints are the same. Hence if
// a group of varints are order lexicographically (that is to say, if they
// are order by memcmp() with shorter varints coming first) then those varints
// will also be in numeric order. This property means that varints can be used
// as keys in the key/value backend storage and the records will occur in
// numerical order of the keys.
//
// The encoding is described by algorithms to decode (convert from varint to
// 8-byte unsigned integer) and to encode (convert from 8-byte unsigned
// integer to varint).
//
// Author: Andrew Bonventre (andybons@gmail.com)

package encoding

// writeBigEndian writes x into buf as a big-endian n-byte
// integer. If the buffer is too small, a panic will ensue.
func writeBigEndian(buf []byte, x uint64, n int) {
	for i := 1; i <= n; i++ {
		buf[i-1] = byte(x >> uint(8*(n-i)))
	}
}

// readBigEndian reads buf as a big-endian integer and returns
// the result.
func readBigEndian(buf []byte) uint64 {
	var x uint64
	for i := 1; i <= len(buf); i++ {
		x |= uint64(buf[i-1]) << uint(8*(len(buf)-i))
	}
	return x
}

const maxVarintSize = 9

// PutUvarint encodes a uint64 into buf and returns the
// number of bytes written. If the buffer is too small,
// a panic will ensue.
func PutUvarint(buf []byte, x uint64) int {
	// Treat each byte of the encoding as an unsigned integer
	// between 0 and 255.
	// Let the bytes of the encoding be called A0, A1, A2, ..., A8.
	// If x<=240 then output a single by A0 equal to x.
	// If x<=2287 then output A0 as (x-240)/256 + 241 and A1 as (x-240)%256.
	// If x<=67823 then output A0 as 249, A1 as (x-2288)/256, and A2 as (x-2288)%256.
	// If x<=16777215 then output A0 as 250 and A1 through A3 as a big-endian 3-byte integer.
	// If x<=4294967295 then output A0 as 251 and A1..A4 as a big-ending 4-byte integer.
	// If x<=1099511627775 then output A0 as 252 and A1..A5 as a big-ending 5-byte integer.
	// If x<=281474976710655 then output A0 as 253 and A1..A6 as a big-ending 6-byte integer.
	// If x<=72057594037927935 then output A0 as 254 and A1..A7 as a big-ending 7-byte integer.
	// Otherwise then output A0 as 255 and A1..A8 as a big-ending 8-byte integer.
	switch {
	case x <= 240:
		buf[0] = byte(x)
		return 1
	case x <= 2287:
		buf[0] = byte((x-240)/256 + 241)
		buf[1] = byte((x - 240) % 256)
		return 2
	case x <= 67823:
		buf[0] = byte(249)
		buf[1] = byte((x - 2288) / 256)
		buf[2] = byte((x - 2288) % 256)
		return 3
	case x <= 16777215:
		buf[0] = byte(250)
		writeBigEndian(buf[1:], x, 3)
		return 4
	case x <= 4294967295:
		buf[0] = byte(251)
		writeBigEndian(buf[1:], x, 4)
		return 5
	case x <= 1099511627775:
		buf[0] = byte(252)
		writeBigEndian(buf[1:], x, 5)
		return 6
	case x <= 281474976710655:
		buf[0] = byte(253)
		writeBigEndian(buf[1:], x, 6)
		return 7
	case x <= 72057594037927935:
		buf[0] = byte(254)
		writeBigEndian(buf[1:], x, 7)
		return 8
	default:
		buf[0] = byte(255)
		writeBigEndian(buf[1:], x, 8)
		return 9
	}
}

// GetUVarint decodes a varint-encoded byte slice and returns the result
// and the length of byte slice been used.
func GetUVarint(b []byte) (uint64, int) {
	// Treat each byte of the encoding as an unsigned integer
	// between 0 and 255.
	// Let the bytes of the encoding be called A0, A1, A2, ..., A8.
	// If A0 is between 0 and 240 inclusive, then the result is the value of A0.
	// If A0 is between 241 and 248 inclusive, then the result is 240+256*(A0-241)+A1.
	// If A0 is 249 then the result is 2288+256*A1+A2.
	// If A0 is 250 then the result is A1..A3 as a 3-byte big-ending integer.
	// If A0 is 251 then the result is A1..A4 as a 4-byte big-ending integer.
	// If A0 is 252 then the result is A1..A5 as a 5-byte big-ending integer.
	// If A0 is 253 then the result is A1..A6 as a 6-byte big-ending integer.
	// If A0 is 254 then the result is A1..A7 as a 7-byte big-ending integer.
	// If A0 is 255 then the result is A1..A8 as a 8-byte big-ending integer.
	switch {
	case b[0] >= 0 && b[0] <= 240:
		return uint64(b[0]), 1
	case b[0] >= 241 && b[0] <= 248:
		return 240 + 256*(uint64(b[0])-241) + uint64(b[1]), 2
	case b[0] == 249:
		return 2288 + 256*uint64(b[1]) + uint64(b[2]), 3
	case b[0] == 250:
		return readBigEndian(b[1:4]), 4
	case b[0] == 251:
		return readBigEndian(b[1:5]), 5
	case b[0] == 252:
		return readBigEndian(b[1:6]), 6
	case b[0] == 253:
		return readBigEndian(b[1:7]), 7
	case b[0] == 254:
		return readBigEndian(b[1:8]), 8
	case b[0] == 255:
		return readBigEndian(b[1:9]), 9
	default:
		panic("varint: invalid format given")
	}
}
