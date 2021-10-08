// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright (C) 2013-2018 by Maxim Bublis <b@codemonkey.ru>
// Use of this source code is governed by a MIT-style
// license that can be found in licenses/MIT-gofrs.txt.

// This code originated in github.com/gofrs/uuid.

package uuid

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Size of a UUID in bytes.
const Size = 16

// UUID is an array type to represent the value of a UUID, as defined in RFC-4122.
type UUID [Size]byte

// UUID versions.
const (
	_  byte = iota
	V1      // Version 1 (date-time and MAC address)
	_       // Version 2 (date-time and MAC address, DCE security version)
	V3      // Version 3 (namespace name-based)
	V4      // Version 4 (random)
	V5      // Version 5 (namespace name-based)
)

// UUID layout variants.
const (
	VariantNCS byte = iota
	VariantRFC4122
	VariantMicrosoft
	VariantFuture
)

// Timestamp is the count of 100-nanosecond intervals since 00:00:00.00,
// 15 October 1582 within a V1 UUID. This type has no meaning for V2-V5
// UUIDs since they don't have an embedded timestamp.
type Timestamp uint64

const _100nsPerSecond = 10000000

// Time returns the UTC time.Time representation of a Timestamp
func (t Timestamp) Time() (time.Time, error) {
	secs := uint64(t) / _100nsPerSecond
	nsecs := 100 * (uint64(t) % _100nsPerSecond)
	return timeutil.Unix(int64(secs)-(epochStart/_100nsPerSecond), int64(nsecs)), nil
}

// TimestampFromV1 returns the Timestamp embedded within a V1 UUID.
// Returns an error if the UUID is any version other than 1.
func TimestampFromV1(u UUID) (Timestamp, error) {
	if u.Version() != 1 {
		err := fmt.Errorf("uuid: %s is version %d, not version 1", u, u.Version())
		return 0, err
	}
	low := binary.BigEndian.Uint32(u[0:4])
	mid := binary.BigEndian.Uint16(u[4:6])
	hi := binary.BigEndian.Uint16(u[6:8]) & 0xfff
	return Timestamp(uint64(low) + (uint64(mid) << 32) + (uint64(hi) << 48)), nil
}

// String parse helpers.
var urnPrefix = []byte("urn:uuid:")

// Nil is the nil UUID, as specified in RFC-4122, that has all 128 bits set to
// zero.
var Nil = UUID{}

// Predefined namespace UUIDs.
var (
	NamespaceDNS  = Must(FromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
	NamespaceURL  = Must(FromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8"))
	NamespaceOID  = Must(FromString("6ba7b812-9dad-11d1-80b4-00c04fd430c8"))
	NamespaceX500 = Must(FromString("6ba7b814-9dad-11d1-80b4-00c04fd430c8"))
)

// Version returns the algorithm version used to generate the UUID.
func (u UUID) Version() byte {
	return u[6] >> 4
}

// Variant returns the UUID layout variant.
func (u UUID) Variant() byte {
	switch {
	case (u[8] >> 7) == 0x00:
		return VariantNCS
	case (u[8] >> 6) == 0x02:
		return VariantRFC4122
	case (u[8] >> 5) == 0x06:
		return VariantMicrosoft
	case (u[8] >> 5) == 0x07:
		fallthrough
	default:
		return VariantFuture
	}
}

// bytes returns a byte slice representation of the UUID. It incurs an
// allocation if the return value escapes.
func (u UUID) bytes() []byte {
	return u[:]
}

// bytesMut returns a mutable byte slice representation of the UUID. Unlike
// bytes, it does not necessarily incur an allocation if the return value
// escapes. Instead, the return value escaping will cause the method's receiver
// (and any struct that it is a part of) to escape.
func (u *UUID) bytesMut() []byte {
	return u[:]
}

// String returns a canonical RFC-4122 string representation of the UUID:
// xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.
func (u UUID) String() string {
	buf := make([]byte, 36)
	u.StringBytes(buf)
	return string(buf)
}

// StringBytes writes the result of String directly into a buffer, which must
// have a length of at least 36.
func (u UUID) StringBytes(buf []byte) {
	_ = buf[:36]
	hex.Encode(buf[0:8], u[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], u[10:])
}

// SetVersion sets the version bits.
func (u *UUID) SetVersion(v byte) {
	u[6] = (u[6] & 0x0f) | (v << 4)
}

// SetVariant sets the variant bits.
func (u *UUID) SetVariant(v byte) {
	switch v {
	case VariantNCS:
		u[8] = (u[8]&(0xff>>1) | (0x00 << 7))
	case VariantRFC4122:
		u[8] = (u[8]&(0xff>>2) | (0x02 << 6))
	case VariantMicrosoft:
		u[8] = (u[8]&(0xff>>3) | (0x06 << 5))
	case VariantFuture:
		fallthrough
	default:
		u[8] = (u[8]&(0xff>>3) | (0x07 << 5))
	}
}

// Must is a helper that wraps a call to a function returning (UUID, error)
// and panics if the error is non-nil. It is intended for use in variable
// initializations such as
//  var packageUUID = uuid.Must(uuid.FromString("123e4567-e89b-12d3-a456-426655440000"))
func Must(u UUID, err error) UUID {
	if err != nil {
		panic(err)
	}
	return u
}

// DeterministicV4 overwrites this UUID with one computed deterministically to
// evenly fill the space of possible V4 UUIDs. `n` represents how many UUIDs
// will fill the space and `i` is an index into these `n` (and thus must be in
// the range `[0,n)`). The resulting UUIDs will be unique, evenly-spaced, and
// sorted.
func (u *UUID) DeterministicV4(i, n uint64) {
	if i >= n {
		panic(errors.Errorf(`i must be in [0,%d) was %d`, n, i))
	}
	// V4 uuids are generated by simply filling 16 bytes with random data (then
	// setting the version and variant), so they're randomly distributed through
	// the space of possible values. This also means they're roughly evenly
	// distributed. We guarantee these values to be similarly distributed.
	//
	// So, space the row indexes out to fill the space of the integers
	// representable with 8 bytes. Then, because this involves some floats (and
	// who knows what kind of crazy rounding things can happen when floats are
	// involved), make sure they're unique by sticking the index
	// in the lower 8 bytes. Note that we need to use BigEndian encodings to keep
	// the uuids sorted in the same order as the ints.
	spacing := uint64(float64(i) * float64(math.MaxUint64) / float64(n))
	binary.BigEndian.PutUint64(u[0:8], spacing)
	binary.BigEndian.PutUint64(u[8:16], i)
	u.SetVersion(V4)
	u.SetVariant(VariantRFC4122)
}
