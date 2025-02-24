// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package uuid

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	shortSize    = 4
	shortStrSize = 8
)

type Short struct {
	b [shortSize]byte
}

var _ redact.SafeValue = Short{}

// SafeValue implements the redact.SafeValue interface.
func (s Short) SafeValue() {}

// String returns the 8-character hexidecimal representation of the abbreviated
// UUID.
func (s Short) String() string {
	var b [shortStrSize]byte
	hex.Encode(b[:], s.b[:])
	return string(b[:])
}

// Short returns an abbreviated version of the UUID containing the first four
// bytes.
func (u UUID) Short() Short {
	return Short{
		b: [shortSize]byte(u[0:shortSize]),
	}
}

// ShortStringer implements fmt.Stringer to output Short() on String().
type ShortStringer UUID

// String is part of fmt.Stringer.
func (s ShortStringer) String() string {
	return UUID(s).Short().String()
}

var _ fmt.Stringer = ShortStringer{}

// Equal returns true iff the receiver equals the argument.
//
// This method exists only to conform to the API expected by gogoproto's
// generated Equal implementations.
func (u UUID) Equal(t UUID) bool {
	return u == t
}

// GetBytes returns the UUID as a byte slice. It incurs an allocation if
// the return value escapes.
func (u UUID) GetBytes() []byte {
	return u.bytes()
}

// GetBytesMut returns the UUID as a mutable byte slice. Unlike GetBytes,
// it does not necessarily incur an allocation if the return value escapes.
// Instead, the return value escaping will cause the method's receiver (and
// any struct that it is a part of) to escape. Use only if GetBytes is causing
// an allocation and the UUID is already on the heap.
func (u *UUID) GetBytesMut() []byte {
	return u.bytesMut()
}

// ToUint128 returns the UUID as a Uint128.
func (u UUID) ToUint128() uint128.Uint128 {
	return uint128.FromBytes(u.bytes())
}

// Size returns the marshaled size of u, in bytes.
func (u UUID) Size() int {
	return len(u)
}

// MarshalTo marshals u to data.
func (u UUID) MarshalTo(data []byte) (int, error) {
	return copy(data, u.GetBytes()), nil
}

// Unmarshal unmarshals data to u.
func (u *UUID) Unmarshal(data []byte) error {
	return u.UnmarshalBinary(data)
}

// MarshalJSON returns the JSON encoding of u.
func (u UUID) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

// UnmarshalJSON unmarshals the JSON encoded data into u.
func (u *UUID) UnmarshalJSON(data []byte) error {
	var uuidString string
	if err := json.Unmarshal(data, &uuidString); err != nil {
		return err
	}
	uuid, err := FromString(uuidString)
	*u = uuid
	return err
}

// MakeV4 calls NewV4.
func MakeV4() UUID {
	return NewV4()
}

// NewPopulatedUUID returns a populated UUID.
func NewPopulatedUUID(r interface {
	Uint32() uint32
}) *UUID {
	var u UUID
	binary.LittleEndian.PutUint32(u[:4], r.Uint32())
	binary.LittleEndian.PutUint32(u[4:8], r.Uint32())
	binary.LittleEndian.PutUint32(u[8:12], r.Uint32())
	binary.LittleEndian.PutUint32(u[12:], r.Uint32())
	return &u
}

// FromUint128 delegates to FromBytes and wraps the result in a UUID.
func FromUint128(input uint128.Uint128) UUID {
	u, err := FromBytes(input.GetBytes())
	if err != nil {
		panic(errors.Wrap(err, "should never happen with 16 byte slice"))
	}
	return u
}
