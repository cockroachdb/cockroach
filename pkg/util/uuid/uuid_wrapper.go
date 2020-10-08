// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package uuid

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/errors"
)

// Short returns the first eight characters of the output of String().
func (u UUID) Short() string {
	return u.String()[:8]
}

// ShortStringer implements fmt.Stringer to output Short() on String().
type ShortStringer UUID

// String is part of fmt.Stringer.
func (s ShortStringer) String() string {
	return UUID(s).Short()
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

// MakeV4 calls Must(NewV4)
func MakeV4() UUID {
	return Must(NewV4())
}

// FastMakeV4 generates a UUID using a fast but not cryptographically secure
// source of randomness.
func FastMakeV4() UUID {
	u, err := fastGen.NewV4()
	if err != nil {
		panic(errors.Wrap(err, "should never happen with math/rand.Rand"))
	}
	return u
}

// defaultRandReader is an io.Reader that calls through to "math/rand".Read
// which is safe for concurrent use.
type defaultRandReader struct{}

func (r defaultRandReader) Read(p []byte) (n int, err error) {
	return rand.Read(p)
}

// fastGen is a non-cryptographically secure Generator.
var fastGen = NewGenWithReader(defaultRandReader{})

// NewPopulatedUUID returns a populated UUID.
func NewPopulatedUUID(r interface {
	Int63() int64
}) *UUID {
	var u UUID
	binary.LittleEndian.PutUint64(u[:8], uint64(r.Int63()))
	binary.LittleEndian.PutUint64(u[8:], uint64(r.Int63()))
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
