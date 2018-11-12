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

package uuid

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// Use math/rand instead of crypto/rand for UUID generation
func init() {
	uuid.SetRand(rand.New(rand.NewSource(time.Now().UnixNano())))
}

// UUID is a thin wrapper around "github.com/google/uuid".UUID that can be
// used as a gogo/protobuf customtype.
// Note that UUIDs are not crytographically secure in their randomness.
type UUID struct {
	uuid.UUID
}

// Nil is the empty UUID with all 128 bits set to zero.
var Nil = UUID{uuid.Nil}

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

// GetBytes returns the UUID as a byte slice.
func (u UUID) GetBytes() []byte {
	return u.UUID[:]
}

// ToUint128 returns the UUID as a Uint128.
func (u UUID) ToUint128() uint128.Uint128 {
	return uint128.FromBytes(u.GetBytes())
}

// Size returns the marshaled size of u, in bytes.
func (u UUID) Size() int {
	return len(u.UUID)
}

// MarshalTo marshals u to data.
func (u UUID) MarshalTo(data []byte) (int, error) {
	return copy(data, u.UUID[:]), nil
}

// Unmarshal unmarshals data to u.
func (u *UUID) Unmarshal(data []byte) error {
	return u.UUID.UnmarshalBinary(data)
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

// MakeV4 delegates to "github.com/google/uuid".New and wraps the result in
// a UUID.
func MakeV4() UUID {
	return UUID{uuid.New()}
}

// NewPopulatedUUID returns a populated UUID.
func NewPopulatedUUID(r interface {
	Int63() int64
}) *UUID {
	var u uuid.UUID
	binary.LittleEndian.PutUint64(u[:8], uint64(r.Int63()))
	binary.LittleEndian.PutUint64(u[8:], uint64(r.Int63()))
	return &UUID{u}
}

// FromBytes delegates to "github.com/google/uuid".FromBytes and wraps the
// result in a UUID.
func FromBytes(input []byte) (UUID, error) {
	u, err := uuid.FromBytes(input)
	return UUID{u}, err
}

// FromString delegates to "github.com/google/uuid".Parse and wraps the
// result in a UUID.
func FromString(input string) (UUID, error) {
	// Google's UUID library does not support parsing outer curly braces so
	// manually pull them off.
	if len(input) > 0 && input[0] == '{' && input[len(input)-1] == '}' {
		input = input[1 : len(input)-2]
	}
	u, err := uuid.Parse(input)
	return UUID{u}, err
}

// FromUint128 delegates to "github.com/google/uuid".FromBytes and wraps the
// result in a UUID.
func FromUint128(input uint128.Uint128) UUID {
	u, err := uuid.FromBytes(input.GetBytes())
	if err != nil {
		panic(errors.Wrap(err, "should never happen with 16 byte slice"))
	}
	return UUID{u}
}
