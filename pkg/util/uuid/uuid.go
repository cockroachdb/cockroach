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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package uuid

import (
	"encoding/binary"

	"github.com/satori/go.uuid"
)

// UUID is a thin wrapper around "github.com/satori/go.uuid".UUID that can be
// used as a gogo/protobuf customtype.
type UUID struct {
	uuid.UUID
}

// Short returns the first eight characters of the output of String().
func (u UUID) Short() string {
	return u.String()[:8]
}

// Bytes shadows (*github.com/satori/go.uuid.UUID).Bytes() to prevent UUID
// from implementing github.com/golang/protobuf/proto.raw, the semantics of
// which do not match the semantics of the shadowed method. See
// https://github.com/golang/protobuf/blob/5386fff/proto/text.go#L173:L176.
//
// TODO(tamird): remove when fixed upstream. See
// https://github.com/gogo/protobuf/pull/227 and
// https://github.com/golang/protobuf/issues/311.
func (UUID) Bytes() {
	panic("intentionally shadowed; use GetBytes()")
}

// Silence unused warning for UUID.Bytes.
var _ = (UUID).Bytes

// GetBytes returns the UUID as a byte slice.
func (u UUID) GetBytes() []byte {
	return u.UUID.Bytes()
}

// Size returns the marshalled size of u, in bytes.
func (u UUID) Size() int {
	return len(u.UUID)
}

// MarshalTo marshals u to data.
func (u UUID) MarshalTo(data []byte) (int, error) {
	return copy(data, u.UUID.Bytes()), nil
}

// Unmarshal unmarshals data to u.
func (u *UUID) Unmarshal(data []byte) error {
	return u.UUID.UnmarshalBinary(data)
}

// MakeV4 delegates to "github.com/satori/go.uuid".NewV4 and wraps the result in
// a UUID.
func MakeV4() UUID {
	return UUID{uuid.NewV4()}
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

// FromBytes delegates to "github.com/satori/go.uuid".FromBytes and wraps the
// result in a UUID.
func FromBytes(input []byte) (UUID, error) {
	u, err := uuid.FromBytes(input)
	return UUID{u}, err
}

// FromString delegates to "github.com/satori/go.uuid".FromString and wraps the
// result in a UUID.
func FromString(input string) (UUID, error) {
	u, err := uuid.FromString(input)
	return UUID{u}, err
}
