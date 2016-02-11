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

import "github.com/satori/go.uuid"

// EmptyUUID is the zero-UUID.
var EmptyUUID = &UUID{}

// UUID is a thin wrapper around "github.com/satori/go.uuid".UUID that can be
// used as a gogo/protobuf customtype.
type UUID struct {
	uuid.UUID
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

// NewV4 delegates to "github.com/satori/go.uuid".NewV4 and wraps the result in
// a UUID.
func NewV4() *UUID {
	return &UUID{uuid.NewV4()}
}

// Equal delegates to "github.com/satori/go.uuid".Equal and wraps the result in
// a UUID.
func Equal(a, b UUID) bool {
	return uuid.Equal(a.UUID, b.UUID)
}

// FromBytes delegates to "github.com/satori/go.uuid".FromBytes and wraps the
// result in a UUID.
func FromBytes(input []byte) (u *UUID, err error) {
	inner, err := uuid.FromBytes(input)
	return &UUID{inner}, err
}

// FromString delegates to "github.com/satori/go.uuid".FromString and wraps the
// result in a UUID.
func FromString(input string) (u *UUID, err error) {
	inner, err := uuid.FromString(input)
	return &UUID{inner}, err
}
