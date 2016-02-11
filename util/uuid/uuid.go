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
	"errors"

	"github.com/satori/go.uuid"
)

// EmptyUUID is the zero-UUID.
var EmptyUUID = &UUID{make([]byte, 16)}

// UUID is a thin wrapper around "github.com/satori/go.uuid".UUID that can be
// used as a gogo/protobuf customtype.
type UUID struct {
	// TODO(tamird): should be an embedded uuid.UUID when
	// https://github.com/gogo/protobuf/pull/146 is fixed.
	// Revert this comment when that happens.
	U []byte
}

// Size returns the marshalled size of u, in bytes.
func (u UUID) Size() int {
	return len(u.Bytes())
}

// MarshalTo marshals u to data.
func (u UUID) MarshalTo(data []byte) (int, error) {
	return copy(data, u.Bytes()), nil
}

// Unmarshal unmarshals data to u.
func (u *UUID) Unmarshal(data []byte) error {
	if copy(u.Bytes(), data) != 16 {
		return errors.New("Did not copy enough bytes!")
	}
	return nil
}

// MakeV4 delegates to "github.com/satori/go.uuid".NewV4 and wraps the result in
// a UUID.
func MakeV4() UUID {
	return UUID{uuid.NewV4().Bytes()}
}

// NewV4 delegates to "github.com/satori/go.uuid".NewV4 and wraps the result in
// a UUID.
func NewV4() *UUID {
	return &UUID{uuid.NewV4().Bytes()}
}

// FromBytes delegates to "github.com/satori/go.uuid".FromBytes and wraps the
// result in a UUID.
func FromBytes(input []byte) (u *UUID, err error) {
	inner, err := uuid.FromBytes(input)
	return &UUID{inner.Bytes()}, err
}

// FromString delegates to "github.com/satori/go.uuid".FromString and wraps the
// result in a UUID.
func FromString(input string) (u *UUID, err error) {
	inner, err := uuid.FromString(input)
	return &UUID{inner.Bytes()}, err
}

func (u UUID) String() string {
	inner, err := uuid.FromBytes(u.Bytes())
	if err != nil {
		panic(err)
	}
	return inner.String()
}

// Bytes returns the underlying byte slice.
func (u *UUID) Bytes() []byte {
	if u.U == nil {
		u.U = make([]byte, 16)
	}
	return u.U
}
