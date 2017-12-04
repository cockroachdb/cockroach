// Copyright 2017 The Cockroach Authors.
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

package scrub

import (
	"fmt"
)

const (
	// MissingIndexEntryError occurs when a primary k/v is missing a
	// corresponding secondary index k/v.
	MissingIndexEntryError = "missing_index_entry"
	// DanglingIndexReferenceError occurs when a secondary index k/v
	// points to a non-existing primary k/v.
	DanglingIndexReferenceError = "dangling_index_reference"
	// PhysicalError is a generic error when there is an error in the
	// SQL physical data.
	PhysicalError = "physical_error"
	// IndexKeyDecodingError occurs while decoding the an index key.
	IndexKeyDecodingError = "index_key_decoding_error"
	// IndexValueDecodingError occurs while decoding the an index value.
	IndexValueDecodingError = "index_value_decoding_error"
	// SecondaryIndexKeyExtraValueDecodingError occurs when the extra
	// columns stored in a key fail to decode.
	SecondaryIndexKeyExtraValueDecodingError = "secondary_index_key_extra_value_decoding_error"
	// UnexpectedNullValueError occurs when a null value is encountered where the
	// value is expected to be non-nullable.
	UnexpectedNullValueError = "null_value_error"
	// CheckConstraintViolation occurs when a row in a table is
	// violating a check constraint.
	CheckConstraintViolation = "check_constraint_violation"
	// ForeignKeyConstraintViolation occurs when a row in a
	// table is violating a foreign key constraint.
	ForeignKeyConstraintViolation = "foreign_key_violation"
)

// Error contains the details on the scrub error that was caught.
type Error struct {
	Code       string
	underlying error
}

func (s *Error) Error() string {
	return fmt.Sprintf("%s: %+v", s.Code, s.underlying)
}

// WrapError wraps an error with a Error.
func WrapError(code string, err error) *Error {
	return &Error{
		Code:       code,
		underlying: err,
	}
}

// IsScrubError checks if an error is a Error.
func IsScrubError(err error) bool {
	_, ok := err.(*Error)
	return ok
}

// UnwrapScrubError gets the underlying error if err is a scrub.Error.
// If err is not a scrub.Error nil is returned.
func UnwrapScrubError(err error) error {
	if IsScrubError(err) {
		return err.(*Error).underlying
	}
	return err
}
