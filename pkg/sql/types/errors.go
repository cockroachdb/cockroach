// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package types

import (
	"fmt"

	"github.com/cockroachdb/errors"
)

// enumValueIsNotPublicYetError generated when a non-public enum is used in a
// query.
type enumValueIsNotPublicYetError struct {
	logicalName string
}

// NewEnumValueIsNotPublicYetError creates a enumValueIsNotPublicYetError.
func newEnumValueIsNotPublicYetError(logicalName string) error {
	return &enumValueIsNotPublicYetError{logicalName: logicalName}
}

// Error implements error.
func (e *enumValueIsNotPublicYetError) Error() string {
	return fmt.Sprintf("enum value %q is not yet public", e.logicalName)
}

// IsNewEnumValueIsNotPublicYetError checks if the error is
// enumValueIsNotPublicYetError
func IsNewEnumValueIsNotPublicYetError(err error) bool {
	return errors.Is(err, &enumValueIsNotPublicYetError{})
}
