// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// ValidateName validates a name.
func ValidateName(name, typ string) error {
	if len(name) == 0 {
		return pgerror.Newf(pgcode.Syntax, "empty %s name", typ)
	}
	// TODO(pmattis): Do we want to be more restrictive than this?
	return nil
}

type inactiveDescriptorError struct {
	cause error
}

// errTableAdding is returned when the descriptor is being added.
//
// Only tables can be in the adding state, and this will be true for the
// foreseeable future, so the error message remains a table-specific version.
var errTableAdding = errors.New("table is being added")

// ErrDescriptorDropped is returned when the descriptor is being dropped.
// TODO (lucy): Make the error message specific to each descriptor type (e.g.,
// "table is being dropped") and add the pgcodes (UndefinedTable, etc.).
var ErrDescriptorDropped = errors.New("descriptor is being dropped")

func (i *inactiveDescriptorError) Error() string { return i.cause.Error() }

func (i *inactiveDescriptorError) Unwrap() error { return i.cause }

// HasAddingTableError returns true if the error contains errTableAdding.
func HasAddingTableError(err error) bool {
	return errors.Is(err, errTableAdding)
}

// HasInactiveDescriptorError returns true if the error contains an
// inactiveDescriptorError.
func HasInactiveDescriptorError(err error) bool {
	return errors.HasType(err, (*inactiveDescriptorError)(nil))
}

// NewInactiveDescriptorError wraps an error in a new inactiveDescriptorError.
func NewInactiveDescriptorError(err error) error {
	return &inactiveDescriptorError{err}
}

// ErrDescriptorNotFound is returned by getTableDescFromID to signal that a
// descriptor could not be found with the given id.
var ErrDescriptorNotFound = errors.New("descriptor not found")
