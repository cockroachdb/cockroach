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
// Only tables (or materialized view) can be in the adding state, and this will
// be true for the foreseeable future, so the error message remains a
// table-specific version.
type addingTableError struct {
	cause error
}

func newAddingTableError(desc TableDescriptor) error {
	typStr := "table"
	if desc.IsView() && desc.IsPhysicalTable() {
		typStr = "materialized view"
	}
	return &addingTableError{
		cause: errors.Errorf("%s %q is being added", typStr, desc.GetName()),
	}
}

func (a *addingTableError) Error() string { return a.cause.Error() }

func (a *addingTableError) Unwrap() error { return a.cause }

// ErrDescriptorDropped is returned when the descriptor is being dropped.
// TODO (lucy): Make the error message specific to each descriptor type (e.g.,
// "table is being dropped") and add the pgcodes (UndefinedTable, etc.).
var ErrDescriptorDropped = errors.New("descriptor is being dropped")

func (i *inactiveDescriptorError) Error() string { return i.cause.Error() }

func (i *inactiveDescriptorError) Unwrap() error { return i.cause }

// HasAddingTableError returns true if the error contains errTableAdding.
func HasAddingTableError(err error) bool {
	return errors.HasType(err, (*addingTableError)(nil))
}

// NewInactiveDescriptorError wraps an error in a new inactiveDescriptorError.
func NewInactiveDescriptorError(err error) error {
	return &inactiveDescriptorError{err}
}

// ErrDescriptorNotFound is returned by getTableDescFromID to signal that a
// descriptor could not be found with the given id.
var ErrDescriptorNotFound = errors.New("descriptor not found")
