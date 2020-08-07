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

import "github.com/cockroachdb/errors"

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

// FilterDescriptorState inspects the state of a given descriptor and returns an
// error if the state is anything but public. The error describes the state of
// the descriptor.
func FilterDescriptorState(desc Descriptor) error {
	switch {
	case desc.Dropped():
		return NewInactiveDescriptorError(ErrDescriptorDropped)
	case desc.Offline():
		err := errors.Errorf("%s %q is offline", desc.TypeName(), desc.GetName())
		if desc.GetOfflineReason() != "" {
			err = errors.Errorf("%s %q is offline: %s", desc.TypeName(), desc.GetName(), desc.GetOfflineReason())
		}
		return NewInactiveDescriptorError(err)
	case desc.Adding():
		return errTableAdding
	default:
		return nil
	}
}
