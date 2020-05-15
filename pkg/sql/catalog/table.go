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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// TODO(ajwerner): It's possible that this belongs in sqlbase.

// ErrTableAdding is returned from FilterTableState.
var ErrTableAdding = errors.New("table is being added")

// HasInactiveTableError returns true if the error is or contains an inactive
// table error. These errors are returned from FilterTableState.
func HasInactiveTableError(err error) bool {
	return errors.HasType(err, (*inactiveTableError)(nil))
}

// inactiveTableError is an error returned when a table is
type inactiveTableError struct {
	cause error
}

func (i *inactiveTableError) Error() string { return i.cause.Error() }

func (i *inactiveTableError) Unwrap() error { return i.cause }

// FilterTableState inspects the state of a given table and returns an error if
// the state is anything but PUBLIC. The error describes the state of the table.
// If the table is inactive, either dropped or offline, then the returned error
// will be an inactiveTableError. If the table is being added, ErrTableAdding
// will be returned.
func FilterTableState(tableDesc *sqlbase.TableDescriptor) error {
	switch tableDesc.State {
	case sqlbase.TableDescriptor_DROP:
		return &inactiveTableError{errors.New("table is being dropped")}
	case sqlbase.TableDescriptor_OFFLINE:
		err := errors.Errorf("table %q is offline", tableDesc.Name)
		if tableDesc.OfflineReason != "" {
			err = errors.Errorf("table %q is offline: %s", tableDesc.Name, tableDesc.OfflineReason)
		}
		return &inactiveTableError{err}
	case sqlbase.TableDescriptor_ADD:
		return ErrTableAdding
	case sqlbase.TableDescriptor_PUBLIC:
		return nil
	default:
		return errors.Errorf("table in unknown state: %s", tableDesc.State.String())
	}
}

// TableEntry is the value type of FkTableMetadata: An optional table
// descriptor, populated when the table is public/leasable, and an IsAdding
// flag.
//
// This also includes an optional CheckHelper for the table (for CHECK
// constraints). This is needed for FK work because CASCADE actions
// can modify rows, and CHECK constraints must be applied to rows
// modified by CASCADE.
type TableEntry struct {
	// Desc is the descriptor of the table. This can be nil if eg.
	// the table is not public.
	Desc *sqlbase.ImmutableTableDescriptor

	// IsAdding indicates the descriptor is being created.
	IsAdding bool

	// CheckHelper is the utility responsible for CHECK constraint
	// checks. The lookup function (see TableLookupFunction below) needs
	// not populate this field; this is populated by the lookup queue
	// below.
	CheckHelper *sqlbase.CheckHelper
}
