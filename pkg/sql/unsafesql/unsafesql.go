// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package unsafesql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
)

// CheckInternalsAccess checks if the current session has permission to access
// unsafe internal tables and functionality. This includes system tables and
// virtual tables / builtins in the crdb_internal schema.
func CheckInternalsAccess(sd *sessiondata.SessionData) error {
	// If the querier is internal, we should allow it.
	if sd.Internal {
		return nil
	}

	// If an override is set, allow access to this virtual table.
	if sd.AllowUnsafeInternals {
		return nil
	}

	return sqlerrors.ErrUnsafeTableAccess
}
