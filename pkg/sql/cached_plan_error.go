// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// RevalidateCachedQuery constructs the "cached plan must not change result
// type" error raised when a prepared statement's result columns no longer
// match its cached plan after a schema change.
//
// The error is deliberately constructed inside a function with this exact
// name: clients receive the constructing function's name as the error's
// Routine field (PG_DIAG_SOURCE_FUNCTION), and PostgreSQL reports
// "RevalidateCachedQuery" for this error (its plancache.c function).
// Drivers and ORMs key on that field to detect a stale prepared statement
// and self-heal by re-preparing — ActiveRecord's PostgreSQL adapter matches
// it verbatim, and the activerecord-cockroachdb-adapter previously matched
// the internal function names that happened to appear here, which broke
// when the raising site moved in #164406. Do not rename this function or
// inline the construction into a caller; TestPGTest's
// prepared_stmt_invalidation datadriven test asserts the wire field.
func RevalidateCachedQuery() error {
	return pgerror.New(pgcode.FeatureNotSupported, "cached plan must not change result type")
}
