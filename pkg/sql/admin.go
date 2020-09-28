// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// HasAdminRole is a helper function to determine if a user is an admin using
// the given internal executor.
func HasAdminRole(ctx context.Context, ie *InternalExecutor, user string) (bool, error) {
	if user == security.RootUser {
		// Shortcut.
		return true, nil
	}
	rows, _, err := ie.QueryWithCols(
		ctx, "check-is-admin", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: user},
		"SELECT crdb_internal.is_admin()")
	if err != nil {
		return false, err
	}
	if len(rows) != 1 {
		return false, errors.AssertionFailedf("hasAdminRole: expected 1 row, got %d", len(rows))
	}
	if len(rows[0]) != 1 {
		return false, errors.AssertionFailedf("hasAdminRole: expected 1 column, got %d", len(rows[0]))
	}
	dbDatum, ok := tree.AsDBool(rows[0][0])
	if !ok {
		return false, errors.AssertionFailedf("hasAdminRole: expected bool, got %T", rows[0][0])
	}
	return bool(dbDatum), nil
}
