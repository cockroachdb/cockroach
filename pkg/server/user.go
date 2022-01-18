// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// UserSQLRoles return a list of the logged in SQL user roles.
func (s *statusServer) UserSQLRoles(
	ctx context.Context, req *serverpb.UserSQLRolesRequest,
) (_ *serverpb.UserSQLRolesResponse, retErr error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)
	username, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	it, err := s.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "sqlroles", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: username},
		"SELECT option FROM system.role_options WHERE username=$1", username,
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	ok, err := it.Next(ctx)
	if err != nil {
		return nil, err
	}

	var resp serverpb.UserSQLRolesResponse
	if !ok {
		// The query returned 0 rows.
		return &resp, nil
	}
	scanner := makeResultScanner(it.Types())
	for ; ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		var role string
		err = scanner.ScanIndex(row, 0, &role)
		if err != nil {
			return nil, err
		}
		resp.Roles = append(resp.Roles, role)
	}

	if err != nil {
		return nil, err
	}
	return &resp, nil
}
