// Copyright 2022 The Cockroach Authors.
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
	"runtime/debug"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// GetUserID returns id of the user if role exists
// GetUserID returns 0 if the system.users table does not have a role id column
// which is the case in CockroachDB versions prior to 22.2.
// The caller of GetUserID must handle the case where the ID is 0 accordingly.
func GetUserID(
	ctx context.Context, executor *InternalExecutor, txn *kv.Txn, role security.SQLUsername,
) (oid.Oid, error) {
	var userID oid.Oid

	values, err := executor.QueryRowEx(ctx, "get-system-users-columns", txn, sessiondata.InternalExecutorOverride{
		User: security.RootUserName(),
	}, `SELECT EXISTS (SELECT 1 FROM [SHOW COLUMNS FROM system.users] WHERE column_name = 'user_id')`)
	if err != nil {
		return userID, err
	}

	// Return 0 as an oid if the column does not exist.
	exists := tree.MustBeDBool(values[0])
	if !exists {
		return oid.Oid(0), nil
	}

	query := `SELECT user_id FROM system.users WHERE username=$1`
	values, err = executor.QueryRowEx(ctx, "GetUserID", txn, sessiondata.InternalExecutorOverride{
		User: security.RootUserName(),
	},
		query, role)

	if err != nil {
		return userID, errors.Wrapf(errors.Wrapf(err, "error looking up user %s", role), string(debug.Stack()))
	}

	if values != nil {
		if v := values[0]; v != tree.DNull {
			userID = oid.Oid(v.(*tree.DOid).DInt)
		}
	}
	return userID, nil

}

// PublicRoleInfo is the SQLUsername for PublicRole.
func PublicRoleInfo(ctx context.Context, p *planner) security.SQLUserInfo {
	id, _ := GetUserID(ctx, p.execCfg.InternalExecutor, nil, security.PublicRoleName())
	return security.SQLUserInfo{
		Username: security.PublicRoleName(),
		UserID:   id,
	}
}

// ToSQLUserInfos converts a slice of security.SQLUsername to slice of security.SQLUserInfo.
func ToSQLUserInfos(
	ctx context.Context, executor *InternalExecutor, txn *kv.Txn, roles []security.SQLUsername,
) ([]security.SQLUserInfo, error) {
	targetRoles := make([]security.SQLUserInfo, len(roles))
	for i, role := range roles {
		id, err := GetUserID(ctx, executor, txn, role)
		if err != nil {
			return nil, err
		}
		targetRoles[i] = security.SQLUserInfo{
			Username: role,
			UserID:   id,
		}
	}
	return targetRoles, nil
}
