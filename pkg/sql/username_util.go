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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// GetUserIDWithCache returns id of the user if role exists
func GetUserIDWithCache(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, role security.SQLUsername,
) (oid.Oid, error) {
	if role == security.RootUserName() {
		return security.RootUserInfo().UserID, nil
	}
	if role == security.AdminRoleName() {
		return security.AdminRoleInfo().UserID, nil
	}
	if role == security.NodeUserName() {
		return security.NodeUserInfo().UserID, nil
	}
	if role == security.PublicRoleName() {
		return security.PublicRoleInfo().UserID, nil
	}

	roleIDCache := execCfg.RoleIDCache
	roleIDCache.Mutex.Lock()
	defer roleIDCache.Unlock()
	// First try to consult cache.
	var usersTableDesc catalog.TableDescriptor
	var err error
	err = execCfg.CollectionFactory.Txn(ctx, execCfg.InternalExecutor, execCfg.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		_, usersTableDesc, err = descriptors.GetImmutableTableByName(
			ctx,
			txn,
			sessioninit.UsersTableName,
			tree.ObjectLookupFlagsWithRequired(),
		)
		return err
	})
	if err != nil {
		return 0, err
	}

	usersTableVersion := usersTableDesc.GetVersion()

	if usersTableVersion > roleIDCache.tableVersion {
		// Clear cache.
		roleIDCache.roleIDMap = make(map[security.SQLUsername]oid.Oid)
		roleID, err := GetUserID(ctx, execCfg.InternalExecutor, txn, role)
		if err != nil {
			return 0, err
		}

		roleIDCache.tableVersion = usersTableVersion
		roleIDCache.roleIDMap[role] = roleID
	}

	// Cache is valid, do a lookup.
	roleID, found := roleIDCache.roleIDMap[role]
	if !found {
		roleID, err = GetUserID(ctx, execCfg.InternalExecutor, txn, role)
		if err != nil {
			return 0, err
		}
		roleIDCache.roleIDMap[role] = roleID
	}

	return roleID, nil
}

// GetUserID returns id of the user if role exists
// GetUserID returns 0 if the system.users table does not have a role id column
// which is the case in CockroachDB versions prior to 22.2.
// The caller of GetUserID must handle the case where the ID is 0 accordingly.
func GetUserID(
	ctx context.Context, executor *InternalExecutor, txn *kv.Txn, role security.SQLUsername,
) (oid.Oid, error) {
	if role == security.RootUserName() {
		return security.RootUserInfo().UserID, nil
	}
	if role == security.AdminRoleName() {
		return security.AdminRoleInfo().UserID, nil
	}
	if role == security.NodeUserName() {
		return security.NodeUserInfo().UserID, nil
	}
	if role == security.PublicRoleName() {
		return security.PublicRoleInfo().UserID, nil
	}

	var userID oid.Oid

	query := `SELECT user_id FROM system.users WHERE username=$1`
	values, err := executor.QueryRowEx(ctx, "GetUserID", txn, sessiondata.InternalExecutorOverride{
		User: security.RootUserName(),
	},
		query, role)

	if err != nil {
		//panic(err)
		return userID, errors.Wrapf(err, "error looking up user %s", role, string(debug.Stack()))
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
	id, err := GetUserID(ctx, p.execCfg.InternalExecutor, p.txn, security.PublicRoleName())
	if err != nil {
		panic(err)
	}
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

// GetSQLUserInfo converts a security.SQLUsername to a security.SQLUserInfo.
func GetSQLUserInfo(
	ctx context.Context,
	execCfg *ExecutorConfig,
	descsCol *descs.Collection,
	executor *InternalExecutor,
	txn *kv.Txn,
	user security.SQLUsername,
) (security.SQLUserInfo, error) {

	userInfo := security.SQLUserInfo{Username: user}
	userID, err := GetUserIDWithCache(ctx, execCfg, txn, user)
	if err != nil {
		return userInfo, err
	}
	userInfo.UserID = userID

	return userInfo, nil

}
