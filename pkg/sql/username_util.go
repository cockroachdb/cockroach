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
	"github.com/lib/pq/oid"
	"hash/fnv"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// GetUserIDWithCache returns id of the user if role exists
func GetUserIDWithCache(
	ctx context.Context,
	execCfg *ExecutorConfig,
	descsCol *descs.Collection,
	executor *InternalExecutor,
	txn *kv.Txn,
	role security.SQLUsername,
) (oid.Oid, error) {

	var userID oid.Oid
	roleMembersCache := execCfg.RoleMemberCache

	// Lookup table version.
	_, tableDesc, err := descsCol.GetImmutableTableByName(
		ctx,
		txn,
		&roleMembersTableName,
		tree.ObjectLookupFlagsWithRequired(),
	)
	if err != nil {
		return userID, err
	}

	tableVersion := tableDesc.GetVersion()
	if tableDesc.IsUncommittedVersion() {
		return GetUserID(ctx, executor, txn, role)
	}

	userID, found := func() (oid.Oid, bool) {
		roleMembersCache.Lock()
		defer roleMembersCache.Unlock()
		if roleMembersCache.tableVersion != tableVersion {
			// Update version and drop the map.
			roleMembersCache.tableVersion = tableVersion
			roleMembersCache.userCache = make(map[uuid.UUID]userRoleMembership)
			roleMembersCache.userIDCache = make(map[security.SQLUsername]oid.Oid)
			roleMembersCache.boundAccount.Empty(ctx)
		}
		userMapping, ok := roleMembersCache.userIDCache[role]
		return userMapping, ok
	}()

	if found {
		// Found: return.
		return userID, nil
	}

	// Lookup memberships outside the lock.
	userID, err = GetUserID(ctx, executor, txn, role)
	//if err != nil {
	return userID, err
}

// GetUserID returns id of the user if role exists
func GetUserID(
	ctx context.Context, executor *InternalExecutor, txn *kv.Txn, role security.SQLUsername,
) (oid.Oid, error) {

	var userID oid.Oid
	query := `SELECT user_id FROM system.users WHERE username=$1`

	values, err := executor.QueryRowEx(ctx, "GetUserID", txn, sessiondata.InternalExecutorOverride{
		User: security.RootUserName(),
	},
		query, role)

	if err != nil {
		return userID, errors.Wrapf(err, "error looking up user %s", role)
	}

	if values != nil {
		if v := values[0]; v != tree.DNull {
			userID = oid.Oid(v.(*tree.DOid).DInt)
		}
	}
	return userID, nil

}

// HashString allows to create
func HashString(str string) int {
	h := fnv.New32()
	h.Write([]byte(str))
	return int(h.Sum32())
}

// ToSQLIDs convert a list of SQLUsernames to a map of SQLUsernames to ids
func ToSQLIDs(
	ctx context.Context,
	l []security.SQLUsername,
	execCfg *ExecutorConfig,
	descsCol *descs.Collection,
	executor *InternalExecutor,
	txn *kv.Txn,
) (map[security.SQLUsername]string, error) {
	targetRoles := make(map[security.SQLUsername]string)
	for _, role := range l {
		roleID, err := GetUserIDWithCache(ctx, execCfg, descsCol, executor, txn, role)
		if err != nil {
			return nil, err
		}
		targetRoles[role] = string(roleID)
	}
	return targetRoles, nil
}

// PublicRoleName is the SQLUsername for PublicRole.
func PublicRoleInfo(ctx context.Context, p *planner) security.SQLUserInfo {
	id, _ := GetUserID(ctx, p.execCfg.InternalExecutor, nil, security.PublicRoleName())
	return security.SQLUserInfo{
		Username: security.PublicRoleName(),
		UserID:   id,
	}
}

// ToSQLUserInfos converts a slice of security.SQLUsername to slice of security.SQLUserInfo.
func ToSQLUsernamesWithCache(
	ctx context.Context,
	execCfg *ExecutorConfig,
	descsCol *descs.Collection,
	executor *InternalExecutor,
	txn *kv.Txn,
	roles []security.SQLUsername,
) ([]security.SQLUserInfo, error) {
	targetRoles := make([]security.SQLUserInfo, len(roles))
	for i, role := range roles {
		id, err := GetUserIDWithCache(ctx, execCfg, descsCol, executor, txn, role)
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
