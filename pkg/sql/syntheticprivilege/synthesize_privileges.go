// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syntheticprivilege

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// SystemPrivilegesTableName represents system.privileges.
var SystemPrivilegesTableName = tree.NewTableNameWithSchema("system", tree.PublicSchemaName, "privileges")

func synthesizePrivilegeDescriptorFromSystemPrivilegesTable(
	ctx context.Context, p eval.Planner, systemTablePrivilegeObject catalog.SyntheticPrivilegeObject,
) (privileges *catpb.PrivilegeDescriptor, retErr error) {
	var tableVersions []descpb.DescriptorVersion
	cache := p.GetSyntheticPrivilegeCache()
	var found bool
	found, privileges, retErr = func() (bool, *catpb.PrivilegeDescriptor, error) {
		cache.Lock()
		defer cache.Unlock()
		version, err := p.GetDescriptorVersionByTableName(ctx, SystemPrivilegesTableName, tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			return false, nil, err
		}
		tableVersions = []descpb.DescriptorVersion{version}
		if isEligibleForCache := cache.ClearCacheIfStaleLocked(ctx, tableVersions); isEligibleForCache {
			val, ok := cache.GetValueLocked(systemTablePrivilegeObject.ToString())
			if ok {
				privilegeDescriptor := val.(*catpb.PrivilegeDescriptor)
				return true, privilegeDescriptor, nil
			}

		}
		return false, nil, nil
	}()

	if found {
		return privileges, retErr
	}

	val, err := cache.LoadValueOutsideOfCache(ctx, systemTablePrivilegeObject.ToString(), func(loadCtx context.Context) (interface{}, error) {
		query := fmt.Sprintf(
			`SELECT username, privileges FROM system.%s WHERE path='%s'`,
			catconstants.SystemPrivilegeTableName,
			systemTablePrivilegeObject.ToString())

		it, err := p.QueryIteratorEx(ctx, `get-system-privileges`,
			sessiondata.NodeUserSessionDataOverride, query)
		if err != nil {
			return nil, err
		}
		defer func() {
			retErr = errors.CombineErrors(retErr, it.Close())
		}()

		privileges = &catpb.PrivilegeDescriptor{}
		for {
			ok, err := it.Next(ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}

			user := tree.MustBeDString(it.Cur()[0])
			arr := tree.MustBeDArray(it.Cur()[1])
			var privilegeStrings []string
			for _, elem := range arr.Array {
				privilegeStrings = append(privilegeStrings, string(tree.MustBeDString(elem)))
			}
			privs, err := privilege.ListFromStrings(privilegeStrings)
			if err != nil {
				return nil, err
			}
			privileges.Grant(
				username.MakeSQLUsernameFromPreNormalizedString(string(user)),
				privs,
				false, /* withGrantOption */
			)
		}

		// To avoid having to insert a row for public for each virtual
		// table into system.privileges, we assume that if there is
		// NO entry for public in the PrivilegeDescriptor, Public has
		// grant. If there is an empty row for Public, then public
		// does not have grant.
		// TODO(richardjcai): Do a migration for this instead.
		if systemTablePrivilegeObject.PrivilegeObjectType() == privilege.VirtualTable {
			if _, found := privileges.FindUser(username.PublicRoleName()); !found {
				privileges.Grant(username.PublicRoleName(), privilege.List{privilege.SELECT}, false /* withGrantOption */)
			}
		}

		privilegeObjectType := systemTablePrivilegeObject.PrivilegeObjectType()
		// We use InvalidID to skip checks on the root/admin roles having
		// privileges.
		if err := privileges.Validate(descpb.InvalidID, privilegeObjectType, systemTablePrivilegeObject.GetName(), privilege.GetValidPrivilegesForObject(privilegeObjectType)); err != nil {
			return nil, err
		}
		return privileges, nil
	})

	if err != nil {
		return nil, err
	}

	cache.MaybeWriteBackToCache(ctx, tableVersions, systemTablePrivilegeObject.ToString(), val)
	return val.(*catpb.PrivilegeDescriptor), nil
}
