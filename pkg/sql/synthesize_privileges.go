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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/cacheutil"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
)

// PrivilegeSynthesizer is used to synthesize a privilege descriptor.
// The cache should be passed in from the server's ExecCfg.
type PrivilegeSynthesizer struct {
	syntheticPrivilegeCache *cacheutil.Cache
	ieFactory               sqlutil.InternalExecutorFactory
}

var _ syntheticprivilege.PrivilegeSynthesizer = &PrivilegeSynthesizer{}

// NewPrivilegeSynthesizer creates a new PrivilegeSynthesizer.
// If IE is nil, SetIEFactory must be called before use.
func NewPrivilegeSynthesizer(
	cache *cacheutil.Cache, ie sqlutil.InternalExecutorFactory,
) *PrivilegeSynthesizer {
	return &PrivilegeSynthesizer{
		syntheticPrivilegeCache: cache,
		ieFactory:               ie,
	}
}

// SetIEFactory sets the ieFactory field of PrivilegeSynthesizer.
func (p *PrivilegeSynthesizer) SetIEFactory(ieFactory sqlutil.InternalExecutorFactory) {
	p.ieFactory = ieFactory
}

// SynthesizePrivilegeDescriptor is part of the Planner interface.
func (p *PrivilegeSynthesizer) SynthesizePrivilegeDescriptor(
	ctx context.Context,
	txn *kv.Txn,
	privilegeObjectPath string,
	privilegeObjectType privilege.ObjectType,
	tableVersion descpb.DescriptorVersion,
) (*catpb.PrivilegeDescriptor, error) {
	var tableVersions []descpb.DescriptorVersion
	cache := p.syntheticPrivilegeCache
	found, privileges := func() (bool, *catpb.PrivilegeDescriptor) {
		cache.Lock()
		defer cache.Unlock()
		tableVersions = []descpb.DescriptorVersion{tableVersion}
		if isEligibleForCache := cache.ClearCacheIfStaleLocked(ctx, tableVersions); isEligibleForCache {
			val, ok := cache.GetValueLocked(privilegeObjectPath)
			if ok {
				privilegeDescriptor := val.(*catpb.PrivilegeDescriptor)
				return true, privilegeDescriptor
			}

		}
		return false, nil
	}()

	if found {
		return privileges, nil
	}

	val, err := cache.LoadValueOutsideOfCache(ctx, fmt.Sprintf("%s-%d", privilegeObjectPath, tableVersion),
		func(loadCtx context.Context) (interface{}, error) {
			query := fmt.Sprintf(
				`SELECT username, privileges, grant_options FROM system.%s WHERE path='%s'`,
				catconstants.SystemPrivilegeTableName,
				privilegeObjectPath)

			err := p.ieFactory.RunWithoutTxn(loadCtx, func(ctx context.Context, ie sqlutil.InternalExecutor) (retErr error) {
				it, err := ie.QueryIteratorEx(ctx, `get-system-privileges`, txn,
					sessiondata.NodeUserSessionDataOverride, query)
				if err != nil {
					return err
				}
				defer func() {
					retErr = errors.CombineErrors(retErr, it.Close())
				}()

				privileges = &catpb.PrivilegeDescriptor{OwnerProto: username.AdminRoleName().EncodeProto()}
				for {
					ok, err := it.Next(ctx)
					if err != nil {
						return err
					}
					if !ok {
						break
					}

					user := tree.MustBeDString(it.Cur()[0])
					privArr := tree.MustBeDArray(it.Cur()[1])
					var privilegeStrings []string
					for _, elem := range privArr.Array {
						privilegeStrings = append(privilegeStrings, string(tree.MustBeDString(elem)))
					}

					grantOptionArr := tree.MustBeDArray(it.Cur()[2])
					var grantOptionStrings []string
					for _, elem := range grantOptionArr.Array {
						grantOptionStrings = append(grantOptionStrings, string(tree.MustBeDString(elem)))
					}
					privs, err := privilege.ListFromStrings(privilegeStrings)
					if err != nil {
						return err
					}
					grantOptions, err := privilege.ListFromStrings(grantOptionStrings)
					if err != nil {
						return err
					}
					privsWithGrantOption := privilege.ListFromBitField(
						privs.ToBitField()&grantOptions.ToBitField(),
						privilegeObjectType,
					)
					privsWithoutGrantOption := privilege.ListFromBitField(
						privs.ToBitField()&^privsWithGrantOption.ToBitField(),
						privilegeObjectType,
					)
					privileges.Grant(
						username.MakeSQLUsernameFromPreNormalizedString(string(user)),
						privsWithGrantOption,
						true, /* withGrantOption */
					)
					privileges.Grant(
						username.MakeSQLUsernameFromPreNormalizedString(string(user)),
						privsWithoutGrantOption,
						false, /* withGrantOption */
					)
				}

				// To avoid having to insert a row for public for each virtual
				// table into system.privileges, we assume that if there is
				// NO entry for public in the PrivilegeDescriptor, Public has
				// grant. If there is an empty row for Public, then public
				// does not have grant.
				if privilegeObjectType == privilege.VirtualTable {
					if _, found := privileges.FindUser(username.PublicRoleName()); !found {
						privileges.Grant(username.PublicRoleName(), privilege.List{privilege.SELECT}, false)
					}
				}

				// We use InvalidID to skip checks on the root/admin roles having
				// privileges.
				if err := privileges.Validate(
					descpb.InvalidID,
					privilegeObjectType,
					privilegeObjectPath,
					privilege.GetValidPrivilegesForObject(privilegeObjectType),
				); err != nil {
					return err
				}
				return nil
			})
			return privileges, err
		})
	if err != nil {
		return nil, err
	}
	cache.MaybeWriteBackToCache(ctx, tableVersions, privilegeObjectPath, val)
	return val.(*catpb.PrivilegeDescriptor), nil
}

// SynthesizePrivilegeDescriptor is part of the Planner interface.
func (p *planner) SynthesizePrivilegeDescriptor(
	ctx context.Context, path string, privilegeObjectType privilege.ObjectType,
) (*catpb.PrivilegeDescriptor, error) {
	found, desc, err := p.Descriptors().GetImmutableTableByName(ctx, p.Txn(), syntheticprivilege.SystemPrivilegesTableName, tree.ObjectLookupFlags{})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.AssertionFailedf("failed to find system.privileges table")
	}
	return p.privilegeSynthesizer.SynthesizePrivilegeDescriptor(ctx, p.Txn(), path, privilegeObjectType, desc.GetVersion())
}
