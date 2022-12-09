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

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
)

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because GRANT/REVOKE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *changeNonDescriptorBackedPrivilegesNode) ReadingOwnWrites() {}

func (n *changeNonDescriptorBackedPrivilegesNode) startExec(params runParams) error {
	if !params.p.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.V22_2SystemPrivilegesTable) {
		return errors.Newf("system cluster privileges are not supported until upgrade to version %s is finalized", clusterversion.V22_2SystemPrivilegesTable.String())
	}
	if err := params.p.preChangePrivilegesValidation(params.ctx, n.grantees, n.withGrantOption, n.isGrant); err != nil {
		return err
	}

	deleteStmt := fmt.Sprintf(
		`DELETE FROM system.%s VALUES WHERE username = $1 AND path = $2`,
		catconstants.SystemPrivilegeTableName,
	)

	// Get the privilege path for this grant.
	systemPrivilegeObjects, err := n.makeSystemPrivilegeObject(params.ctx, params.p)
	if err != nil {
		return err
	}
	for _, systemPrivilegeObject := range systemPrivilegeObjects {
		if err := catprivilege.ValidateSyntheticPrivilegeObject(systemPrivilegeObject); err != nil {
			return err
		}
		syntheticPrivDesc, err := params.p.getPrivilegeDescriptor(params.ctx, systemPrivilegeObject)
		if err != nil {
			return err
		}

		err = params.p.MustCheckGrantOptionsForUser(params.ctx, syntheticPrivDesc, systemPrivilegeObject, n.desiredprivs, params.p.User(), n.isGrant)
		if err != nil {
			return err
		}

		if n.isGrant {
			// Privileges are valid, write them to the system.privileges table.
			for _, user := range n.grantees {
				syntheticPrivDesc.Grant(user, n.desiredprivs, n.withGrantOption)

				userPrivs, found := syntheticPrivDesc.FindUser(user)
				if !found {
					return errors.AssertionFailedf("user %s not found", user)
				}
				// If the row is only "public" with SELECT
				// explicitly delete the row. Lack of row for
				// public means public has SELECT which
				// is the default case.
				if user == username.PublicRoleName() && userPrivs.Privileges == privilege.SELECT.Mask() {
					_, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
						params.ctx,
						`delete-system-privilege`,
						params.p.txn,
						sessiondata.InternalExecutorOverride{User: username.RootUserName()},
						deleteStmt,
						user.Normalized(),
						systemPrivilegeObject.GetPath(),
					)
					if err != nil {
						return err
					}
					continue
				}

				insertStmt := fmt.Sprintf(`UPSERT INTO system.%s VALUES ($1, $2, $3, $4)`, catconstants.SystemPrivilegeTableName)
				_, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
					params.ctx,
					`insert-system-privilege`,
					params.p.txn,
					sessiondata.InternalExecutorOverride{User: username.RootUserName()},
					insertStmt,
					user.Normalized(),
					systemPrivilegeObject.GetPath(),
					privilege.ListFromBitField(userPrivs.Privileges, n.grantOn).SortedNames(),
					privilege.ListFromBitField(userPrivs.WithGrantOption, n.grantOn).SortedNames(),
				)
				if err != nil {
					return err
				}
			}
		} else {

			// Handle revoke case.
			for _, user := range n.grantees {
				syntheticPrivDesc.Revoke(user, n.desiredprivs, n.grantOn, n.withGrantOption)
				userPrivs, found := syntheticPrivDesc.FindUser(user)
				upsert := fmt.Sprintf(`UPSERT INTO system.%s VALUES ($1, $2, $3, $4)`, catconstants.SystemPrivilegeTableName)
				// For Public role and virtual tables, leave an empty
				// row to indicate that SELECT has been revoked.
				if !found && (n.grantOn == privilege.VirtualTable && user == username.PublicRoleName()) {
					_, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
						params.ctx,
						`insert-system-privilege`,
						params.p.txn,
						sessiondata.InternalExecutorOverride{User: username.RootUserName()},
						upsert,
						user.Normalized(),
						systemPrivilegeObject.GetPath(),
						[]string{},
						[]string{},
					)
					if err != nil {
						return err
					}
					continue
				}

				// If there are no entries remaining on the PrivilegeDescriptor for the user
				// we can remove the entire row for the user.
				if !found {
					_, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
						params.ctx,
						`delete-system-privilege`,
						params.p.txn,
						sessiondata.InternalExecutorOverride{User: username.RootUserName()},
						deleteStmt,
						user.Normalized(),
						systemPrivilegeObject.GetPath(),
					)
					if err != nil {
						return err
					}
					continue
				}

				_, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
					params.ctx,
					`insert-system-privilege`,
					params.p.txn,
					sessiondata.InternalExecutorOverride{User: username.RootUserName()},
					upsert,
					user.Normalized(),
					systemPrivilegeObject.GetPath(),
					privilege.ListFromBitField(userPrivs.Privileges, n.grantOn).SortedNames(),
					privilege.ListFromBitField(userPrivs.WithGrantOption, n.grantOn).SortedNames(),
				)
				if err != nil {
					return err
				}
			}
		}
	}

	// Bump table version to invalidate cache.
	return params.p.BumpPrivilegesTableVersion(params.ctx)
}

func (*changeNonDescriptorBackedPrivilegesNode) Next(runParams) (bool, error) { return false, nil }
func (*changeNonDescriptorBackedPrivilegesNode) Values() tree.Datums          { return tree.Datums{} }
func (*changeNonDescriptorBackedPrivilegesNode) Close(context.Context)        {}

func (n *changeNonDescriptorBackedPrivilegesNode) makeSystemPrivilegeObject(
	ctx context.Context, p *planner,
) ([]syntheticprivilege.Object, error) {
	switch n.grantOn {
	case privilege.Global:
		return []syntheticprivilege.Object{syntheticprivilege.GlobalPrivilegeObject}, nil
	case privilege.VirtualTable:
		var ret []syntheticprivilege.Object
		for _, tableTarget := range n.targets.Tables.TablePatterns {
			tableGlob, err := tableTarget.NormalizeTablePattern()
			if err != nil {
				return nil, err
			}
			tableNames, _, err := expandTableGlob(ctx, p, tableGlob)
			if err != nil {
				return nil, err
			}

			if len(tableNames) == 0 {
				return nil, errors.AssertionFailedf("no tables found")
			}

			for _, name := range tableNames {
				if name.ExplicitCatalog {
					p.BufferClientNotice(ctx, pgnotice.Newf("virtual table privileges are not database specific"))
				}
				ret = append(ret, &syntheticprivilege.VirtualTablePrivilege{
					SchemaName: name.Schema(),
					TableName:  name.Table(),
				})
			}
		}
		return ret, nil
	case privilege.ExternalConnection:
		if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.V22_2SystemExternalConnectionsTable) {
			return nil, errors.Newf("External Connections are not supported until upgrade to version %s is finalized",
				clusterversion.V22_2SystemExternalConnectionsTable.String())
		}

		var ret []syntheticprivilege.Object
		for _, externalConnectionName := range n.targets.ExternalConnections {
			// Ensure that an External Connection of this name actually exists.
			if _, err := externalconn.LoadExternalConnection(ctx, string(externalConnectionName),
				p.ExecCfg().InternalExecutor, p.Txn()); err != nil {
				return nil, errors.Wrap(err, "failed to resolve External Connection")
			}

			ret = append(ret, &syntheticprivilege.ExternalConnectionPrivilege{
				ConnectionName: string(externalConnectionName),
			})
		}
		return ret, nil

	default:
		panic(errors.AssertionFailedf("unknown grant on object %v", n.grantOn))
	}
}

// getPrivilegeDescriptor returns the privilege descriptor for the
// object. Note that for non-descriptor backed objects, we query the
// system.privileges table to synthesize a PrivilegeDescriptor.
func (p *planner) getPrivilegeDescriptor(
	ctx context.Context, po privilege.Object,
) (*catpb.PrivilegeDescriptor, error) {
	switch d := po.(type) {
	case catalog.TableDescriptor:
		if d.IsVirtualTable() {
			// Virtual tables are somewhat of a weird case in that they
			// have descriptors.
			// For virtual tables, we don't store privileges on the
			// descriptor as we don't allow the privilege descriptor to
			// change.
			// It is also problematic that virtual table descriptors
			// do not store a database id, so the descriptors are not
			// "per database" even though regular tables are per database.
			vs, found := schemadesc.GetVirtualSchemaByID(d.GetParentSchemaID())
			if !found {
				return nil, errors.AssertionFailedf("no virtual schema found for virtual table %s", d.GetName())
			}
			vDesc := &syntheticprivilege.VirtualTablePrivilege{
				SchemaName: vs.GetName(),
				TableName:  d.GetName(),
			}
			return synthesizePrivilegeDescriptor(
				ctx, p.ExecCfg(), p.ExecCfg().InternalExecutor, p.Descriptors(), p.Txn(), vDesc,
			)
		}
		return d.GetPrivileges(), nil
	case catalog.Descriptor:
		return d.GetPrivileges(), nil
	case syntheticprivilege.Object:
		return synthesizePrivilegeDescriptor(
			ctx, p.ExecCfg(), p.ExecCfg().InternalExecutor, p.Descriptors(), p.Txn(), d,
		)
	}
	return nil, errors.AssertionFailedf("unknown privilege.Object type %T", po)
}

// synthesizePrivilegeDescriptor returns the synthetic privilege descriptor
// for the object. We query the system.privileges table to synthesize a
// PrivilegeDescriptor.
func synthesizePrivilegeDescriptor(
	ctx context.Context,
	execCfg *ExecutorConfig,
	ie sqlutil.InternalExecutor,
	descsCol *descs.Collection,
	txn *kv.Txn,
	spo syntheticprivilege.Object,
) (*catpb.PrivilegeDescriptor, error) {
	if !execCfg.Settings.Version.IsActive(ctx, spo.SystemPrivilegesTableVersionGate()) {
		// Fall back to defaults if the version gate is not active yet.
		return spo.GetFallbackPrivileges(), nil
	}
	_, desc, err := descsCol.GetImmutableTableByName(
		ctx,
		txn,
		syntheticprivilege.SystemPrivilegesTableName,
		tree.ObjectLookupFlagsWithRequired(),
	)
	if err != nil {
		return nil, err
	}
	if desc.IsUncommittedVersion() {
		return synthesizePrivilegeDescriptorFromSystemPrivilegesTable(ctx, ie, txn, spo)
	}
	var tableVersions []descpb.DescriptorVersion
	cache := execCfg.SyntheticPrivilegeCache
	found, privileges, retErr := func() (bool, catpb.PrivilegeDescriptor, error) {
		cache.Lock()
		defer cache.Unlock()
		version := desc.GetVersion()
		tableVersions = []descpb.DescriptorVersion{version}

		if isEligibleForCache := cache.ClearCacheIfStaleLocked(ctx, tableVersions); isEligibleForCache {
			val, ok := cache.GetValueLocked(spo.GetPath())
			if ok {
				return true, val.(catpb.PrivilegeDescriptor), nil
			}
		}
		return false, catpb.PrivilegeDescriptor{}, nil
	}()

	if found {
		return &privileges, retErr
	}

	val, err := cache.LoadValueOutsideOfCacheSingleFlight(ctx, fmt.Sprintf("%s-%d", spo.GetPath(), desc.GetVersion()),
		func(loadCtx context.Context) (_ interface{}, retErr error) {
			return synthesizePrivilegeDescriptorFromSystemPrivilegesTable(ctx, ie, txn, spo)
		})
	if err != nil {
		return nil, err
	}
	privDesc := val.(*catpb.PrivilegeDescriptor)
	// Only write back to the cache if the table version is
	// committed.
	cache.MaybeWriteBackToCache(ctx, tableVersions, spo.GetPath(), *privDesc)
	return privDesc, nil
}

// synthesizePrivilegeDescriptorFromSystemPrivilegesTable reads from the
// system.privileges table to create the PrivilegeDescriptor from the
// corresponding privilege object. This is only used if the we cannot
// resolve the PrivilegeDescriptor from the cache.
func synthesizePrivilegeDescriptorFromSystemPrivilegesTable(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn, spo syntheticprivilege.Object,
) (privileges *catpb.PrivilegeDescriptor, retErr error) {

	query := fmt.Sprintf(
		`SELECT username, privileges, grant_options FROM system.%s WHERE path='%s'`,
		catconstants.SystemPrivilegeTableName,
		spo.GetPath())

	it, err := ie.QueryIteratorEx(
		ctx, `get-system-privileges`, txn, sessiondata.NodeUserSessionDataOverride, query,
	)
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
			return nil, err
		}
		grantOptions, err := privilege.ListFromStrings(grantOptionStrings)
		if err != nil {
			return nil, err
		}
		privsWithGrantOption := privilege.ListFromBitField(
			privs.ToBitField()&grantOptions.ToBitField(),
			spo.GetObjectType(),
		)
		privsWithoutGrantOption := privilege.ListFromBitField(
			privs.ToBitField()&^privsWithGrantOption.ToBitField(),
			spo.GetObjectType(),
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
	if spo.GetObjectType() == privilege.VirtualTable {
		if _, found := privileges.FindUser(username.PublicRoleName()); !found {
			privileges.Grant(username.PublicRoleName(), privilege.List{privilege.SELECT}, false)
		}
	}

	// We use InvalidID to skip checks on the root/admin roles having
	// privileges.
	if err := privileges.Validate(
		descpb.InvalidID,
		spo.GetObjectType(),
		spo.GetPath(),
		privilege.GetValidPrivilegesForObject(spo.GetObjectType()),
	); err != nil {
		return nil, err
	}
	return privileges, err
}
