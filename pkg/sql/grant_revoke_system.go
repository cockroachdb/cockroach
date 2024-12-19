// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
)

type changeNonDescriptorBackedPrivilegesNode struct {
	zeroInputPlanNode
	changePrivilegesNode
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because GRANT/REVOKE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *changeNonDescriptorBackedPrivilegesNode) ReadingOwnWrites() {}

func (n *changeNonDescriptorBackedPrivilegesNode) startExec(params runParams) error {
	if err := params.p.preChangePrivilegesValidation(params.ctx, n.grantees, n.withGrantOption, n.isGrant); err != nil {
		return err
	}

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

		deleteStmt := fmt.Sprintf(
			`DELETE FROM system.%s VALUES WHERE username = $1 AND path = $2`, catconstants.SystemPrivilegeTableName)
		upsertStmt := fmt.Sprintf(`
UPSERT INTO system.%s (username, path, privileges, grant_options, user_id)
VALUES ($1, $2, $3, $4, (
	SELECT CASE $1
		WHEN '%s' THEN %d
		ELSE (SELECT user_id FROM system.users WHERE username = $1)
	END
))`,
			catconstants.SystemPrivilegeTableName, username.PublicRole, username.PublicRoleID)

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
					_, err := params.p.InternalSQLTxn().ExecEx(
						params.ctx,
						`delete-system-privilege`,
						params.p.txn,
						sessiondata.NodeUserSessionDataOverride,
						deleteStmt,
						user.Normalized(),
						systemPrivilegeObject.GetPath(),
					)
					if err != nil {
						return err
					}
					continue
				}

				privList, err := privilege.ListFromBitField(userPrivs.Privileges, n.grantOn)
				if err != nil {
					return err
				}
				grantOptionList, err := privilege.ListFromBitField(userPrivs.WithGrantOption, n.grantOn)
				if err != nil {
					return err
				}
				if _, err := params.p.InternalSQLTxn().ExecEx(
					params.ctx,
					`insert-system-privilege`,
					params.p.txn,
					sessiondata.NodeUserSessionDataOverride,
					upsertStmt,
					user.Normalized(),
					systemPrivilegeObject.GetPath(),
					privList.SortedKeys(),
					grantOptionList.SortedKeys(),
				); err != nil {
					return err
				}
			}
		} else {
			// Handle revoke case.
			for _, user := range n.grantees {
				if err := syntheticPrivDesc.Revoke(user, n.desiredprivs, n.grantOn, n.withGrantOption); err != nil {
					return err
				}
				userPrivs, found := syntheticPrivDesc.FindUser(user)

				// For Public role and virtual tables, leave an empty
				// row to indicate that SELECT has been revoked.
				if !found && (n.grantOn == privilege.VirtualTable && user == username.PublicRoleName()) {
					_, err := params.p.InternalSQLTxn().ExecEx(
						params.ctx,
						`insert-system-privilege`,
						params.p.txn,
						sessiondata.NodeUserSessionDataOverride,
						upsertStmt,
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
					_, err := params.p.InternalSQLTxn().ExecEx(
						params.ctx,
						`delete-system-privilege`,
						params.p.txn,
						sessiondata.NodeUserSessionDataOverride,
						deleteStmt,
						user.Normalized(),
						systemPrivilegeObject.GetPath(),
					)
					if err != nil {
						return err
					}
					continue
				}

				privList, err := privilege.ListFromBitField(userPrivs.Privileges, n.grantOn)
				if err != nil {
					return err
				}
				grantOptionList, err := privilege.ListFromBitField(userPrivs.WithGrantOption, n.grantOn)
				if err != nil {
					return err
				}
				if _, err := params.p.InternalSQLTxn().ExecEx(
					params.ctx,
					`insert-system-privilege`,
					params.p.txn,
					sessiondata.NodeUserSessionDataOverride,
					upsertStmt,
					user.Normalized(),
					systemPrivilegeObject.GetPath(),
					privList.SortedKeys(),
					grantOptionList.SortedKeys(),
				); err != nil {
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
			tableNames, _, err := p.ExpandTableGlob(ctx, tableGlob)
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
		var ret []syntheticprivilege.Object
		for _, externalConnectionName := range n.targets.ExternalConnections {
			// Ensure that an External Connection of this name actually exists.
			if _, err := externalconn.LoadExternalConnection(
				ctx, string(externalConnectionName), p.InternalSQLTxn(),
			); err != nil {
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
			return p.ExecCfg().SyntheticPrivilegeCache.Get(
				ctx, p.InternalSQLTxn(), p.Descriptors(), vDesc,
			)
		}
		return d.GetPrivileges(), nil
	case catalog.Descriptor:
		return d.GetPrivileges(), nil
	case syntheticprivilege.Object:
		return p.ExecCfg().SyntheticPrivilegeCache.Get(
			ctx, p.InternalSQLTxn(), p.Descriptors(), d,
		)
	}
	return nil, errors.AssertionFailedf("unknown privilege.Object type %T", po)
}
