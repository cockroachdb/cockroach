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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
)

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because GRANT/REVOKE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *changeNonDescriptorBackedPrivilegesNode) ReadingOwnWrites() {}

func (n *changeNonDescriptorBackedPrivilegesNode) startExec(params runParams) error {
	if !params.p.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.SystemPrivilegesTable) {
		return errors.Newf("system cluster privileges are not supported until upgrade to version %s is finalized", clusterversion.SystemPrivilegesTable.String())
	}
	if err := n.changePrivilegesNode.preChangePrivilegesValidation(params); err != nil {
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
		syntheticPrivDesc, err := systemPrivilegeObject.GetPrivilegeDescriptor(params.ctx, params.p)
		if err != nil {
			return err
		}

		err = params.p.CheckGrantOptionsForUser(params.ctx, syntheticPrivDesc, systemPrivilegeObject, n.desiredprivs, params.p.User(), n.isGrant)
		if err != nil {
			return err
		}

		if n.isGrant {
			// Privileges are valid, write them to the system.privileges table.
			for _, user := range n.grantees {
				syntheticPrivDesc.Grant(user, n.desiredprivs, false)

				userPrivs, found := syntheticPrivDesc.FindUser(user)
				if !found {
					return errors.AssertionFailedf("user %s not found", user)
				}
				insertStmt := fmt.Sprintf(`UPSERT INTO system.%s VALUES ($1, $2, $3, $4)`, catconstants.SystemPrivilegeTableName)
				_, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
					params.ctx,
					`insert-system-privilege`,
					params.p.txn,
					sessiondata.InternalExecutorOverride{User: username.RootUserName()},
					insertStmt,
					user.Normalized(),
					systemPrivilegeObject.ToString(),
					privilege.ListFromBitField(userPrivs.Privileges, n.grantOn).SortedNames(),
					privilege.ListFromBitField(userPrivs.WithGrantOption, n.grantOn).SortedNames(),
				)
				if err != nil {
					return err
				}
			}
			return nil
		}

		// Handle revoke case.
		for _, user := range n.grantees {
			syntheticPrivDesc.Revoke(user, n.desiredprivs, n.grantOn, false)
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
					systemPrivilegeObject.ToString(),
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
				deleteStmt := fmt.Sprintf(
					`DELETE FROM system.%s VALUES WHERE path = $1 AND username = $2`,
					catconstants.SystemPrivilegeTableName,
				)
				_, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
					params.ctx,
					`delete-system-privilege`,
					params.p.txn,
					sessiondata.InternalExecutorOverride{User: username.RootUserName()},
					deleteStmt,
					systemPrivilegeObject.ToString(),
					user.Normalized(),
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
				systemPrivilegeObject.ToString(),
				privilege.ListFromBitField(userPrivs.Privileges, n.grantOn).SortedNames(),
				privilege.ListFromBitField(userPrivs.WithGrantOption, n.grantOn).SortedNames(),
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (*changeNonDescriptorBackedPrivilegesNode) Next(runParams) (bool, error) { return false, nil }
func (*changeNonDescriptorBackedPrivilegesNode) Values() tree.Datums          { return tree.Datums{} }
func (*changeNonDescriptorBackedPrivilegesNode) Close(context.Context)        {}

func (n *changeNonDescriptorBackedPrivilegesNode) makeSystemPrivilegeObject(
	ctx context.Context, p *planner,
) ([]catalog.SyntheticPrivilegeObject, error) {
	switch n.grantOn {
	case privilege.Global:
		return []catalog.SyntheticPrivilegeObject{syntheticprivilege.GlobalPrivilegeObject}, nil
	case privilege.VirtualTable:
		var ret []catalog.SyntheticPrivilegeObject
		for _, tableTarget := range n.targets.Tables.TablePatterns {
			tableGlob, err := tableTarget.NormalizeTablePattern()
			if err != nil {
				return nil, err
			}
			_, objectIDs, err := expandTableGlob(ctx, p, tableGlob)
			if err != nil {
				return nil, err
			}

			if len(objectIDs) == 0 {
				return nil, errors.AssertionFailedf("no tables found")
			}

			for _, id := range objectIDs {
				ret = append(ret, &syntheticprivilege.VirtualTablePrivilege{
					ID: id,
				})
			}
		}
		return ret, nil
	default:
		panic(errors.AssertionFailedf("unknown grant on object %v", n.grantOn))
	}
}
