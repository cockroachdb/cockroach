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
	if err := params.p.preChangePrivilegesValidation(params.ctx, n.grantees, n.withGrantOption, n.isGrant); err != nil {
		return err
	}

	// Get the privilege path for this grant.
	systemPrivilegeObject := n.makeSystemPrivilegeObject()
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
			syntheticPrivDesc.Grant(user, n.desiredprivs, n.withGrantOption)
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
		syntheticPrivDesc.Revoke(user, n.desiredprivs, n.grantOn, n.withGrantOption)
		userPrivs, found := syntheticPrivDesc.FindUser(user)

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

		upsert := fmt.Sprintf(`UPSERT INTO system.%s VALUES ($1, $2, $3, $4)`, catconstants.SystemPrivilegeTableName)
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

	return nil
}

func (*changeNonDescriptorBackedPrivilegesNode) Next(runParams) (bool, error) { return false, nil }
func (*changeNonDescriptorBackedPrivilegesNode) Values() tree.Datums          { return tree.Datums{} }
func (*changeNonDescriptorBackedPrivilegesNode) Close(context.Context)        {}

func (n *changeNonDescriptorBackedPrivilegesNode) makeSystemPrivilegeObject() catalog.SyntheticPrivilegeObject {
	switch n.grantOn {
	case privilege.Global:
		return syntheticprivilege.GlobalPrivilegeObject
	default:
		panic(errors.AssertionFailedf("unknown grant on object %v", n.grantOn))
	}
}
