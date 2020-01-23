// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// LookupNamespaceID implements tree.PrivilegedAccessor.
func (p *planner) LookupNamespaceID(
	ctx context.Context, parentID int64, name string,
) (tree.DInt, bool, error) {
	const query = `SELECT id FROM system.namespace WHERE "parentID" = $1 AND name = $2`
	r, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
		ctx,
		"crdb-internal-get-descriptor-id",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		query,
		parentID,
		name,
	)
	if err != nil {
		return 0, false, err
	}
	if r == nil {
		return 0, false, nil
	}
	id := tree.MustBeDInt(r[0])
	if err = p.checkDescriptorPermissions(ctx, sqlbase.ID(id)); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// LookupZoneConfigByNamespaceID implements tree.PrivilegedAccessor.
func (p *planner) LookupZoneConfigByNamespaceID(
	ctx context.Context, id int64,
) (tree.DBytes, bool, error) {
	if err := p.checkDescriptorPermissions(ctx, sqlbase.ID(id)); err != nil {
		return "", false, err
	}

	const query = `SELECT config FROM system.zones WHERE id = $1`
	r, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
		ctx,
		"crdb-internal-get-zone",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		query,
		id,
	)
	if err != nil {
		return "", false, err
	}
	if r == nil {
		return "", false, nil
	}
	return tree.MustBeDBytes(r[0]), true, nil
}

// checkDescriptorPermissions returns nil if the executing user has permissions
// to check the permissions of a descriptor given its ID, or the id given
// is not a descriptor of a table or database.
func (p *planner) checkDescriptorPermissions(ctx context.Context, id sqlbase.ID) error {
	desc, found, err := lookupDescriptorByID(ctx, p.txn, id)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	if err := p.CheckAnyPrivilege(ctx, desc); err != nil {
		return pgerror.New(pgcode.InsufficientPrivilege, "insufficient privilege")
	}
	return nil
}
