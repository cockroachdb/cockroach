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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// LookupNamespaceID implements tree.PrivilegedAccessor.
// TODO(sqlexec): make this work for any arbitrary schema.
// This currently only works for public schemas and databases.
func (p *planner) LookupNamespaceID(
	ctx context.Context, parentID int64, name string,
) (tree.DInt, bool, error) {
	query := fmt.Sprintf(
		`SELECT id FROM [%d AS namespace] WHERE "parentID" = $1 AND "parentSchemaID" IN (0, 29) AND name = $2`,
		keys.NamespaceTableID,
	)
	r, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
		ctx,
		"crdb-internal-get-descriptor-id",
		p.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
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
	if err := p.checkDescriptorPermissions(ctx, descpb.ID(id)); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// LookupZoneConfigByNamespaceID implements tree.PrivilegedAccessor.
func (p *planner) LookupZoneConfigByNamespaceID(
	ctx context.Context, id int64,
) (tree.DBytes, bool, error) {
	if err := p.checkDescriptorPermissions(ctx, descpb.ID(id)); err != nil {
		return "", false, err
	}

	const query = `SELECT config FROM system.zones WHERE id = $1`
	r, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
		ctx,
		"crdb-internal-get-zone",
		p.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
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
func (p *planner) checkDescriptorPermissions(ctx context.Context, id descpb.ID) error {
	desc, err := p.Descriptors().GetImmutableDescriptorByID(
		ctx, p.txn, id,
		tree.CommonLookupFlags{
			IncludeDropped: true,
			IncludeOffline: true,
			// Note that currently the ByID API implies required regardless of whether it
			// is set. Set it just to be explicit.
			Required: true,
		},
	)
	if err != nil {
		// Filter the error due to the descriptor not existing.
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			err = nil
		}
		return err
	}
	if err := p.CheckAnyPrivilege(ctx, desc); err != nil {
		return pgerror.New(pgcode.InsufficientPrivilege, "insufficient privilege")
	}
	return nil
}
