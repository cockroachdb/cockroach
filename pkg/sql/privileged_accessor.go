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
	ctx context.Context, parentID int64, parentSchemaID int64, name string,
) (tree.DInt, bool, error) {
	query := fmt.Sprintf(
		`SELECT id FROM [%d AS namespace] WHERE "parentID" = $1 AND "parentSchemaID" = $2 AND name = $3`,
		keys.NamespaceTableID,
	)
	r, err := p.InternalSQLTxn().QueryRowEx(
		ctx,
		"crdb-internal-get-descriptor-id",
		p.txn,
		sessiondata.RootUserSessionDataOverride,
		query,
		parentID,
		parentSchemaID,
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

	zc, err := p.Descriptors().GetZoneConfig(ctx, p.Txn(), descpb.ID(id))
	if err != nil {
		return "", false, err
	}
	if zc == nil {
		return "", false, nil
	}

	return tree.DBytes(zc.GetRawBytesInStorage()), true, nil
}

// checkDescriptorPermissions returns nil if the executing user has permissions
// to check the permissions of a descriptor given its ID, or the id given
// is not a descriptor of a table or database.
func (p *planner) checkDescriptorPermissions(ctx context.Context, id descpb.ID) error {
	desc, err := p.Descriptors().ByIDWithLeased(p.txn).Get().Desc(ctx, id)
	if err != nil {
		// Filter the error due to the descriptor not existing.
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			err = nil
		}
		return err
	}
	if err := p.CheckAnyPrivilege(ctx, desc); err != nil {
		return pgerror.Wrapf(err, pgcode.InsufficientPrivilege, "insufficient privilege")
	}
	return nil
}
