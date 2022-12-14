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
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/replicationslot"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

func (p *planner) CreateSlot(ctx context.Context, slotName string) (uint64, error) {
	if strings.Contains(slotName, replicationslot.SlotNameSeparator) {
		return 0, errors.Newf("invalid character \"%s\" found in slot name: %s", replicationslot.SlotNameSeparator, slotName)
	}

	dbDesc, err := p.Descriptors().GetImmutableDatabaseByName(ctx, p.txn, p.CurrentDatabase(),
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return 0, err
	}
	var minLSN uint64 = math.MaxUint64
	if err = p.forEachMutableTableInDatabase(
		ctx,
		dbDesc,
		func(ctx context.Context, scName string, tbDesc *tabledesc.Mutable) error {
			// ???? drop again just in case.
			schemaDescriptor, err := p.Descriptors().GetImmutableDescriptorByID(ctx, p.txn, tbDesc.GetParentSchemaID(),
				tree.CommonLookupFlags{Required: true})
			if err != nil {
				return err
			}
			tableSlotName := replicationslot.BuildFullSlotName(slotName, schemaDescriptor.GetName()+"."+tbDesc.GetName())
			if err := p.DropSlot(ctx, tableSlotName); err != nil {
				return err
			}
			_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
				ctx,
				"crdb-internal-create-slot",
				p.txn,
				sessiondata.InternalExecutorOverride{User: username.RootUserName()},
				// i love me some sql injection
				fmt.Sprintf("CREATE CHANGEFEED FOR TABLE %s.public.%s INTO $1", p.SessionData().Database, tbDesc.Name),
				"replication://"+tableSlotName,
			)
			if err != nil {
				return err
			}
			slot := replicationslot.ReplicationSlots[tableSlotName]
			slot.SetDatabase(p.CurrentDatabase())
			slot.SetTableID(tbDesc.ID)
			slot.SetBaseSlotName(slotName)
			slot.SetFullSlotName(tableSlotName)
			slot.SetDescsCollection(p.descCollection)
			// Not sure if we want to store the DB in this struct
			slot.SetDB(p.txn.DB())
			lsn := slot.LSN()
			// Not sure if returning the min is the right thing to do here.
			if lsn < minLSN {
				minLSN = lsn
			}
			return nil
		},
	); err != nil {
		return 0, err
	}
	return minLSN, nil
}

func (p *planner) DropSlot(ctx context.Context, slotName string) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"crdb-internal-drop-slot",
		p.txn,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		"cancel jobs select job_id from [show changefeed jobs] where sink_uri = $1 and status = 'running'",
		"replication://"+slotName,
	)
	if err != nil {
		return err
	}
	for name, s := range replicationslot.ReplicationSlots {
		if s.GetBaseSlotName() == slotName {
			delete(replicationslot.ReplicationSlots, name)
		}
	}
	return nil
}

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
	r, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
		ctx,
		"crdb-internal-get-descriptor-id",
		p.txn,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
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
