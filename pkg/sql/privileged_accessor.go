// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/zoneconfig"
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
		sessiondata.NodeUserSessionDataOverride,
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

// IsSystemTable implements tree.PrivilegedAccessor.
func (p *planner) IsSystemTable(ctx context.Context, id int64) (bool, error) {
	tbl, err := p.Descriptors().ByIDWithoutLeased(p.Txn()).Get().Table(ctx, catid.DescID(id))
	if err != nil {
		return false, err
	}
	return catalog.IsSystemDescriptor(tbl), nil
}

// ResolvedZoneConfigForKey implements eval.PrivilegedAccessor.
//
// It returns the fully resolved (inheritance-applied) zone configuration for
// the given range start key as a JSONB datum. The resolution works as follows:
//
//   - Pre-table keyspace: the key is mapped to its named zone (meta, liveness,
//     system, timeseries, tenants) and resolved via GetHydratedForNamedZone.
//   - Table keyspace (>= TableDataMin): the table ID is decoded from the key
//     and the zone config is resolved via GetHydratedForTable, which walks the
//     full inheritance chain (table → database → RANGE DEFAULT).
func (p *planner) ResolvedZoneConfigForKey(
	ctx context.Context, key roachpb.Key,
) (tree.Datum, error) {
	var zc *zonepb.ZoneConfig
	var err error

	rKey := roachpb.RKey(key)
	if rKey.Equal(roachpb.RKeyMin) ||
		bytes.HasPrefix(rKey, keys.Meta1Prefix) ||
		bytes.HasPrefix(rKey, keys.Meta2Prefix) {
		zc, err = zoneconfig.GetHydratedForNamedZone(ctx, p.Txn(), p.Descriptors(), zonepb.MetaZoneName)
	} else if bytes.HasPrefix(rKey, keys.SystemPrefix) {
		if bytes.HasPrefix(rKey, keys.NodeLivenessPrefix) {
			zc, err = zoneconfig.GetHydratedForNamedZone(ctx, p.Txn(), p.Descriptors(), zonepb.LivenessZoneName)
		} else if bytes.HasPrefix(rKey, keys.TimeseriesPrefix) {
			zc, err = zoneconfig.GetHydratedForNamedZone(ctx, p.Txn(), p.Descriptors(), zonepb.TimeseriesZoneName)
		} else {
			zc, err = zoneconfig.GetHydratedForNamedZone(ctx, p.Txn(), p.Descriptors(), zonepb.SystemZoneName)
		}
	} else if bytes.HasPrefix(rKey, keys.TenantPrefix) {
		zc, err = zoneconfig.GetHydratedForNamedZone(ctx, p.Txn(), p.Descriptors(), zonepb.TenantsZoneName)
	} else if key.Compare(keys.TableDataMin) >= 0 {
		// Table keyspace: decode the table ID and resolve its zone config.
		_, tableID, decErr := keys.SystemSQLCodec.DecodeTablePrefix(key)
		if decErr != nil {
			return nil, errors.Wrap(decErr, "decoding table prefix from key")
		}
		zc, err = zoneconfig.GetHydratedForTable(ctx, p.Txn(), p.Descriptors(), descpb.ID(tableID))
	} else {
		// Fallback: use RANGE DEFAULT.
		zc, err = zoneconfig.GetHydratedForNamedZone(ctx, p.Txn(), p.Descriptors(), zonepb.DefaultZoneName)
	}
	if err != nil {
		return nil, err
	}

	j, err := protoreflect.MessageToJSON(zc, protoreflect.FmtFlags{EmitDefaults: true})
	if err != nil {
		return nil, err
	}
	return tree.NewDJSON(j), nil
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
