// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package rgkvaccessor implements the host-side writer that persists
// resource group configuration changes received from per-tenant
// reconcilers into system.tenant_resource_groups.
package rgkvaccessor

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// Writer applies a batch of resource group upserts and deletes for a
// single tenant to system.tenant_resource_groups.
type Writer interface {
	// Apply transactionally writes the given upserts and deletes for
	// the given tenant. Upserts overwrite any existing row at the same
	// (tenant_id, id); deletes remove the row if present. Both are
	// idempotent. An empty call (no upserts, no deletes) is a no-op
	// and returns nil without contacting KV.
	Apply(
		ctx context.Context,
		tenantID roachpb.TenantID,
		upserts []*rgpb.ResourceGroupUpsert,
		deletes []*rgpb.ResourceGroupDelete,
	) error
}

// NewWriter constructs a Writer that persists rows via the supplied
// internal SQL DB.
func NewWriter(db isql.DB) Writer {
	return &writer{db: db}
}

type writer struct {
	db isql.DB
}

const upsertStmt = `
UPSERT INTO system.tenant_resource_groups (tenant_id, id, name, config) VALUES ($1, $2, $3, $4)`

const deleteStmt = `
DELETE FROM system.tenant_resource_groups WHERE tenant_id = $1 AND id = $2`

// Apply implements the Writer interface.
func (w *writer) Apply(
	ctx context.Context,
	tenantID roachpb.TenantID,
	upserts []*rgpb.ResourceGroupUpsert,
	deletes []*rgpb.ResourceGroupDelete,
) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}
	tid := int64(tenantID.ToUint64())
	return w.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		for _, u := range upserts {
			cfgBytes, err := protoutil.Marshal(&u.Config)
			if err != nil {
				return errors.Wrap(err, "marshal resource group config")
			}
			if _, err := txn.ExecEx(
				ctx, "rg-writer-upsert", txn.KV(),
				sessiondata.InternalExecutorOverride{User: username.NodeUserName()},
				upsertStmt, tid, u.Id, u.Name, cfgBytes,
			); err != nil {
				return errors.Wrapf(err, "upsert resource group (tenant=%d id=%d)", tid, u.Id)
			}
		}
		for _, d := range deletes {
			if _, err := txn.ExecEx(
				ctx, "rg-writer-delete", txn.KV(),
				sessiondata.InternalExecutorOverride{User: username.NodeUserName()},
				deleteStmt, tid, d.Id,
			); err != nil {
				return errors.Wrapf(err, "delete resource group (tenant=%d id=%d)", tid, d.Id)
			}
		}
		return nil
	})
}
