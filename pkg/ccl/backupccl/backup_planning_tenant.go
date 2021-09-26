// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const tenantMetadataQuery = `SELECT id, active, info FROM system.tenants`

func tenantMetadataFromRow(row tree.Datums) (descpb.TenantInfo, error) {
	id := uint64(tree.MustBeDInt(row[0]))
	info := descpb.TenantInfo{ID: id}
	infoBytes := []byte(tree.MustBeDBytes(row[2]))
	if err := protoutil.Unmarshal(infoBytes, &info); err != nil {
		return descpb.TenantInfo{}, err
	}
	return info, nil
}

func retrieveSingleTenantMetadata(
	ctx context.Context, ie *sql.InternalExecutor, txn *kv.Txn, tenantID roachpb.TenantID,
) (descpb.TenantInfo, error) {
	row, err := ie.QueryRow(
		ctx, "backup-lookup-tenant", txn,
		tenantMetadataQuery+` WHERE id = $1`, tenantID.ToUint64(),
	)
	if err != nil {
		return descpb.TenantInfo{}, err
	}
	if row == nil {
		return descpb.TenantInfo{}, errors.Errorf("tenant %s does not exist", tenantID)
	}
	if !tree.MustBeDBool(row[1]) {
		return descpb.TenantInfo{}, errors.Errorf("tenant %s is not active", tenantID)
	}

	return tenantMetadataFromRow(row)
}

func retrieveAllTenantsMetadata(
	ctx context.Context, ie *sql.InternalExecutor, txn *kv.Txn,
) ([]descpb.TenantInfo, error) {
	rows, err := ie.QueryBuffered(
		ctx, "backup-lookup-tenants", txn,
		// XXX Should we add a `WHERE active`? We require the tenant to be active
		// when it is specified..
		tenantMetadataQuery,
	)
	if err != nil {
		return nil, err
	}
	res := make([]descpb.TenantInfo, len(rows))
	for i := range rows {
		res[i], err = tenantMetadataFromRow(rows[i])
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}
