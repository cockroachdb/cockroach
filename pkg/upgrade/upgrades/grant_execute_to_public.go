// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// grantExecuteToPublicOnAllFunctions grants the EXECUTE privilege to the public
// role for all functions.
func grantExecuteToPublicOnAllFunctions(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	grantExecuteToFuncs := func(
		ctx context.Context, d upgrade.TenantDeps, toUpgrade []descpb.ID,
	) error {
		return d.DB.DescsTxn(ctx, func(
			ctx context.Context, txn descs.Txn,
		) error {
			descs, err := txn.Descriptors().MutableByID(txn.KV()).Descs(ctx, toUpgrade)
			if err != nil {
				return err
			}
			batch := txn.KV().NewBatch()
			for _, desc := range descs {
				if desc.DescriptorType() != catalog.Function {
					continue
				}
				desc.GetPrivileges().Grant(
					username.PublicRoleName(),
					privilege.List{privilege.EXECUTE},
					false, /* withGrantOption */
				)
				if err := txn.Descriptors().WriteDescToBatch(
					ctx, false /* kvTrace */, desc, batch,
				); err != nil {
					return err
				}
			}
			return txn.KV().Run(ctx, batch)
		})
	}

	query := `SELECT id, length(descriptor) FROM system.descriptor ORDER BY id DESC`
	rows, err := d.DB.Executor().QueryIterator(
		ctx, "retrieve-descriptors-for-function-execute-privilege", nil /* txn */, query,
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var toUpgrade []descpb.ID
	var curBatchBytes int
	const maxBatchSize = 1 << 19 // 512 KiB
	ok, err := rows.Next(ctx)
	for ; ok && err == nil; ok, err = rows.Next(ctx) {
		datums := rows.Cur()
		id := tree.MustBeDInt(datums[0])
		size := tree.MustBeDInt(datums[1])
		if curBatchBytes+int(size) > maxBatchSize && curBatchBytes > 0 {
			if err := grantExecuteToFuncs(ctx, d, toUpgrade); err != nil {
				return err
			}
			toUpgrade = toUpgrade[:0]
		}
		curBatchBytes += int(size)
		toUpgrade = append(toUpgrade, descpb.ID(id))
	}
	if err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(toUpgrade) == 0 {
		return nil
	}
	return grantExecuteToFuncs(ctx, d, toUpgrade)
}
