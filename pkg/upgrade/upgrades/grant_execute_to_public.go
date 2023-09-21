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
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// grantExecuteToPublicOnAllFunctions grants the EXECUTE privilege to the public
// role for all functions.
func grantExecuteToPublicOnAllFunctions(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Collect the descriptor IDs of all function descriptors.
	var fnIDs []descpb.ID
	if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) (err error) {
		dbs, err := txn.Descriptors().GetAllDatabases(ctx, txn.KV())
		if err != nil {
			return err
		}
		return dbs.ForEachDescriptor(func(dd catalog.Descriptor) error {
			dbDesc, err := catalog.AsDatabaseDescriptor(dd)
			if err != nil {
				return err
			}
			schemas, err := txn.Descriptors().GetAllSchemasInDatabase(ctx, txn.KV(), dbDesc)
			if err != nil {
				return err
			}
			return schemas.ForEachDescriptor(func(sd catalog.Descriptor) error {
				scDesc, err := catalog.AsSchemaDescriptor(sd)
				if err != nil {
					return err
				}
				return scDesc.ForEachFunctionSignature(func(sig descpb.SchemaDescriptor_FunctionSignature) error {
					fnIDs = append(fnIDs, sig.ID)
					return nil
				})
			})
		})
	}); err != nil {
		return err
	}

	const maxBatchSize = 100
	// nextBatch returns a batch of up to maxBatchSize descriptors. The returned
	// descriptors are removed from fnDescs.
	nextBatch := func() []descpb.ID {
		batchSize := maxBatchSize
		if len(fnIDs) < batchSize {
			batchSize = len(fnIDs)
		}
		batch := fnIDs[:batchSize]
		fnIDs = fnIDs[batchSize:]
		return batch
	}

	for batch := nextBatch(); len(batch) > 0; batch = nextBatch() {
		// Grant the EXECUTE privilege to each descriptor in the batch.
		err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			descs, err := txn.Descriptors().MutableByID(txn.KV()).Descs(ctx, batch)
			if err != nil {
				return err
			}
			kvBatch := txn.KV().NewBatch()
			for _, desc := range descs {
				desc.GetPrivileges().Grant(
					username.PublicRoleName(),
					privilege.List{privilege.EXECUTE},
					false, /* withGrantOption */
				)
				if err := txn.Descriptors().WriteDescToBatch(
					ctx, false /* kvTrace */, desc, kvBatch,
				); err != nil {
					return err
				}
			}
			return txn.KV().Run(ctx, kvBatch)
		})
		if err != nil {
			return err
		}
	}

	return nil
}
