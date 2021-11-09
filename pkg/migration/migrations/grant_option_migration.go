// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func maybeAddGrantOptions(b catalog.DescriptorBuilder) {
	switch b.DescriptorType() {
	case catalog.Database:
		dbdesc.MaybeAddGrantOptionsDatabase(b.(dbdesc.DatabaseDescriptorBuilder))
	case catalog.Schema:
		schemadesc.MaybeAddGrantOptionsSchema(b.(schemadesc.SchemaDescriptorBuilder))
	case catalog.Table:
		tabledesc.MaybeAddGrantOptionsTable(b.(tabledesc.TableDescriptorBuilder))
	case catalog.Type:
		typedesc.MaybeAddGrantOptionsType(b.(typedesc.TypeDescriptorBuilder))
	}
}

// grantOptionMigration iterates through every descriptor and sets a user's grant option bits
// equal to its privilege bits if it holds the "GRANT" privilege.
func grantOptionMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	query := `SELECT id, descriptor, crdb_internal_mvcc_timestamp FROM system.descriptor ORDER BY ID ASC`
	rows, err := d.InternalExecutor.QueryIterator(
		ctx, "retrieve-grant-options", nil /* txn */, query,
	)
	if err != nil {
		return err
	}

	addGrantOptionFunc := func(ids []descpb.ID, descs []descpb.Descriptor, timestamps []hlc.Timestamp) error {
		var modifiedDescs []catalog.MutableDescriptor
		for i, id := range ids {
			b := catalogkv.NewBuilderWithMVCCTimestamp(&descs[i], timestamps[i])
			if b == nil {
				return errors.Newf("unable to find descriptor for id %d", id)
			}

			err := b.RunMigrationOnlyChanges(maybeAddGrantOptions)
			if err != nil {
				return err
			}
			mutableDesc := b.BuildExistingMutable()

			modifiedDescs = append(modifiedDescs, mutableDesc)
		}
		if err := writeModifiedDescriptors(ctx, d, modifiedDescs); err != nil {
			return err
		}
		return nil
	}

	return addGrantOptionMigration(ctx, rows, addGrantOptionFunc, 1<<19 /* 512 KiB batch size */)
}

// addGrantOptionFunction is used in addGrantOptionMigration to maybe add grant options
// of descriptors specified by the id.
type addGrantOptionFunction func(ids []descpb.ID, descs []descpb.Descriptor, timestamps []hlc.Timestamp) error

// addGrantOptionMigration is an abstraction for adding grant options.
// The rows provided should be the result of a select ID, descriptor, crdb_internal_mvcc_timestamp
// from system.descriptor table.
// The datums returned from the query are parsed to grab the descpb.Descriptor
// and addGrantOptionFunction is called on the desc.
// If minBatchSizeInBytes is specified, fixDescriptors will only be called once the
// size of the descriptors in the id array surpasses minBatchSizeInBytes.
func addGrantOptionMigration(
	ctx context.Context,
	rows sqlutil.InternalRows,
	grantOptionFunc addGrantOptionFunction,
	minBatchSizeInBytes int,
) error {
	defer func() { _ = rows.Close() }()
	ok, err := rows.Next(ctx)
	if err != nil {
		return err
	}
	currSize := 0 // in bytes.
	var ids []descpb.ID
	var descs []descpb.Descriptor
	var timestamps []hlc.Timestamp
	for ; ok; ok, err = rows.Next(ctx) {
		if err != nil {
			return err
		}
		datums := rows.Cur()
		id, desc, ts, err := unmarshalDescFromDescriptorRow(datums)
		if err != nil {
			return err
		}
		ids = append(ids, id)
		descs = append(descs, desc)
		timestamps = append(timestamps, ts)
		currSize += desc.Size()
		if currSize > minBatchSizeInBytes || minBatchSizeInBytes == 0 {
			err = grantOptionFunc(ids, descs, timestamps)
			if err != nil {
				return err
			}
			// Reset size and id array after the batch is fixed.
			currSize = 0
			ids = nil
			descs = nil
			timestamps = nil
		}
	}
	// Fix remaining descriptors.
	return grantOptionFunc(ids, descs, timestamps)
}

// writeModifiedDescriptors writes the descriptors that we have given grant option privileges
// to back to batch
func writeModifiedDescriptors(
	ctx context.Context, d migration.TenantDeps, modifiedDescs []catalog.MutableDescriptor,
) error {
	return d.CollectionFactory.Txn(ctx, d.InternalExecutor, d.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		batch := txn.NewBatch()
		for _, desc := range modifiedDescs {
			err := catalogkv.WriteDescToBatch(ctx, false, d.Settings, batch, d.Codec, desc.GetID(), desc)
			if err != nil {
				return err
			}
		}
		return txn.Run(ctx, batch)
	})
}
