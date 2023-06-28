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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

// FirstUpgradeFromRelease implements the tenant upgrade step for all
// V[0-9]+_[0-9]+Start cluster versions, which is every first internal version
// following each major release version.
//
// The purpose of this upgrade function is to keep the data in the system
// database up to date to mitigate the impact of metadata corruptions
// caused by bugs in earlier versions.
//
// As of the time of this writing, this implementation covers only the
// system.descriptor table, but in the future this may grow to
// include other system tables as well.
func FirstUpgradeFromRelease(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	var all nstree.Catalog
	if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) (err error) {
		all, err = txn.Descriptors().GetAll(ctx, txn.KV())
		return err
	}); err != nil {
		return err
	}
	var batch catalog.DescriptorIDSet
	const batchSize = 1000
	if err := all.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if !desc.GetPostDeserializationChanges().HasChanges() {
			return nil
		}
		batch.Add(desc.GetID())
		if batch.Len() >= batchSize {
			if err := upgradeDescriptors(ctx, d, batch); err != nil {
				return err
			}
			batch = catalog.MakeDescriptorIDSet()
		}
		return nil
	}); err != nil {
		return err
	}
	return upgradeDescriptors(ctx, d, batch)
}

// upgradeDescriptors round-trips the descriptor protobufs for the given keys
// and updates them in place if they've changed.
func upgradeDescriptors(
	ctx context.Context, d upgrade.TenantDeps, ids catalog.DescriptorIDSet,
) error {
	if ids.Empty() {
		return nil
	}
	return d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		muts, err := txn.Descriptors().MutableByID(txn.KV()).Descs(ctx, ids.Ordered())
		if err != nil {
			return err
		}
		b := txn.KV().NewBatch()
		for _, mut := range muts {
			if !mut.GetPostDeserializationChanges().HasChanges() {
				continue
			}
			key := catalogkeys.MakeDescMetadataKey(d.Codec, mut.GetID())
			b.CPut(key, mut.DescriptorProto(), mut.GetRawBytesInStorage())
		}
		return txn.KV().Run(ctx, b)
	})
}

// FirstUpgradeFromReleasePrecondition is the precondition check for upgrading
// from any supported major release.
//
// This precondition function performs health checks on the catalog metadata.
// This prevents cluster version upgrades from proceeding when it detects
// known corruptions. This forces cluster operations to repair the metadata,
// which may be somewhat annoying, but it's considered a lesser evil to being
// stuck in a mixed-version state due to a later upgrade step throwing
// errors.
//
// The implementation of this function may evolve in the future to include
// other checks beyond querying the invalid_objects virtual table, which
// mainly checks descriptor validity, as that has historically been the main
// source of problems.
func FirstUpgradeFromReleasePrecondition(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	const q = `SELECT count(*) FROM "".crdb_internal.invalid_objects AS OF SYSTEM TIME '-10s'`
	row, err := d.InternalExecutor.QueryRow(ctx, "query-invalid-objects", nil /* txn */, q)
	if err != nil {
		return err
	}
	if n := row[0].String(); n != "0" {
		return errors.AssertionFailedf("%s row(s) found in \"\".crdb_internal.invalid_objects, expected none", n)
	}
	return nil
}
