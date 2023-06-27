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
// This function round-trips the descriptor protobufs and updates in place
// protobufs in storage which end up being changed.
//
// TODO(postamar): remove dangling back-references in descriptors (#85265)
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
func FirstUpgradeFromReleasePrecondition(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	const q = `SELECT count(*) FROM "".crdb_internal.invalid_objects AS OF SYSTEM TIME '-10s'`
	row, err := d.InternalExecutor.QueryRow(ctx, "query-invalid-objects", nil /* txn */, q)
	if err != nil {
		return err
	}
	if n := row[0].String(); n != "0" {
		return errors.Errorf("%s row(s) found in \"\".crdb_internal.invalid_objects, expected none", n)
	}
	return nil
}
