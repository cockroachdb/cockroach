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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// databaseWithPublicConnectPrivilegeMigration adds the CONNECT privilege to the public
// role on each database.
func databaseWithPublicConnectPrivilegeMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	coll := d.CollectionFactory.NewCollection(nil /* temporarySchemaProvider */)
	var dbDescs []catalog.DatabaseDescriptor
	if err := d.DB.Txn(
		ctx,
		func(ctx context.Context, txn *kv.Txn) error {
			var err error
			dbDescs, err = coll.GetAllDatabaseDescriptors(ctx, txn)
			return err
		},
	); err != nil {
		return err
	}

	// Write each database descriptor one at a time to prevent a lease over every
	// single database descriptor. It does not have to be atomic.
	for _, db := range dbDescs {
		if err := d.DB.Txn(
			ctx,
			func(ctx context.Context, txn *kv.Txn) error {
				mutDesc, err := coll.GetMutableDescriptorByID(ctx, db.GetID(), txn)
				if err != nil {
					return err
				}
				mutDesc.GetPrivileges().Grant(security.PublicRoleName(), privilege.List{privilege.CONNECT})
				return coll.WriteDesc(
					ctx,
					false, /* kvTrace */
					mutDesc,
					txn,
				)
			},
		); err != nil {
			return err
		}
	}
	return nil
}
