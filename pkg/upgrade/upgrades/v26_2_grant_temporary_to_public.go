// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// grantTemporaryToPublic grants the TEMPORARY privilege to the public role
// on all existing databases. This matches PostgreSQL behavior where TEMPORARY
// is granted to PUBLIC by default on all databases.
func grantTemporaryToPublic(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Get all database descriptors.
	var databases nstree.Catalog
	if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) (err error) {
		databases, err = txn.Descriptors().GetAllDatabases(ctx, txn.KV())
		return err
	}); err != nil {
		return err
	}

	// Process each database.
	return databases.ForEachDescriptor(func(desc catalog.Descriptor) error {
		db := desc.(catalog.DatabaseDescriptor)
		privs := db.GetPrivileges()

		// Check if PUBLIC already has the TEMPORARY privilege.
		if pubPrivs, found := privs.FindUser(username.PublicRoleName()); found {
			if privilege.TEMPORARY.IsSetIn(pubPrivs.Privileges) {
				return nil
			}
		}

		log.Dev.Infof(ctx, "granting TEMPORARY privilege to public on database %s", db.GetName())

		return d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			mutableDB, err := txn.Descriptors().MutableByID(txn.KV()).Database(ctx, db.GetID())
			if err != nil {
				return err
			}
			mutableDB.GetPrivileges().Grant(
				username.PublicRoleName(),
				privilege.List{privilege.TEMPORARY},
				false, /* withGrantOption */
			)
			return txn.Descriptors().WriteDesc(ctx, false /* kvTrace */, mutableDB, txn.KV())
		})
	})
}
