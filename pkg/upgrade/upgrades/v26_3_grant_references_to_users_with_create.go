// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// grantReferencesToUsersWithCreate grants the REFERENCES privilege to all
// users/roles that currently hold CREATE on any table. This ensures backwards
// compatibility after FK creation switches from requiring CREATE to requiring
// REFERENCES on both the origin and referenced tables.
func grantReferencesToUsersWithCreate(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Collect all databases.
	var databases nstree.Catalog
	if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) (err error) {
		databases, err = txn.Descriptors().GetAllDatabases(ctx, txn.KV())
		return err
	}); err != nil {
		return err
	}

	// Process each database separately to avoid loading every descriptor at once.
	return databases.ForEachDescriptor(func(desc catalog.Descriptor) error {
		db := desc.(catalog.DatabaseDescriptor)

		// Get all tables in this database.
		var tables nstree.Catalog
		if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) (err error) {
			tables, err = txn.Descriptors().GetAllTablesInDatabase(ctx, txn.KV(), db)
			return err
		}); err != nil {
			return err
		}

		// Process each table that needs updating in its own transaction to
		// minimize contention with concurrent DDL operations.
		return tables.ForEachDescriptor(func(desc catalog.Descriptor) error {
			tbl := desc.(catalog.TableDescriptor)
			privs := tbl.GetPrivileges()

			// Quick check: does any user need REFERENCES granted?
			needsUpdate := false
			for _, u := range privs.Users {
				if privilege.CREATE.IsSetIn(u.Privileges) && !privilege.REFERENCES.IsSetIn(u.Privileges) {
					needsUpdate = true
					break
				}
			}
			if !needsUpdate {
				return nil
			}

			return d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
				mutableTbl, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, tbl.GetID())
				if err != nil {
					return err
				}
				for _, u := range mutableTbl.GetPrivileges().Users {
					if privilege.CREATE.IsSetIn(u.Privileges) && !privilege.REFERENCES.IsSetIn(u.Privileges) {
						log.Dev.Infof(ctx, "granting REFERENCES on table %s (%d) to %s",
							tbl.GetName(), tbl.GetID(), u.User())
						mutableTbl.GetPrivileges().Grant(
							u.User(),
							privilege.List{privilege.REFERENCES},
							privilege.CREATE.IsSetIn(u.WithGrantOption),
						)
					}
				}
				return txn.Descriptors().WriteDesc(ctx, false /* kvTrace */, mutableTbl, txn.KV())
			})
		})
	})
}
