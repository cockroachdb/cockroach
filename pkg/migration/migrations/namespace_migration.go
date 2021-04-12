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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func namespaceMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	// The migration runs in batches. For each batch, we need to materialize all
	// the namespace rows in memory before updating the entries.
	const batchSize = 1000
	type entry struct {
		parentID descpb.ID
		name     string
		id       descpb.ID
	}
	entries := make([]entry, 0, batchSize)

	fetchEntriesForBatch := func(ctx context.Context, txn *kv.Txn) (workLeft bool, _ error) {
		entries = entries[:0]
		// Fetch all entries that are not present in the new namespace table. Each
		// of these entries will be copied to the new table.
		//
		// Note that we are very careful to always delete from both namespace tables
		// in 20.1, so there's no possibility that we'll be overwriting a deleted
		// table that existed in the old table and the new table but was deleted
		// from only the new table.
		q := fmt.Sprintf(
			`SELECT "parentID", name, id FROM [%d AS namespace_deprecated]
              WHERE id NOT IN (SELECT id FROM [%d AS namespace]) LIMIT %d`,
			systemschema.DeprecatedNamespaceTable.GetID(), systemschema.NamespaceTable.GetID(), batchSize+1)
		it, err := d.InternalExecutor.QueryIteratorEx(
			ctx, "read-deprecated-namespace-table", txn,
			sessiondata.InternalExecutorOverride{
				User: security.RootUserName(),
			},
			q)
		if err != nil {
			return workLeft, err
		}
		defer func() { _ = it.Close() }()
		var ok bool
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			workLeft = false
			// We found some rows from the query, which means that we can't quit
			// just yet.
			if len(entries) >= batchSize {
				workLeft = true
				// Just process 1000 rows at a time.
				break
			}
			row := it.Cur()
			entries = append(entries, entry{
				parentID: descpb.ID(tree.MustBeDInt(row[0])),
				name:     string(tree.MustBeDString(row[1])),
				id:       descpb.ID(tree.MustBeDInt(row[2])),
			})
		}
		return workLeft, err
	}

	// Loop until there's no more work to be done.
	workLeft := true
	for workLeft {
		if err := d.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			var err error
			workLeft, err = fetchEntriesForBatch(ctx, txn)
			if err != nil {
				return err
			}

			log.Infof(ctx, "migrating system.namespace chunk with %d rows", len(entries))
			for i := range entries {
				e := &entries[i]
				if e.parentID == keys.RootNamespaceID {
					// This row represents a database. Add it to the new namespace table.
					databaseKey := catalogkeys.NewDatabaseKey(e.name)
					if err := txn.Put(ctx, databaseKey.Key(d.Codec), e.id); err != nil {
						return err
					}
					// Also create a 'public' schema for this database.
					schemaKey := catalogkeys.NewSchemaKey(e.id, "public")
					log.VEventf(ctx, 2, "migrating system.namespace entry for database %s", e.name)
					if err := txn.Put(ctx, schemaKey.Key(d.Codec), keys.PublicSchemaID); err != nil {
						return err
					}
				} else {
					// This row represents a table. Add it to the new namespace table with the
					// schema set to 'public'.
					if e.id == keys.DeprecatedNamespaceTableID {
						// The namespace table itself was already handled in
						// createNewSystemNamespaceDescriptor. Do not overwrite it with the
						// deprecated ID.
						continue
					}
					tableKey := catalogkeys.NewTableKey(e.parentID, keys.PublicSchemaID, e.name)
					log.VEventf(ctx, 2, "migrating system.namespace entry for table %s", e.name)
					if err := txn.Put(ctx, tableKey.Key(d.Codec), e.id); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
