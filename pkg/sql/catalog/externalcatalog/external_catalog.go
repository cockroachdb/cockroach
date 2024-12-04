// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package externalcatalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/externalcatalog/externalpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/ingesting"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// ExtractExternalCatalog extracts the table descriptors via the schema
// resolver.
func ExtractExternalCatalog(
	ctx context.Context, schemaResolver resolver.SchemaResolver, tableNames ...string,
) (externalpb.ExternalCatalog, error) {
	externalCatalog := externalpb.ExternalCatalog{}

	for _, name := range tableNames {
		uon, err := parser.ParseTableName(name)
		if err != nil {
			return externalpb.ExternalCatalog{}, err
		}
		tn := uon.ToTableName()
		_, td, err := resolver.ResolveMutableExistingTableObject(ctx, schemaResolver, &tn, true, tree.ResolveRequireTableDesc)
		if err != nil {
			return externalpb.ExternalCatalog{}, err
		}

		externalCatalog.Tables = append(externalCatalog.Tables, td.TableDescriptor)
	}
	return externalCatalog, nil
}

// IngestExternalCatalog ingests the tables in the external catalog into into
// the database and schema.
//
// TODO: provide a list of databaseID/schemaID pairs to ingest into.
func IngestExternalCatalog(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	externalCatalog externalpb.ExternalCatalog,
	txn isql.Txn,
	descsCol *descs.Collection,
	databaseID descpb.ID,
	schemaID descpb.ID,
	setOffline bool,
) error {
	dbDesc, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, databaseID)
	if err != nil {
		return err
	}
	tablesToWrite := make([]catalog.TableDescriptor, 0, len(externalCatalog.Tables))
	var originalParentID descpb.ID
	for _, table := range externalCatalog.Tables {
		if originalParentID == 0 {
			originalParentID = table.ParentID
		} else if originalParentID != table.ParentID {
			return errors.New("all tables must belong to the same parent")
		}
		// TODO: rewrite the tables to fresh ids.
		mutTable := tabledesc.NewBuilder(&table).BuildCreatedMutableTable()
		if setOffline {
			// TODO: Add some functional ops so client can set offline msg, among
			// other things.
			mutTable.SetOffline("")
		}
		mutTable.UnexposedParentSchemaID = schemaID
		mutTable.ParentID = dbDesc.GetID()
		mutTable.Version = 1
		tablesToWrite = append(tablesToWrite, mutTable)
	}
	return ingesting.WriteDescriptors(
		ctx, txn.KV(), user, descsCol, nil, nil, tablesToWrite, nil, nil,
		tree.RequestedDescriptors, nil /* extra */, "", true,
	)
}
