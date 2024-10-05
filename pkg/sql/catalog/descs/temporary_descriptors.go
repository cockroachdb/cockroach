// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
)

// getTemporarySchemaByName assumes that the schema name carries the `pg_temp`
// prefix.
// It will exhaustively search for the schema, first checking the local session
// data and then consulting the namespace table to discover if this schema
// exists as a part of another session.
// If it did not find a schema, it also returns a boolean flag indicating
// whether the search is known to have been exhaustive or not.
func (tc *Collection) getTemporarySchemaByName(
	dbID descpb.ID, schemaName string,
) (avoidFurtherLookups bool, _ catalog.SchemaDescriptor) {
	// If a temp schema is requested, check if it's for the current session, or
	// else fall back to reading from the store.
	if !tc.temporarySchemaProvider.HasTemporarySchema() {
		return false, nil
	}
	tempSchemaName := tc.temporarySchemaProvider.GetTemporarySchemaName()
	if schemaName != catconstants.PgTempSchemaName && schemaName != tempSchemaName {
		return false, nil
	}
	schemaID := tc.temporarySchemaProvider.GetTemporarySchemaIDForDB(dbID)
	if schemaID == descpb.InvalidID {
		return true, nil
	}
	return true, schemadesc.NewTemporarySchema(
		tempSchemaName,
		schemaID,
		dbID,
	)
}

// getTemporarySchemaByID returns the schema descriptor if it is temporary and
// belongs to the current session.
func (tc *Collection) getTemporarySchemaByID(schemaID descpb.ID) catalog.SchemaDescriptor {
	dbID := tc.temporarySchemaProvider.MaybeGetDatabaseForTemporarySchemaID(schemaID)
	if dbID == descpb.InvalidID {
		return nil
	}
	return schemadesc.NewTemporarySchema(
		tc.temporarySchemaProvider.GetTemporarySchemaName(),
		schemaID,
		dbID,
	)
}
