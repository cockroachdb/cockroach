// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

type temporaryDescriptors struct {
	sds *sessiondata.Stack
}

func (tc *Collection) temporary() temporaryDescriptors {
	return temporaryDescriptors{
		sds: tc.sds,
	}
}

// getTemporarySchemaName delegates to GetTemporarySchemaName for the top of the
// session data stack.
func (td temporaryDescriptors) getTemporarySchemaName() string {
	if td.sds == nil {
		return ""
	}
	return td.sds.Top().SearchPath.GetTemporarySchemaName()
}

// getTemporarySchemaIDForDB delegates to GetTemporarySchemaIDForDB for the top
// of the session data stack.
func (td temporaryDescriptors) getTemporarySchemaIDForDB(id descpb.ID) (descpb.ID, bool) {
	if td.sds == nil {
		return descpb.InvalidID, false
	}
	ret, found := td.sds.Top().GetTemporarySchemaIDForDB(uint32(id))
	return descpb.ID(ret), found
}

// maybeGetDatabaseForTemporarySchemaID deletages to
// MaybeGetDatabaseForTemporarySchemaID for the top of the session data stack.
func (td temporaryDescriptors) maybeGetDatabaseForTemporarySchemaID(
	id descpb.ID,
) (descpb.ID, bool) {
	if td.sds == nil {
		return descpb.InvalidID, false
	}
	ret, found := td.sds.Top().MaybeGetDatabaseForTemporarySchemaID(uint32(id))
	return descpb.ID(ret), found
}

// getSchemaByName assumes that the schema name carries the `pg_temp` prefix.
// It will exhaustively search for the schema, first checking the local session
// data and then consulting the namespace table to discover if this schema
// exists as a part of another session.
// If it did not find a schema, it also returns a boolean flag indicating
// whether the search is known to have been exhaustive or not.
func (td temporaryDescriptors) getSchemaByName(
	dbID descpb.ID, schemaName string,
) (avoidFurtherLookups bool, _ catalog.SchemaDescriptor) {
	// If a temp schema is requested, check if it's for the current session, or
	// else fall back to reading from the store.
	if td.sds == nil {
		return false, nil
	}
	if schemaName != catconstants.PgTempSchemaName && schemaName != td.getTemporarySchemaName() {
		return false, nil
	}
	schemaID, found := td.getTemporarySchemaIDForDB(dbID)
	if !found {
		return true, nil
	}
	return true, schemadesc.NewTemporarySchema(
		td.getTemporarySchemaName(),
		schemaID,
		dbID,
	)
}

// getSchemaByID returns the schema descriptor if it is temporary and belongs
// to the current session.
func (td temporaryDescriptors) getSchemaByID(schemaID descpb.ID) catalog.SchemaDescriptor {
	if dbID, exists := td.maybeGetDatabaseForTemporarySchemaID(schemaID); exists {
		return schemadesc.NewTemporarySchema(
			td.getTemporarySchemaName(),
			schemaID,
			dbID,
		)
	}
	return nil
}
