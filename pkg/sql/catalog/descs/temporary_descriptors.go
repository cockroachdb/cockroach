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
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

type temporaryDescriptors struct {
	codec       keys.SQLCodec
	sessionData *sessiondata.SessionData
}

func makeTemporaryDescriptors(
	codec keys.SQLCodec, data *sessiondata.SessionData,
) temporaryDescriptors {
	return temporaryDescriptors{
		codec:       codec,
		sessionData: data,
	}
}

// getSchemaByName assumes that the schema name carries the `pg_temp` prefix.
// It will exhaustively search for the schema, first checking the local session
// data and then consulting the namespace table to discover if this schema
// exists as a part of another session.
//
// TODO(ajwerner): Understand and rationalize the namespace lookup given the
// schema lookup by ID path only returns descriptors owned by this session.
// TODO(ajwerner):
func (td *temporaryDescriptors) getSchemaByName(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string,
) (refuseFurtherLookup bool, _ catalog.SchemaDescriptor, _ error) {
	// If a temp schema is requested, check if it's for the current session, or
	// else fall back to reading from the store.
	if td.sessionData != nil {
		if schemaName == catconstants.PgTempSchemaName ||
			schemaName == td.sessionData.SearchPath.GetTemporarySchemaName() {
			schemaID, found := td.sessionData.GetTemporarySchemaIDForDb(uint32(dbID))
			if found {
				return true, schemadesc.NewTemporarySchema(
					td.sessionData.SearchPath.GetTemporarySchemaName(),
					descpb.ID(schemaID),
					dbID,
				), nil
			}
		}
	}
	exists, schemaID, err := catalogkv.ResolveSchemaID(ctx, txn, td.codec, dbID, schemaName)
	if !exists || err != nil {
		return true, nil, err
	}
	return true, schemadesc.NewTemporarySchema(
		schemaName,
		schemaID,
		dbID,
	), nil
}

// getSchemaByID returns the schema descriptor if it is temporary and belongs
// to the current session.
func (td *temporaryDescriptors) getSchemaByID(
	ctx context.Context, schemaID descpb.ID,
) catalog.SchemaDescriptor {
	if td.sessionData == nil {
		return nil
	}
	if dbID, exists := td.sessionData.MaybeGetDatabaseForTemporarySchemaID(
		uint32(schemaID),
	); exists {
		return schemadesc.NewTemporarySchema(
			td.sessionData.SearchPath.GetTemporarySchemaName(),
			schemaID,
			descpb.ID(dbID),
		)
	}
	return nil
}
