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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
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
	codec keys.SQLCodec
	tsp   TemporarySchemaProvider
}

func makeTemporaryDescriptors(
	codec keys.SQLCodec, temporarySchemaProvider TemporarySchemaProvider,
) temporaryDescriptors {
	return temporaryDescriptors{
		codec: codec,
		tsp:   temporarySchemaProvider,
	}
}

// TemporarySchemaProvider is an interface that provides temporary schema
// details on the current session.
type TemporarySchemaProvider interface {
	GetTemporarySchemaName() string
	GetTemporarySchemaIDForDB(descpb.ID) (descpb.ID, bool)
	MaybeGetDatabaseForTemporarySchemaID(descpb.ID) (descpb.ID, bool)
}

type temporarySchemaProviderImpl sessiondata.Stack

var _ TemporarySchemaProvider = (*temporarySchemaProviderImpl)(nil)

// NewTemporarySchemaProvider creates a TemporarySchemaProvider.
func NewTemporarySchemaProvider(sds *sessiondata.Stack) TemporarySchemaProvider {
	return (*temporarySchemaProviderImpl)(sds)
}

// GetTemporarySchemaName implements the TemporarySchemaProvider interface.
func (impl *temporarySchemaProviderImpl) GetTemporarySchemaName() string {
	return (*sessiondata.Stack)(impl).Top().SearchPath.GetTemporarySchemaName()
}

// GetTemporarySchemaIDForDB implements the TemporarySchemaProvider interface.
func (impl *temporarySchemaProviderImpl) GetTemporarySchemaIDForDB(id descpb.ID) (descpb.ID, bool) {
	ret, found := (*sessiondata.Stack)(impl).Top().GetTemporarySchemaIDForDB(uint32(id))
	return descpb.ID(ret), found
}

// MaybeGetDatabaseForTemporarySchemaID implements the TemporarySchemaProvider interface.
func (impl *temporarySchemaProviderImpl) MaybeGetDatabaseForTemporarySchemaID(
	id descpb.ID,
) (descpb.ID, bool) {
	ret, found := (*sessiondata.Stack)(impl).Top().MaybeGetDatabaseForTemporarySchemaID(uint32(id))
	return descpb.ID(ret), found
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
	ctx context.Context,
	txn *kv.Txn,
	dbID descpb.ID,
	schemaName string,
	version clusterversion.Handle,
) (refuseFurtherLookup bool, _ catalog.SchemaDescriptor, _ error) {
	// If a temp schema is requested, check if it's for the current session, or
	// else fall back to reading from the store.
	if tsp := td.tsp; tsp != nil {
		if schemaName == catconstants.PgTempSchemaName ||
			schemaName == tsp.GetTemporarySchemaName() {
			schemaID, found := tsp.GetTemporarySchemaIDForDB(dbID)
			if found {
				return true, schemadesc.NewTemporarySchema(
					tsp.GetTemporarySchemaName(),
					schemaID,
					dbID,
				), nil
			}
		}
	}
	exists, schemaID, err := catalogkv.ResolveSchemaID(
		ctx, txn, td.codec, dbID, schemaName, version,
	)
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
	tsp := td.tsp
	if tsp == nil {
		return nil
	}
	if dbID, exists := tsp.MaybeGetDatabaseForTemporarySchemaID(schemaID); exists {
		return schemadesc.NewTemporarySchema(
			tsp.GetTemporarySchemaName(),
			schemaID,
			dbID,
		)
	}
	return nil
}
