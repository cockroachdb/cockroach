// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// PrepareSetSchema verifies that a table/type can be set to the desired
// schema and returns the schema id of the desired schema.
func PrepareSetSchema(
	params runParams, desc catalog.MutableDescriptor, schema string,
) (sqlbase.ID, error) {
	ctx := params.ctx
	p := params.p

	databaseID := desc.GetParentID()
	schemaID := desc.GetParentSchemaID()

	// Lookup the the schema we want to set to.
	exists, res, err := params.p.LogicalSchemaAccessor().GetSchema(
		ctx, p.txn,
		p.ExecCfg().Codec,
		databaseID,

		schema,
	)
	if err != nil {
		return 0, err
	}

	if !exists {
		return 0, pgerror.Newf(pgcode.InvalidSchemaName,
			"schema %s does not exist", schema)
	}

	switch res.Kind {
	case sqlbase.SchemaTemporary:
		return 0, pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot move objects into or out of temporary schemas")
	case sqlbase.SchemaVirtual:
		return 0, pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot move objects into or out of virtual schemas")
	case sqlbase.SchemaPublic:
		// We do not need to check for privileges on the public schema.
	default:
		// The user needs CREATE privilege on the target schema to move an object
		// to the schema.
		err = p.CheckPrivilege(ctx, res.Desc, privilege.CREATE)
		if err != nil {
			return 0, err
		}
	}

	desiredSchemaID := res.ID

	// If the schema being changed to is the same as the current schema a no-op
	// will happen so we don't have to check if there is an object in the schema
	// with the same name.
	if desiredSchemaID == schemaID {
		return desiredSchemaID, nil
	}

	exists, _, err = sqlbase.LookupObjectID(
		ctx, p.txn, p.ExecCfg().Codec, databaseID, desiredSchemaID, desc.GetName(),
	)
	if err == nil && exists {
		if desc.TableDesc() != nil {
			return 0, pgerror.Newf(pgcode.DuplicateRelation,
				"relation %s already exists in schema %s", desc.GetName(), schema)
		} else if desc.TypeDesc() != nil {
			return 0, pgerror.Newf(pgcode.DuplicateObject,
				"type %s already exists in schema %s", desc.GetName(), schema)
		}
	}

	return desiredSchemaID, nil
}
