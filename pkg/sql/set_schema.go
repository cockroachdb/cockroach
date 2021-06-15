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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// prepareSetSchema verifies that a table/type can be set to the desired
// schema and returns the schema id of the desired schema.
func (p *planner) prepareSetSchema(
	ctx context.Context, db catalog.DatabaseDescriptor, desc catalog.MutableDescriptor, schema string,
) (descpb.ID, error) {

	var objectName tree.ObjectName
	switch t := desc.(type) {
	case *tabledesc.Mutable:
		objectName = tree.NewUnqualifiedTableName(tree.Name(desc.GetName()))
	case *typedesc.Mutable:
		objectName = tree.NewUnqualifiedTypeName(desc.GetName())
	default:
		return 0, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"no table or type was found for SET SCHEMA command, found %T", t)
	}

	// Lookup the schema we want to set to.
	res, err := p.Descriptors().GetMutableSchemaByName(
		ctx, p.txn, db, schema, tree.SchemaLookupFlags{
			Required:       true,
			RequireMutable: true,
		})
	if err != nil {
		return 0, err
	}

	switch res.SchemaKind() {
	case catalog.SchemaTemporary:
		return 0, pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot move objects into or out of temporary schemas")
	case catalog.SchemaVirtual:
		return 0, pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot move objects into or out of virtual schemas")
	case catalog.SchemaPublic:
		// We do not need to check for privileges on the public schema.
	default:
		// The user needs CREATE privilege on the target schema to move an object
		// to the schema.
		err = p.CheckPrivilege(ctx, res, privilege.CREATE)
		if err != nil {
			return 0, err
		}
	}

	desiredSchemaID := res.GetID()

	// If the schema being changed to is the same as the current schema a no-op
	// will happen so we don't have to check if there is an object in the schema
	// with the same name.
	if desiredSchemaID == desc.GetParentSchemaID() {
		return desiredSchemaID, nil
	}

	err = catalogkv.CheckObjectCollision(ctx, p.txn, p.ExecCfg().Codec, db.GetID(), desiredSchemaID, objectName)
	if err != nil {
		return descpb.InvalidID, err
	}

	return desiredSchemaID, nil
}
