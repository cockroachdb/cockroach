// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

func CreateSchema(b BuildCtx, n *tree.CreateSchema) {
	// 1. Use current database if database unspecified.
	schemaName := getSchemaName(b, n)
	n.Schema.SchemaName = tree.Name(schemaName)
	n.Schema.ExplicitSchema = true
	if n.Schema.CatalogName == "" {
		b.ResolveDatabasePrefix(&n.Schema)
		if n.Schema.CatalogName == "" {
			// Users can't create a schema without being connected to a DB.
			panic(pgerror.New(pgcode.UndefinedDatabase,
				"cannot create schema without being connected to a database"))
		}
	}

	b.IncrementSchemaChangeCreateCounter("schema")
	sqltelemetry.IncrementUserDefinedSchemaCounter(sqltelemetry.UserDefinedSchemaCreate)
	dbElts := b.ResolveDatabase(n.Schema.CatalogName, ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
		RequireOwnership:    false,
	})
	_, _, dbElem := scpb.FindDatabase(dbElts)

	// 2. Disallow CREATEs for system tenant.
	if err := shouldRestrictAccessToSystemInterface(b,
		"DDL execution",   /* operation */
		"running the DDL", /* alternate action */
	); err != nil {
		panic(err)
	}

	// 3. Users cannot create schemas within the system database.
	if dbElem.DatabaseID == keys.SystemDatabaseID {
		panic(pgerror.New(pgcode.InvalidObjectDefinition, "cannot create schemas in "+
			"the system database"))
	}

	// 4. If schema name is already used, panic unless IF NOT EXISTS is set.
	if schemaExists(b, n.Schema) {
		if n.IfNotExists {
			return
		}
		panic(sqlerrors.NewSchemaAlreadyExistsError(schemaName))
	}

	// 5. Check validity of the schema name.
	if err := schemadesc.IsSchemaNameValid(schemaName); err != nil {
		panic(err)
	}

	// 6. Owner of the schema is either current user or an existing user specified
	// via AUTHORIZATION clause.
	owner := b.CurrentUser()
	if !n.AuthRole.Undefined() {
		authRole, err := decodeusername.FromRoleSpec(
			b.SessionData(), username.PurposeValidation, n.AuthRole,
		)
		if err != nil {
			panic(err)
		}
		// Block CREATE SCHEMA AUTHORIZATION "foo" when "foo" isn't an existing user.
		if err = b.CheckRoleExists(b, authRole); err != nil {
			panic(sqlerrors.NewUndefinedUserError(authRole))
		}
		owner = authRole
	}

	// 7. Finally, create and add constituent elements to builder state.
	schemaID := b.GenerateUniqueDescID()
	schemaElem := &scpb.Schema{
		SchemaID:    schemaID,
		IsTemporary: false,
		IsPublic:    false,
		IsVirtual:   false,
	}
	b.Add(schemaElem)
	b.Add(&scpb.Namespace{
		DatabaseID:   dbElem.DatabaseID,
		SchemaID:     0,
		DescriptorID: schemaID,
		Name:         schemaName,
	})
	b.Add(&scpb.SchemaParent{
		SchemaID:         schemaID,
		ParentDatabaseID: dbElem.DatabaseID,
	})
	ownerElem, userPrivsElems :=
		b.BuildUserPrivilegesFromDefaultPrivileges(dbElem, nil, schemaID, privilege.Schemas, owner)
	b.Add(ownerElem)
	for _, userPrivsElem := range userPrivsElems {
		b.Add(userPrivsElem)
	}
	b.LogEventForExistingTarget(schemaElem)
}

// getSchemaName gets the name of the to-be-created schema.
// If a CREATE SCHEMA statement has an AUTHORIZATION clause, but no schema name
// is specified, the schema will be named after the specified owner of the
// schema.
func getSchemaName(b BuildCtx, n *tree.CreateSchema) (schemaName string) {
	authRole, err := decodeusername.FromRoleSpec(
		b.SessionData(), username.PurposeValidation, n.AuthRole,
	)
	if err != nil {
		panic(err)
	}
	if !n.Schema.ExplicitSchema {
		schemaName = authRole.Normalized()
	} else {
		schemaName = n.Schema.Schema()
	}
	return schemaName
}

// schemaExists returns true if `schema` has already been used.
func schemaExists(b BuildCtx, schema tree.ObjectNamePrefix) bool {
	// Check statically known schemas: "public" or virtual schemas ("pg_catalog",
	// "pg_information", "crdb_internal").
	if schema.Schema() == catconstants.PublicSchemaName {
		return true
	}
	if _, isVirtualSchema := catconstants.VirtualSchemaNames[schema.Schema()]; isVirtualSchema {
		return true
	}

	// Check user defined schemas.
	schemaElts := b.ResolveSchema(schema, ResolveParams{
		IsExistenceOptional: true,
		RequiredPrivilege:   privilege.USAGE,
		WithOffline:         true, // We search schema with name `schema`, including offline ones.
	})
	return schemaElts != nil
}
