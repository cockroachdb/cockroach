// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catconstants

// PublicSchemaID redefines keys.PublicSchemaID to avoid an import. It exists
// to deal with time-travel queries from moments before the time when all
// databases other than system were given a public schema.
const PublicSchemaID = 29

// StaticSchemaIDMapVirtualPublicSchema is a map of statically known schema IDs
// on versions prior to PublicSchemasWithDescriptors.
var StaticSchemaIDMapVirtualPublicSchema = map[uint32]string{
	PublicSchemaID:      PublicSchemaName,
	PgCatalogID:         PgCatalogName,
	InformationSchemaID: InformationSchemaName,
	CrdbInternalID:      CRDBInternalSchemaName,
	PgExtensionSchemaID: PgExtensionSchemaName,
}

// StaticSchemaIDMap is a map of statically known schema IDs on versions
// PublicSchemasWithDescriptors and onwards.
var StaticSchemaIDMap = map[uint32]string{
	PgCatalogID:         PgCatalogName,
	InformationSchemaID: InformationSchemaName,
	CrdbInternalID:      CRDBInternalSchemaName,
	PgExtensionSchemaID: PgExtensionSchemaName,
}

// GetStaticSchemaIDMap returns a map of schema ids to schema names for the
// static schemas.
func GetStaticSchemaIDMap() map[uint32]string {
	return StaticSchemaIDMapVirtualPublicSchema
}

// PgCatalogName is the name of the pg_catalog system schema.
const PgCatalogName = "pg_catalog"

// PublicSchemaName is the name of the public schema.
const PublicSchemaName = "public"

// UserSchemaName is the alias for schema names for users.
const UserSchemaName = "$user"

// InformationSchemaName is the name of the information_schema system schema.
const InformationSchemaName = "information_schema"

// CRDBInternalSchemaName is the name of the crdb_internal system schema.
const CRDBInternalSchemaName = "crdb_internal"

// PgSchemaPrefix is a prefix for Postgres system schemas. Users cannot
// create schemas with this prefix.
const PgSchemaPrefix = "pg_"

// PgTempSchemaName is the alias for temporary schemas across sessions.
const PgTempSchemaName = "pg_temp"

// PgExtensionSchemaName is the alias for schemas which are usually "public" in postgres
// when installing an extension, but must be stored as a separate schema in CRDB.
const PgExtensionSchemaName = "pg_extension"

// VirtualSchemaNames is a set of all virtual schema names.
var VirtualSchemaNames = map[string]struct{}{
	PgCatalogName:          {},
	InformationSchemaName:  {},
	CRDBInternalSchemaName: {},
	PgExtensionSchemaName:  {},
}
