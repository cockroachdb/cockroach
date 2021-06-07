// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catconstants

import "github.com/cockroachdb/cockroach/pkg/keys"

// StaticSchemaIDMap is a map of statically known schema IDs.
var StaticSchemaIDMap = map[uint32]string{
	keys.PublicSchemaID: PublicSchemaName,
	PgCatalogID:         PgCatalogName,
	InformationSchemaID: InformationSchemaName,
	CrdbInternalID:      CRDBInternalSchemaName,
	PgExtensionSchemaID: PgExtensionSchemaName,
}

// PgCatalogName is the name of the pg_catalog system schema.
const PgCatalogName = "pg_catalog"

// PublicSchemaName is the name of the pg_catalog system schema.
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
