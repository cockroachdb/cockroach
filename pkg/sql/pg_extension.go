// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// pgExtension is virtual schema which contains virtual tables and/or views
// which are used by postgres extensions. Postgres extensions typically install
// these tables and views on the public schema, but we instead do it in
// our own defined virtual table / schema.
var pgExtension = virtualSchema{
	name: sessiondata.PgExtensionSchemaName,
	tableDefs: map[sqlbase.ID]virtualSchemaDef{
		sqlbase.PgExtensionGeographyColumnsTableID: pgExtensionGeographyColumnsTable,
		sqlbase.PgExtensionGeometryColumnsTableID:  pgExtensionGeometryColumnsTable,
		sqlbase.PgExtensionSpatialRefSysTableID:    pgExtensionSpatialRefSysTable,
	},
	validWithNoDatabaseContext: false,
}

var pgExtensionGeographyColumnsTable = virtualSchemaTable{
	comment: `Shows all defined geography columns. Matches PostGIS' geography_columns functionality.`,
	schema: `
CREATE TABLE pg_extension.geography_columns (
	f_table_catalog name,
	f_table_schema name,
	f_table_name name,
	f_geography_column name,
	coord_dimension integer,
	srid integer,
	type text
)`,
	generator: func(ctx context.Context, p *planner, db *DatabaseDescriptor) (virtualTableGenerator, cleanupFunc, error) {
		return nil, func() {}, errors.Newf("not yet implemented")
	},
}

var pgExtensionGeometryColumnsTable = virtualSchemaTable{
	comment: `Shows all defined geometry columns. Matches PostGIS' geometry_columns functionality.`,
	schema: `
CREATE TABLE pg_extension.geometry_columns (
	f_table_catalog name,
	f_table_schema name,
	f_table_name name,
	f_geometry_column name,
	coord_dimension integer,
	srid integer,
	type text
)`,
	generator: func(ctx context.Context, p *planner, db *DatabaseDescriptor) (virtualTableGenerator, cleanupFunc, error) {
		return nil, func() {}, errors.Newf("not yet implemented")
	},
}

var pgExtensionSpatialRefSysTable = virtualSchemaTable{
	comment: `Shows all defined Spatial Reference Identifiers (SRIDs). Matches PostGIS' spatial_ref_sys table.`,
	schema: `
CREATE TABLE pg_extension.spatial_ref_sys (
	srid integer,
	auth_name varchar(256),
	auth_srid integer,
	srtext varchar(2048),
	proj4text varchar(2048)
)`,
	generator: func(ctx context.Context, p *planner, db *DatabaseDescriptor) (virtualTableGenerator, cleanupFunc, error) {
		return nil, func() {}, errors.Newf("not yet implemented")
	},
}
