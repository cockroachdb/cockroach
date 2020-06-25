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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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

func postgisColumnsTablePopulator(
	matchingFamily types.Family,
) func(context.Context, *planner, *sqlbase.ImmutableDatabaseDescriptor, func(...tree.Datum) error) error {
	return func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(
			ctx,
			p,
			dbContext,
			hideVirtual,
			func(db *sqlbase.ImmutableDatabaseDescriptor, scName string, table *sqlbase.ImmutableTableDescriptor) error {
				if !table.IsPhysicalTable() {
					return nil
				}
				if p.CheckAnyPrivilege(ctx, table) != nil {
					return nil
				}
				for _, colDesc := range table.Columns {
					if colDesc.Type.Family() != matchingFamily {
						continue
					}
					m, err := colDesc.Type.GeoMetadata()
					if err != nil {
						return err
					}

					var datumNDims tree.Datum
					switch m.ShapeType {
					case geopb.ShapeType_Point, geopb.ShapeType_LineString, geopb.ShapeType_Polygon,
						geopb.ShapeType_MultiPoint, geopb.ShapeType_MultiLineString, geopb.ShapeType_MultiPolygon,
						geopb.ShapeType_GeometryCollection:
						datumNDims = tree.NewDInt(2)
					case geopb.ShapeType_Geometry, geopb.ShapeType_Unset:
						// For geometry_columns, the query in PostGIS COALESCES the value to 2.
						// Otherwise, the value is NULL.
						if matchingFamily == types.GeometryFamily {
							datumNDims = tree.NewDInt(2)
						} else {
							datumNDims = tree.DNull
						}
					}

					shapeName := m.ShapeType.String()
					if m.ShapeType == geopb.ShapeType_Unset {
						shapeName = geopb.ShapeType_Geometry.String()
					}

					if err := addRow(
						tree.NewDString(db.GetName()),
						tree.NewDString(scName),
						tree.NewDString(table.GetName()),
						tree.NewDString(colDesc.Name),
						datumNDims,
						tree.NewDInt(tree.DInt(m.SRID)),
						tree.NewDString(strings.ToUpper(shapeName)),
					); err != nil {
						return err
					}
				}
				return nil
			},
		)
	}
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
	populate: postgisColumnsTablePopulator(types.GeographyFamily),
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
	populate: postgisColumnsTablePopulator(types.GeometryFamily),
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
	populate: func(ctx context.Context, p *planner, dbContext *sqlbase.ImmutableDatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for _, projection := range geoprojbase.Projections {
			if err := addRow(
				tree.NewDInt(tree.DInt(projection.SRID)),
				tree.NewDString(projection.AuthName),
				tree.NewDInt(tree.DInt(projection.AuthSRID)),
				tree.NewDString(projection.SRText),
				tree.NewDString(projection.Proj4Text.String()),
			); err != nil {
				return err
			}
		}
		return nil
	},
}
