// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catformat

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// IndexForDisplay formats a column descriptor as a SQL string. It
// converts user defined types in partial index predicate expressions to a
// human-readable form.
//
// If tableName is anonymous then no table name is included in the formatted
// string. For example:
//
//   INDEX i (a) WHERE b > 0
//
// If tableName is not anonymous, then "ON" and the name is included:
//
//   INDEX i ON t (a) WHERE b > 0
//
func IndexForDisplay(
	ctx context.Context,
	table catalog.TableDescriptor,
	tableName *tree.TableName,
	index *descpb.IndexDescriptor,
	partition string,
	interleave string,
	semaCtx *tree.SemaContext,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	if index.Unique {
		f.WriteString("UNIQUE ")
	}
	if index.Type == descpb.IndexDescriptor_INVERTED {
		f.WriteString("INVERTED ")
	}
	f.WriteString("INDEX ")
	f.FormatNameP(&index.Name)
	if *tableName != descpb.AnonymousTable {
		f.WriteString(" ON ")
		f.FormatNode(tableName)
	}
	f.WriteString(" (")
	index.ColNamesFormat(f)
	f.WriteByte(')')

	if index.IsSharded() {
		fmt.Fprintf(f, " USING HASH WITH BUCKET_COUNT = %v",
			index.Sharded.ShardBuckets)
	}

	if len(index.StoreColumnNames) > 0 {
		f.WriteString(" STORING (")
		for i := range index.StoreColumnNames {
			if i > 0 {
				f.WriteString(", ")
			}
			f.FormatNameP(&index.StoreColumnNames[i])
		}
		f.WriteByte(')')
	}

	f.WriteString(interleave)
	f.WriteString(partition)

	if index.GeoConfig.S2Geometry != nil || index.GeoConfig.S2Geography != nil {
		var s2Config *geoindex.S2Config

		if index.GeoConfig.S2Geometry != nil {
			s2Config = index.GeoConfig.S2Geometry.S2Config
		}
		if index.GeoConfig.S2Geography != nil {
			s2Config = index.GeoConfig.S2Geography.S2Config
		}

		defaultS2Config := geoindex.DefaultS2Config()
		numCustomSettings := 0
		if *s2Config != *defaultS2Config {
			for _, check := range []struct {
				key        string
				val        int32
				defaultVal int32
			}{
				{`s2_max_level`, s2Config.MaxLevel, defaultS2Config.MaxLevel},
				{`s2_level_mod`, s2Config.LevelMod, defaultS2Config.LevelMod},
				{`s2_max_cells`, s2Config.MaxCells, defaultS2Config.MaxCells},
			} {
				if check.val != check.defaultVal {
					if numCustomSettings > 0 {
						f.WriteString(", ")
					} else {
						f.WriteString(" WITH (")
					}
					numCustomSettings++
					f.WriteString(check.key)
					f.WriteString("=")
					f.WriteString(strconv.Itoa(int(check.val)))
				}
			}
		}

		if index.GeoConfig.S2Geometry != nil {
			col, err := table.FindColumnWithID(index.InvertedColumnID())
			if err != nil {
				return "", errors.Wrapf(err, "expected column %q to exist in table", index.InvertedColumnName())
			}
			defaultConfig, err := geoindex.GeometryIndexConfigForSRID(col.GetType().GeoSRIDOrZero())
			if err != nil {
				return "", errors.Wrapf(err, "expected SRID definition for %d", col.GetType().GeoSRIDOrZero())
			}
			cfg := index.GeoConfig.S2Geometry

			for _, check := range []struct {
				key        string
				val        float64
				defaultVal float64
			}{
				{`geometry_min_x`, cfg.MinX, defaultConfig.S2Geometry.MinX},
				{`geometry_max_x`, cfg.MaxX, defaultConfig.S2Geometry.MaxX},
				{`geometry_min_y`, cfg.MinY, defaultConfig.S2Geometry.MinY},
				{`geometry_max_y`, cfg.MaxY, defaultConfig.S2Geometry.MaxY},
			} {
				if check.val != check.defaultVal {
					if numCustomSettings > 0 {
						f.WriteString(", ")
					} else {
						f.WriteString(" WITH (")
					}
					numCustomSettings++
					f.WriteString(check.key)
					f.WriteString("=")
					f.WriteString(strconv.FormatFloat(check.val, 'f', -1, 64))
				}
			}
		}

		if numCustomSettings > 0 {
			f.WriteString(")")
		}
	}

	if index.IsPartial() {
		f.WriteString(" WHERE ")
		pred, err := schemaexpr.FormatExprForDisplay(ctx, table, index.Predicate, semaCtx, tree.FmtParsable)
		if err != nil {
			return "", err
		}
		f.WriteString(pred)
	}

	return f.CloseAndGetString(), nil
}
