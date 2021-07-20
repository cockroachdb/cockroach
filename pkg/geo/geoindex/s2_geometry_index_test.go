// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geoindex

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestS2GeometryIndexBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var index GeometryIndex
	shapes := make(map[string]geo.Geometry)
	datadriven.RunTest(t, "testdata/s2_geometry", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init":
			cfg := s2Config(t, d)
			var minX, minY, maxX, maxY int
			d.ScanArgs(t, "minx", &minX)
			d.ScanArgs(t, "miny", &minY)
			d.ScanArgs(t, "maxx", &maxX)
			d.ScanArgs(t, "maxy", &maxY)
			index = NewS2GeometryIndex(S2GeometryConfig{
				MinX:     float64(minX),
				MinY:     float64(minY),
				MaxX:     float64(maxX),
				MaxY:     float64(maxY),
				S2Config: &cfg,
			})
			return ""
		case "geometry":
			g, err := geo.ParseGeometry(d.Input)
			if err != nil {
				return err.Error()
			}
			shapes[nameArg(t, d)] = g
			return ""
		case "index-keys":
			return keysToString(index.InvertedIndexKeys(ctx, shapes[nameArg(t, d)]))
		case "inner-covering":
			return cellUnionToString(index.TestingInnerCovering(shapes[nameArg(t, d)]))
		case "covers":
			return spansToString(index.Covers(ctx, shapes[nameArg(t, d)]))
		case "intersects":
			return spansToString(index.Intersects(ctx, shapes[nameArg(t, d)]))
		case "covered-by":
			return checkExprAndToString(index.CoveredBy(ctx, shapes[nameArg(t, d)]))
		case "d-within":
			var distance int
			d.ScanArgs(t, "distance", &distance)
			return spansToString(index.DWithin(ctx, shapes[nameArg(t, d)], float64(distance)))
		case "d-fully-within":
			var distance int
			d.ScanArgs(t, "distance", &distance)
			return spansToString(index.DFullyWithin(ctx, shapes[nameArg(t, d)], float64(distance)))
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestClipByRect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var g geo.Geometry
	var err error
	datadriven.RunTest(t, "testdata/clip", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "geometry":
			g, err = geo.ParseGeometry(d.Input)
			if err != nil {
				return err.Error()
			}
			return ""
		case "clip":
			var xMin, yMin, xMax, yMax int
			d.ScanArgs(t, "xmin", &xMin)
			d.ScanArgs(t, "ymin", &yMin)
			d.ScanArgs(t, "xmax", &xMax)
			d.ScanArgs(t, "ymax", &yMax)
			ewkb, err := geos.ClipByRect(
				g.EWKB(),
				float64(xMin),
				float64(yMin),
				float64(xMax),
				float64(yMax),
			)
			if err != nil {
				return err.Error()
			}
			// TODO(sumeer):
			// - add WKB to WKT and print exact output
			// - expand test with more inputs
			return fmt.Sprintf(
				"%d => %d (srid: %d)",
				len(g.EWKB()),
				len(ewkb),
				g.SRID(),
			)
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestNoClippingAtSRIDBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test that indexes that use the SRID bounds don't clip shapes that touch
	// those bounds. This test uses point shapes representing the four corners
	// of the bounds.
	for _, projInfo := range geoprojbase.AllProjections() {
		t.Run(strconv.Itoa(int(projInfo.SRID)), func(t *testing.T) {
			b := projInfo.Bounds
			config, err := GeometryIndexConfigForSRID(projInfo.SRID)
			require.NoError(t, err)
			index := NewS2GeometryIndex(*config.S2Geometry)
			// Four corners of the bounds, proceeding clockwise from the lower-left.
			xCorners := []float64{b.MinX, b.MinX, b.MaxX, b.MaxX}
			yCorners := []float64{b.MinY, b.MaxY, b.MaxY, b.MinY}
			for i := range xCorners {
				g, err := geo.MakeGeometryFromPointCoords(xCorners[i], yCorners[i])
				require.NoError(t, err)
				keys, _, err := index.InvertedIndexKeys(context.Background(), g)
				require.NoError(t, err)
				require.Equal(t, 1, len(keys))
				require.NotEqual(t, Key(exceedsBoundsCellID), keys[0],
					"SRID: %d, Point: %f, %f", projInfo.SRID, xCorners[i], yCorners[i])
			}
		})
	}
}
