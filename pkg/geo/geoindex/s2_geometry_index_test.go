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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

func TestS2GeometryIndexBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var index GeometryIndex
	shapes := make(map[string]*geo.Geometry)
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
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
