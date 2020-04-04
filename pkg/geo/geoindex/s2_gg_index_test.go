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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// TODO(sumeer): store shapes in a map so can exercise same shapes with different
// index configs.

func TestS2GGIndexBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var index *s2GGIndex
	var g *geo.Geography
	var err error
	datadriven.RunTest(t, "testdata/s2_geography", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init":
			var minLevel, maxLevel, maxCells int
			d.ScanArgs(t, "minlevel", &minLevel)
			d.ScanArgs(t, "maxlevel", &maxLevel)
			d.ScanArgs(t, "maxcells", &maxCells)
			s2Config := S2Config{
				MinLevel: int32(minLevel),
				MaxLevel: int32(maxLevel),
				LevelMod: 1,
				MaxCells: int32(maxCells),
			}
			index = NewS2GGIndex(S2GeographyConfig{S2Config: &s2Config})
			return ""
		case "geometry":
			g, err = geo.ParseGeography(geopb.WKT(d.Input))
			if err != nil {
				return err.Error()
			}
			return ""
		case "index-keys":
			keys, err := index.InvertedIndexKeys(ctx, g)
			if err != nil {
				return err.Error()
			}
			var cells []string
			for _, k := range keys {
				cells = append(cells, k.String())
			}
			return strings.Join(cells, ",")
		case "inner-covering":
			keys := index.innerCovering(g)
			var cells []string
			for _, k := range keys {
				cells = append(cells, Key(k).String())
			}
			return strings.Join(cells, ",")
		case "covers":
			spans, err := index.Covers(ctx, g)
			if err != nil {
				return err.Error()
			}
			return spans.String()
		case "covered-by":
			expr, err := index.CoveredBy(ctx, g)
			if err != nil {
				return err.Error()
			}
			return expr.String()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
