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
	"github.com/cockroachdb/cockroach/pkg/geo/geogfn"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// TODO(sumeer): applies to this and the geometry test. The current test is
// verbose in printing out the actual expressions, which are useful as a
// sanity check, but hard to validate.
// - Add datadriven cases that test relationships between shapes.
// - Add randomized tests

func TestS2GeographyIndexBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var index GeographyIndex
	shapes := make(map[string]geo.Geography)
	datadriven.RunTest(t, "testdata/s2_geography", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init":
			cfg := s2Config(t, d)
			index = NewS2GeographyIndex(S2GeographyConfig{S2Config: &cfg})
			return ""
		case "geometry":
			g, err := geo.ParseGeography(d.Input)
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
			return spansToString(index.DWithin(ctx, shapes[nameArg(t, d)], float64(distance), geogfn.UseSphere))
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

// TODO(sumeer): more tests, including spheroid for DWithin.
