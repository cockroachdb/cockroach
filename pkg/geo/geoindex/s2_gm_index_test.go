package geoindex

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

func TestS2GMIndexBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var index *s2GMIndex
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
			index = NewS2GMIndex(S2GeometryConfig{
				MinX:     float64(minX),
				MinY:     float64(minY),
				MaxX:     float64(maxX),
				MaxY:     float64(maxY),
				S2Config: &cfg,
			})
			return ""
		case "geometry":
			g, err := geo.ParseGeometry(geopb.WKT(d.Input))
			if err != nil {
				return err.Error()
			}
			shapes[nameArg(t, d)] = g
			return ""
		case "index-keys":
			return keysToString(index.InvertedIndexKeys(ctx, shapes[nameArg(t, d)]))
		case "inner-covering":
			return cellUnionToString(index.innerCovering(shapes[nameArg(t, d)]))
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
