// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geomfn

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// TileEnvelope returns a geometry as a rectangular Polygon giving the
// extent of a tile in the XYZ tile system
func TileEnvelope(
	tileZoom int, tileX int, tileY int, bounds geo.Geometry, margin float64,
) (geo.Geometry, error) {
	bbox := bounds.BoundingBoxRef()
	if bbox == nil {
		return geo.Geometry{}, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"Unable to compute bbox")
	}
	srid := bounds.SRID()

	if tileZoom < 0 || tileZoom >= 32 {
		return geo.Geometry{}, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"Invalid tile zoom value, %d",
			tileZoom,
		)
	}

	if bbox.HiX-bbox.LoX <= 0 || bbox.HiY-bbox.LoY <= 0 {
		return geo.Geometry{}, pgerror.New(
			pgcode.InvalidParameterValue,
			"Geometric bounds are too small",
		)
	}

	if margin < -0.5 {
		return geo.Geometry{}, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"Margin value must not be less than -50%%, %d",
			margin,
		)
	}

	tileZoomu := uint32(tileZoom)
	if tileZoomu > 31 {
		tileZoomu = 31
	}

	worldTileSize := uint32(0x01) << tileZoomu

	if tileX < 0 || uint32(tileX) >= worldTileSize {
		return geo.Geometry{}, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"Invalid tile x value, %d",
			tileX,
		)
	}

	if tileY < 0 || uint32(tileY) >= worldTileSize {
		return geo.Geometry{}, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"Invalid tile y value, %d",
			tileY,
		)
	}

	tileGeoSizeX := (bbox.HiX - bbox.LoX) / float64(worldTileSize)
	tileGeoSizeY := (bbox.HiY - bbox.LoY) / float64(worldTileSize)

	var x1 float64
	var x2 float64

	// 1 margin (100%) is the same as a single tile width
	// if the size of the tile with margins span more than the total number of tiles,
	// reset x1/x2 to the bounds
	if 1+margin*2 > float64(worldTileSize) {
		x1 = bbox.LoX
		x2 = bbox.HiX
	} else {
		x1 = bbox.LoX + tileGeoSizeX*(float64(tileX)-margin)
		x2 = bbox.LoX + tileGeoSizeX*(float64(tileX)+1+margin)
	}

	y1 := bbox.HiY - tileGeoSizeY*(float64(tileY)+1+margin)
	y2 := bbox.HiY - tileGeoSizeY*(float64(tileY)-margin)

	// Clip y-axis to the given bounds
	if y1 < bbox.LoY {
		y1 = bbox.LoY
	}
	if y2 > bbox.HiY {
		y2 = bbox.HiY
	}

	envelope := &geo.CartesianBoundingBox{
		BoundingBox: geopb.BoundingBox{
			LoX: x1,
			LoY: y1,
			HiX: x2,
			HiY: y2,
		},
	}

	return geo.MakeGeometryFromGeomT(envelope.ToGeomT(srid))
}
