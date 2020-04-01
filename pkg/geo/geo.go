// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geo is a package that contains the base types for
// spatial data type operations.
package geo

import (
	_ "github.com/golang/geo/s2"
	_ "github.com/twpayne/go-geom"
	_ "github.com/twpayne/go-geom/encoding/ewkb"
	_ "github.com/twpayne/go-geom/encoding/ewkbhex"
	_ "github.com/twpayne/go-geom/encoding/geojson"
	_ "github.com/twpayne/go-geom/encoding/kml"
	_ "github.com/twpayne/go-geom/encoding/wkb"
	_ "github.com/twpayne/go-geom/encoding/wkbhex"
	_ "github.com/twpayne/go-geom/encoding/wkt"
)

// WKT is the Well Known Text form of a spatial object.
type WKT string

// EWKT is the Extended Well Known Text form of a spatial object.
type EWKT string

// WKB is the Well Known Bytes form of a spatial object.
type WKB []byte

// EWKB is the Extended Well Known Bytes form of a spatial object.
// It is safe to cast any WKB to any EWKB, but EWKB to WKB.
type EWKB []byte

// spatialObjectBase is the base for spatial objects.
type spatialObjectBase struct {
	ewkb EWKB
	// TODO: denormalize SRID from EWKB.
}

// Geometry is planar spatial object.
type Geometry struct {
	spatialObjectBase
}

// NewGeometry returns a new Geometry.
func NewGeometry(ewkb EWKB) *Geometry {
	return &Geometry{spatialObjectBase{ewkb: ewkb}}
}

// AsGeography converts a Geometry object into a Geography object.
// It will check to ensure the SRIDs are compatible with a Geography object.
func (g *Geometry) AsGeography() (Geography, error) {
	// TODO(otan): check SRIDs are lat/lng.
	return Geography{spatialObjectBase: g.spatialObjectBase}, nil
}

// Geography is a spherical spatial object.
type Geography struct {
	spatialObjectBase
}

// NewGeography returns a new Geography.
func NewGeography(ewkb EWKB) *Geography {
	return &Geography{spatialObjectBase{ewkb: ewkb}}
}

// AsGeometry converts a Geography object into a Geometry object.
func (g *Geography) AsGeometry() (Geometry, error) {
	return Geometry{spatialObjectBase: g.spatialObjectBase}, nil
}
