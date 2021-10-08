// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geopb

// The following are the common standard SRIDs that we support.
const (
	// UnknownSRID is the default SRID if none is provided.
	UnknownSRID = SRID(0)
	// DefaultGeometrySRID is the same as being unknown.
	DefaultGeometrySRID = UnknownSRID
	// DefaultGeographySRID (aka 4326) is the GPS lat/lng we all know and love.
	// In this system, (long, lat) corresponds to (X, Y), bounded by
	// ([-180, 180], [-90 90]).
	DefaultGeographySRID = SRID(4326)
)

const (
	// ZShapeTypeFlag indicates a Z dimension on the ShapeType.
	ZShapeTypeFlag = 1 << 30
	// MShapeTypeFlag indicates a M dimension on the ShapeType.
	MShapeTypeFlag = 1 << 29
)

// To2D returns the ShapeType for the corresponding 2D geometry type.
func (s ShapeType) To2D() ShapeType {
	return ShapeType(uint32(s) & (MShapeTypeFlag - 1))
}

// SRID is a Spatial Reference Identifer. All geometry and geography shapes are
// stored and represented as using coordinates that are bare floats. SRIDs tie these
// floats to the planar or spherical coordinate system, allowing them to be interpreted
// and compared.
//
// The zero value is special and means an unknown coordinate system.
type SRID int32

// WKT is the Well Known Text form of a spatial object.
type WKT string

// EWKT is the Extended Well Known Text form of a spatial object.
type EWKT string

// WKB is the Well Known Bytes form of a spatial object.
type WKB []byte

// EWKB is the Extended Well Known Bytes form of a spatial object.
type EWKB []byte
