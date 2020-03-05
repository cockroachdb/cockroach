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

// SRID is a Spatial Reference Identifer. All geometry and geography shapes are
// stored and represented as bare floats. SRIDs tie these floats to the planar
// or spherical coordinate system, allowing them to be intrepred and compared.
//
// The zero value is special and means an unknown coordinate system.
type SRID int32

// The following are the common standard SRIDs that we support.
const (
	UnknownSRID         = SRID(0)
	DefaultGeometrySRID = UnknownSRID
	// 4326 is the GPS lat/lng we all know and love.
	DefaultGeographySRID = SRID(4326)
)
