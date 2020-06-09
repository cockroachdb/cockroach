// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geoprojbase is a minimal dependency package that contains
// basic metadata and data structures for SRIDs and their CRS
// transformations.
package geoprojbase

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
)

// Proj4Text is the text representation of a PROJ4 transformation.
type Proj4Text struct {
	cStr []byte
}

// MakeProj4Text returns a new Proj4Text with spec based on the given string.
func MakeProj4Text(str string) Proj4Text {
	return Proj4Text{
		cStr: []byte(str + "\u0000"),
	}
}

// String returns the string representation of the given proj text.
func (p *Proj4Text) String() string {
	return string(p.cStr[:len(p.cStr)-1])
}

// Bytes returns the raw bytes for the given proj text.
func (p *Proj4Text) Bytes() []byte {
	return p.cStr
}

// Equal returns whether the two Proj4Texts are equal.
func (p *Proj4Text) Equal(o Proj4Text) bool {
	return bytes.Equal(p.cStr, o.cStr)
}

// ProjInfo is a struct containing metadata related to a given SRID.
type ProjInfo struct {
	SRID geopb.SRID
	// AuthName is the authority who has provided this projection (e.g. ESRI, EPSG).
	AuthName string
	// AuthSRID is the SRID the given AuthName interprets the SRID as.
	AuthSRID int
	// SRText is the WKT representation of the projection.
	SRText string
	// Proj4Text is the PROJ4 text representation of the projection.
	Proj4Text Proj4Text

	// Denormalized fields.

	// IsLatLng stores whether the projection is a LatLng based projection (denormalized from above)
	IsLatLng bool
	// The spheroid represented by the SRID.
	Spheroid *geographiclib.Spheroid
}

// Projection returns the ProjInfo identifier for the given SRID, as well as an bool
// indicating whether the projection exists.
func Projection(srid geopb.SRID) (ProjInfo, bool) {
	p, exists := Projections[srid]
	return p, exists
}
