// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package geoprojbase is a minimal dependency package that contains
// basic metadata and data structures for SRIDs and their CRS
// transformations.
package geoprojbase

import (
	"bytes"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
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

// Bounds represents the projected or lat/lng bounds.
type Bounds struct {
	MinX float64
	MaxX float64
	MinY float64
	MaxY float64
}

// ProjInfo is a struct containing metadata related to a given SRID.
type ProjInfo struct {
	// SRID is the SRID of the projection.
	SRID geopb.SRID
	// AuthName is the authority who has provided this projection (e.g. ESRI, EPSG).
	AuthName string
	// AuthSRID is the SRID the given AuthName interprets the SRID as.
	AuthSRID int
	// SRText is the WKT representation of the projection.
	SRText string
	// Proj4Text is the PROJ4 text representation of the projection.
	Proj4Text Proj4Text
	// Bounds defines the bounds (projected or lat/lng) of the given coordinate system.
	Bounds Bounds

	// Denormalized fields.

	// IsLatLng stores whether the projection is a LatLng based projection (denormalized from above)
	IsLatLng bool
	// The spheroid represented by the SRID.
	Spheroid Spheroid
}

// Spheroid represents a spheroid object.
type Spheroid interface {
	Inverse(a, b s2.LatLng) (s12, az1, az2 float64)
	InverseBatch(points []s2.Point) float64
	AreaAndPerimeter(points []s2.Point) (area float64, perimeter float64)
	Project(point s2.LatLng, distance float64, azimuth s1.Angle) s2.LatLng
	Radius() float64
	Flattening() float64
	SphereRadius() float64
}

// ErrProjectionNotFound indicates a project was not found.
var ErrProjectionNotFound error = errors.Newf("projection not found")

// Projection returns the ProjInfo for the given SRID, as well as an
// error if the projection does not exist.
func Projection(srid geopb.SRID) (ProjInfo, error) {
	projections := getProjections()
	p, exists := projections[srid]
	if !exists {
		return ProjInfo{}, errors.Mark(
			pgerror.Newf(pgcode.InvalidParameterValue, "projection for SRID %d does not exist", srid),
			ErrProjectionNotFound,
		)
	}
	return p, nil
}

// MustProjection returns the ProjInfo for the given SRID, panicking if the
// projection does not exist.
func MustProjection(srid geopb.SRID) ProjInfo {
	ret, err := Projection(srid)
	if err != nil {
		panic(err)
	}
	return ret
}

// AllProjections returns a sorted list of all projections.
func AllProjections() []ProjInfo {
	projections := getProjections()
	ret := make([]ProjInfo, 0, len(projections))
	for _, p := range projections {
		ret = append(ret, p)
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].SRID < ret[j].SRID
	})
	return ret
}
