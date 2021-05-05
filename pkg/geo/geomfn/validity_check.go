// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geomfn

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// ValidDetail contains information about the validity of a geometry.
type ValidDetail struct {
	IsValid bool
	// Reason is only populated if IsValid = false.
	Reason string
	// InvalidLocation is only populated if IsValid = false.
	InvalidLocation geo.Geometry
}

// IsValid returns whether the given Geometry is valid.
func IsValid(g geo.Geometry) (bool, error) {
	isValid, err := geos.IsValid(g.EWKB())
	if err != nil {
		return false, err
	}
	return isValid, nil
}

// IsValidReason returns the reasoning for whether the Geometry is valid or invalid.
func IsValidReason(g geo.Geometry) (string, error) {
	reason, err := geos.IsValidReason(g.EWKB())
	if err != nil {
		return "", err
	}
	return reason, nil
}

// IsValidDetail returns information about the validity of a Geometry.
// It takes in a flag parameter which behaves the same as the GEOS module, where 1
// means that self-intersecting rings forming holes are considered valid.
func IsValidDetail(g geo.Geometry, flags int) (ValidDetail, error) {
	isValid, reason, locEWKB, err := geos.IsValidDetail(g.EWKB(), flags)
	if err != nil {
		return ValidDetail{}, err
	}
	var loc geo.Geometry
	if len(locEWKB) > 0 {
		loc, err = geo.ParseGeometryFromEWKB(locEWKB)
		if err != nil {
			return ValidDetail{}, err
		}
	}
	return ValidDetail{
		IsValid:         isValid,
		Reason:          reason,
		InvalidLocation: loc,
	}, nil
}

// IsValidTrajectory returns whether a geometry encodes a valid trajectory
func IsValidTrajectory(line geo.Geometry) (bool, error) {
	t, err := line.AsGeomT()
	if err != nil {
		return false, err
	}
	lineString, ok := t.(*geom.LineString)
	if !ok {
		return false, errors.Newf("expected LineString, got %s", line.ShapeType().String())
	}
	mIndex := t.Layout().MIndex()
	if mIndex < 0 {
		return false, errors.New("LineString does not have M coordinates")
	}

	coords := lineString.Coords()
	for i := 1; i < len(coords); i++ {
		if coords[i][mIndex] <= coords[i-1][mIndex] {
			return false, nil
		}
	}
	return true, nil
}

// MakeValid returns a valid form of the given Geometry.
func MakeValid(g geo.Geometry) (geo.Geometry, error) {
	validEWKB, err := geos.MakeValid(g.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(validEWKB)
}

// EqualsExact validates if two geometry objects are exact equal under some epsilon
func EqualsExact(lhs, rhs geo.Geometry, epsilon float64) bool {
	equalsExact, err := geos.EqualsExact(lhs.EWKB(), rhs.EWKB(), epsilon)
	if err != nil {
		return false
	}
	return equalsExact
}
