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
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// SetPoint sets the point at the given index of lineString; index is 0-based.
func SetPoint(lineString *geo.Geometry, index int, point *geo.Geometry) (*geo.Geometry, error) {
	g, err := lineString.AsGeomT()
	if err != nil {
		return nil, err
	}

	lineStringG, ok := g.(*geom.LineString)
	if !ok {
		e := geom.ErrUnsupportedType{Value: g}
		return nil, errors.Wrap(e, "geometry to be modified must be a LineString")
	}

	g, err = point.AsGeomT()
	if err != nil {
		return nil, err
	}

	pointG, ok := g.(*geom.Point)
	if !ok {
		e := geom.ErrUnsupportedType{Value: g}
		return nil, errors.Wrapf(e, "invalid geometry used to replace a Point on a LineString")
	}

	g, err = setPoint(lineStringG, index, pointG)
	if err != nil {
		return nil, err
	}

	return geo.NewGeometryFromGeomT(g)
}

func setPoint(lineString *geom.LineString, index int, point *geom.Point) (*geom.LineString, error) {
	if lineString.Layout() != point.Layout() {
		return nil, geom.ErrLayoutMismatch{Got: point.Layout(), Want: lineString.Layout()}
	}

	coords := lineString.Coords()
	hasNegIndex := index < 0

	if index >= len(coords) || (hasNegIndex && index*-1 > len(coords)) {
		return nil, errors.Newf("index %d out of range of LineString with %d coordinates", index, len(coords))
	}

	if hasNegIndex {
		index = len(coords) + index
	}

	coords[index].Set(point.Coords())

	return lineString.SetCoords(coords)
}

// RemovePoint removes the point at the given index of lineString; index is 0-based.
func RemovePoint(lineString *geo.Geometry, index int) (*geo.Geometry, error) {
	g, err := lineString.AsGeomT()
	if err != nil {
		return nil, err
	}

	lineStringG, ok := g.(*geom.LineString)
	if !ok {
		e := geom.ErrUnsupportedType{Value: g}
		return nil, errors.Wrap(e, "geometry to be modified must be a LineString")
	}

	if lineStringG.NumCoords() == 2 {
		return nil, errors.Newf("cannot remove a point from a LineString with only two Points")
	}

	g, err = removePoint(lineStringG, index)
	if err != nil {
		return nil, err
	}

	return geo.NewGeometryFromGeomT(g)
}

func removePoint(lineString *geom.LineString, index int) (*geom.LineString, error) {
	coords := lineString.Coords()

	if index >= len(coords) || index < 0 {
		return nil, errors.Newf("index %d out of range of LineString with %d coordinates", index, len(coords))
	}

	coords = append(coords[:index], coords[index+1:]...)

	return lineString.SetCoords(coords)
}
