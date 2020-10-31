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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy"
)

// extractUniqueEndpoints extracts the endpoints from geometry and removes duplicates
func extractUniqueEndpoints(g geo.Geometry) (geo.Geometry, error) {
	endpoints, err := extractEndpoints(g)
	if err != nil {
		return geo.Geometry{}, fmt.Errorf("error extracting endpoints: %v", err)
	}

	output, err := UnaryUnion(endpoints)
	if err != nil {
		return geo.Geometry{}, fmt.Errorf("error removing duplicates: %v", err)
	}
	return output, nil
}

// extractEndpoints extracts the endpoints from geometry provided and returns them as a multipoint geometry
func extractEndpoints(g geo.Geometry) (geo.Geometry, error) {
	mp := geom.NewMultiPoint(geom.XY)

	gt, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, fmt.Errorf("error transforming geometry: %v", err)
	}

	switch gt := gt.(type) {
	case *geom.LineString:
		if err := collectEndpoints(gt, mp); err != nil {
			return geo.Geometry{}, fmt.Errorf("error collecting endpoints: %v", err)
		}
	case *geom.MultiLineString:
		for i := 0; i < gt.NumLineStrings(); i++ {
			ls := gt.LineString(i)
			if err := collectEndpoints(ls, mp); err != nil {
				return geo.Geometry{}, fmt.Errorf("error collecting endpoints: %v", err)
			}
		}
	default:
		return geo.Geometry{}, fmt.Errorf("unsupported type: %T", gt)
	}

	result, err := geo.MakeGeometryFromGeomT(mp)
	if err != nil {
		return geo.Geometry{}, fmt.Errorf("error creating output geometry: %v", err)
	}
	return result, nil
}

// collectEndpoints takes endpoints of the line provided and adds them to the provided multipoint
func collectEndpoints(ls *geom.LineString, mp *geom.MultiPoint) error {
	coord := ls.Coord(0)
	err := mp.Push(
		geom.NewPointFlat(geom.XY, []float64{coord.X(), coord.Y()}),
	)
	if err != nil {
		return fmt.Errorf("error creating output geometry: %v", err)
	}
	coord = ls.Coord(ls.NumCoords() - 1)
	err = mp.Push(
		geom.NewPointFlat(geom.XY, []float64{coord.X(), coord.Y()}),
	)
	if err != nil {
		return fmt.Errorf("error creating output geometry: %v", err)
	}
	return nil
}

// splitLineByPoint splits the line using the point provided. Returns true and a slice of output line strings.
// The point must be on provided line and not an endpoint, otherwise false is returned along with unsplitted line.
func splitLineByPoint(l *geom.LineString, p geom.Coord) (bool, []*geom.LineString, error) {
	// Do not split if point is not on line
	if !xy.IsOnLine(l.Layout(), p, l.FlatCoords()) {
		return false, []*geom.LineString{l}, nil
	}
	// Do not split if point is the endpoint of the line
	startCoord := l.Coord(0)
	endCoord := l.Coord(l.NumCoords() - 1)
	if p.Equal(l.Layout(), startCoord) || p.Equal(l.Layout(), endCoord) {
		return false, []*geom.LineString{l}, nil
	}
	// Find where to split the line and group coords
	coordsA := []geom.Coord{}
	coordsB := []geom.Coord{}
	for i := 1; i < l.NumCoords(); i++ {
		if xy.IsPointWithinLineBounds(p, l.Coord(i-1), l.Coord(i)) {
			coordsA = append(l.Coords()[0:i], p)
			if p.Equal(l.Layout(), l.Coord(i)) {
				coordsB = l.Coords()[i:]
			} else {
				coordsB = append([]geom.Coord{p}, l.Coords()[i:]...)
			}
			break
		}
	}
	l1 := geom.NewLineString(l.Layout())
	_, err := l1.SetCoords(coordsA)
	if err != nil {
		return false, []*geom.LineString{}, fmt.Errorf("could not set coords: %v", err)
	}
	l2 := geom.NewLineString(l.Layout())
	_, err = l2.SetCoords(coordsB)
	if err != nil {
		return false, []*geom.LineString{}, fmt.Errorf("could not set coords: %v", err)
	}
	return true, []*geom.LineString{l1, l2}, nil
}
