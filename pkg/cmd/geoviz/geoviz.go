// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"crypto/sha1"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/golang/geo/s2"
)

// Image represents an image to build.
type Image struct {
	Objects []Object `json:"objects"`
}

// LatLng represents the LatLng coordinate.
type LatLng struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// Polygon represents a polygon object.
type Polygon struct {
	Geodesic    bool       `json:"geodesic"`
	StrokeColor string     `json:"strokeColor"`
	FillColor   string     `json:"fillColor"`
	Paths       [][]LatLng `json:"paths"`
	Marker      Marker     `json:"marker"`
	Label       string     `json:"label"`
}

// Polyline represents a polyline object.
type Polyline struct {
	Geodesic    bool     `json:"geodesic"`
	StrokeColor string   `json:"strokeColor"`
	Path        []LatLng `json:"path"`
	Marker      Marker   `json:"marker"`
}

// Marker represents a marker object.
type Marker struct {
	Title string `json:"title"`
	// Content is displayed in an infowindow.
	Content  string `json:"content"`
	Position LatLng `json:"position"`
	Label    string `json:"label"`
}

// Object is a set of correlated things to draw.
type Object struct {
	Title     string     `json:"title"`
	Color     string     `json:"color"`
	Polygons  []Polygon  `json:"polygons"`
	Polylines []Polyline `json:"polylines"`
	Markers   []Marker   `json:"markers"`
}

func s2PointToLatLng(fromPoint s2.Point) LatLng {
	from := s2.LatLngFromPoint(fromPoint)
	return LatLng{Lat: from.Lat.Degrees(), Lng: from.Lng.Degrees()}
}

// AddGeography adds a given Geography to an image.
func (img *Image) AddGeography(g geo.Geography, title string, color string) {
	regions, err := g.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		panic(err)
	}
	object := Object{Title: title, Color: color}
	for _, region := range regions {
		switch region := region.(type) {
		case s2.Point:
			latlng := s2PointToLatLng(region)
			content := fmt.Sprintf("<h3>%s (point) (%f,%f)</h3>", title, latlng.Lat, latlng.Lng)
			object.Markers = append(
				object.Markers,
				Marker{
					Title:    title,
					Position: latlng,
					Content:  content,
					Label:    title,
				},
			)
		case *s2.Polyline:
			polyline := Polyline{
				Geodesic:    true,
				StrokeColor: color,
				Marker:      Marker{Title: title, Content: fmt.Sprintf("<h3>%s (line)</h3>", title)},
			}
			for _, point := range *region {
				polyline.Path = append(polyline.Path, s2PointToLatLng(point))
			}
			object.Polylines = append(object.Polylines, polyline)
		case *s2.Polygon:
			polygon := Polygon{
				Geodesic:    true,
				StrokeColor: color,
				FillColor:   color,
				Marker:      Marker{Title: title, Content: fmt.Sprintf("<h3>%s (polygon)</h3>", title)},
				Label:       title,
			}
			outerLoop := []LatLng{}
			for _, point := range region.Loop(0).Vertices() {
				outerLoop = append(outerLoop, s2PointToLatLng(point))
			}
			innerLoops := [][]LatLng{}
			for _, loop := range region.Loops()[1:] {
				vertices := loop.Vertices()
				loopRet := make([]LatLng, len(vertices))
				for i, vertex := range vertices {
					loopRet[len(vertices)-1-i] = s2PointToLatLng(vertex)
				}
				innerLoops = append(innerLoops, loopRet)
			}
			polygon.Paths = append([][]LatLng{outerLoop}, innerLoops...)
			object.Polygons = append(object.Polygons, polygon)
		}
	}
	img.Objects = append(img.Objects, object)
}

// AddS2Cells adds S2Cells to an image.
func (img *Image) AddS2Cells(title string, color string, cellIDs ...s2.CellID) {
	object := Object{Title: title, Color: color}
	for _, cellID := range cellIDs {
		cell := s2.CellFromCellID(cellID)
		loop := []LatLng{}
		// Draws on 4 corners.
		for i := 0; i < 4; i++ {
			point := cell.Vertex(i)
			loop = append(loop, s2PointToLatLng(point))
		}
		content := fmt.Sprintf(
			"<h3>%s (S2 cell)</h3><br/>cell ID: %d<br/>cell string: %s<br/>cell token: %s<br/>cell binary: %064b",
			title,
			cellID,
			cellID.String(),
			cellID.ToToken(),
			cellID,
		)
		polygon := Polygon{
			Geodesic:    true,
			StrokeColor: color,
			FillColor:   color,
			Paths:       [][]LatLng{loop},
			Marker:      Marker{Title: title, Content: content},
			Label:       title,
		}
		object.Polygons = append(object.Polygons, polygon)
	}
	img.Objects = append(img.Objects, object)
}

// ImageFromReader returns an Image based on a reader.
func ImageFromReader(r io.Reader) (*Image, error) {
	csvReader := csv.NewReader(r)
	gviz := &Image{}
	recordNum := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		recordNum++

		if len(record) != 4 {
			return nil, fmt.Errorf("records must be of format op,name(can be blank),color(can be blank),data")
		}
		op, name, color, data := record[0], record[1], record[2], record[3]
		// If color is blank, fill it in with a consistent hash.
		if color == "" {
			hashed := sha1.Sum([]byte(op + name + data))
			color = fmt.Sprintf("#%x", hashed[:3])
		}
		// If name is blank fill it in with a consistent hash.
		if name == "" {
			name = fmt.Sprintf("shape #%d", recordNum)
		}
		switch op {
		case "draw", "drawgeography":
			g, err := geo.ParseGeography(data)
			if err != nil {
				return nil, fmt.Errorf("error parsing: %s", data)
			}
			gviz.AddGeography(g, name, color)
		case "innercovering":
			g, err := geo.ParseGeography(data)
			if err != nil {
				return nil, fmt.Errorf("error parsing: %s", data)
			}
			index := geoindex.NewS2GeographyIndex(geoindex.S2GeographyConfig{
				S2Config: &geoindex.S2Config{
					MinLevel: 4,
					MaxLevel: 30,
					MaxCells: 20,
				},
			})
			gviz.AddS2Cells(name, color, index.TestingInnerCovering(g)...)
		case "drawcellid":
			cellIDs := []s2.CellID{}
			for _, d := range strings.Split(data, ",") {
				parsed, err := strconv.ParseInt(d, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("error parsing: %s", data)
				}
				cellIDs = append(cellIDs, s2.CellID(parsed))
			}
			gviz.AddS2Cells(name, color, cellIDs...)
		case "drawcelltoken":
			cellIDs := []s2.CellID{}
			for _, d := range strings.Split(data, ",") {
				cellID := s2.CellIDFromToken(d)
				if cellID == 0 {
					return nil, fmt.Errorf("error parsing: %s", data)
				}
				cellIDs = append(cellIDs, cellID)
			}
			gviz.AddS2Cells(name, color, cellIDs...)
		case "covering":
			g, err := geo.ParseGeography(data)
			if err != nil {
				return nil, fmt.Errorf("error parsing: %s", data)
			}
			gS2, err := g.AsS2(geo.EmptyBehaviorOmit)
			if err != nil {
				return nil, err
			}
			rc := &s2.RegionCoverer{MinLevel: 0, MaxLevel: 30, MaxCells: 4}
			cellIDs := []s2.CellID{}
			for _, s := range gS2 {
				cellIDs = append(cellIDs, rc.Covering(s)...)
			}
			gviz.AddS2Cells(name, color, cellIDs...)
		case "interiorcovering":
			g, err := geo.ParseGeography(data)
			if err != nil {
				return nil, fmt.Errorf("error parsing: %s", data)
			}
			gS2, err := g.AsS2(geo.EmptyBehaviorOmit)
			if err != nil {
				return nil, err
			}
			rc := &s2.RegionCoverer{MinLevel: 0, MaxLevel: 30, MaxCells: 4}
			cellIDs := []s2.CellID{}
			for _, s := range gS2 {
				cellIDs = append(cellIDs, rc.Covering(s)...)
			}
			gviz.AddS2Cells(name, color, cellIDs...)
		}
	}
	return gviz, nil
}
