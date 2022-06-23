// Package wkb implements Well Known Binary encoding and decoding.
//
// If you are encoding geometries in WKB to send to PostgreSQL/PostGIS, then
// you must specify binary_parameters=yes in the data source name that you pass
// to sql.Open.
package wkb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkbcommon"
)

var (
	// XDR is big endian.
	XDR = wkbcommon.XDR
	// NDR is little endian.
	NDR = wkbcommon.NDR
)

const (
	wkbXYID   = 0
	wkbXYZID  = 1000
	wkbXYMID  = 2000
	wkbXYZMID = 3000
)

// Read reads an arbitrary geometry from r.
func Read(r io.Reader, opts ...wkbcommon.WKBOption) (geom.T, error) {
	params := wkbcommon.InitWKBParams(
		wkbcommon.WKBParams{
			EmptyPointHandling: wkbcommon.EmptyPointHandlingError,
		},
		opts...,
	)

	wkbByteOrder, err := wkbcommon.ReadByte(r)
	if err != nil {
		return nil, err
	}
	var byteOrder binary.ByteOrder
	switch wkbByteOrder {
	case wkbcommon.XDRID:
		byteOrder = XDR
	case wkbcommon.NDRID:
		byteOrder = NDR
	default:
		return nil, wkbcommon.ErrUnknownByteOrder(wkbByteOrder)
	}

	wkbGeometryType, err := wkbcommon.ReadUInt32(r, byteOrder)
	if err != nil {
		return nil, err
	}
	t := wkbcommon.Type(wkbGeometryType)

	var layout geom.Layout
	switch 1000 * (t / 1000) {
	case wkbXYID:
		layout = geom.XY
	case wkbXYZID:
		layout = geom.XYZ
	case wkbXYMID:
		layout = geom.XYM
	case wkbXYZMID:
		layout = geom.XYZM
	default:
		return nil, wkbcommon.ErrUnknownType(t)
	}

	switch t % 1000 {
	case wkbcommon.PointID:
		flatCoords, err := wkbcommon.ReadFlatCoords0(r, byteOrder, layout.Stride())
		if err != nil {
			return nil, err
		}
		if params.EmptyPointHandling == wkbcommon.EmptyPointHandlingNaN {
			return geom.NewPointFlatMaybeEmpty(layout, flatCoords), nil
		}
		return geom.NewPointFlat(layout, flatCoords), nil
	case wkbcommon.LineStringID:
		flatCoords, err := wkbcommon.ReadFlatCoords1(r, byteOrder, layout.Stride())
		if err != nil {
			return nil, err
		}
		return geom.NewLineStringFlat(layout, flatCoords), nil
	case wkbcommon.PolygonID:
		flatCoords, ends, err := wkbcommon.ReadFlatCoords2(r, byteOrder, layout.Stride())
		if err != nil {
			return nil, err
		}
		return geom.NewPolygonFlat(layout, flatCoords, ends), nil
	case wkbcommon.MultiPointID:
		n, err := wkbcommon.ReadUInt32(r, byteOrder)
		if err != nil {
			return nil, err
		}
		if limit := wkbcommon.MaxGeometryElements[1]; limit >= 0 && int(n) > limit {
			return nil, wkbcommon.ErrGeometryTooLarge{Level: 1, N: int(n), Limit: limit}
		}
		mp := geom.NewMultiPoint(layout)
		for i := uint32(0); i < n; i++ {
			g, err := Read(r, opts...)
			if err != nil {
				return nil, err
			}
			p, ok := g.(*geom.Point)
			if !ok {
				return nil, wkbcommon.ErrUnexpectedType{Got: g, Want: &geom.Point{}}
			}
			if err = mp.Push(p); err != nil {
				return nil, err
			}
		}
		return mp, nil
	case wkbcommon.MultiLineStringID:
		n, err := wkbcommon.ReadUInt32(r, byteOrder)
		if err != nil {
			return nil, err
		}
		if limit := wkbcommon.MaxGeometryElements[2]; limit >= 0 && int(n) > limit {
			return nil, wkbcommon.ErrGeometryTooLarge{Level: 2, N: int(n), Limit: limit}
		}
		mls := geom.NewMultiLineString(layout)
		for i := uint32(0); i < n; i++ {
			g, err := Read(r, opts...)
			if err != nil {
				return nil, err
			}
			p, ok := g.(*geom.LineString)
			if !ok {
				return nil, wkbcommon.ErrUnexpectedType{Got: g, Want: &geom.LineString{}}
			}
			if err = mls.Push(p); err != nil {
				return nil, err
			}
		}
		return mls, nil
	case wkbcommon.MultiPolygonID:
		n, err := wkbcommon.ReadUInt32(r, byteOrder)
		if err != nil {
			return nil, err
		}
		if limit := wkbcommon.MaxGeometryElements[3]; limit >= 0 && int(n) > limit {
			return nil, wkbcommon.ErrGeometryTooLarge{Level: 3, N: int(n), Limit: limit}
		}
		mp := geom.NewMultiPolygon(layout)
		for i := uint32(0); i < n; i++ {
			g, err := Read(r, opts...)
			if err != nil {
				return nil, err
			}
			p, ok := g.(*geom.Polygon)
			if !ok {
				return nil, wkbcommon.ErrUnexpectedType{Got: g, Want: &geom.Polygon{}}
			}
			if err = mp.Push(p); err != nil {
				return nil, err
			}
		}
		return mp, nil
	case wkbcommon.GeometryCollectionID:
		n, err := wkbcommon.ReadUInt32(r, byteOrder)
		if err != nil {
			return nil, err
		}
		gc := geom.NewGeometryCollection()
		for i := uint32(0); i < n; i++ {
			g, err := Read(r, opts...)
			if err != nil {
				return nil, err
			}
			if err := gc.Push(g); err != nil {
				return nil, err
			}
		}
		// If EMPTY, mark the collection with a fixed layout to differentiate
		// GEOMETRYCOLLECTION EMPTY between 2D/Z/M/ZM.
		if gc.Empty() && gc.NumGeoms() == 0 {
			if err := gc.SetLayout(layout); err != nil {
				return nil, err
			}
		}
		return gc, nil
	default:
		return nil, wkbcommon.ErrUnsupportedType(wkbGeometryType)
	}
}

// Unmarshal unmrshals an arbitrary geometry from a []byte.
func Unmarshal(data []byte, opts ...wkbcommon.WKBOption) (geom.T, error) {
	return Read(bytes.NewBuffer(data), opts...)
}

// Write writes an arbitrary geometry to w.
func Write(w io.Writer, byteOrder binary.ByteOrder, g geom.T, opts ...wkbcommon.WKBOption) error {
	params := wkbcommon.InitWKBParams(
		wkbcommon.WKBParams{
			EmptyPointHandling: wkbcommon.EmptyPointHandlingError,
		},
		opts...,
	)

	var wkbByteOrder byte
	switch byteOrder {
	case XDR:
		wkbByteOrder = wkbcommon.XDRID
	case NDR:
		wkbByteOrder = wkbcommon.NDRID
	default:
		return wkbcommon.ErrUnsupportedByteOrder{}
	}
	if err := wkbcommon.WriteByte(w, wkbByteOrder); err != nil {
		return err
	}

	var wkbGeometryType uint32
	switch g.(type) {
	case *geom.Point:
		wkbGeometryType = wkbcommon.PointID
	case *geom.LineString:
		wkbGeometryType = wkbcommon.LineStringID
	case *geom.Polygon:
		wkbGeometryType = wkbcommon.PolygonID
	case *geom.MultiPoint:
		wkbGeometryType = wkbcommon.MultiPointID
	case *geom.MultiLineString:
		wkbGeometryType = wkbcommon.MultiLineStringID
	case *geom.MultiPolygon:
		wkbGeometryType = wkbcommon.MultiPolygonID
	case *geom.GeometryCollection:
		wkbGeometryType = wkbcommon.GeometryCollectionID
	default:
		return geom.ErrUnsupportedType{Value: g}
	}
	switch g.Layout() {
	case geom.NoLayout:
		// Special case for empty GeometryCollections
		if _, ok := g.(*geom.GeometryCollection); !ok || !g.Empty() {
			return geom.ErrUnsupportedLayout(g.Layout())
		}
	case geom.XY:
		wkbGeometryType += wkbXYID
	case geom.XYZ:
		wkbGeometryType += wkbXYZID
	case geom.XYM:
		wkbGeometryType += wkbXYMID
	case geom.XYZM:
		wkbGeometryType += wkbXYZMID
	default:
		return geom.ErrUnsupportedLayout(g.Layout())
	}
	if err := wkbcommon.WriteUInt32(w, byteOrder, wkbGeometryType); err != nil {
		return err
	}

	switch g := g.(type) {
	case *geom.Point:
		if g.Empty() {
			switch params.EmptyPointHandling {
			case wkbcommon.EmptyPointHandlingNaN:
				return wkbcommon.WriteEmptyPointAsNaN(w, byteOrder, g.Stride())
			case wkbcommon.EmptyPointHandlingError:
				return fmt.Errorf("cannot encode empty Point in WKB")
			default:
				return fmt.Errorf("cannot encode empty Point in WKB (unknown option: %d)", wkbcommon.EmptyPointHandlingNaN)
			}
		}
		return wkbcommon.WriteFlatCoords0(w, byteOrder, g.FlatCoords())
	case *geom.LineString:
		return wkbcommon.WriteFlatCoords1(w, byteOrder, g.FlatCoords(), g.Stride())
	case *geom.Polygon:
		return wkbcommon.WriteFlatCoords2(w, byteOrder, g.FlatCoords(), g.Ends(), g.Stride())
	case *geom.MultiPoint:
		n := g.NumPoints()
		if err := wkbcommon.WriteUInt32(w, byteOrder, uint32(n)); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := Write(w, byteOrder, g.Point(i), opts...); err != nil {
				return err
			}
		}
		return nil
	case *geom.MultiLineString:
		n := g.NumLineStrings()
		if err := wkbcommon.WriteUInt32(w, byteOrder, uint32(n)); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := Write(w, byteOrder, g.LineString(i), opts...); err != nil {
				return err
			}
		}
		return nil
	case *geom.MultiPolygon:
		n := g.NumPolygons()
		if err := wkbcommon.WriteUInt32(w, byteOrder, uint32(n)); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := Write(w, byteOrder, g.Polygon(i), opts...); err != nil {
				return err
			}
		}
		return nil
	case *geom.GeometryCollection:
		n := g.NumGeoms()
		if err := wkbcommon.WriteUInt32(w, byteOrder, uint32(n)); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := Write(w, byteOrder, g.Geom(i), opts...); err != nil {
				return err
			}
		}
		return nil
	default:
		return geom.ErrUnsupportedType{Value: g}
	}
}

// Marshal marshals an arbitrary geometry to a []byte.
func Marshal(g geom.T, byteOrder binary.ByteOrder, opts ...wkbcommon.WKBOption) ([]byte, error) {
	w := bytes.NewBuffer(nil)
	if err := Write(w, byteOrder, g, opts...); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}
