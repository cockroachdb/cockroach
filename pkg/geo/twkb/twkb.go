// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package twkb implements the Tiny Well-known Binary (TWKB) encoding
// as described in https://github.com/TWKB/Specification/blob/master/twkb.md.
package twkb

import (
	"bytes"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/twpayne/go-geom"
)

// twkbType is represents a TWKB byte.
type twkbType uint8

const (
	twkbTypePoint              twkbType = 1
	twkbTypeLineString         twkbType = 2
	twkbTypePolygon            twkbType = 3
	twkbTypeMultiPoint         twkbType = 4
	twkbTypeMultiLineString    twkbType = 5
	twkbTypeMultiPolygon       twkbType = 6
	twkbTypeGeometryCollection twkbType = 7
)

type marshalOptions struct {
	precisionXY int8
	precisionZ  int8
	precisionM  int8
}

type marshaller struct {
	bytes.Buffer
	o marshalOptions
	// prevCoords keeps track of the previously written coordinates.
	// This is needed as for Polygons, MultiLineString and MultiPolygons, the
	// deltas written can be based off the previous ring/linestring/polygon.
	prevCoords []int64
}

// MarshalOption is an option to pass into Marshal.
type MarshalOption func(o *marshalOptions) error

// MarshalOptionPrecisionXY sets the XY precision for the TWKB.
func MarshalOptionPrecisionXY(p int64) MarshalOption {
	return func(o *marshalOptions) error {
		if p < -7 || p > 7 {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"XY precision must be between -7 and 7 inclusive",
			)
		}
		o.precisionXY = int8(p)
		return nil
	}
}

// MarshalOptionPrecisionZ sets the Z precision for the TWKB.
func MarshalOptionPrecisionZ(p int64) MarshalOption {
	return func(o *marshalOptions) error {
		if p < 0 || p > 7 {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"Z precision must not be negative or greater than 7",
			)
		}

		o.precisionZ = int8(p)
		return nil
	}
}

// MarshalOptionPrecisionM sets the M precision for the TWKB.
func MarshalOptionPrecisionM(p int64) MarshalOption {
	return func(o *marshalOptions) error {
		if p < 0 || p > 7 {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"M precision must not be negative or greater than 7",
			)
		}

		o.precisionM = int8(p)
		return nil
	}
}

// Marshal converts a geom.T into a TWKB encoded byte array.
func Marshal(t geom.T, opts ...MarshalOption) ([]byte, error) {
	var o marshalOptions
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	// TODO(#48886): PostGIS increases the precision by 5
	// for all lat/lng SRIDs in certain cases.
	m := marshaller{
		o:          o,
		prevCoords: make([]int64, t.Layout().Stride()),
	}
	if err := (&m).marshal(t); err != nil {
		return nil, err
	}
	return m.Bytes(), nil
}

func (m *marshaller) marshal(t geom.T) error {
	// Write metadata byte.
	typeAndPrecisionHeader, err := typeAndPrecisionHeaderByte(t, m.o)
	if err != nil {
		return err
	}
	if err := m.WriteByte(typeAndPrecisionHeader); err != nil {
		return err
	}
	if err := m.WriteByte(metadataByte(t, m.o)); err != nil {
		return err
	}
	if t.Layout().Stride() > 2 {
		if err := m.WriteByte(extendedDimensionsByte(t, m.o)); err != nil {
			return err
		}
	}
	if !t.Empty() {
		// TODO(#48886): write bounding box if we support writing bounding boxes.
		switch t := t.(type) {
		case *geom.Point:
			if err := m.writeFlatCoords(
				t.FlatCoords(),
				t.Layout(),
				false, /* writeLen */
			); err != nil {
				return err
			}
		case *geom.LineString:
			if err := m.writeFlatCoords(
				t.FlatCoords(),
				t.Layout(),
				true, /* writeLen */
			); err != nil {
				return err
			}
		case *geom.Polygon:
			if err := m.writeGeomWithEnds(
				t.FlatCoords(),
				t.Ends(),
				t.Layout(),
				0, /* startOffset */
			); err != nil {
				return err
			}
		case *geom.MultiPoint:
			// TODO(#48886): write out the id list for MultiPoint.
			if err := m.writeFlatCoords(
				t.FlatCoords(),
				t.Layout(),
				true, /* writeLen */
			); err != nil {
				return err
			}
		case *geom.MultiLineString:
			// TODO(#48886): write out the id list for MultiLineString.
			if err := m.writeGeomWithEnds(
				t.FlatCoords(),
				t.Ends(),
				t.Layout(),
				0, /* startOffset */
			); err != nil {
				return err
			}
		case *geom.MultiPolygon:
			// TODO(#48886): write out the id list for MultiPolygon.
			flatCoords := t.FlatCoords()
			endss := t.Endss()
			// Write the number of MultiPolygons.
			if err := m.putUvarint(uint64(len(endss))); err != nil {
				return err
			}
			startOffset := 0
			for _, ends := range endss {
				// Write out each polygon.
				if err := m.writeGeomWithEnds(
					flatCoords,
					ends,
					t.Layout(),
					startOffset,
				); err != nil {
					return err
				}
				// Update the start offset to be the last end we encountered.
				if len(ends) > 0 {
					startOffset = ends[len(ends)-1]
				}
			}
		case *geom.GeometryCollection:
			// Write the number of geometries.
			if err := m.putUvarint(uint64(t.NumGeoms())); err != nil {
				return err
			}
			for _, gcT := range t.Geoms() {
				// Reset prev coords each time.
				for i := range m.prevCoords {
					m.prevCoords[i] = 0
				}
				if err := m.marshal(gcT); err != nil {
					return err
				}
			}
		default:
			return pgerror.Newf(pgcode.InvalidParameterValue, "unknown TWKB type: %T", t)
		}
	}
	return nil
}

func (m *marshaller) writeGeomWithEnds(
	flatCoords []float64, ends []int, layout geom.Layout, startOffset int,
) error {
	// Write the number of elements.
	if err := m.putUvarint(uint64(len(ends))); err != nil {
		return err
	}
	start := startOffset
	// Write each segment with the length of each ring at the beginning.
	for _, end := range ends {
		if err := m.writeFlatCoords(
			flatCoords[start:end],
			layout,
			true, /* writeLen */
		); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// writeFlatCoords writes the flat coordinates into the buffer.
func (m *marshaller) writeFlatCoords(
	flatCoords []float64, layout geom.Layout, writeLen bool,
) error {
	stride := layout.Stride()
	if writeLen {
		if err := m.putUvarint(uint64(len(flatCoords) / stride)); err != nil {
			return err
		}
	}

	zIndex := layout.ZIndex()
	mIndex := layout.MIndex()
	for i, coord := range flatCoords {
		// Precision varies by coordinate.
		precision := m.o.precisionXY
		if i%stride == zIndex {
			precision = m.o.precisionZ
		}
		if i%stride == mIndex {
			precision = m.o.precisionM
		}
		curr := int64(math.Round(coord * math.Pow(10, float64(precision))))
		prev := m.prevCoords[i%stride]

		// The value written is the int version of the delta multiplied
		// by 10^precision.
		if err := m.putVarint(curr - prev); err != nil {
			return err
		}
		m.prevCoords[i%stride] = curr
	}
	return nil
}

// putVarint encodes an int64 into buf and returns the number of bytes written.
func (m *marshaller) putVarint(x int64) error {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return m.putUvarint(ux)
}

// putUvarint encodes a uint64 into buf and returns the number of bytes written.
func (m *marshaller) putUvarint(x uint64) error {
	for x >= 0x80 {
		if err := m.WriteByte(byte(x) | 0x80); err != nil {
			return err
		}
		x >>= 7
	}
	return m.WriteByte(byte(x))
}

func typeAndPrecisionHeaderByte(t geom.T, o marshalOptions) (byte, error) {
	var typ twkbType
	switch t.(type) {
	case *geom.Point:
		typ = twkbTypePoint
	case *geom.LineString:
		typ = twkbTypeLineString
	case *geom.Polygon:
		typ = twkbTypePolygon
	case *geom.MultiPoint:
		typ = twkbTypeMultiPoint
	case *geom.MultiLineString:
		typ = twkbTypeMultiLineString
	case *geom.MultiPolygon:
		typ = twkbTypeMultiPolygon
	case *geom.GeometryCollection:
		typ = twkbTypeGeometryCollection
	default:
		return 0, pgerror.Newf(pgcode.InvalidParameterValue, "unknown TWKB type: %T", t)
	}

	precisionZigZagged := zigzagInt8(o.precisionXY)
	return (uint8(typ) & 0x0F) | ((precisionZigZagged & 0x0F) << 4), nil
}

func metadataByte(t geom.T, o marshalOptions) byte {
	var b byte
	if t.Empty() {
		b |= 0b10000
	}
	if t.Layout().Stride() > 2 {
		b |= 0b1000
	}
	return b
}

func extendedDimensionsByte(t geom.T, o marshalOptions) byte {
	var extDimByte byte
	// Follow PostGIS and do no bounds checking for precision under/overflow.
	switch t.Layout() {
	case geom.XYZ:
		extDimByte |= 0b1
		extDimByte |= (byte(o.precisionZ) & 0b111) << 2
	case geom.XYM:
		extDimByte |= 0b10
		extDimByte |= (byte(o.precisionM) & 0b111) << 5
	case geom.XYZM:
		extDimByte |= 0b11
		extDimByte |= (byte(o.precisionZ) & 0b111) << 2
		extDimByte |= (byte(o.precisionM) & 0b111) << 5
	}
	return extDimByte
}

func zigzagInt8(x int8) byte {
	if x < 0 {
		return (uint8(-1-x) << 1) | 0x01
	}
	return uint8(x) << 1
}
