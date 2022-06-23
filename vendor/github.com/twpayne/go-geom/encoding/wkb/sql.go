package wkb

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkbcommon"
)

// ErrExpectedByteSlice is returned when a []byte is expected.
type ErrExpectedByteSlice struct {
	Value interface{}
}

func (e ErrExpectedByteSlice) Error() string {
	return fmt.Sprintf("wkb: want []byte, got %T", e.Value)
}

// A Geom is a WKB-ecoded Geometry that implements the sql.Scanner and
// driver.Value interfaces.
// It can be used when the geometry shape is not defined.
type Geom struct {
	geom.T
	opts []wkbcommon.WKBOption
}

// A Point is a WKB-encoded Point that implements the sql.Scanner and
// driver.Valuer interfaces.
type Point struct {
	*geom.Point
	opts []wkbcommon.WKBOption
}

// A LineString is a WKB-encoded LineString that implements the sql.Scanner and
// driver.Valuer interfaces.
type LineString struct {
	*geom.LineString
	opts []wkbcommon.WKBOption
}

// A Polygon is a WKB-encoded Polygon that implements the sql.Scanner and
// driver.Valuer interfaces.
type Polygon struct {
	*geom.Polygon
	opts []wkbcommon.WKBOption
}

// A MultiPoint is a WKB-encoded MultiPoint that implements the sql.Scanner and
// driver.Valuer interfaces.
type MultiPoint struct {
	*geom.MultiPoint
	opts []wkbcommon.WKBOption
}

// A MultiLineString is a WKB-encoded MultiLineString that implements the
// sql.Scanner and driver.Valuer interfaces.
type MultiLineString struct {
	*geom.MultiLineString
	opts []wkbcommon.WKBOption
}

// A MultiPolygon is a WKB-encoded MultiPolygon that implements the sql.Scanner
// and driver.Valuer interfaces.
type MultiPolygon struct {
	*geom.MultiPolygon
	opts []wkbcommon.WKBOption
}

// A GeometryCollection is a WKB-encoded GeometryCollection that implements the
// sql.Scanner and driver.Valuer interfaces.
type GeometryCollection struct {
	*geom.GeometryCollection
	opts []wkbcommon.WKBOption
}

// Scan scans from a []byte.
func (g *Geom) Scan(src interface{}) error {
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	// NOTE(tb) other Scanners do not check the len of b, is it really useful ?
	if len(b) == 0 {
		return nil
	}
	var err error
	g.T, err = Unmarshal(b, g.opts...)
	return err
}

// Value returns the WKB encoding of g.
func (g *Geom) Value() (driver.Value, error) {
	return value(g.T)
}

// Geom returns the underlying geom.T.
func (g *Geom) Geom() geom.T {
	return g.T
}

// Scan scans from a []byte.
func (p *Point) Scan(src interface{}) error {
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b, p.opts...)
	if err != nil {
		return err
	}
	p1, ok := got.(*geom.Point)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: got, Want: p}
	}
	p.Point = p1
	return nil
}

// Value returns the WKB encoding of p.
func (p *Point) Value() (driver.Value, error) {
	return value(p.Point)
}

// Scan scans from a []byte.
func (ls *LineString) Scan(src interface{}) error {
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b, ls.opts...)
	if err != nil {
		return err
	}
	ls1, ok := got.(*geom.LineString)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: got, Want: ls}
	}
	ls.LineString = ls1
	return nil
}

// Value returns the WKB encoding of ls.
func (ls *LineString) Value() (driver.Value, error) {
	return value(ls.LineString)
}

// Scan scans from a []byte.
func (p *Polygon) Scan(src interface{}) error {
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b, p.opts...)
	if err != nil {
		return err
	}
	p1, ok := got.(*geom.Polygon)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: got, Want: p}
	}
	p.Polygon = p1
	return nil
}

// Value returns the WKB encoding of p.
func (p *Polygon) Value() (driver.Value, error) {
	return value(p.Polygon)
}

// Scan scans from a []byte.
func (mp *MultiPoint) Scan(src interface{}) error {
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b, mp.opts...)
	if err != nil {
		return err
	}
	mp1, ok := got.(*geom.MultiPoint)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: got, Want: mp}
	}
	mp.MultiPoint = mp1
	return nil
}

// Value returns the WKB encoding of mp.
func (mp *MultiPoint) Value() (driver.Value, error) {
	return value(mp.MultiPoint)
}

// Scan scans from a []byte.
func (mls *MultiLineString) Scan(src interface{}) error {
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b, mls.opts...)
	if err != nil {
		return err
	}
	mls1, ok := got.(*geom.MultiLineString)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: got, Want: mls}
	}
	mls.MultiLineString = mls1
	return nil
}

// Value returns the WKB encoding of mls.
func (mls *MultiLineString) Value() (driver.Value, error) {
	return value(mls.MultiLineString)
}

// Scan scans from a []byte.
func (mp *MultiPolygon) Scan(src interface{}) error {
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b, mp.opts...)
	if err != nil {
		return err
	}
	mp1, ok := got.(*geom.MultiPolygon)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: got, Want: mp}
	}
	mp.MultiPolygon = mp1
	return nil
}

// Value returns the WKB encoding of mp.
func (mp *MultiPolygon) Value() (driver.Value, error) {
	return value(mp.MultiPolygon)
}

// Scan scans from a []byte.
func (gc *GeometryCollection) Scan(src interface{}) error {
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b, gc.opts...)
	if err != nil {
		return err
	}
	gc1, ok := got.(*geom.GeometryCollection)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: got, Want: gc}
	}
	gc.GeometryCollection = gc1
	return nil
}

// Value returns the WKB encoding of gc.
func (gc *GeometryCollection) Value() (driver.Value, error) {
	return value(gc.GeometryCollection)
}

func value(g geom.T) (driver.Value, error) {
	sb := &strings.Builder{}
	if err := Write(sb, NDR, g); err != nil {
		return nil, err
	}
	return []byte(sb.String()), nil
}
