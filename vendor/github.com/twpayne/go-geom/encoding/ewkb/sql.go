package ewkb

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

// A Point is a EWKB-encoded Point that implements the sql.Scanner and
// driver.Value interfaces.
type Point struct {
	*geom.Point
}

// A LineString is a EWKB-encoded LineString that implements the
// sql.Scanner and driver.Value interfaces.
type LineString struct {
	*geom.LineString
}

// A Polygon is a EWKB-encoded Polygon that implements the sql.Scanner and
// driver.Value interfaces.
type Polygon struct {
	*geom.Polygon
}

// A MultiPoint is a EWKB-encoded MultiPoint that implements the
// sql.Scanner and driver.Value interfaces.
type MultiPoint struct {
	*geom.MultiPoint
}

// A MultiLineString is a EWKB-encoded MultiLineString that implements the
// sql.Scanner and driver.Value interfaces.
type MultiLineString struct {
	*geom.MultiLineString
}

// A MultiPolygon is a EWKB-encoded MultiPolygon that implements the
// sql.Scanner and driver.Value interfaces.
type MultiPolygon struct {
	*geom.MultiPolygon
}

// A GeometryCollection is a EWKB-encoded GeometryCollection that implements
// the sql.Scanner and driver.Value interfaces.
type GeometryCollection struct {
	*geom.GeometryCollection
}

// Scan scans from a []byte.
func (p *Point) Scan(src interface{}) error {
	if src == nil {
		p.Point = nil
		return nil
	}
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b)
	if err != nil {
		return err
	}
	p1, ok := got.(*geom.Point)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: p1, Want: p}
	}
	p.Point = p1
	return nil
}

// Valid returns true if p has a value.
func (p *Point) Valid() bool {
	return p != nil && p.Point != nil
}

// Value returns the EWKB encoding of p.
func (p *Point) Value() (driver.Value, error) {
	if p.Point == nil {
		return nil, nil
	}
	return value(p.Point)
}

// Scan scans from a []byte.
func (ls *LineString) Scan(src interface{}) error {
	if src == nil {
		ls.LineString = nil
		return nil
	}
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b)
	if err != nil {
		return err
	}
	ls1, ok := got.(*geom.LineString)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: ls1, Want: ls}
	}
	ls.LineString = ls1
	return nil
}

// Valid return true if ls has a value.
func (ls *LineString) Valid() bool {
	return ls != nil && ls.LineString != nil
}

// Value returns the EWKB encoding of ls.
func (ls *LineString) Value() (driver.Value, error) {
	if ls.LineString == nil {
		return nil, nil
	}
	return value(ls.LineString)
}

// Scan scans from a []byte.
func (p *Polygon) Scan(src interface{}) error {
	if src == nil {
		p.Polygon = nil
		return nil
	}
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b)
	if err != nil {
		return err
	}
	p1, ok := got.(*geom.Polygon)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: p1, Want: p}
	}
	p.Polygon = p1
	return nil
}

// Valid returns true if p has a value.
func (p *Polygon) Valid() bool {
	return p != nil && p.Polygon != nil
}

// Value returns the EWKB encoding of p.
func (p *Polygon) Value() (driver.Value, error) {
	if p.Polygon == nil {
		return nil, nil
	}
	return value(p.Polygon)
}

// Scan scans from a []byte.
func (mp *MultiPoint) Scan(src interface{}) error {
	if src == nil {
		mp.MultiPoint = nil
		return nil
	}
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b)
	if err != nil {
		return err
	}
	mp1, ok := got.(*geom.MultiPoint)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: mp1, Want: mp}
	}
	mp.MultiPoint = mp1
	return nil
}

// Valid returns true if mp has a value.
func (mp *MultiPoint) Valid() bool {
	return mp != nil && mp.MultiPoint != nil
}

// Value returns the EWKB encoding of mp.
func (mp *MultiPoint) Value() (driver.Value, error) {
	if mp.MultiPoint == nil {
		return nil, nil
	}
	return value(mp.MultiPoint)
}

// Scan scans from a []byte.
func (mls *MultiLineString) Scan(src interface{}) error {
	if src == nil {
		mls.MultiLineString = nil
		return nil
	}
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b)
	if err != nil {
		return err
	}
	mls1, ok := got.(*geom.MultiLineString)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: mls1, Want: mls}
	}
	mls.MultiLineString = mls1
	return nil
}

// Valid returns true if mls has a value.
func (mls *MultiLineString) Valid() bool {
	return mls != nil && mls.MultiLineString != nil
}

// Value returns the EWKB encoding of mls.
func (mls *MultiLineString) Value() (driver.Value, error) {
	if mls.MultiLineString == nil {
		return nil, nil
	}
	return value(mls.MultiLineString)
}

// Scan scans from a []byte.
func (mp *MultiPolygon) Scan(src interface{}) error {
	if src == nil {
		mp.MultiPolygon = nil
		return nil
	}
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b)
	if err != nil {
		return err
	}
	mp1, ok := got.(*geom.MultiPolygon)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: mp1, Want: mp}
	}
	mp.MultiPolygon = mp1
	return nil
}

// Valid returns true if mp has a value.
func (mp *MultiPolygon) Valid() bool {
	return mp != nil && mp.MultiPolygon != nil
}

// Value returns the EWKB encoding of mp.
func (mp *MultiPolygon) Value() (driver.Value, error) {
	if mp.MultiPolygon == nil {
		return nil, nil
	}
	return value(mp.MultiPolygon)
}

// Scan scans from a []byte.
func (gc *GeometryCollection) Scan(src interface{}) error {
	if src == nil {
		gc.GeometryCollection = nil
		return nil
	}
	b, ok := src.([]byte)
	if !ok {
		return ErrExpectedByteSlice{Value: src}
	}
	got, err := Unmarshal(b)
	if err != nil {
		return err
	}
	gc1, ok := got.(*geom.GeometryCollection)
	if !ok {
		return wkbcommon.ErrUnexpectedType{Got: gc1, Want: gc}
	}
	gc.GeometryCollection = gc1
	return nil
}

// Valid returns true if gc has a value.
func (gc *GeometryCollection) Valid() bool {
	return gc != nil && gc.GeometryCollection != nil
}

// Value returns the EWKB encoding of gc.
func (gc *GeometryCollection) Value() (driver.Value, error) {
	if gc.GeometryCollection == nil {
		return nil, nil
	}
	return value(gc.GeometryCollection)
}

func value(g geom.T) (driver.Value, error) {
	sb := &strings.Builder{}
	if err := Write(sb, NDR, g); err != nil {
		return nil, err
	}
	return []byte(sb.String()), nil
}
