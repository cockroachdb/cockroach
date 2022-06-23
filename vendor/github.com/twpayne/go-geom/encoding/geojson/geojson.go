// Package geojson implements GeoJSON encoding and decoding.
package geojson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	geom "github.com/twpayne/go-geom"
)

var nullGeometry = []byte("null")

// DefaultLayout is the default layout for empty geometries.
// FIXME This should be Codec-specific, not global.
var DefaultLayout = geom.XY

// ErrDimensionalityTooLow is returned when the dimensionality is too low.
type ErrDimensionalityTooLow int

func (e ErrDimensionalityTooLow) Error() string {
	return fmt.Sprintf("geojson: dimensionality too low (%d)", int(e))
}

// ErrUnsupportedType is returned when the type is unsupported.
type ErrUnsupportedType string

func (e ErrUnsupportedType) Error() string {
	return fmt.Sprintf("geojson: unsupported type: %s", string(e))
}

// CRS is a deprecated field but still populated in some programs (e.g. PostGIS).
// See https://geojson.org/geojson-spec for original specification of CRS.
type CRS struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
}

// A Geometry is a geometry in GeoJSON format.
type Geometry struct {
	Type        string           `json:"type"`
	BBox        *json.RawMessage `json:"bbox,omitempty"`
	CRS         *CRS             `json:"crs,omitempty"`
	Coordinates *json.RawMessage `json:"coordinates,omitempty"`
	Geometries  *json.RawMessage `json:"geometries,omitempty"`
}

// A Feature is a GeoJSON Feature.
type Feature struct {
	ID         string
	BBox       *geom.Bounds
	Geometry   geom.T
	Properties map[string]interface{}
}

type geojsonFeature struct {
	Type       string                 `json:"type"`
	ID         string                 `json:"id,omitempty"`
	BBox       []float64              `json:"bbox,omitempty"`
	Geometry   *Geometry              `json:"geometry"`
	Properties map[string]interface{} `json:"properties"`
}

// A FeatureCollection is a GeoJSON FeatureCollection.
type FeatureCollection struct {
	BBox     *geom.Bounds
	Features []*Feature
}

type geojsonFeatureCollection struct {
	Type     string     `json:"type"`
	BBox     []float64  `json:"bbox,omitempty"`
	Features []*Feature `json:"features"`
}

func guessLayout0(coords0 []float64) (geom.Layout, error) {
	switch n := len(coords0); n {
	case 0, 1:
		return geom.NoLayout, ErrDimensionalityTooLow(len(coords0))
	case 2:
		return geom.XY, nil
	case 3:
		return geom.XYZ, nil
	case 4:
		return geom.XYZM, nil
	default:
		return geom.Layout(n), nil
	}
}

func guessLayout1(coords1 []geom.Coord) (geom.Layout, error) {
	if len(coords1) == 0 {
		return DefaultLayout, nil
	}
	return guessLayout0(coords1[0])
}

func guessLayout2(coords2 [][]geom.Coord) (geom.Layout, error) {
	if len(coords2) == 0 {
		return DefaultLayout, nil
	}
	return guessLayout1(coords2[0])
}

func guessLayout3(coords3 [][][]geom.Coord) (geom.Layout, error) {
	if len(coords3) == 0 {
		return DefaultLayout, nil
	}
	return guessLayout2(coords3[0])
}

// Decode decodes g to a geometry.
func (g *Geometry) Decode() (geom.T, error) {
	if g == nil {
		return nil, nil
	}
	switch g.Type {
	case "Point":
		if g.Coordinates == nil {
			return geom.NewPointEmpty(geom.NoLayout), nil
		}
		var coords geom.Coord
		if err := json.Unmarshal(*g.Coordinates, &coords); err != nil {
			return nil, err
		}
		if len(coords) == 0 {
			return geom.NewPointEmpty(DefaultLayout), nil
		}
		layout, err := guessLayout0(coords)
		if err != nil {
			return nil, err
		}
		return geom.NewPoint(layout).SetCoords(coords)
	case "LineString":
		if g.Coordinates == nil {
			return geom.NewLineString(geom.NoLayout), nil
		}
		var coords []geom.Coord
		if err := json.Unmarshal(*g.Coordinates, &coords); err != nil {
			return nil, err
		}
		layout, err := guessLayout1(coords)
		if err != nil {
			return nil, err
		}
		return geom.NewLineString(layout).SetCoords(coords)
	case "Polygon":
		if g.Coordinates == nil {
			return geom.NewPolygon(geom.NoLayout), nil
		}
		var coords [][]geom.Coord
		if err := json.Unmarshal(*g.Coordinates, &coords); err != nil {
			return nil, err
		}
		layout, err := guessLayout2(coords)
		if err != nil {
			return nil, err
		}
		return geom.NewPolygon(layout).SetCoords(coords)
	case "MultiPoint":
		if g.Coordinates == nil {
			return geom.NewMultiPoint(geom.NoLayout), nil
		}
		var coords []geom.Coord
		if err := json.Unmarshal(*g.Coordinates, &coords); err != nil {
			return nil, err
		}
		layout, err := guessLayout1(coords)
		if err != nil {
			return nil, err
		}
		return geom.NewMultiPoint(layout).SetCoords(coords)
	case "MultiLineString":
		if g.Coordinates == nil {
			return geom.NewMultiLineString(geom.NoLayout), nil
		}
		var coords [][]geom.Coord
		if err := json.Unmarshal(*g.Coordinates, &coords); err != nil {
			return nil, err
		}
		layout, err := guessLayout2(coords)
		if err != nil {
			return nil, err
		}
		return geom.NewMultiLineString(layout).SetCoords(coords)
	case "MultiPolygon":
		if g.Coordinates == nil {
			return geom.NewMultiPolygon(geom.NoLayout), nil
		}
		var coords [][][]geom.Coord
		if err := json.Unmarshal(*g.Coordinates, &coords); err != nil {
			return nil, err
		}
		layout, err := guessLayout3(coords)
		if err != nil {
			return nil, err
		}
		return geom.NewMultiPolygon(layout).SetCoords(coords)
	case "GeometryCollection":
		var geometries []Geometry
		if g.Geometries != nil {
			err := json.Unmarshal(*g.Geometries, &geometries)
			if err != nil {
				return nil, err
			}
		}
		geoms := make([]geom.T, len(geometries))
		for i, subGeometry := range geometries {
			var err error
			geoms[i], err = subGeometry.Decode()
			if err != nil {
				return nil, err
			}
		}
		gc := geom.NewGeometryCollection()
		if err := gc.Push(geoms...); err != nil {
			return nil, err
		}
		return gc, nil
	default:
		return nil, ErrUnsupportedType(g.Type)
	}
}

// EncodeGeometryOption applies extra metadata to the Geometry GeoJSON encoding.
type EncodeGeometryOption struct {
	onGeometryHandler func(*Geometry, geom.T, ...EncodeGeometryOption) error
	onFloat64Handler  func(interface{}) interface{}
}

// nestedFloat64WithMaxDecimalDigits is a wrapper around any nested array
// of float64s that will marshal into JSON with the maximum JSON digits.
type nestedFloat64WithMaxDecimalDigits struct {
	obj              interface{}
	maxDecimalDigits int
}

// MarshalJSON implements the json.Marshaller interface.
func (c *nestedFloat64WithMaxDecimalDigits) MarshalJSON() ([]byte, error) {
	return c.marshalJSON([]byte{}, reflect.ValueOf(c.obj))
}

// marshalJSON is a helper routine that recurses down slices of float64s,
// appending float64 to a JSON list structure.
func (c *nestedFloat64WithMaxDecimalDigits) marshalJSON(
	buf []byte, val reflect.Value,
) ([]byte, error) {
	switch val.Kind() {
	case reflect.Slice:
		buf = append(buf, '[')
		for i := 0; i < val.Len(); i++ {
			if i > 0 {
				buf = append(buf, ',')
			}
			var err error
			buf, err = c.marshalJSON(buf, val.Index(i))
			if err != nil {
				return nil, err
			}
		}
		buf = append(buf, ']')
	case reflect.Float64:
		//nolint:forcetypeassert
		buf = strconv.AppendFloat(buf, val.Interface().(float64), 'f', c.maxDecimalDigits, 64)
		if c.maxDecimalDigits > 0 {
			buf = bytes.TrimRight(bytes.TrimRight(buf, "0"), ".")
		}
	default:
		return nil, fmt.Errorf("unknown type of coord: %T", val)
	}
	return buf, nil
}

// encodeJSONFloat64WithMaxDecimalDigits is an option implementation that converts slices of float64s
// to round to the maxDecimalDigits if necessary.
func encodeJSONFloat64WithMaxDecimalDigits(maxDecimalDigits int) func(interface{}) interface{} {
	return func(obj interface{}) interface{} {
		return &nestedFloat64WithMaxDecimalDigits{obj: obj, maxDecimalDigits: maxDecimalDigits}
	}
}

// EncodeGeometryWithBBox adds a bbox field to the Geometry GeoJSON encoding.
func EncodeGeometryWithBBox() EncodeGeometryOption {
	return EncodeGeometryOption{
		onGeometryHandler: func(g *Geometry, t geom.T, opts ...EncodeGeometryOption) error {
			bounds := t.Bounds()
			if t.Empty() {
				bounds = geom.NewBounds(t.Layout())
			}
			bbox, err := encodeBBox(bounds)
			if err != nil {
				return err
			}
			var coords json.RawMessage
			var bboxIn interface{} = bbox
			for _, opt := range opts {
				if opt.onFloat64Handler != nil {
					bboxIn = opt.onFloat64Handler(bboxIn)
				}
			}
			coords, err = json.Marshal(bboxIn)
			if err != nil {
				return err
			}
			g.BBox = &coords
			return nil
		},
	}
}

// EncodeGeometryWithCRS adds the crs field to the Geometry GeoJSON encoding.
func EncodeGeometryWithCRS(crs *CRS) EncodeGeometryOption {
	return EncodeGeometryOption{
		onGeometryHandler: func(g *Geometry, t geom.T, opts ...EncodeGeometryOption) error {
			var err error
			g.CRS = crs
			return err
		},
	}
}

// EncodeGeometryWithMaxDecimalDigits encodes the Geometry with maximum decimal digits
// in the JSON representation.
func EncodeGeometryWithMaxDecimalDigits(maxDecimalDigits int) EncodeGeometryOption {
	return EncodeGeometryOption{
		onFloat64Handler: encodeJSONFloat64WithMaxDecimalDigits(maxDecimalDigits),
	}
}

// Encode encodes g as a GeoJSON geometry.
func Encode(g geom.T, opts ...EncodeGeometryOption) (*Geometry, error) {
	if g == nil {
		return nil, nil
	}
	ret, err := encode(g, opts...)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		if opt.onGeometryHandler != nil {
			if err := opt.onGeometryHandler(ret, g, opts...); err != nil {
				return nil, err
			}
		}
	}
	return ret, nil
}

// encode encodes the geometry assuming it is not nil.
func encode(g geom.T, opts ...EncodeGeometryOption) (*Geometry, error) {
	if g == nil {
		return nil, nil
	}
	switch g := g.(type) {
	case *geom.Point:
		var coords json.RawMessage
		var coordsIn interface{}
		if !g.Empty() {
			coordsIn = g.Coords()
		} else {
			coordsIn = []geom.Coord{}
		}
		for _, opt := range opts {
			if opt.onFloat64Handler != nil {
				coordsIn = opt.onFloat64Handler(coordsIn)
			}
		}
		var err error
		coords, err = json.Marshal(coordsIn)
		if err != nil {
			return nil, err
		}
		return &Geometry{
			Type:        "Point",
			Coordinates: &coords,
		}, nil
	case *geom.LineString:
		var coords json.RawMessage
		var coordsIn interface{} = g.Coords()
		for _, opt := range opts {
			if opt.onFloat64Handler != nil {
				coordsIn = opt.onFloat64Handler(coordsIn)
			}
		}
		coords, err := json.Marshal(coordsIn)
		if err != nil {
			return nil, err
		}
		return &Geometry{
			Type:        "LineString",
			Coordinates: &coords,
		}, nil
	case *geom.Polygon:
		var coords json.RawMessage
		var coordsIn interface{} = g.Coords()
		for _, opt := range opts {
			if opt.onFloat64Handler != nil {
				coordsIn = opt.onFloat64Handler(coordsIn)
			}
		}
		coords, err := json.Marshal(coordsIn)
		if err != nil {
			return nil, err
		}
		return &Geometry{
			Type:        "Polygon",
			Coordinates: &coords,
		}, nil
	case *geom.MultiPoint:
		var coords json.RawMessage
		var coordsIn interface{} = g.Coords()
		for _, opt := range opts {
			if opt.onFloat64Handler != nil {
				coordsIn = opt.onFloat64Handler(coordsIn)
			}
		}
		coords, err := json.Marshal(coordsIn)
		if err != nil {
			return nil, err
		}
		return &Geometry{
			Type:        "MultiPoint",
			Coordinates: &coords,
		}, nil
	case *geom.MultiLineString:
		var coords json.RawMessage
		var coordsIn interface{} = g.Coords()
		for _, opt := range opts {
			if opt.onFloat64Handler != nil {
				coordsIn = opt.onFloat64Handler(coordsIn)
			}
		}
		coords, err := json.Marshal(coordsIn)
		if err != nil {
			return nil, err
		}
		return &Geometry{
			Type:        "MultiLineString",
			Coordinates: &coords,
		}, nil
	case *geom.MultiPolygon:
		var coords json.RawMessage
		var coordsIn interface{} = g.Coords()
		for _, opt := range opts {
			if opt.onFloat64Handler != nil {
				coordsIn = opt.onFloat64Handler(coordsIn)
			}
		}
		coords, err := json.Marshal(coordsIn)
		if err != nil {
			return nil, err
		}
		return &Geometry{
			Type:        "MultiPolygon",
			Coordinates: &coords,
		}, nil
	case *geom.GeometryCollection:
		var marshalledGeometries json.RawMessage
		geometries := make([]*Geometry, len(g.Geoms()))
		for i, subGeometry := range g.Geoms() {
			var err error
			geometries[i], err = encode(subGeometry, opts...)
			if err != nil {
				return nil, err
			}
		}
		marshalledGeometries, err := json.Marshal(geometries)
		if err != nil {
			return nil, err
		}
		return &Geometry{
			Type:       "GeometryCollection",
			Geometries: &marshalledGeometries,
		}, nil
	default:
		return nil, geom.ErrUnsupportedType{Value: g}
	}
}

// Marshal marshals an arbitrary geometry to a []byte.
func Marshal(g geom.T, opts ...EncodeGeometryOption) ([]byte, error) {
	if g == nil {
		return nullGeometry, nil
	}
	geojson, err := Encode(g, opts...)
	if err != nil {
		return nil, err
	}
	return json.Marshal(geojson)
}

// Unmarshal unmarshalls a []byte to an arbitrary geometry.
func Unmarshal(data []byte, g *geom.T) error {
	if bytes.Equal(data, nullGeometry) {
		*g = nil
		return nil
	}
	// FIXME The following lint error is suppressed, but there is probably a genuine error here
	//
	//nolint:staticcheck
	gg := &Geometry{}
	if err := json.Unmarshal(data, gg); err != nil {
		return err
	}
	//nolint:staticcheck
	if gg == nil {
		*g = nil
		return nil
	}
	var err error
	*g, err = gg.Decode()
	return err
}

// decodeBBox decodes bb into a Bounds.
func decodeBBox(bb []float64) (*geom.Bounds, error) {
	var layout geom.Layout
	switch l := len(bb); l {
	case 4:
		layout = geom.XY
	case 6:
		layout = geom.XYZ
	default:
		return nil, ErrDimensionalityTooLow(l)
	}

	return geom.NewBounds(layout).Set(bb...), nil
}

// encodeBBox encodes b as a GeoJson Bounding Box.
func encodeBBox(b *geom.Bounds) ([]float64, error) {
	switch l := b.Layout(); l {
	case geom.XY, geom.XYM:
		return []float64{b.Min(0), b.Min(1), b.Max(0), b.Max(1)}, nil
	case geom.XYZ, geom.XYZM:
		return []float64{
			b.Min(0), b.Min(1), b.Min(2),
			b.Max(0), b.Max(1), b.Max(2),
		}, nil
	default:
		return []float64{}, ErrUnsupportedType(rune(l))
	}
}

// MarshalJSON implements json.Marshaler.MarshalJSON.
func (f *Feature) MarshalJSON() ([]byte, error) {
	geometry, err := Encode(f.Geometry)
	if err != nil {
		return nil, err
	}

	var bounds []float64
	if f.BBox != nil {
		bounds, err = encodeBBox(f.BBox)
		if err != nil {
			return nil, err
		}
	}

	return json.Marshal(&geojsonFeature{
		ID:         f.ID,
		Type:       "Feature",
		BBox:       bounds,
		Geometry:   geometry,
		Properties: f.Properties,
	})
}

// UnmarshalJSON implements json.Unmarshaler.UnmarshalJSON.
func (f *Feature) UnmarshalJSON(data []byte) error {
	var gf geojsonFeature
	if err := json.Unmarshal(data, &gf); err != nil {
		return err
	}
	if gf.Type != "Feature" {
		return ErrUnsupportedType(gf.Type)
	}
	f.ID = gf.ID
	var err error
	if gf.BBox != nil {
		f.BBox, err = decodeBBox(gf.BBox)
	}
	if err != nil {
		return err
	}
	f.Geometry, err = gf.Geometry.Decode()
	if err != nil {
		return err
	}
	f.Properties = gf.Properties
	return nil
}

// MarshalJSON implements json.Marshaler.MarshalJSON.
func (fc *FeatureCollection) MarshalJSON() ([]byte, error) {
	gfc := &geojsonFeatureCollection{
		Type:     "FeatureCollection",
		Features: fc.Features,
	}

	if fc.BBox != nil {
		bounds, err := encodeBBox(fc.BBox)
		if err != nil {
			return nil, err
		}
		gfc.BBox = bounds
	}

	if gfc.Features == nil {
		gfc.Features = []*Feature{}
	}
	return json.Marshal(gfc)
}

// UnmarshalJSON implements json.Unmarshaler.UnmarshalJSON.
func (fc *FeatureCollection) UnmarshalJSON(data []byte) error {
	var gfc geojsonFeatureCollection
	if err := json.Unmarshal(data, &gfc); err != nil {
		return err
	}
	var err error
	if gfc.BBox != nil {
		fc.BBox, err = decodeBBox(gfc.BBox)
		if err != nil {
			return err
		}
	}
	if gfc.Type != "FeatureCollection" {
		return ErrUnsupportedType(gfc.Type)
	}
	fc.Features = gfc.Features
	return nil
}
