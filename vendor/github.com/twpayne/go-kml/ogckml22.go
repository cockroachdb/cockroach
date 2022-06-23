package kml

import (
	"encoding/xml"
	"io"
	"strconv"
)

// Namespace is the default namespace.
const Namespace = "http://www.opengis.net/kml/2.2"

var (
	coordinatesStartElement = xml.StartElement{
		Name: xml.Name{
			Local: "coordinates",
		},
	}
	coordinatesEndElement = coordinatesStartElement.End()
)

// A Coordinate represents a single geographical coordinate.
// Lon and Lat are in degrees, Alt is in meters.
type Coordinate struct {
	Lon, Lat, Alt float64
}

// A Vec2 represents a screen position.
type Vec2 struct {
	X, Y           float64
	XUnits, YUnits UnitsEnum
}

// CoordinatesElement is a coordinates element.
type CoordinatesElement struct {
	coordinates []Coordinate
}

// CoordinatesArrayElement is a coordinates element.
type CoordinatesArrayElement struct {
	coordinates [][]float64
}

// CoordinatesFlatElement is a coordinates element.
type CoordinatesFlatElement struct {
	flatCoords               []float64
	offset, end, stride, dim int
}

// MarshalXML marshals ce to e. start is ignored.
func (ce *CoordinatesElement) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeToken(coordinatesStartElement); err != nil {
		return err
	}
	for i, c := range ce.coordinates {
		s := ""
		if i != 0 {
			s = " "
		}
		s += strconv.FormatFloat(c.Lon, 'f', -1, 64) + "," + strconv.FormatFloat(c.Lat, 'f', -1, 64)
		if c.Alt != 0 {
			s += "," + strconv.FormatFloat(c.Alt, 'f', -1, 64)
		}
		if err := e.EncodeToken(xml.CharData([]byte(s))); err != nil {
			return err
		}
	}
	return e.EncodeToken(coordinatesEndElement)
}

// Write writes an XML header and ce to w.
func (ce *CoordinatesElement) Write(w io.Writer) error {
	return write(w, "", "  ", ce)
}

// WriteIndent writes an XML header and ce to w.
func (ce *CoordinatesElement) WriteIndent(w io.Writer, prefix, indent string) error {
	return write(w, prefix, indent, ce)
}

// MarshalXML marshals cae to e. start is ignored.
func (cae *CoordinatesArrayElement) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeToken(coordinatesStartElement); err != nil {
		return err
	}
	for i, c := range cae.coordinates {
		s := ""
		if i != 0 {
			s = " "
		}
		s += strconv.FormatFloat(c[0], 'f', -1, 64) + "," + strconv.FormatFloat(c[1], 'f', -1, 64)
		if len(c) > 2 && c[2] != 0 {
			s += "," + strconv.FormatFloat(c[2], 'f', -1, 64)
		}
		if err := e.EncodeToken(xml.CharData([]byte(s))); err != nil {
			return err
		}
	}
	return e.EncodeToken(coordinatesEndElement)
}

// Write writes an XML header and cae to w.
func (cae *CoordinatesArrayElement) Write(w io.Writer) error {
	return write(w, "", "  ", cae)
}

// WriteIndent writes an XML header and cae to w.
func (cae *CoordinatesArrayElement) WriteIndent(w io.Writer, prefix, indent string) error {
	return write(w, prefix, indent, cae)
}

// MarshalXML marshals cfe to e. start is ignored.
func (cfe *CoordinatesFlatElement) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeToken(coordinatesStartElement); err != nil {
		return err
	}
	for i := cfe.offset; i < cfe.end; i += cfe.stride {
		s := ""
		if i != cfe.offset {
			s = " "
		}
		s += strconv.FormatFloat(cfe.flatCoords[i], 'f', -1, 64) + "," + strconv.FormatFloat(cfe.flatCoords[i+1], 'f', -1, 64)
		if cfe.dim > 2 && cfe.flatCoords[i+2] != 0 {
			s += "," + strconv.FormatFloat(cfe.flatCoords[i+2], 'f', -1, 64)
		}
		if err := e.EncodeToken(xml.CharData([]byte(s))); err != nil {
			return err
		}
	}
	return e.EncodeToken(coordinatesEndElement)
}

// Write writes an XML header and cfe to w.
func (cfe *CoordinatesFlatElement) Write(w io.Writer) error {
	return write(w, "", "  ", cfe)
}

// WriteIndent writes an XML header and cfe to w.
func (cfe *CoordinatesFlatElement) WriteIndent(w io.Writer, prefix, indent string) error {
	return write(w, prefix, indent, cfe)
}

// Coordinates returns a new CoordinatesElement.
func Coordinates(value ...Coordinate) *CoordinatesElement {
	return &CoordinatesElement{coordinates: value}
}

// CoordinatesArray returns a new CoordinatesArrayElement.
func CoordinatesArray(value ...[]float64) *CoordinatesArrayElement {
	return &CoordinatesArrayElement{coordinates: value}
}

// CoordinatesFlat returns a new Coordinates element from flat coordinates.
func CoordinatesFlat(flatCoords []float64, offset, end, stride, dim int) *CoordinatesFlatElement {
	return &CoordinatesFlatElement{
		flatCoords: flatCoords,
		offset:     offset,
		end:        end,
		stride:     stride,
		dim:        dim,
	}
}

// LinkSnippet returns a new linkSnippet element.
func LinkSnippet(maxLines int, value string) *SimpleElement {
	return &SimpleElement{
		StartElement: xml.StartElement{
			Name: xml.Name{Local: "linkSnippet"},
			Attr: []xml.Attr{
				{Name: xml.Name{Local: "maxLines"}, Value: strconv.Itoa(maxLines)},
			},
		},
		value: value,
	}
}

// Schema returns a new Schema element.
func Schema(id, name string, children ...Element) *SharedElement {
	return &SharedElement{
		CompoundElement: CompoundElement{
			StartElement: xml.StartElement{
				Name: xml.Name{Local: "Schema"},
				Attr: []xml.Attr{
					{Name: xml.Name{Local: "id"}, Value: id},
					{Name: xml.Name{Local: "name"}, Value: name},
				},
			},
			children: children,
		},
		id: id,
	}
}

// SchemaData returns a new SchemaData element.
func SchemaData(schemaURL string, children ...Element) *CompoundElement {
	return &CompoundElement{
		StartElement: xml.StartElement{
			Name: xml.Name{Local: "SchemaData"},
			Attr: []xml.Attr{
				{Name: xml.Name{Local: "schemaUrl"}, Value: schemaURL},
			},
		},
		children: children,
	}
}

// SharedStyle returns a new shared Style element.
func SharedStyle(id string, children ...Element) *SharedElement {
	return newSharedE("Style", id, children)
}

// SharedStyleMap returns a new shared StyleMap element.
func SharedStyleMap(id string, children ...Element) *SharedElement {
	return newSharedE("StyleMap", id, children)
}

// SimpleData returns a new SimpleData element.
func SimpleData(name, value string) *SimpleElement {
	return &SimpleElement{
		StartElement: xml.StartElement{
			Name: xml.Name{Local: "SimpleData"},
			Attr: []xml.Attr{
				{Name: xml.Name{Local: "name"}, Value: name},
			},
		},
		value: value,
	}
}

// SimpleField returns a new SimpleField element.
func SimpleField(name, _type string, children ...Element) *CompoundElement {
	return &CompoundElement{
		StartElement: xml.StartElement{
			Name: xml.Name{Local: "SimpleField"},
			Attr: []xml.Attr{
				{Name: xml.Name{Local: "name"}, Value: name},
				{Name: xml.Name{Local: "type"}, Value: _type},
			},
		},
		children: children,
	}
}

// KML returns a new kml element.
func KML(child Element) *CompoundElement {
	return &CompoundElement{
		StartElement: xml.StartElement{
			Name: xml.Name{Space: Namespace, Local: "kml"},
		},
		children: []Element{child},
	}
}
