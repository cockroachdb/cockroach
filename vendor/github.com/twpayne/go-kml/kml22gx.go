package kml

import (
	"encoding/xml"
	"strconv"
)

// GxNamespace is the default namespace for Google Earth extensions.
const GxNamespace = "http://www.google.com/kml/ext/2.2"

// A GxOptionName is a gx:option name.
type GxOptionName string

// GxOptionNames.
const (
	GxOptionNameHistoricalImagery GxOptionName = "historicalimagery"
	GxOptionNameStreetView        GxOptionName = "streetview"
	GxOptionNameSunlight          GxOptionName = "sunlight"
)

// A GxAngle represents an angle.
type GxAngle struct {
	Heading, Tilt, Roll float64
}

// GxAngles returns a new gx:angles element.
func GxAngles(value GxAngle) *SimpleElement {
	return &SimpleElement{
		StartElement: xml.StartElement{
			Name: xml.Name{Local: "gx:angles"},
		},
		value: strconv.FormatFloat(value.Heading, 'f', -1, 64) + " " +
			strconv.FormatFloat(value.Tilt, 'f', -1, 64) + " " +
			strconv.FormatFloat(value.Roll, 'f', -1, 64),
	}
}

// GxCoord returns a new gx:coord element.
func GxCoord(value Coordinate) *SimpleElement {
	return &SimpleElement{
		StartElement: xml.StartElement{
			Name: xml.Name{Local: "gx:coord"},
		},
		value: strconv.FormatFloat(value.Lon, 'f', -1, 64) + " " +
			strconv.FormatFloat(value.Lat, 'f', -1, 64) + " " +
			strconv.FormatFloat(value.Alt, 'f', -1, 64),
	}
}

// GxKML returns a new kml element with Google Earth extensions.
func GxKML(child Element) *CompoundElement {
	kml := KML(child)
	// FIXME find a more correct way to do this
	kml.Attr = append(kml.Attr, xml.Attr{Name: xml.Name{Local: "xmlns:gx"}, Value: GxNamespace})
	return kml
}

// GxOption returns a new gx:option element.
func GxOption(name GxOptionName, enabled bool) *SimpleElement {
	return &SimpleElement{
		StartElement: xml.StartElement{
			Name: xml.Name{Local: "gx:option"},
			Attr: []xml.Attr{
				{Name: xml.Name{Local: "name"}, Value: string(name)},
				{Name: xml.Name{Local: "enabled"}, Value: strconv.FormatBool(enabled)},
			},
		},
	}
}

// GxSimpleArrayField returns a new gx:SimpleArrayField element.
func GxSimpleArrayField(name, _type string) *CompoundElement {
	return &CompoundElement{
		StartElement: xml.StartElement{
			Name: xml.Name{Local: "gx:SimpleArrayField"},
			Attr: []xml.Attr{
				{Name: xml.Name{Local: "name"}, Value: name},
				{Name: xml.Name{Local: "type"}, Value: _type},
			},
		},
	}
}
