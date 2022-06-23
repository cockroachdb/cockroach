package wkt

import (
	"strconv"
	"strings"

	"github.com/twpayne/go-geom"
)

// Encode translates a geometry to the corresponding WKT.
func (e *Encoder) Encode(g geom.T) (string, error) {
	sb := &strings.Builder{}
	if err := e.write(sb, g); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func (e *Encoder) write(sb *strings.Builder, g geom.T) error {
	var typeString string
	switch g := g.(type) {
	case *geom.Point:
		typeString = tPoint
	case *geom.LineString, *geom.LinearRing:
		typeString = tLineString
	case *geom.Polygon:
		typeString = tPolygon
	case *geom.MultiPoint:
		typeString = tMultiPoint
	case *geom.MultiLineString:
		typeString = tMultiLineString
	case *geom.MultiPolygon:
		typeString = tMultiPolygon
	case *geom.GeometryCollection:
		typeString = tGeometryCollection
	default:
		return geom.ErrUnsupportedType{Value: g}
	}
	layout := g.Layout()
	switch layout {
	case geom.NoLayout:
		// Special case for empty GeometryCollections
		if g, ok := g.(*geom.GeometryCollection); !ok || !g.Empty() {
			return geom.ErrUnsupportedLayout(layout)
		}
	case geom.XY:
	case geom.XYZ:
		typeString += tZ
	case geom.XYM:
		typeString += tM
	case geom.XYZM:
		typeString += tZm
	default:
		return geom.ErrUnsupportedLayout(layout)
	}
	if _, err := sb.WriteString(typeString); err != nil {
		return err
	}
	switch g := g.(type) {
	case *geom.Point:
		if g.Empty() {
			return e.writeEMPTY(sb)
		}
		return e.writeFlatCoords0(sb, g.FlatCoords(), layout.Stride())
	case *geom.LineString:
		if g.Empty() {
			return e.writeEMPTY(sb)
		}
		return e.writeFlatCoords1(sb, g.FlatCoords(), layout.Stride())
	case *geom.LinearRing:
		if g.Empty() {
			return e.writeEMPTY(sb)
		}
		return e.writeFlatCoords1(sb, g.FlatCoords(), layout.Stride())
	case *geom.Polygon:
		if g.Empty() {
			return e.writeEMPTY(sb)
		}
		return e.writeFlatCoords2(sb, g.FlatCoords(), 0, g.Ends(), layout.Stride())
	case *geom.MultiPoint:
		if g.NumPoints() == 0 {
			return e.writeEMPTY(sb)
		}
		return e.writeFlatCoords1Ends(sb, g.FlatCoords(), 0, g.Ends())
	case *geom.MultiLineString:
		if g.NumLineStrings() == 0 {
			return e.writeEMPTY(sb)
		}
		return e.writeFlatCoords2(sb, g.FlatCoords(), 0, g.Ends(), layout.Stride())
	case *geom.MultiPolygon:
		if g.NumPolygons() == 0 {
			return e.writeEMPTY(sb)
		}
		return e.writeFlatCoords3(sb, g.FlatCoords(), g.Endss(), layout.Stride())
	case *geom.GeometryCollection:
		if g.NumGeoms() == 0 {
			return e.writeEMPTY(sb)
		}
		if _, err := sb.WriteRune('('); err != nil {
			return err
		}
		for i, g := range g.Geoms() {
			if i != 0 {
				if _, err := sb.WriteString(", "); err != nil {
					return err
				}
			}
			if err := e.write(sb, g); err != nil {
				return err
			}
		}
		_, err := sb.WriteRune(')')
		return err
	}
	return nil
}

func (e *Encoder) writeCoord(sb *strings.Builder, coord []float64) error {
	for i, x := range coord {
		if i != 0 {
			if _, err := sb.WriteRune(' '); err != nil {
				return err
			}
		}
		coordStr := strconv.FormatFloat(x, 'f', e.maxDecimalDigits, 64)
		if e.maxDecimalDigits > 0 {
			coordStr = strings.TrimRight(strings.TrimRight(coordStr, "0"), ".")
		}
		if _, err := sb.WriteString(coordStr); err != nil {
			return err
		}
	}
	return nil
}

//nolint:interfacer
func (e *Encoder) writeEMPTY(sb *strings.Builder) error {
	_, err := sb.WriteString(tEmpty)
	return err
}

func (e *Encoder) writeFlatCoords0(sb *strings.Builder, flatCoords []float64, stride int) error {
	if _, err := sb.WriteRune('('); err != nil {
		return err
	}
	if err := e.writeCoord(sb, flatCoords[:stride]); err != nil {
		return err
	}
	_, err := sb.WriteRune(')')
	return err
}

func (e *Encoder) writeFlatCoords1(sb *strings.Builder, flatCoords []float64, stride int) error {
	if _, err := sb.WriteRune('('); err != nil {
		return err
	}
	for i, n := 0, len(flatCoords); i < n; i += stride {
		if i != 0 {
			if _, err := sb.WriteString(", "); err != nil {
				return err
			}
		}
		if err := e.writeCoord(sb, flatCoords[i:i+stride]); err != nil {
			return err
		}
	}
	_, err := sb.WriteRune(')')
	return err
}

func (e *Encoder) writeFlatCoords1Ends(
	sb *strings.Builder, flatCoords []float64, start int, ends []int,
) error {
	if _, err := sb.WriteRune('('); err != nil {
		return err
	}
	for i, end := range ends {
		if i != 0 {
			if _, err := sb.WriteString(", "); err != nil {
				return err
			}
		}
		if end <= start {
			if err := e.writeEMPTY(sb); err != nil {
				return err
			}
		} else {
			if err := e.writeCoord(sb, flatCoords[start:end]); err != nil {
				return err
			}
		}
		start = end
	}
	_, err := sb.WriteRune(')')
	return err
}

func (e *Encoder) writeFlatCoords2(
	sb *strings.Builder, flatCoords []float64, start int, ends []int, stride int,
) error {
	if _, err := sb.WriteRune('('); err != nil {
		return err
	}
	for i, end := range ends {
		if i != 0 {
			if _, err := sb.WriteString(", "); err != nil {
				return err
			}
		}
		if end <= start {
			if err := e.writeEMPTY(sb); err != nil {
				return err
			}
		} else {
			if err := e.writeFlatCoords1(sb, flatCoords[start:end], stride); err != nil {
				return err
			}
		}
		start = end
	}
	_, err := sb.WriteRune(')')
	return err
}

func (e *Encoder) writeFlatCoords3(
	sb *strings.Builder, flatCoords []float64, endss [][]int, stride int,
) error {
	if _, err := sb.WriteRune('('); err != nil {
		return err
	}
	start := 0
	for i, ends := range endss {
		if i != 0 {
			if _, err := sb.WriteString(", "); err != nil {
				return err
			}
		}
		if len(ends) == 0 {
			if err := e.writeEMPTY(sb); err != nil {
				return err
			}
		} else {
			if err := e.writeFlatCoords2(sb, flatCoords, start, ends, stride); err != nil {
				return err
			}
			start = ends[len(ends)-1]
		}
	}
	_, err := sb.WriteRune(')')
	return err
}
