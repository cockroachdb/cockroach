// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geo

import (
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

const (
	// defaultLatLonFormat is the default PostGIS format string for ST_AsLatLonText.
	defaultLatLonFormat = `D°M'S.SSS"C`
	// maxLatLonFormatLength matches PostGIS WORK_SIZE (1024).
	maxLatLonFormatLength = 1024
	// maxLatLonPieces is the maximum number of pieces in a valid format:
	// up to 4 tokens (D, M, S, C) and 5 surrounding literal segments.
	maxLatLonPieces = 9
)

var defaultParsedLatLonFormat = mustParseDefaultLatLonFormat()

func mustParseDefaultLatLonFormat() latLonFormat {
	f, err := parseLatLonFormat(defaultLatLonFormat)
	if err != nil {
		panic(err)
	}
	return f
}

// latLonPieceKind identifies the kind of output segment in a parsed
// ST_AsLatLonText format template.
type latLonPieceKind uint8

const (
	pieceLiteral latLonPieceKind = iota + 1
	pieceDeg
	pieceMin
	pieceSec
	pieceCompass
)

// latLonPiece is a single ordered segment in a parsed format template. When
// kind is pieceLiteral, literal holds a substring of the original format string.
type latLonPiece struct {
	kind    latLonPieceKind
	literal string
}

// dmsFieldSpec records the integer width (digits) and fractional precision
// (decDigits) parsed from a D, M, or S token run. A digits value of 0 indicates
// that the token was omitted from the format template.
type dmsFieldSpec struct {
	digits    int
	decDigits int
}

// latLonFormat is a parsed, validated ST_AsLatLonText format template created
// by parseLatLonFormat and consumed by writeCoord. pieces[0:numPieces] stores
// output segments in display order, deg/min/sec describe the DMS token specs,
// and hasCompass reports whether a C cardinal-direction token was included.
type latLonFormat struct {
	pieces     [maxLatLonPieces]latLonPiece
	numPieces  int
	deg        dmsFieldSpec
	min        dmsFieldSpec
	sec        dmsFieldSpec
	hasCompass bool
}

// GeometryToLatLonText returns a Point geometry formatted as latitude/longitude
// text using the given template (matching PostGIS ST_AsLatLonText behavior).
// An empty format string uses the default template D°M'S.SSS"C.
func GeometryToLatLonText(g Geometry, format string) (string, error) {
	if g.ShapeType2D() != geopb.ShapeType_Point {
		return "", pgerror.Newf(
			pgcode.InvalidParameterValue,
			"only points are supported, got %s",
			g.ShapeType(),
		)
	}
	if g.Empty() {
		return "", pgerror.New(
			pgcode.InvalidParameterValue,
			"cannot convert an empty point into formatted text",
		)
	}

	t, err := g.AsGeomT()
	if err != nil {
		return "", err
	}
	pt, ok := t.(*geom.Point)
	if !ok || pt.Empty() {
		return "", errors.AssertionFailedf("expected non-empty *geom.Point, got %T", t)
	}
	for _, c := range pt.FlatCoords() {
		if math.IsNaN(c) || math.IsInf(c, 0) {
			return "", pgerror.New(
				pgcode.InvalidParameterValue,
				"invalid coordinate",
			)
		}
	}

	var parsedFmt latLonFormat
	if len(format) == 0 {
		parsedFmt = defaultParsedLatLonFormat
	} else {
		parsedFmt, err = parseLatLonFormat(format)
		if err != nil {
			return "", err
		}
	}

	lat, lon := normalizeLatLonForText(pt.Y(), pt.X())

	var sb strings.Builder
	sb.Grow(len(format)*2 + 32)
	parsedFmt.writeCoord(&sb, lat, "N", "S")
	sb.WriteByte(' ')
	parsedFmt.writeCoord(&sb, lon, "E", "W")
	return sb.String(), nil
}

// normalizeLatLonForText normalizes latitude to [-90, 90] and longitude to
// [-180, 180], wrapping longitude by 180 degrees when latitude crosses a pole
// (matching PostGIS lwprint_normalize_latlon).
func normalizeLatLonForText(lat, lon float64) (float64, float64) {
	lat = math.Mod(lat, 360)
	if lat > 270 {
		lat -= 360
	} else if lat < -270 {
		lat += 360
	}

	lon = math.Mod(lon, 360)
	if lat > 90 {
		lat = 180 - lat
		lon += 180
	} else if lat < -90 {
		lat = -180 - lat
		lon += 180
	}

	lon = math.Mod(lon, 360)
	if lon > 180 {
		lon -= 360
	} else if lon < -180 {
		lon += 360
	}

	// Canonicalize -0.0 to +0.0 so zero never prints with a leading minus sign.
	if lat == 0 {
		lat = 0
	}
	if lon == 0 {
		lon = 0
	}
	return lat, lon
}

func parseLatLonFormat(format string) (latLonFormat, error) {
	if len(format) > maxLatLonFormatLength {
		return latLonFormat{}, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"bad format, exceeds maximum length (%d)",
			maxLatLonFormatLength,
		)
	}
	if !utf8.ValidString(format) {
		return latLonFormat{}, pgerror.New(
			pgcode.InvalidParameterValue,
			"bad format, format string is not valid UTF-8",
		)
	}

	var f latLonFormat
	litStart := -1
	flushLit := func(end int) {
		if litStart >= 0 {
			f.pieces[f.numPieces] = latLonPiece{kind: pieceLiteral, literal: format[litStart:end]}
			f.numPieces++
			litStart = -1
		}
	}

	var curSpec *dmsFieldSpec
	inFrac := false
	handleDMS := func(idx int, kind latLonPieceKind, spec *dmsFieldSpec, label string) error {
		if curSpec == spec {
			if inFrac {
				spec.decDigits++
			} else {
				spec.digits++
			}
			return nil
		}
		if spec.digits > 0 {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"bad format, cannot include %s more than once",
				label,
			)
		}
		flushLit(idx)
		*spec = dmsFieldSpec{digits: 1}
		f.pieces[f.numPieces] = latLonPiece{kind: kind}
		f.numPieces++
		curSpec = spec
		inFrac = false
		return nil
	}

	for i := 0; i < len(format); i++ {
		switch format[i] {
		case 'D':
			if err := handleDMS(i, pieceDeg, &f.deg, "degrees (DD.DDD)"); err != nil {
				return latLonFormat{}, err
			}
		case 'M':
			if err := handleDMS(i, pieceMin, &f.min, "minutes (MM.MMM)"); err != nil {
				return latLonFormat{}, err
			}
		case 'S':
			if err := handleDMS(i, pieceSec, &f.sec, "seconds (SS.SSS)"); err != nil {
				return latLonFormat{}, err
			}
		case 'C':
			curSpec = nil
			if f.hasCompass {
				return latLonFormat{}, pgerror.New(
					pgcode.InvalidParameterValue,
					"bad format, cannot include compass dir (C) more than once",
				)
			}
			flushLit(i)
			f.hasCompass = true
			f.pieces[f.numPieces] = latLonPiece{kind: pieceCompass}
			f.numPieces++
		case '.':
			if curSpec != nil {
				inFrac = true
			} else if litStart < 0 {
				litStart = i
			}
		default:
			curSpec = nil
			if litStart < 0 {
				litStart = i
			}
		}
	}
	flushLit(len(format))

	if f.deg.digits == 0 {
		return latLonFormat{}, pgerror.New(
			pgcode.InvalidParameterValue,
			"bad format, degrees (DD.DDD) must be included",
		)
	}
	if f.sec.digits > 0 && f.min.digits == 0 {
		return latLonFormat{}, pgerror.New(
			pgcode.InvalidParameterValue,
			"bad format, cannot include seconds (SS.SSS) without including minutes (MM.MMM)",
		)
	}
	if f.deg.digits+f.deg.decDigits+2 > maxLatLonFormatLength {
		return latLonFormat{}, pgerror.New(
			pgcode.InvalidParameterValue,
			"bad format, degrees (DD.DDD) number of digits exceeds working limit",
		)
	}
	if f.min.digits+f.min.decDigits+2 > maxLatLonFormatLength {
		return latLonFormat{}, pgerror.New(
			pgcode.InvalidParameterValue,
			"bad format, minutes (MM.MMM) number of digits exceeds working limit",
		)
	}

	return f, nil
}

func (f *latLonFormat) writeCoord(sb *strings.Builder, val float64, posDir, negDir string) {
	isNegative := val < 0
	if isNegative {
		val = -val
	}

	degrees := val
	var minutes float64
	if f.min.digits > 0 {
		var frac float64
		degrees, frac = math.Modf(val)
		minutes = frac * 60
	}

	var secBuf [64]byte
	var secBytes []byte
	if f.sec.digits > 0 {
		var frac float64
		minutes, frac = math.Modf(minutes)
		seconds := frac * 60
		secBytes = strconv.AppendFloat(secBuf[:0], seconds, 'f', f.sec.decDigits, 64)
		if len(secBytes) >= 2 && secBytes[0] == '6' && secBytes[1] == '0' {
			minutes++
			secBytes = strconv.AppendFloat(secBuf[:0], 0, 'f', f.sec.decDigits, 64)
			if minutes >= 60 {
				degrees++
				minutes = 0
			}
		}
	}

	if !f.hasCompass && isNegative {
		degrees = -degrees
	}

	var numBuf [64]byte
	for i := 0; i < f.numPieces; i++ {
		p := f.pieces[i]
		switch p.kind {
		case pieceLiteral:
			sb.WriteString(p.literal)
		case pieceDeg:
			writePaddedFloat(sb, numBuf[:0], f.deg.digits, f.deg.decDigits, degrees)
		case pieceMin:
			writePaddedFloat(sb, numBuf[:0], f.min.digits, f.min.decDigits, minutes)
		case pieceSec:
			writePaddedBytes(sb, f.sec.digits, secBytes)
		case pieceCompass:
			if isNegative {
				sb.WriteString(negDir)
			} else {
				sb.WriteString(posDir)
			}
		}
	}
}

func writePaddedFloat(sb *strings.Builder, scratch []byte, width, prec int, val float64) {
	writePaddedBytes(sb, width, strconv.AppendFloat(scratch, val, 'f', prec, 64))
}

func writePaddedBytes(sb *strings.Builder, width int, b []byte) {
	for pad := width - len(b); pad > 0; pad-- {
		sb.WriteByte(' ')
	}
	sb.Write(b)
}
