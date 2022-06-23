//go:generate goyacc -l -o wkt.gen.go -p wkt wkt.y
// TODO remove the following line if https://github.com/golang/tools/pull/304 is accepted
//go:generate sed -i -e "s/wktErrorVerbose = false/wktErrorVerbose = true/" wkt.gen.go
//go:generate gofumpt -extra -w wkt.gen.go

// Package wkt implements Well Known Text encoding and decoding.
package wkt

import (
	"errors"

	"github.com/twpayne/go-geom"
)

const (
	tPoint              = "POINT "
	tMultiPoint         = "MULTIPOINT "
	tLineString         = "LINESTRING "
	tMultiLineString    = "MULTILINESTRING "
	tPolygon            = "POLYGON "
	tMultiPolygon       = "MULTIPOLYGON "
	tGeometryCollection = "GEOMETRYCOLLECTION "
	tZ                  = "Z "
	tM                  = "M "
	tZm                 = "ZM "
	tEmpty              = "EMPTY"
)

// ErrBraceMismatch is returned when braces do not match.
var ErrBraceMismatch = errors.New("wkt: brace mismatch")

// Encoder encodes WKT based on specified parameters.
type Encoder struct {
	maxDecimalDigits int
}

// NewEncoder returns a new encoder with the given options set.
func NewEncoder(applyOptFns ...EncodeOption) *Encoder {
	encoder := &Encoder{
		maxDecimalDigits: -1,
	}
	for _, applyOptFn := range applyOptFns {
		applyOptFn(encoder)
	}
	return encoder
}

// An EncodeOption is an encoder option.
type EncodeOption func(*Encoder)

// EncodeOptionWithMaxDecimalDigits sets the maximum decimal digits to encode.
func EncodeOptionWithMaxDecimalDigits(maxDecimalDigits int) EncodeOption {
	return func(e *Encoder) {
		e.maxDecimalDigits = maxDecimalDigits
	}
}

// Marshal translates a geometry to the corresponding WKT.
func Marshal(g geom.T, applyOptFns ...EncodeOption) (string, error) {
	return NewEncoder(applyOptFns...).Encode(g)
}

// Unmarshal translates a WKT to the corresponding geometry.
func Unmarshal(wkt string) (geom.T, error) {
	wktlex := newWKTLex(wkt)
	wktParse(wktlex)
	if wktlex.lastErr != nil {
		return nil, wktlex.lastErr
	}
	return wktlex.ret, nil
}
