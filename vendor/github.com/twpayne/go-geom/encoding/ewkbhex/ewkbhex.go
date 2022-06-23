// Package ewkbhex implements Extended Well Known Binary encoding and decoding of
// strings.
package ewkbhex

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
)

var (
	// XDR is big endian.
	XDR = ewkb.XDR
	// NDR is little endian.
	NDR = ewkb.NDR
)

// Encode encodes an arbitrary geometry to a string.
func Encode(g geom.T, byteOrder binary.ByteOrder) (string, error) {
	ewkb, err := ewkb.Marshal(g, byteOrder)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(ewkb), nil
}

// Decode decodes an arbitrary geometry from a string.
func Decode(s string) (geom.T, error) {
	data, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return ewkb.Unmarshal(data)
}
