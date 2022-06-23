// Package wkbcommon contains code common to WKB and EWKB encoding.
package wkbcommon

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/twpayne/go-geom"
)

func readFloat(buf []byte, byteOrder binary.ByteOrder) float64 {
	u := byteOrder.Uint64(buf)
	return math.Float64frombits(u)
}

// ReadUInt32 reads a uint32 from r.
func ReadUInt32(r io.Reader, byteOrder binary.ByteOrder) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return byteOrder.Uint32(buf[:]), nil
}

// ReadFloatArray reads a []float64 from r.
func ReadFloatArray(r io.Reader, byteOrder binary.ByteOrder, array []float64) error {
	buf := make([]byte, 8*len(array))
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	// Convert to an array of floats
	for i := range array {
		array[i] = readFloat(buf[8*i:], byteOrder)
	}
	return nil
}

// ReadByte reads a byte from r.
func ReadByte(r io.Reader) (byte, error) {
	var buf [1]byte
	if _, err := r.Read(buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func writeFloat(buf []byte, byteOrder binary.ByteOrder, value float64) {
	u := math.Float64bits(value)
	byteOrder.PutUint64(buf, u)
}

// WriteFloatArray writes a []float64 to w.
func WriteFloatArray(w io.Writer, byteOrder binary.ByteOrder, array []float64) error {
	buf := make([]byte, 8*len(array))
	for i, f := range array {
		writeFloat(buf[8*i:], byteOrder, f)
	}
	_, err := w.Write(buf)
	return err
}

// WriteUInt32 writes a uint32 to w.
func WriteUInt32(w io.Writer, byteOrder binary.ByteOrder, value uint32) error {
	var buf [4]byte
	byteOrder.PutUint32(buf[:], value)
	_, err := w.Write(buf[:])
	return err
}

// WriteByte wrties a byte to w.
func WriteByte(w io.Writer, value byte) error {
	var buf [1]byte
	buf[0] = value
	_, err := w.Write(buf[:])
	return err
}

// WriteEmptyPointAsNaN outputs EmptyPoint as NaN values.
func WriteEmptyPointAsNaN(w io.Writer, byteOrder binary.ByteOrder, numCoords int) error {
	coords := make([]float64, numCoords)
	for i := 0; i < numCoords; i++ {
		coords[i] = geom.PointEmptyCoord()
	}
	return WriteFlatCoords0(w, byteOrder, coords)
}
