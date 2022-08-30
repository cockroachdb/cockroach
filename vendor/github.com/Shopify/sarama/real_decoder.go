package sarama

import (
	"encoding/binary"
	"math"
)

var (
	errInvalidArrayLength      = PacketDecodingError{"invalid array length"}
	errInvalidByteSliceLength  = PacketDecodingError{"invalid byteslice length"}
	errInvalidStringLength     = PacketDecodingError{"invalid string length"}
	errVarintOverflow          = PacketDecodingError{"varint overflow"}
	errUVarintOverflow         = PacketDecodingError{"uvarint overflow"}
	errInvalidBool             = PacketDecodingError{"invalid bool"}
	errUnsupportedTaggedFields = PacketDecodingError{"non-empty tagged fields are not supported yet"}
)

type realDecoder struct {
	raw   []byte
	off   int
	stack []pushDecoder
}

// primitives

func (rd *realDecoder) getInt8() (int8, error) {
	if rd.remaining() < 1 {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	}
	tmp := int8(rd.raw[rd.off])
	rd.off++
	return tmp, nil
}

func (rd *realDecoder) getInt16() (int16, error) {
	if rd.remaining() < 2 {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	}
	tmp := int16(binary.BigEndian.Uint16(rd.raw[rd.off:]))
	rd.off += 2
	return tmp, nil
}

func (rd *realDecoder) getInt32() (int32, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	}
	tmp := int32(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4
	return tmp, nil
}

func (rd *realDecoder) getInt64() (int64, error) {
	if rd.remaining() < 8 {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	}
	tmp := int64(binary.BigEndian.Uint64(rd.raw[rd.off:]))
	rd.off += 8
	return tmp, nil
}

func (rd *realDecoder) getVarint() (int64, error) {
	tmp, n := binary.Varint(rd.raw[rd.off:])
	if n == 0 {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	}
	if n < 0 {
		rd.off -= n
		return -1, errVarintOverflow
	}
	rd.off += n
	return tmp, nil
}

func (rd *realDecoder) getUVarint() (uint64, error) {
	tmp, n := binary.Uvarint(rd.raw[rd.off:])
	if n == 0 {
		rd.off = len(rd.raw)
		return 0, ErrInsufficientData
	}

	if n < 0 {
		rd.off -= n
		return 0, errUVarintOverflow
	}

	rd.off += n
	return tmp, nil
}

func (rd *realDecoder) getArrayLength() (int, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	}
	tmp := int(int32(binary.BigEndian.Uint32(rd.raw[rd.off:])))
	rd.off += 4
	if tmp > rd.remaining() {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	} else if tmp > 2*math.MaxUint16 {
		return -1, errInvalidArrayLength
	}
	return tmp, nil
}

func (rd *realDecoder) getCompactArrayLength() (int, error) {
	n, err := rd.getUVarint()
	if err != nil {
		return 0, err
	}

	if n == 0 {
		return 0, nil
	}

	return int(n) - 1, nil
}

func (rd *realDecoder) getBool() (bool, error) {
	b, err := rd.getInt8()
	if err != nil || b == 0 {
		return false, err
	}
	if b != 1 {
		return false, errInvalidBool
	}
	return true, nil
}

func (rd *realDecoder) getEmptyTaggedFieldArray() (int, error) {
	tagCount, err := rd.getUVarint()
	if err != nil {
		return 0, err
	}

	if tagCount != 0 {
		return 0, errUnsupportedTaggedFields
	}

	return 0, nil
}

// collections

func (rd *realDecoder) getBytes() ([]byte, error) {
	tmp, err := rd.getInt32()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}

	return rd.getRawBytes(int(tmp))
}

func (rd *realDecoder) getVarintBytes() ([]byte, error) {
	tmp, err := rd.getVarint()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}

	return rd.getRawBytes(int(tmp))
}

func (rd *realDecoder) getCompactBytes() ([]byte, error) {
	n, err := rd.getUVarint()
	if err != nil {
		return nil, err
	}

	length := int(n - 1)
	return rd.getRawBytes(length)
}

func (rd *realDecoder) getStringLength() (int, error) {
	length, err := rd.getInt16()
	if err != nil {
		return 0, err
	}

	n := int(length)

	switch {
	case n < -1:
		return 0, errInvalidStringLength
	case n > rd.remaining():
		rd.off = len(rd.raw)
		return 0, ErrInsufficientData
	}

	return n, nil
}

func (rd *realDecoder) getString() (string, error) {
	n, err := rd.getStringLength()
	if err != nil || n == -1 {
		return "", err
	}

	tmpStr := string(rd.raw[rd.off : rd.off+n])
	rd.off += n
	return tmpStr, nil
}

func (rd *realDecoder) getNullableString() (*string, error) {
	n, err := rd.getStringLength()
	if err != nil || n == -1 {
		return nil, err
	}

	tmpStr := string(rd.raw[rd.off : rd.off+n])
	rd.off += n
	return &tmpStr, err
}

func (rd *realDecoder) getCompactString() (string, error) {
	n, err := rd.getUVarint()
	if err != nil {
		return "", err
	}

	length := int(n - 1)

	tmpStr := string(rd.raw[rd.off : rd.off+length])
	rd.off += length
	return tmpStr, nil
}

func (rd *realDecoder) getCompactNullableString() (*string, error) {
	n, err := rd.getUVarint()
	if err != nil {
		return nil, err
	}

	length := int(n - 1)

	if length < 0 {
		return nil, err
	}

	tmpStr := string(rd.raw[rd.off : rd.off+length])
	rd.off += length
	return &tmpStr, err
}

func (rd *realDecoder) getCompactInt32Array() ([]int32, error) {
	n, err := rd.getUVarint()
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, nil
	}

	arrayLength := int(n) - 1

	ret := make([]int32, arrayLength)

	for i := range ret {
		ret[i] = int32(binary.BigEndian.Uint32(rd.raw[rd.off:]))
		rd.off += 4
	}
	return ret, nil
}

func (rd *realDecoder) getInt32Array() ([]int32, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.raw)
		return nil, ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4

	if rd.remaining() < 4*n {
		rd.off = len(rd.raw)
		return nil, ErrInsufficientData
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]int32, n)
	for i := range ret {
		ret[i] = int32(binary.BigEndian.Uint32(rd.raw[rd.off:]))
		rd.off += 4
	}
	return ret, nil
}

func (rd *realDecoder) getInt64Array() ([]int64, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.raw)
		return nil, ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4

	if rd.remaining() < 8*n {
		rd.off = len(rd.raw)
		return nil, ErrInsufficientData
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]int64, n)
	for i := range ret {
		ret[i] = int64(binary.BigEndian.Uint64(rd.raw[rd.off:]))
		rd.off += 8
	}
	return ret, nil
}

func (rd *realDecoder) getStringArray() ([]string, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.raw)
		return nil, ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]string, n)
	for i := range ret {
		str, err := rd.getString()
		if err != nil {
			return nil, err
		}

		ret[i] = str
	}
	return ret, nil
}

// subsets

func (rd *realDecoder) remaining() int {
	return len(rd.raw) - rd.off
}

func (rd *realDecoder) getSubset(length int) (packetDecoder, error) {
	buf, err := rd.getRawBytes(length)
	if err != nil {
		return nil, err
	}
	return &realDecoder{raw: buf}, nil
}

func (rd *realDecoder) getRawBytes(length int) ([]byte, error) {
	if length < 0 {
		return nil, errInvalidByteSliceLength
	} else if length > rd.remaining() {
		rd.off = len(rd.raw)
		return nil, ErrInsufficientData
	}

	start := rd.off
	rd.off += length
	return rd.raw[start:rd.off], nil
}

func (rd *realDecoder) peek(offset, length int) (packetDecoder, error) {
	if rd.remaining() < offset+length {
		return nil, ErrInsufficientData
	}
	off := rd.off + offset
	return &realDecoder{raw: rd.raw[off : off+length]}, nil
}

func (rd *realDecoder) peekInt8(offset int) (int8, error) {
	const byteLen = 1
	if rd.remaining() < offset+byteLen {
		return -1, ErrInsufficientData
	}
	return int8(rd.raw[rd.off+offset]), nil
}

// stacks

func (rd *realDecoder) push(in pushDecoder) error {
	in.saveOffset(rd.off)

	var reserve int
	if dpd, ok := in.(dynamicPushDecoder); ok {
		if err := dpd.decode(rd); err != nil {
			return err
		}
	} else {
		reserve = in.reserveLength()
		if rd.remaining() < reserve {
			rd.off = len(rd.raw)
			return ErrInsufficientData
		}
	}

	rd.stack = append(rd.stack, in)

	rd.off += reserve

	return nil
}

func (rd *realDecoder) pop() error {
	// this is go's ugly pop pattern (the inverse of append)
	in := rd.stack[len(rd.stack)-1]
	rd.stack = rd.stack[:len(rd.stack)-1]

	return in.check(rd.off, rd.raw)
}
