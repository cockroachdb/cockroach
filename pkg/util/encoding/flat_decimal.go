// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package encoding

import (
	"encoding/binary"
	"math/big"
	"reflect"
	"unsafe"

	"github.com/cockroachdb/apd/v2"
)

// This snippet is taken from
// https://stackoverflow.com/questions/51332658/any-better-way-to-check-endianness-in-go/53286786#53286786
var nativeEndian binary.ByteOrder

func init() {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		nativeEndian = binary.LittleEndian
	case [2]byte{0xAB, 0xCD}:
		nativeEndian = binary.BigEndian
	default:
		panic("Could not determine native endianness.")
	}
}

func newSliceHeader(p unsafe.Pointer, size int) unsafe.Pointer {
	return unsafe.Pointer(&reflect.SliceHeader{
		Len:  size,
		Cap:  size,
		Data: uintptr(p),
	})
}

func newRawSliceHeader(sh *reflect.SliceHeader, b []byte, stride int) *reflect.SliceHeader {
	sh.Len = len(b) / stride
	sh.Cap = len(b) / stride
	sh.Data = (uintptr)(unsafe.Pointer(&b[0]))
	return sh
}

func newSliceHeaderFromBytes(b []byte, stride int) unsafe.Pointer {
	sh := &reflect.SliceHeader{}
	return unsafe.Pointer(newRawSliceHeader(sh, b, stride))
}

func byteSliceFromWordSlice(b []big.Word) []byte {
	return *(*[]byte)(newSliceHeader(unsafe.Pointer(&b[0]), len(b)*bigWordSize))
}

func wordSliceFromByteSlice(b []byte) []big.Word {
	return *(*[]big.Word)(newSliceHeaderFromBytes(b, bigWordSize))
}

// FlatDecimalLen returns the number of bytes in the flat-bytes encoded version
// of the input decimal.
func FlatDecimalLen(decimal *apd.Decimal) int {
	coeffWords := decimal.Coeff.Bits()
	headerLen, coeffBytes := flatDecimalLenFromBits(coeffWords)
	return headerLen + coeffBytes
}

func flatDecimalLenFromBits(bits []big.Word) (headerLen, coeffBytes int) {
	nCoeffBytes := WordLen(bits)
	// See below for the layout of a flat decimal.
	return 1 + 1 + (2 * int(unsafe.Sizeof(int32(0)))), nCoeffBytes
}

// EncodeFlatDecimal encodes the input decimal into the input byte slice.
// A "Flat Decimal" is a bytes representation of an apd.Decimal. It is organized
// as follows:
//
// |  Form   | Negative | Exponent | Length of coeff bytes | coeff bytes |
//
//   1 byte     1 byte     4 bytes      4 bytes: n            n bytes
func EncodeFlatDecimal(decimal *apd.Decimal, appendTo []byte) []byte {
	coeffWords := decimal.Coeff.Bits()
	// Get the number of bytes that we'll need to reserve space for to store the
	// entire coefficient of the decimal. We need to reserve space for the
	// coefficient all at once, to prevent copy() from getting flummoxed by
	// running off the end of the slice its handed.
	headerLen, nCoeffBytes := flatDecimalLenFromBits(coeffWords)
	nBytes := headerLen + nCoeffBytes

	l := len(appendTo)
	if cap(appendTo) < len(appendTo)+nBytes {
		appendTo = append(appendTo, make([]byte, nBytes)...)
	} else {
		appendTo = appendTo[:len(appendTo)+nBytes]
	}
	decimalSlice := appendTo[l:]

	decimalSlice[0] = byte(decimal.Form)
	var negativeByte byte
	if decimal.Negative {
		negativeByte = 1
	}
	decimalSlice[1] = negativeByte
	nativeEndian.PutUint32(decimalSlice[2:], uint32(decimal.Exponent))
	nativeEndian.PutUint32(decimalSlice[6:], uint32(nCoeffBytes))
	if nCoeffBytes > 0 {
		coeffBytes := byteSliceFromWordSlice(coeffWords)
		copy(decimalSlice[10:], coeffBytes)
	}
	return appendTo
}

// DecodeFlatDecimal decodes a flat-bytes decimal representation into an apd.Decimal,
// without doing any allocations. Because this flat-bytes representation never
// is serialized to the network or to disk, we can perform all slice accesses
// without worrying about out-of-bounds, since we've indubitably allocated
// sufficient space in the input byte slice in an earlier call to EncodeFlatDecimal.
func DecodeFlatDecimal(bytes []byte, decodeInto *apd.Decimal) []byte {
	form := apd.Form(bytes[0])
	negativeByte := int(bytes[1])
	exponent := int32(nativeEndian.Uint32(bytes[2:]))
	*decodeInto = apd.Decimal{
		Form:     form,
		Exponent: exponent,
	}
	if negativeByte == 1 {
		decodeInto.Negative = true
	}
	nCoeffBytes := int(nativeEndian.Uint32(bytes[6:]))
	if nCoeffBytes > 0 {
		decodeInto.Coeff.SetBits(wordSliceFromByteSlice(bytes[10 : 10+nCoeffBytes]))
	}
	return bytes[10+nCoeffBytes:]
}
