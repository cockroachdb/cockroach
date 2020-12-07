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

func WordSliceFromByteSlice(b []byte) []big.Word {
	return *(*[]big.Word)(newSliceHeaderFromBytes(b, bigWordSize))
}

// FlatDecimalLen returns the number of bytes in the flat-bytes encoded version
// of the input decimal.
func FlatDecimalLen(decimal *apd.Decimal) int {
	return WordLen(decimal.Coeff.Bits())
}

const decimalSize = unsafe.Sizeof(apd.Decimal{})

// UnsafeCastDecimal interprets an arbitrary byte slice as an apd.Decimal pointer.
// Use with caution
func UnsafeCastDecimal(b []byte) *apd.Decimal {
	return (*apd.Decimal)(unsafe.Pointer(&b[0]))
}

// absOffset is essentially unsafe.OffsetOf(big.Int{}.abs).
var absOffset uintptr

func init() {
	// Get the offset of the `abs` field on big.Int. We have to do this in in such
	// a roundabout way because `abs` is an exported field, so we can't just use
	// unsafe.OffsetOf as normal.
	v := reflect.TypeOf(big.Int{})
	y, ok := v.FieldByName("abs")
	if !ok {
		panic("uhoh, looks like big.Int changed its binary layout...")
	}
	absOffset = y.Offset
}

// UnsafeGetAbsPtr gets an unsafe.Pointer pointed at the `abs` field of a
// big.Int
func UnsafeGetAbsPtr(b *big.Int) unsafe.Pointer {
	return unsafe.Pointer(uintptr(unsafe.Pointer(b)) + absOffset)
}

// EncodeFlatDecimal encodes the input decimal into the input byte slice.
// A "Flat Decimal" is a bytes representation of an apd.Decimal. It is organized
// as follows:
//
// |  apd.Decimal | coeff bytes |
//
//    decimalSize   n bytes
//
// The point of this method is to take an arbitrary apd.Decimal pointer, which looks like this:
// a: { scalar, fields, here, heapptr -> x}     x: [some bytes]
//
// and convert into a flat representation directly on appendTo, which will look
// like this:
//
// a: { scalar, fields, here, heapptr -> a+sizeof(apd.Decimal{})}   a+sizeof(apd.Decimal{}): [some bytes]
//
func EncodeFlatDecimal(decimal *apd.Decimal, appendTo []byte) []byte {
	coeffWords := decimal.Coeff.Bits()
	// Get the number of bytes that we'll need to reserve space for to store the
	// entire coefficient of the decimal. We need to reserve space for the
	// coefficient all at once, to prevent copy() from getting flummoxed by
	// running off the end of the slice its handed.
	nCoeffBytes := WordLen(coeffWords)

	decimalPtr := unsafe.Pointer(decimal)
	byteSlice := (*(*[decimalSize]byte)(decimalPtr))[:]

	totalLen := int(decimalSize) + nCoeffBytes
	origLen := len(appendTo)
	if cap(appendTo) < len(appendTo)+totalLen {
		appendTo = append(appendTo, make([]byte, totalLen)...)
	}
	appendTo = appendTo[:origLen]

	appendTo = append(appendTo, byteSlice...)

	if nCoeffBytes > 0 {
		decimal := UnsafeCastDecimal(appendTo[origLen:])
		coeffPtr := (*reflect.SliceHeader)(UnsafeGetAbsPtr(&decimal.Coeff))
		coeffBytes := byteSliceFromWordSlice(coeffWords)
		appendTo = append(appendTo, coeffBytes...)
		coeffPtr.Data = uintptr(unsafe.Pointer(&appendTo[origLen+int(decimalSize)]))
	}

	/*
		if !reflect.DeepEqual(returned, decimal) {
			panic(fmt.Sprintf("Hmm... %v != %v", *returned, *decimal))
		}
	*/

	return appendTo
}

// DecodeFlatDecimal decodes a flat-bytes decimal representation into an apd.Decimal,
// without doing any allocations. Because this flat-bytes representation never
// is serialized to the network or to disk, we can perform all slice accesses
// without worrying about out-of-bounds, since we've indubitably allocated
// sufficient space in the input byte slice in an earlier call to EncodeFlatDecimal.
func DecodeFlatDecimal(bytes []byte, decodeInto *apd.Decimal) {
	d := UnsafeCastDecimal(bytes)
	*decodeInto = *d
}
