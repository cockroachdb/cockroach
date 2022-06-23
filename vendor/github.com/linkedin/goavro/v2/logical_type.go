// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"errors"
	"fmt"
	"math/big"
	"time"
)

type toNativeFn func([]byte) (interface{}, []byte, error)
type fromNativeFn func([]byte, interface{}) ([]byte, error)

//////////////////////////////////////////////////////////////////////////////////////////////
// date logical type - to/from time.Time, time.UTC location
//////////////////////////////////////////////////////////////////////////////////////////////
func nativeFromDate(fn toNativeFn) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		l, b, err := fn(bytes)
		if err != nil {
			return l, b, err
		}
		i, ok := l.(int32)
		if !ok {
			return l, b, fmt.Errorf("cannot transform to native date, expected int, received %T", l)
		}
		t := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(i)).UTC()
		return t, b, nil
	}
}

func dateFromNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		switch val := d.(type) {
		case int, int32, int64, float32, float64:
			// "Language implementations may choose to represent logical types with an appropriate native type, although this is not required."
			// especially permitted default values depend on the field's schema type and goavro encodes default values using the field schema
			return fn(b, val)

		case time.Time:
			// rephrasing the avro 1.9.2 spec a date is actually stored as the duration since unix epoch in days
			// time.Unix() returns this duration in seconds and time.UnixNano() in nanoseconds
			// reviewing the source code, both functions are based on the internal function unixSec()
			// unixSec() returns the seconds since unix epoch as int64, whereby Unix() provides the greater range and UnixNano() the higher precision
			// As a date requires a precision of days Unix() provides more then enough precision and a greater range, including the go zero time
			numDays := val.Unix() / 86400
			return fn(b, numDays)

		default:
			return nil, fmt.Errorf("cannot transform to binary date, expected time.Time or Go numeric, received %T", d)
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////
// time-millis logical type - to/from time.Time, time.UTC location
//////////////////////////////////////////////////////////////////////////////////////////////
func nativeFromTimeMillis(fn toNativeFn) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		l, b, err := fn(bytes)
		if err != nil {
			return l, b, err
		}
		i, ok := l.(int32)
		if !ok {
			return l, b, fmt.Errorf("cannot transform to native time.Duration, expected int, received %T", l)
		}
		t := time.Duration(i) * time.Millisecond
		return t, b, nil
	}
}

func timeMillisFromNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		switch val := d.(type) {
		case int, int32, int64, float32, float64:
			// "Language implementations may choose to represent logical types with an appropriate native type, although this is not required."
			// especially permitted default values depend on the field's schema type and goavro encodes default values using the field schema
			return fn(b, val)

		case time.Duration:
			duration := int32(val.Nanoseconds() / int64(time.Millisecond))
			return fn(b, duration)

		default:
			return nil, fmt.Errorf("cannot transform to binary time-millis, expected time.Duration or Go numeric, received %T", d)
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////
// time-micros logical type - to/from time.Time, time.UTC location
//////////////////////////////////////////////////////////////////////////////////////////////
func nativeFromTimeMicros(fn toNativeFn) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		l, b, err := fn(bytes)
		if err != nil {
			return l, b, err
		}
		i, ok := l.(int64)
		if !ok {
			return l, b, fmt.Errorf("cannot transform to native time.Duration, expected long, received %T", l)
		}
		t := time.Duration(i) * time.Microsecond
		return t, b, nil
	}
}

func timeMicrosFromNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		switch val := d.(type) {
		case int, int32, int64, float32, float64:
			// "Language implementations may choose to represent logical types with an appropriate native type, although this is not required."
			// especially permitted default values depend on the field's schema type and goavro encodes default values using the field schema
			return fn(b, val)

		case time.Duration:
			duration := int32(val.Nanoseconds() / int64(time.Microsecond))
			return fn(b, duration)

		default:
			return nil, fmt.Errorf("cannot transform to binary time-micros, expected time.Duration or Go numeric, received %T", d)
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////
// timestamp-millis logical type - to/from time.Time, time.UTC location
//////////////////////////////////////////////////////////////////////////////////////////////
func nativeFromTimeStampMillis(fn toNativeFn) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		l, b, err := fn(bytes)
		if err != nil {
			return l, b, err
		}
		milliseconds, ok := l.(int64)
		if !ok {
			return l, b, fmt.Errorf("cannot transform native timestamp-millis, expected int64, received %T", l)
		}
		seconds := milliseconds / 1e3
		nanoseconds := (milliseconds - (seconds * 1e3)) * 1e6
		return time.Unix(seconds, nanoseconds).UTC(), b, nil
	}
}

func timeStampMillisFromNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		switch val := d.(type) {
		case int, int32, int64, float32, float64:
			// "Language implementations may choose to represent logical types with an appropriate native type, although this is not required."
			// especially permitted default values depend on the field's schema type and goavro encodes default values using the field schema
			return fn(b, val)

		case time.Time:
			// While this code performs a few more steps than seem required, it is
			// written this way to allow the best time resolution without overflowing the int64 value.
			return fn(b, val.Unix()*1e3+int64(val.Nanosecond()/1e6))

		default:
			return nil, fmt.Errorf("cannot transform to binary timestamp-millis, expected time.Time or Go numeric, received %T", d)
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////
// timestamp-micros logical type - to/from time.Time, time.UTC location
//////////////////////////////////////////////////////////////////////////////////////////////
func nativeFromTimeStampMicros(fn toNativeFn) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		l, b, err := fn(bytes)
		if err != nil {
			return l, b, err
		}
		microseconds, ok := l.(int64)
		if !ok {
			return l, b, fmt.Errorf("cannot transform native timestamp-micros, expected int64, received %T", l)
		}
		// While this code performs a few more steps than seem required, it is
		// written this way to allow the best time resolution on UNIX and
		// Windows without overflowing the int64 value.  Windows has a zero-time
		// value of 1601-01-01 UTC, and the number of nanoseconds since that
		// zero-time overflows 64-bit integers.
		seconds := microseconds / 1e6
		nanoseconds := (microseconds - (seconds * 1e6)) * 1e3
		return time.Unix(seconds, nanoseconds).UTC(), b, nil
	}
}

func timeStampMicrosFromNative(fn fromNativeFn) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		switch val := d.(type) {
		case int, int32, int64, float32, float64:
			// "Language implementations may choose to represent logical types with an appropriate native type, although this is not required."
			// especially permitted default values depend on the field's schema type and goavro encodes default values using the field schema
			return fn(b, val)

		case time.Time:
			// While this code performs a few more steps than seem required, it is
			// written this way to allow the best time resolution on UNIX and
			// Windows without overflowing the int64 value.  Windows has a zero-time
			// value of 1601-01-01 UTC, and the number of nanoseconds since that
			// zero-time overflows 64-bit integers.
			return fn(b, val.Unix()*1e6+int64(val.Nanosecond()/1e3))

		default:
			return nil, fmt.Errorf("cannot transform to binary timestamp-micros, expected time.Time or Go numeric, received %T", d)
		}
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////
// decimal logical-type - byte/fixed - to/from math/big.Rat
// two's complement algorithm taken from:
// https://groups.google.com/d/msg/golang-nuts/TV4bRVrHZUw/UcQt7S4IYlcJ by rog
/////////////////////////////////////////////////////////////////////////////////////////////
type makeCodecFn func(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error)

func precisionAndScaleFromSchemaMap(schemaMap map[string]interface{}) (int, int, error) {
	p1, ok := schemaMap["precision"]
	if !ok {
		return 0, 0, errors.New("cannot create decimal logical type without precision")
	}
	p2, ok := p1.(float64)
	if !ok {
		return 0, 0, fmt.Errorf("cannot create decimal logical type with wrong precision type; expected: float64; received: %T", p1)
	}
	p3 := int(p2)
	if p3 <= 1 {
		return 0, 0, fmt.Errorf("cannot create decimal logical type when precision is less than one: %d", p3)
	}
	var s3 int // scale defaults to 0 if not set
	if s1, ok := schemaMap["scale"]; ok {
		s2, ok := s1.(float64)
		if !ok {
			return 0, 0, fmt.Errorf("cannot create decimal logical type with wrong precision type; expected: float64; received: %T", p1)
		}
		s3 = int(s2)
		if s3 < 0 {
			return 0, 0, fmt.Errorf("cannot create decimal logical type when scale is less than zero: %d", s3)
		}
		if s3 > p3 {
			return 0, 0, fmt.Errorf("cannot create decimal logical type when scale is larger than precision: %d > %d", s3, p3)
		}
	}
	return p3, s3, nil
}

var one = big.NewInt(1)

func makeDecimalBytesCodec(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	precision, scale, err := precisionAndScaleFromSchemaMap(schemaMap)
	if err != nil {
		return nil, err
	}
	if _, ok := schemaMap["name"]; !ok {
		schemaMap["name"] = "bytes.decimal"
	}
	c, err := registerNewCodec(st, schemaMap, enclosingNamespace)
	if err != nil {
		return nil, fmt.Errorf("Bytes ought to have valid name: %s", err)
	}
	c.binaryFromNative = decimalBytesFromNative(bytesBinaryFromNative, toSignedBytes, precision, scale)
	c.textualFromNative = decimalBytesFromNative(bytesTextualFromNative, toSignedBytes, precision, scale)
	c.nativeFromBinary = nativeFromDecimalBytes(bytesNativeFromBinary, precision, scale)
	c.nativeFromTextual = nativeFromDecimalBytes(bytesNativeFromTextual, precision, scale)
	return c, nil
}

func nativeFromDecimalBytes(fn toNativeFn, precision, scale int) toNativeFn {
	return func(bytes []byte) (interface{}, []byte, error) {
		d, b, err := fn(bytes)
		if err != nil {
			return d, b, err
		}
		bs, ok := d.([]byte)
		if !ok {
			return nil, bytes, fmt.Errorf("cannot transform to native decimal, expected []byte, received %T", d)
		}
		num := big.NewInt(0)
		fromSignedBytes(num, bs)
		denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
		r := new(big.Rat).SetFrac(num, denom)
		return r, b, nil
	}
}

func decimalBytesFromNative(fromNativeFn fromNativeFn, toBytesFn toBytesFn, precision, scale int) fromNativeFn {
	return func(b []byte, d interface{}) ([]byte, error) {
		r, ok := d.(*big.Rat)
		if !ok {
			return nil, fmt.Errorf("cannot transform to bytes, expected *big.Rat, received %T", d)
		}
		// Reduce accuracy to precision by dividing and multiplying by digit length
		num := big.NewInt(0).Set(r.Num())
		denom := big.NewInt(0).Set(r.Denom())
		i := new(big.Int).Mul(num, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
		// divide that by the denominator
		precnum := new(big.Int).Div(i, denom)
		bout, err := toBytesFn(precnum)
		if err != nil {
			return nil, err
		}
		return fromNativeFn(b, bout)
	}
}

func makeDecimalFixedCodec(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	precision, scale, err := precisionAndScaleFromSchemaMap(schemaMap)
	if err != nil {
		return nil, err
	}
	if _, ok := schemaMap["name"]; !ok {
		schemaMap["name"] = "fixed.decimal"
	}
	c, err := makeFixedCodec(st, enclosingNamespace, schemaMap)
	if err != nil {
		return nil, err
	}
	size, err := sizeFromSchemaMap(c.typeName, schemaMap)
	if err != nil {
		return nil, err
	}
	c.binaryFromNative = decimalBytesFromNative(c.binaryFromNative, toSignedFixedBytes(size), precision, scale)
	c.textualFromNative = decimalBytesFromNative(c.textualFromNative, toSignedFixedBytes(size), precision, scale)
	c.nativeFromBinary = nativeFromDecimalBytes(c.nativeFromBinary, precision, scale)
	c.nativeFromTextual = nativeFromDecimalBytes(c.nativeFromTextual, precision, scale)
	return c, nil
}

func padBytes(bytes []byte, fixedSize uint) []byte {
	s := int(fixedSize)
	padded := make([]byte, s, s)
	if s >= len(bytes) {
		copy(padded[s-len(bytes):], bytes)
	}
	return padded
}

type toBytesFn func(n *big.Int) ([]byte, error)

// fromSignedBytes sets the value of n to the big-endian two's complement
// value stored in the given data. If data[0]&80 != 0, the number
// is negative. If data is empty, the result will be 0.
func fromSignedBytes(n *big.Int, data []byte) {
	n.SetBytes(data)
	if len(data) > 0 && data[0]&0x80 > 0 {
		n.Sub(n, new(big.Int).Lsh(one, uint(len(data))*8))
	}
}

// toSignedBytes returns the big-endian two's complement
// form of n.
func toSignedBytes(n *big.Int) ([]byte, error) {
	switch n.Sign() {
	case 0:
		return []byte{0}, nil
	case 1:
		b := n.Bytes()
		if b[0]&0x80 > 0 {
			b = append([]byte{0}, b...)
		}
		return b, nil
	case -1:
		length := uint(n.BitLen()/8+1) * 8
		b := new(big.Int).Add(n, new(big.Int).Lsh(one, length)).Bytes()
		// When the most significant bit is on a byte
		// boundary, we can get some extra significant
		// bits, so strip them off when that happens.
		if len(b) >= 2 && b[0] == 0xff && b[1]&0x80 != 0 {
			b = b[1:]
		}
		return b, nil
	}
	return nil, fmt.Errorf("toSignedBytes: error big.Int.Sign() returned unexpected value")
}

// toSignedFixedBytes returns the big-endian two's complement
// form of n for a given length of bytes.
func toSignedFixedBytes(size uint) func(*big.Int) ([]byte, error) {
	return func(n *big.Int) ([]byte, error) {
		switch n.Sign() {
		case 0:
			return []byte{0}, nil
		case 1:
			b := n.Bytes()
			if b[0]&0x80 > 0 {
				b = append([]byte{0}, b...)
			}
			return padBytes(b, size), nil
		case -1:
			length := size * 8
			b := new(big.Int).Add(n, new(big.Int).Lsh(one, length)).Bytes()
			// Unlike a variable length byte length we need the extra bits to meet byte length
			return b, nil
		}
		return nil, fmt.Errorf("toSignedBytes: error big.Int.Sign() returned unexpected value")
	}
}
