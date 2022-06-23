// +build gofuzz

package goparquet

import (
	"bytes"
)

func FuzzBooleanPlain(data []byte) int {
	maxSize := len(data) * 8
	d := booleanPlainDecoder{}
	err := d.init(bytes.NewReader(data))
	if err != nil {
		return -1
	}
	dst1 := make([]interface{}, maxSize)
	_, err = d.decodeValues(dst1)
	if err != nil {
		return 0
	}

	e := booleanPlainEncoder{}

	if err := e.init(&bytes.Buffer{}); err != nil {
		return -1
	}

	if err := e.encodeValues(dst1); err != nil {
		return 0
	}

	if err := e.Close(); err != nil {
		return 0
	}

	return 1
}

func FuzzBooleanRLE(data []byte) int {
	maxSize := len(data) * 8
	d := booleanRLEDecoder{}
	err := d.init(bytes.NewReader(data))
	if err != nil {
		return -1
	}
	dst1 := make([]interface{}, maxSize)
	_, err = d.decodeValues(dst1)
	if err != nil {
		return 0
	}

	e := booleanRLEEncoder{}

	if err := e.init(&bytes.Buffer{}); err != nil {
		return -1
	}

	if err := e.encodeValues(dst1); err != nil {
		return 0
	}

	if err := e.Close(); err != nil {
		return 0
	}

	return 1
}

func FuzzInt32DeltaBP(data []byte) int {
	maxSize := len(data) / 4
	d := int32DeltaBPDecoder{
		deltaBitPackDecoder32: deltaBitPackDecoder32{
			blockSize:      128,
			miniBlockCount: 4,
		},
	}
	err := d.init(bytes.NewReader(data))
	if err != nil {
		return -1
	}
	dst1 := make([]interface{}, maxSize)
	_, err = d.decodeValues(dst1)
	if err != nil {
		return 0
	}

	e := int32DeltaBPEncoder{
		deltaBitPackEncoder32: deltaBitPackEncoder32{
			blockSize:      128,
			miniBlockCount: 4,
		},
	}

	if err := e.init(&bytes.Buffer{}); err != nil {
		return -1
	}

	if err := e.encodeValues(dst1); err != nil {
		return 0
	}

	if err := e.Close(); err != nil {
		return 0
	}

	return 1
}

func FuzzInt32Plain(data []byte) int {
	maxSize := len(data) / 4
	d := int32PlainDecoder{}
	err := d.init(bytes.NewReader(data))
	if err != nil {
		panic("unexpected error in init")
	}
	dst1 := make([]interface{}, maxSize)
	_, err = d.decodeValues(dst1)
	if err != nil {
		return 0
	}

	e := int32PlainEncoder{}

	if err := e.init(&bytes.Buffer{}); err != nil {
		panic("unexpected error in init")
	}

	if err := e.encodeValues(dst1); err != nil {
		return 0
	}

	if err := e.Close(); err != nil {
		return 0
	}

	return 1
}

func FuzzFloatPlain(data []byte) int {
	maxSize := len(data) / 4
	d := floatPlainDecoder{}
	err := d.init(bytes.NewReader(data))
	if err != nil {
		panic("unexpected error in init")
	}
	dst1 := make([]interface{}, maxSize)
	_, err = d.decodeValues(dst1)
	if err != nil {
		return -1
	}

	e := floatPlainEncoder{}

	if err := e.init(&bytes.Buffer{}); err != nil {
		panic("unexpected error in init")
	}

	if err := e.encodeValues(dst1); err != nil {
		return -1
	}

	if err := e.Close(); err != nil {
		return -1
	}

	return 1
}

func FuzzDoublePlain(data []byte) int {
	maxSize := len(data) / 8
	d := doublePlainDecoder{}
	err := d.init(bytes.NewReader(data))
	if err != nil {
		panic("unexpected error in init")
	}
	dst1 := make([]interface{}, maxSize)
	_, err = d.decodeValues(dst1)
	if err != nil {
		return -1
	}

	e := doublePlainEncoder{}

	if err := e.init(&bytes.Buffer{}); err != nil {
		panic("unexpected error in init")
	}

	if err := e.encodeValues(dst1); err != nil {
		return -1
	}

	if err := e.Close(); err != nil {
		return -1
	}

	return 1
}
