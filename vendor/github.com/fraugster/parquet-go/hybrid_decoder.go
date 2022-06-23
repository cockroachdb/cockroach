package goparquet

// This file is based on the code from https://github.com/kostya-sh/parquet-go
// Copyright (c) 2015 Konstantin Shaposhnikov

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math/bits"

	"github.com/pkg/errors"
)

type decoder interface {
	next() (int32, error)

	init(io.Reader) error
	initSize(io.Reader) error
}

type levelDecoder interface {
	decoder

	maxLevel() uint16
}

type hybridDecoder struct {
	r io.Reader

	bitWidth     int
	unpackerFn   unpack8int32Func
	rleValueSize int

	bpRun [8]int32

	rleCount uint32
	rleValue int32

	bpCount  uint32
	bpRunPos uint8

	buffered bool
}

func newHybridDecoder(bitWidth int) *hybridDecoder {
	return &hybridDecoder{
		bitWidth:   bitWidth,
		unpackerFn: unpack8Int32FuncByWidth[bitWidth],

		rleValueSize: (bitWidth + 7) / 8,
	}
}

func (hd *hybridDecoder) initSize(r io.Reader) error {
	if hd.bitWidth == 0 {
		return nil
	}
	var size uint32
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return err
	}
	reader := io.LimitReader(r, int64(size))
	return hd.init(reader)
}

func (hd *hybridDecoder) init(r io.Reader) error {
	if hd.buffered {
		buf, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}
		hd.r = bytes.NewReader(buf)
	} else {
		hd.r = r
	}
	return nil
}

func (hd *hybridDecoder) next() (next int32, err error) {
	// when the bit width is zero, it means we can only have infinite zero.
	if hd.bitWidth == 0 {
		return 0, nil
	}
	if hd.r == nil {
		return 0, errors.New("reader is not initialized")
	}
	if hd.rleCount == 0 && hd.bpCount == 0 && hd.bpRunPos == 0 {
		if err = hd.readRunHeader(); err != nil {
			return 0, err
		}
	}

	switch {
	case hd.rleCount > 0:
		next = hd.rleValue
		hd.rleCount--
	case hd.bpCount > 0 || hd.bpRunPos > 0:
		if hd.bpRunPos == 0 {
			if err = hd.readBitPackedRun(); err != nil {
				return 0, err
			}
			hd.bpCount--
		}
		next = hd.bpRun[hd.bpRunPos]
		hd.bpRunPos = (hd.bpRunPos + 1) % 8
	default:
		return 0, io.EOF
	}

	return next, err
}

func (hd *hybridDecoder) readRLERunValue() error {
	v := make([]byte, hd.rleValueSize)
	n, err := hd.r.Read(v)
	if err != nil {
		return err
	}
	if n != hd.rleValueSize {
		return io.ErrUnexpectedEOF
	}

	hd.rleValue = decodeRLEValue(v)
	if bits.LeadingZeros32(uint32(hd.rleValue)) < 32-hd.bitWidth {
		return errors.New("rle: RLE run value is too large")
	}
	return nil
}

func (hd *hybridDecoder) readBitPackedRun() error {
	data := make([]byte, hd.bitWidth)
	_, err := hd.r.Read(data)
	if err != nil {
		return err
	}
	hd.bpRun = hd.unpackerFn(data)
	return nil
}

func (hd *hybridDecoder) readRunHeader() error {
	h, err := readUVariant32(hd.r)
	if err != nil {
		// this error could be EOF which is ok by this implementation the only issue is the binary.ReadUVariant can not
		// return UnexpectedEOF is there is some bit read from the stream with no luck, it always return EOF
		return err
	}

	// The lower bit indicate if this is bitpack or rle
	if h&1 == 1 {
		hd.bpCount = uint32(h >> 1)
		if hd.bpCount == 0 {
			return fmt.Errorf("rle: empty bit-packed run")
		}
		hd.bpRunPos = 0
	} else {
		hd.rleCount = uint32(h >> 1)
		if hd.rleCount == 0 {
			return fmt.Errorf("rle: empty RLE run")
		}
		return hd.readRLERunValue()
	}
	return nil
}
