package goparquet

import (
	"bytes"
	"encoding/binary"
	"io"
)

type hybridEncoder struct {
	w io.Writer

	left       []int32
	original   io.Writer
	bitWidth   int
	unpackerFn pack8int32Func

	data *packedArray
}

func newHybridEncoder(bitWidth int) *hybridEncoder {
	p := &packedArray{}
	return &hybridEncoder{
		bitWidth:   bitWidth,
		unpackerFn: pack8Int32FuncByWidth[bitWidth],
		data:       p,
	}
}

func (he *hybridEncoder) init(w io.Writer) error {
	he.w = w
	he.left = nil
	he.original = nil

	he.data.reset(he.bitWidth)
	return nil
}

func (he *hybridEncoder) initSize(w io.Writer) error {
	_ = he.init(&bytes.Buffer{})
	he.original = w

	return nil
}

func (he *hybridEncoder) write(items ...[]byte) error {
	for i := range items {
		if err := writeFull(he.w, items[i]); err != nil {
			return err
		}
	}

	return nil
}

func (he *hybridEncoder) bpEncode() error {
	// If the bit width is zero, no need to write any
	if he.bitWidth == 0 {
		return nil
	}
	l := he.data.count
	if x := l % 8; x != 0 {
		l += 8 - x
	}

	header := ((l / 8) << 1) | 1
	buf := make([]byte, 4) // big enough for int
	cnt := binary.PutUvarint(buf, uint64(header))

	return he.write(buf[:cnt], he.data.data)
}

func (he *hybridEncoder) encode(data []int32) error {
	for i := range data {
		he.data.appendSingle(data[i])
	}

	return nil
}

func (he *hybridEncoder) encodePacked(data *packedArray) error {
	he.data.appendArray(data)

	return nil
}

func (he *hybridEncoder) flush() error {
	he.data.flush()
	return he.bpEncode()
}

func (he *hybridEncoder) Close() error {
	if he.bitWidth == 0 {
		return nil
	}
	if err := he.flush(); err != nil {
		return err
	}

	if he.original != nil {
		data := he.w.(*bytes.Buffer).Bytes()
		var size = uint32(len(data))
		if err := binary.Write(he.original, binary.LittleEndian, size); err != nil {
			return err
		}
		return writeFull(he.original, data)
	}

	return nil
}
