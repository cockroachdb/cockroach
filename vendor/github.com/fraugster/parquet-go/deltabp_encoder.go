package goparquet

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/bits"

	"math"

	"github.com/pkg/errors"
)

type deltaBitPackEncoder32 struct {
	deltas   []int32
	bitWidth []uint8
	packed   [][]byte
	w        io.Writer

	// this value should be there before the init
	blockSize      int // Must be multiple of 128
	miniBlockCount int // blockSize % miniBlockCount should be 0

	miniBlockValueCount int

	valuesCount int
	buffer      *bytes.Buffer

	firstValue    int32 // the first value to write
	minDelta      int32
	previousValue int32
}

func (d *deltaBitPackEncoder32) init(w io.Writer) error {
	d.w = w

	if d.blockSize%128 != 0 || d.blockSize <= 0 {
		return errors.Errorf("invalid block size, it should be multiple of 128, it is %d", d.blockSize)
	}

	if d.miniBlockCount <= 0 || d.blockSize%d.miniBlockCount != 0 {
		return errors.Errorf("invalid mini block count, it is %d", d.miniBlockCount)
	}

	d.miniBlockValueCount = d.blockSize / d.miniBlockCount
	if d.miniBlockValueCount%8 != 0 {
		return errors.Errorf("invalid mini block count, the mini block value count should be multiple of 8, it is %d", d.miniBlockCount)
	}

	d.firstValue = 0
	d.valuesCount = 0
	d.minDelta = math.MaxInt32
	d.deltas = make([]int32, 0, d.blockSize)
	d.previousValue = 0
	d.buffer = &bytes.Buffer{}
	d.bitWidth = make([]uint8, 0, d.miniBlockCount)
	return nil
}

func (d *deltaBitPackEncoder32) flush() error {
	// Technically, based on the spec after this step all values are positive, but NO, it's not. the problem is when
	// the min delta is small enough (lets say MinInt) and one of deltas are MaxInt, the the result of MaxInt-MinInt is
	// -1, get the idea, there is a lot of numbers here because of overflow can produce negative value
	for i := range d.deltas {
		d.deltas[i] -= d.minDelta
	}

	if err := writeVariant(d.buffer, int64(d.minDelta)); err != nil {
		return err
	}

	d.bitWidth = d.bitWidth[:0] //reset the bitWidth buffer
	d.packed = d.packed[:0]
	for i := 0; i < len(d.deltas); i += d.miniBlockValueCount {
		end := i + d.miniBlockValueCount
		if end >= len(d.deltas) {
			end = len(d.deltas)
		}
		// The cast to uint32 here, is the key. or the max not works at all
		max := uint32(d.deltas[i])
		buf := make([][8]int32, d.miniBlockValueCount/8)
		for j := i; j < end; j++ {
			if max < uint32(d.deltas[j]) {
				max = uint32(d.deltas[j])
			}
			t := j - i
			buf[t/8][t%8] = d.deltas[j]
		}
		bw := bits.Len32(max)
		d.bitWidth = append(d.bitWidth, uint8(bw))

		data := make([]byte, 0, bw*len(buf))
		packer := pack8Int32FuncByWidth[bw]
		for j := range buf {
			data = append(data, packer(buf[j])...)
		}
		d.packed = append(d.packed, data)
	}

	for len(d.bitWidth) < d.miniBlockCount {
		d.bitWidth = append(d.bitWidth, 0)
	}

	if err := binary.Write(d.buffer, binary.LittleEndian, d.bitWidth); err != nil {
		return err
	}

	for i := range d.packed {
		if err := writeFull(d.buffer, d.packed[i]); err != nil {
			return err
		}
	}
	d.minDelta = math.MaxInt32
	d.deltas = d.deltas[:0]

	return nil
}

func (d *deltaBitPackEncoder32) addInt32(i int32) error {
	d.valuesCount++
	if d.valuesCount == 1 {
		d.firstValue = i
		d.previousValue = i
		return nil
	}

	delta := i - d.previousValue
	d.previousValue = i
	d.deltas = append(d.deltas, delta)
	if delta < d.minDelta {
		d.minDelta = delta
	}

	if len(d.deltas) == d.blockSize {
		// flush
		return d.flush()
	}

	return nil
}

func (d *deltaBitPackEncoder32) write() error {
	if d.valuesCount == 1 || len(d.deltas) > 0 {
		if err := d.flush(); err != nil {
			return err
		}
	}

	if err := writeUVariant(d.w, uint64(d.blockSize)); err != nil {
		return err
	}

	if err := writeUVariant(d.w, uint64(d.miniBlockCount)); err != nil {
		return err
	}

	if err := writeUVariant(d.w, uint64(d.valuesCount)); err != nil {
		return err
	}

	if err := writeVariant(d.w, int64(d.firstValue)); err != nil {
		return err
	}

	return writeFull(d.w, d.buffer.Bytes())
}

func (d *deltaBitPackEncoder32) Close() error {
	return d.write()
}

type deltaBitPackEncoder64 struct {
	// this value should be there before the init
	blockSize      int // Must be multiple of 128
	miniBlockCount int // blockSize % miniBlockCount should be 0

	//
	miniBlockValueCount int

	w io.Writer

	firstValue    int64 // the first value to write
	valuesCount   int
	minDelta      int64
	deltas        []int64
	previousValue int64

	buffer   *bytes.Buffer
	bitWidth []uint8
	packed   [][]byte
}

func (d *deltaBitPackEncoder64) init(w io.Writer) error {
	d.w = w

	if d.blockSize%128 != 0 || d.blockSize <= 0 {
		return errors.Errorf("invalid block size, it should be multiple of 128, it is %d", d.blockSize)
	}

	if d.miniBlockCount <= 0 || d.blockSize%d.miniBlockCount != 0 {
		return errors.Errorf("invalid mini block count, it is %d", d.miniBlockCount)
	}

	d.miniBlockValueCount = d.blockSize / d.miniBlockCount
	if d.miniBlockValueCount%8 != 0 {
		return errors.Errorf("invalid mini block count, the mini block value count should be multiple of 8, it is %d", d.miniBlockCount)
	}

	d.firstValue = 0
	d.valuesCount = 0
	d.minDelta = math.MaxInt32
	d.deltas = make([]int64, 0, d.blockSize)
	d.previousValue = 0
	d.buffer = &bytes.Buffer{}
	d.bitWidth = make([]uint8, 0, d.miniBlockCount)
	return nil
}

func (d *deltaBitPackEncoder64) flush() error {
	// Technically, based on the spec after this step all values are positive, but NO, it's not. the problem is when
	// the min delta is small enough (lets say MinInt) and one of deltas are MaxInt, the the result of MaxInt-MinInt is
	// -1, get the idea, there is a lot of numbers here because of overflow can produce negative value
	for i := range d.deltas {
		d.deltas[i] -= d.minDelta
	}

	if err := writeVariant(d.buffer, d.minDelta); err != nil {
		return err
	}

	d.bitWidth = d.bitWidth[:0] //reset the bitWidth buffer
	d.packed = d.packed[:0]
	for i := 0; i < len(d.deltas); i += d.miniBlockValueCount {
		end := i + d.miniBlockValueCount
		if end >= len(d.deltas) {
			end = len(d.deltas)
		}
		// The cast to uint64 here, is the key. or the max not works at all
		max := uint64(d.deltas[i])
		buf := make([][8]int64, d.miniBlockValueCount/8)
		for j := i; j < end; j++ {
			if max < uint64(d.deltas[j]) {
				max = uint64(d.deltas[j])
			}
			t := j - i
			buf[t/8][t%8] = d.deltas[j]
		}
		bw := bits.Len64(max)
		d.bitWidth = append(d.bitWidth, uint8(bw))

		data := make([]byte, 0, bw*len(buf))
		packer := pack8Int64FuncByWidth[bw]
		for j := range buf {
			data = append(data, packer(buf[j])...)
		}
		d.packed = append(d.packed, data)
	}

	for len(d.bitWidth) < d.miniBlockCount {
		d.bitWidth = append(d.bitWidth, 0)
	}

	if err := binary.Write(d.buffer, binary.LittleEndian, d.bitWidth); err != nil {
		return err
	}

	for i := range d.packed {
		if err := writeFull(d.buffer, d.packed[i]); err != nil {
			return err
		}
	}
	d.minDelta = math.MaxInt32
	d.deltas = d.deltas[:0]

	return nil
}

func (d *deltaBitPackEncoder64) addInt64(i int64) error {
	d.valuesCount++
	if d.valuesCount == 1 {
		d.firstValue = i
		d.previousValue = i
		return nil
	}

	delta := i - d.previousValue
	d.previousValue = i
	d.deltas = append(d.deltas, delta)
	if delta < d.minDelta {
		d.minDelta = delta
	}

	if len(d.deltas) == d.blockSize {
		// flush
		return d.flush()
	}

	return nil
}

func (d *deltaBitPackEncoder64) write() error {
	if d.valuesCount == 1 || len(d.deltas) > 0 {
		if err := d.flush(); err != nil {
			return err
		}
	}

	if err := writeUVariant(d.w, uint64(d.blockSize)); err != nil {
		return err
	}

	if err := writeUVariant(d.w, uint64(d.miniBlockCount)); err != nil {
		return err
	}

	if err := writeUVariant(d.w, uint64(d.valuesCount)); err != nil {
		return err
	}

	if err := writeVariant(d.w, d.firstValue); err != nil {
		return err
	}

	return writeFull(d.w, d.buffer.Bytes())
}

func (d *deltaBitPackEncoder64) Close() error {
	return d.write()
}
