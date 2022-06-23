package goparquet

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
)

// The two following decoder are identical, since there is no generic, I had two option, one use the interfaces
// which was my first choice but its branchy and full of if and else. so I decided to go for second solution and
// almost copy/paste this two types

type deltaBitPackDecoder32 struct {
	r io.Reader

	blockSize           int32
	miniBlockCount      int32
	valuesCount         int32
	miniBlockValueCount int32

	previousValue int32
	minDelta      int32

	miniBlockBitWidth        []uint8
	currentMiniBlock         int32
	currentMiniBlockBitWidth uint8
	miniBlockPosition        int32 // position inside the current mini block
	position                 int32 // position in the value. since delta may have padding we need to track this
	currentUnpacker          unpack8int32Func
	miniBlockInt32           [8]int32
}

func (d *deltaBitPackDecoder32) initSize(r io.Reader) error {
	return d.init(r)
}

func (d *deltaBitPackDecoder32) init(r io.Reader) error {
	d.r = r

	if err := d.readBlockHeader(); err != nil {
		return err
	}

	if err := d.readMiniBlockHeader(); err != nil {
		return err
	}

	return nil
}

func (d *deltaBitPackDecoder32) readBlockHeader() error {
	var err error
	if d.blockSize, err = readUVariant32(d.r); err != nil {
		return errors.Wrap(err, "failed to read block size")
	}
	if d.blockSize <= 0 && d.blockSize%128 != 0 {
		return errors.New("invalid block size")
	}

	if d.miniBlockCount, err = readUVariant32(d.r); err != nil {
		return errors.Wrap(err, "failed to read number of mini blocks")
	}

	if d.miniBlockCount <= 0 || d.blockSize%d.miniBlockCount != 0 {
		return errors.New("int/delta: invalid number of mini blocks")
	}

	d.miniBlockValueCount = d.blockSize / d.miniBlockCount
	if d.miniBlockValueCount == 0 {
		return errors.Errorf("invalid mini block value count, it can't be zero")
	}

	if d.valuesCount, err = readUVariant32(d.r); err != nil {
		return errors.Wrapf(err, "failed to read total value count")
	}

	if d.valuesCount < 0 {
		return errors.New("invalid total value count")
	}

	if d.previousValue, err = readVariant32(d.r); err != nil {
		return errors.Wrap(err, "failed to read first value")
	}

	return nil
}

func (d *deltaBitPackDecoder32) readMiniBlockHeader() error {
	var err error

	if d.minDelta, err = readVariant32(d.r); err != nil {
		return errors.Wrap(err, "failed to read min delta")
	}

	// the mini block bitwidth is always there, even if the value is zero
	d.miniBlockBitWidth = make([]uint8, d.miniBlockCount)
	if _, err = io.ReadFull(d.r, d.miniBlockBitWidth); err != nil {
		return errors.Wrap(err, "not enough data to read all miniblock bit widths")
	}

	for i := range d.miniBlockBitWidth {
		if d.miniBlockBitWidth[i] > 32 {
			return errors.Errorf("invalid miniblock bit width : %d", d.miniBlockBitWidth[i])
		}
	}

	// start from the first min block in a big block
	d.currentMiniBlock = 0

	return nil
}

func (d *deltaBitPackDecoder32) next() (int32, error) {
	if d.position >= d.valuesCount {
		// No value left in the buffer
		return 0, io.EOF
	}

	// need new byte?
	if d.position%8 == 0 {
		// do we need to advance a mini block?
		if d.position%d.miniBlockValueCount == 0 {
			// do we need to advance a big block?
			if d.currentMiniBlock >= d.miniBlockCount {
				if err := d.readMiniBlockHeader(); err != nil {
					return 0, err
				}
			}

			d.currentMiniBlockBitWidth = d.miniBlockBitWidth[d.currentMiniBlock]
			d.currentUnpacker = unpack8Int32FuncByWidth[int(d.currentMiniBlockBitWidth)]

			d.miniBlockPosition = 0
			d.currentMiniBlock++
		}

		// read next 8 values
		w := int32(d.currentMiniBlockBitWidth)
		buf := make([]byte, w)
		if _, err := io.ReadFull(d.r, buf); err != nil {
			return 0, err
		}

		d.miniBlockInt32 = d.currentUnpacker(buf)
		d.miniBlockPosition += w
		// there is padding here, read them all from the reader, first deal with the remaining of the current block,
		// then the next blocks. if the blocks bit width is zero then simply ignore them, but the docs said reader
		// should accept any arbitrary bit width here.
		if d.position+8 >= d.valuesCount {
			//  current block
			l := (d.miniBlockValueCount/8)*w - d.miniBlockPosition
			if l < 0 {
				return 0, errors.New("invalid stream")
			}
			remaining := make([]byte, l)
			_, _ = io.ReadFull(d.r, remaining)
			for i := d.currentMiniBlock; i < d.miniBlockCount; i++ {
				w := int32(d.miniBlockBitWidth[d.currentMiniBlock])
				if w != 0 {
					remaining := make([]byte, (d.miniBlockValueCount/8)*w)
					_, _ = io.ReadFull(d.r, remaining)
				}
			}
		}
	}

	// value is the previous value + delta stored in the reader and the min delta for the block, also we always read one
	// value ahead
	ret := d.previousValue
	d.previousValue += d.miniBlockInt32[d.position%8] + d.minDelta
	d.position++

	return ret, nil
}

type deltaBitPackDecoder64 struct {
	r io.Reader

	blockSize           int32
	miniBlockCount      int32
	valuesCount         int32
	miniBlockValueCount int32

	previousValue int64
	minDelta      int64

	miniBlockBitWidth        []uint8
	currentMiniBlock         int32
	currentMiniBlockBitWidth uint8
	miniBlockPosition        int32 // position inside the current mini block
	position                 int32 // position in the value. since delta may have padding we need to track this
	currentUnpacker          unpack8int64Func
	miniBlockInt64           [8]int64
}

func (d *deltaBitPackDecoder64) init(r io.Reader) error {
	d.r = r

	if err := d.readBlockHeader(); err != nil {
		return err
	}

	if err := d.readMiniBlockHeader(); err != nil {
		return err
	}

	return nil
}

func (d *deltaBitPackDecoder64) readBlockHeader() error {
	var err error
	if d.blockSize, err = readUVariant32(d.r); err != nil {
		return errors.Wrap(err, "failed to read block size")
	}
	if d.blockSize <= 0 && d.blockSize%128 != 0 {
		return errors.New("invalid block size")
	}

	if d.miniBlockCount, err = readUVariant32(d.r); err != nil {
		return errors.Wrap(err, "failed to read number of mini blocks")
	}

	if d.miniBlockCount <= 0 || d.blockSize%d.miniBlockCount != 0 {
		return errors.New("int/delta: invalid number of mini blocks")
	}

	d.miniBlockValueCount = d.blockSize / d.miniBlockCount
	if d.miniBlockValueCount == 0 {
		return errors.Errorf("invalid mini block value count, it can't be zero")
	}

	if d.valuesCount, err = readUVariant32(d.r); err != nil {
		return errors.Wrapf(err, "failed to read total value count")
	}

	if d.valuesCount < 0 {
		return errors.New("invalid total value count")
	}

	if d.previousValue, err = readVariant64(d.r); err != nil {
		return errors.Wrap(err, "failed to read first value")
	}

	return nil
}

func (d *deltaBitPackDecoder64) readMiniBlockHeader() error {
	var err error

	if d.minDelta, err = readVariant64(d.r); err != nil {
		return errors.Wrap(err, "failed to read min delta")
	}

	// the mini block bitwidth is always there, even if the value is zero
	d.miniBlockBitWidth = make([]uint8, d.miniBlockCount)
	if _, err = io.ReadFull(d.r, d.miniBlockBitWidth); err != nil {
		return errors.Wrap(err, "not enough data to read all miniblock bit widths")
	}

	for i := range d.miniBlockBitWidth {
		if d.miniBlockBitWidth[i] > 64 {
			return errors.Errorf("invalid miniblock bit width : %d", d.miniBlockBitWidth[i])
		}
	}

	// start from the first min block in a big block
	d.currentMiniBlock = 0

	return nil
}

func (d *deltaBitPackDecoder64) next() (int64, error) {
	if d.position >= d.valuesCount {
		// No value left in the buffer
		return 0, io.EOF
	}

	// need new byte?
	if d.position%8 == 0 {
		// do we need to advance a mini block?
		if d.position%d.miniBlockValueCount == 0 {
			// do we need to advance a big block?
			if d.currentMiniBlock >= d.miniBlockCount {
				if err := d.readMiniBlockHeader(); err != nil {
					return 0, err
				}
			}

			d.currentMiniBlockBitWidth = d.miniBlockBitWidth[d.currentMiniBlock]
			d.currentUnpacker = unpack8Int64FuncByWidth[int(d.currentMiniBlockBitWidth)]

			d.miniBlockPosition = 0
			d.currentMiniBlock++
		}

		// read next 8 values
		w := int32(d.currentMiniBlockBitWidth)
		buf := make([]byte, w)
		if _, err := io.ReadFull(d.r, buf); err != nil {
			return 0, err
		}

		d.miniBlockInt64 = d.currentUnpacker(buf)
		d.miniBlockPosition += w
		// there is padding here, read them all from the reader, first deal with the remaining of the current block,
		// then the next blocks. if the blocks bit width is zero then simply ignore them, but the docs said reader
		// should accept any arbitrary bit width here.
		if d.position+8 >= d.valuesCount {
			//  current block
			sliceLen := (d.miniBlockValueCount/8)*w - d.miniBlockPosition
			if sliceLen < 0 {
				return 0, fmt.Errorf("invalid remaining values, mini block value count = %d, width = %d, mini block position = %d", d.miniBlockValueCount, w, d.miniBlockPosition)
			}
			remaining := make([]byte, sliceLen)
			_, _ = io.ReadFull(d.r, remaining)
			for i := d.currentMiniBlock; i < d.miniBlockCount; i++ {
				w := int32(d.miniBlockBitWidth[d.currentMiniBlock])
				if w != 0 {
					remaining := make([]byte, (d.miniBlockValueCount/8)*w)
					_, _ = io.ReadFull(d.r, remaining)
				}
			}
		}
	}

	// value is the previous value + delta stored in the reader and the min delta for the block, also we always read one
	// value ahead
	ret := d.previousValue
	d.previousValue += d.miniBlockInt64[d.position%8] + d.minDelta
	d.position++

	return ret, nil
}
