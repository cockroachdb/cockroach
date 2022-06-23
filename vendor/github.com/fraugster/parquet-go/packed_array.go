package goparquet

import (
	"errors"
	"fmt"
)

// packedArray is a bitmap encoded array mainly for repetition and definition
// levels, which normally have low values (~<10), a []uint16 array is not the
// most memory efficient structure due to the large number of values. Memory
// storage requirements for the packed array are ~1/8th compared to
// []uint16 array.
type packedArray struct {
	count int
	bw    int
	data  []byte

	buf    [8]int32
	bufPos int

	writer pack8int32Func
	reader unpack8int32Func
}

// This function is only for testing, as it flushes first, so be careful!
func (pa *packedArray) toArray() []int32 {
	ret := make([]int32, pa.count)
	for i := range ret {
		ret[i], _ = pa.at(i)
	}
	return ret
}

func (pa *packedArray) reset(bw int) {
	if bw < 0 || bw > 32 {
		panic("invalid bit width")
	}
	pa.bw = bw
	pa.count = 0
	pa.bufPos = 0
	pa.data = pa.data[:0]
	pa.writer = pack8Int32FuncByWidth[bw]
	pa.reader = unpack8Int32FuncByWidth[bw]
}

func (pa *packedArray) flush() {
	for i := pa.bufPos; i < 8; i++ {
		pa.buf[i] = 0
	}
	pa.data = append(pa.data, pa.writer(pa.buf)...)
	pa.bufPos = 0
}

func (pa *packedArray) appendSingle(v int32) {
	if pa.bufPos == 8 {
		pa.flush()
	}
	pa.buf[pa.bufPos] = v
	pa.bufPos++
	pa.count++
}

func (pa *packedArray) at(pos int) (int32, error) {
	if pos < 0 || pos >= pa.count {
		return 0, errors.New("out of range")
	}
	if pa.bw == 0 {
		return 0, nil
	}

	block := (pos / 8) * pa.bw
	idx := pos % 8

	if block >= len(pa.data) {
		return pa.buf[idx], nil
	}

	buf := pa.reader(pa.data[block : block+pa.bw])
	return buf[idx], nil
}

func (pa *packedArray) appendArray(other *packedArray) {
	if other == nil {
		return
	}

	if pa.bw != other.bw {
		panic(fmt.Sprintf("Can not append array with different bit width : %d and %d", pa.bw, other.bw))
	}

	if cap(pa.data) < len(pa.data)+len(other.data)+1 {
		data := make([]byte, len(pa.data), len(pa.data)+len(other.data)+1)
		copy(data, pa.data)
		pa.data = data
	}

	for i := 0; i < other.count; i++ {
		v, _ := other.at(i)
		pa.appendSingle(v)
	}
}
