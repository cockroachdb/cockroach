package goparquet

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/fraugster/parquet-go/parquet"

	"github.com/pkg/errors"
)

type byteArrayPlainDecoder struct {
	r io.Reader
	// if the length is set, then this is a fix size array decoder, unless it reads the len first
	length int
}

func (b *byteArrayPlainDecoder) init(r io.Reader) error {
	b.r = r
	return nil
}

func (b *byteArrayPlainDecoder) next() ([]byte, error) {
	var l = int32(b.length)
	if l == 0 {
		if err := binary.Read(b.r, binary.LittleEndian, &l); err != nil {
			return nil, err
		}

		if l < 0 {
			return nil, errors.New("bytearray/plain: len is negative")
		}
	} else if l < 0 {
		return nil, errors.New("bytearray/plain: len is negative")
	}

	buf := make([]byte, l)
	_, err := io.ReadFull(b.r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (b *byteArrayPlainDecoder) decodeValues(dst []interface{}) (int, error) {
	var err error
	for i := range dst {
		if dst[i], err = b.next(); err != nil {
			return i, err
		}
	}
	return len(dst), nil
}

type byteArrayPlainEncoder struct {
	w io.Writer

	length int
}

func (b *byteArrayPlainEncoder) init(w io.Writer) error {
	b.w = w

	return nil
}

func (b *byteArrayPlainEncoder) writeBytes(data []byte) error {
	l := b.length
	if l == 0 { // variable length
		l = len(data)
		l32 := int32(l)
		if err := binary.Write(b.w, binary.LittleEndian, l32); err != nil {
			return err
		}
	} else if len(data) != l {
		return errors.Errorf("the byte array should be with length %d but is %d", l, len(data))
	}

	return writeFull(b.w, data)
}

func (b *byteArrayPlainEncoder) encodeValues(values []interface{}) error {
	for i := range values {
		if err := b.writeBytes(values[i].([]byte)); err != nil {
			return err
		}
	}

	return nil
}

func (*byteArrayPlainEncoder) Close() error {
	return nil
}

type byteArrayDeltaLengthDecoder struct {
	r        io.Reader
	position int
	lens     []int32
}

func (b *byteArrayDeltaLengthDecoder) init(r io.Reader) error {
	b.r = r
	b.position = 0
	lensDecoder := int32DeltaBPDecoder{}
	if err := lensDecoder.init(r); err != nil {
		return err
	}

	b.lens = make([]int32, lensDecoder.valuesCount)
	return decodeInt32(&lensDecoder, b.lens)
}

func (b *byteArrayDeltaLengthDecoder) next() ([]byte, error) {
	if b.position >= len(b.lens) {
		return nil, io.EOF
	}
	size := int(b.lens[b.position])
	value := make([]byte, size)
	if _, err := io.ReadFull(b.r, value); err != nil {
		return nil, errors.Wrap(err, "there is no byte left")
	}
	b.position++

	return value, nil
}

func (b *byteArrayDeltaLengthDecoder) decodeValues(dst []interface{}) (int, error) {
	total := len(dst)
	for i := 0; i < total; i++ {
		v, err := b.next()
		if err != nil {
			return i, err
		}
		dst[i] = v
	}
	return total, nil
}

// this type is used inside the byteArrayDeltaEncoder, the Close method should do the actual write, not before.
type byteArrayDeltaLengthEncoder struct {
	w    io.Writer
	buf  *bytes.Buffer
	lens []interface{}
}

func (b *byteArrayDeltaLengthEncoder) init(w io.Writer) error {
	b.w = w
	b.buf = &bytes.Buffer{}
	return nil
}

func (b *byteArrayDeltaLengthEncoder) writeOne(data []byte) error {
	b.lens = append(b.lens, int32(len(data)))
	return writeFull(b.buf, data)
}

func (b *byteArrayDeltaLengthEncoder) encodeValues(values []interface{}) error {
	if b.lens == nil {
		// this is just for the first time, maybe we need to copy and increase the cap in the next calls?
		b.lens = make([]interface{}, 0, len(values))
	}
	for i := range values {
		if err := b.writeOne(values[i].([]byte)); err != nil {
			return err
		}
	}

	return nil
}

func (b *byteArrayDeltaLengthEncoder) Close() error {
	enc := &int32DeltaBPEncoder{
		deltaBitPackEncoder32: deltaBitPackEncoder32{
			blockSize:      128,
			miniBlockCount: 4,
		},
	}

	if err := encodeValue(b.w, enc, b.lens); err != nil {
		return err
	}

	return writeFull(b.w, b.buf.Bytes())
}

type byteArrayDeltaDecoder struct {
	suffixDecoder byteArrayDeltaLengthDecoder
	prefixLens    []int32
	previousValue []byte
}

func (d *byteArrayDeltaDecoder) init(r io.Reader) error {
	lensDecoder := deltaBitPackDecoder32{}
	if err := lensDecoder.init(r); err != nil {
		return err
	}

	d.prefixLens = make([]int32, lensDecoder.valuesCount)
	if err := decodeInt32(&lensDecoder, d.prefixLens); err != nil {
		return err
	}
	if err := d.suffixDecoder.init(r); err != nil {
		return err
	}

	if len(d.prefixLens) != len(d.suffixDecoder.lens) {
		return errors.New("bytearray/delta: different number of suffixes and prefixes")
	}
	d.previousValue = make([]byte, 0)

	return nil
}

func (d *byteArrayDeltaDecoder) decodeValues(dst []interface{}) (int, error) {
	total := len(dst)
	for i := 0; i < total; i++ {
		suffix, err := d.suffixDecoder.next()
		if err != nil {
			return i, err
		}
		// after this line no error is acceptable
		prefixLen := int(d.prefixLens[d.suffixDecoder.position-1])
		value := make([]byte, 0, prefixLen+len(suffix))
		if len(d.previousValue) < prefixLen {
			// prevent panic from invalid input
			return 0, errors.Errorf("invalid prefix len in the stream, the value is %d byte but the it needs %d byte", len(d.previousValue), prefixLen)
		}
		if prefixLen > 0 {
			value = append(value, d.previousValue[:prefixLen]...)
		}
		value = append(value, suffix...)
		d.previousValue = value
		dst[i] = value
	}

	return total, nil
}

type byteArrayDeltaEncoder struct {
	w io.Writer

	prefixLens    []interface{}
	previousValue []byte

	values *byteArrayDeltaLengthEncoder
}

func (b *byteArrayDeltaEncoder) init(w io.Writer) error {
	b.w = w
	b.prefixLens = nil
	b.previousValue = []byte{}
	b.values = &byteArrayDeltaLengthEncoder{}
	return b.values.init(w)
}

func (b *byteArrayDeltaEncoder) encodeValues(values []interface{}) error {
	if b.prefixLens == nil {
		b.prefixLens = make([]interface{}, 0, len(values))
		b.values.lens = make([]interface{}, 0, len(values))
	}

	for i := range values {
		data := values[i].([]byte)
		pLen := prefix(b.previousValue, data)
		b.prefixLens = append(b.prefixLens, int32(pLen))
		if err := b.values.writeOne(data[pLen:]); err != nil {
			return err
		}
		b.previousValue = data
	}

	return nil
}

func (b *byteArrayDeltaEncoder) Close() error {
	// write the lens first
	enc := &int32DeltaBPEncoder{
		deltaBitPackEncoder32: deltaBitPackEncoder32{
			blockSize:      128,
			miniBlockCount: 4,
		},
	}

	if err := encodeValue(b.w, enc, b.prefixLens); err != nil {
		return err
	}

	return b.values.Close()
}

type byteArrayStore struct {
	repTyp    parquet.FieldRepetitionType
	stats     statistics
	pageStats statistics

	*ColumnParameters
}

func (is *byteArrayStore) getStats() minMaxValues {
	return &is.stats
}

func (is *byteArrayStore) getPageStats() minMaxValues {
	return &is.pageStats
}

func (is *byteArrayStore) params() *ColumnParameters {
	if is.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return is.ColumnParameters
}

func (is *byteArrayStore) sizeOf(v interface{}) int {
	return len(v.([]byte))
}

func (is *byteArrayStore) parquetType() parquet.Type {
	if is.TypeLength != nil && *is.TypeLength > 0 {
		return parquet.Type_FIXED_LEN_BYTE_ARRAY
	}
	return parquet.Type_BYTE_ARRAY
}

func (is *byteArrayStore) repetitionType() parquet.FieldRepetitionType {
	return is.repTyp
}

func (is *byteArrayStore) reset(repetitionType parquet.FieldRepetitionType) {
	is.repTyp = repetitionType

	is.stats.reset()
	is.pageStats.reset()
}

func (is *byteArrayStore) setMinMax(j []byte) error {
	if is.TypeLength != nil && *is.TypeLength > 0 && int32(len(j)) != *is.TypeLength {
		return errors.Errorf("the size of data should be %d but is %d", *is.TypeLength, len(j))
	}
	// For nil value there is no need to set the min/max
	if j == nil {
		return nil
	}

	is.stats.setMinMax(j)
	is.pageStats.setMinMax(j)

	return nil
}

func (is *byteArrayStore) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case []byte:
		vals = []interface{}{typed}
	case [][]byte:
		if is.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			vals[j] = typed[j]
		}
	default:
		return nil, errors.Errorf("unsupported type for storing in []byte column %T => %+v", v, v)
	}

	return vals, nil
}

func (*byteArrayStore) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([][]byte, 0, 1)
	}
	return append(arrayIn.([][]byte), value.([]byte))
}
