package goparquet

import (
	"io"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

type booleanPlainDecoder struct {
	r    io.Reader
	left []bool
}

// copy the left overs from the previous call. instead of returning an empty subset of the old slice,
// it delete the slice (by returning nil) so there is no memory leak because of the underlying array
// the return value is the new left over and the number of read message
func copyLeftOvers(dst []interface{}, src []bool) ([]bool, int) {
	size := len(dst)
	var clean bool
	if len(src) <= size {
		size = len(src)
		clean = true
	}

	for i := 0; i < size; i++ {
		dst[i] = src[i]
	}
	if clean {
		return nil, size
	}

	return src[size:], size
}

func (b *booleanPlainDecoder) init(r io.Reader) error {
	b.r = r
	b.left = nil

	return nil
}

func (b *booleanPlainDecoder) decodeValues(dst []interface{}) (int, error) {
	var start int
	if len(b.left) > 0 {
		// there is a leftover from the last run
		b.left, start = copyLeftOvers(dst, b.left)
		if b.left != nil {
			return len(dst), nil
		}
	}

	buf := make([]byte, 1)
	for i := start; i < len(dst); i += 8 {
		if _, err := io.ReadFull(b.r, buf); err != nil {
			return i, err
		}
		d := unpack8int32_1(buf)
		for j := 0; j < 8; j++ {
			if i+j < len(dst) {
				dst[i+j] = d[j] == 1
			} else {
				b.left = append(b.left, d[j] == 1)
			}
		}
	}

	return len(dst), nil
}

type booleanPlainEncoder struct {
	w    io.Writer
	data *packedArray
}

func (b *booleanPlainEncoder) Close() error {
	b.data.flush()
	return writeFull(b.w, b.data.data)
}

func (b *booleanPlainEncoder) init(w io.Writer) error {
	b.w = w
	b.data = &packedArray{}
	b.data.reset(1)
	return nil
}

func (b *booleanPlainEncoder) encodeValues(values []interface{}) error {
	for i := range values {
		var v int32
		if values[i].(bool) {
			v = 1
		}
		b.data.appendSingle(v)
	}

	return nil
}

type booleanRLEDecoder struct {
	decoder *hybridDecoder
}

func (b *booleanRLEDecoder) init(r io.Reader) error {
	b.decoder = newHybridDecoder(1)
	return b.decoder.initSize(r)
}

func (b *booleanRLEDecoder) decodeValues(dst []interface{}) (int, error) {
	total := len(dst)
	for i := 0; i < total; i++ {
		n, err := b.decoder.next()
		if err != nil {
			return i, err
		}
		dst[i] = n == 1
	}

	return total, nil
}

type booleanRLEEncoder struct {
	encoder *hybridEncoder
}

func (b *booleanRLEEncoder) Close() error {
	return b.encoder.Close()
}

func (b *booleanRLEEncoder) init(w io.Writer) error {
	b.encoder = newHybridEncoder(1)
	return b.encoder.initSize(w)
}

func (b *booleanRLEEncoder) encodeValues(values []interface{}) error {
	buf := make([]int32, len(values))
	for i := range values {
		if values[i].(bool) {
			buf[i] = 1
		} else {
			buf[i] = 0
		}
	}

	return b.encoder.encode(buf)
}

type booleanStore struct {
	repTyp parquet.FieldRepetitionType
	*ColumnParameters
}

func (b *booleanStore) params() *ColumnParameters {
	if b.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return b.ColumnParameters
}

func (b *booleanStore) sizeOf(v interface{}) int {
	// Cheating here. boolean size is one bit, but the size is in byte. so zero to make sure
	// we never use dictionary on this.
	return 0
}

func (b *booleanStore) parquetType() parquet.Type {
	return parquet.Type_BOOLEAN
}

func (b *booleanStore) repetitionType() parquet.FieldRepetitionType {
	return b.repTyp
}

func (b *booleanStore) reset(repetitionType parquet.FieldRepetitionType) {
	b.repTyp = repetitionType
}

func (b *booleanStore) getStats() minMaxValues {
	return &nilStats{}
}

func (b *booleanStore) getPageStats() minMaxValues {
	return &nilStats{}
}

func (b *booleanStore) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case bool:
		vals = []interface{}{typed}
	case []bool:
		if b.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			vals[j] = typed[j]
		}
	default:
		return nil, errors.Errorf("unsupported type for storing in bool column: %T => %+v", v, v)
	}

	return vals, nil
}

func (b *booleanStore) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]bool, 0, 1)
	}
	return append(arrayIn.([]bool), value.(bool))
}
