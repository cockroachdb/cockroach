package goparquet

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/pkg/errors"

	"github.com/fraugster/parquet-go/parquet"
)

type floatPlainDecoder struct {
	r io.Reader
}

func (f *floatPlainDecoder) init(r io.Reader) error {
	f.r = r

	return nil
}

func (f *floatPlainDecoder) decodeValues(dst []interface{}) (int, error) {
	var data uint32
	for i := range dst {
		if err := binary.Read(f.r, binary.LittleEndian, &data); err != nil {
			return i, err
		}
		dst[i] = math.Float32frombits(data)
	}

	return len(dst), nil
}

type floatPlainEncoder struct {
	w io.Writer
}

func (d *floatPlainEncoder) Close() error {
	return nil
}

func (d *floatPlainEncoder) init(w io.Writer) error {
	d.w = w

	return nil
}

func (d *floatPlainEncoder) encodeValues(values []interface{}) error {
	data := make([]uint32, len(values))
	for i := range values {
		data[i] = math.Float32bits(values[i].(float32))
	}

	return binary.Write(d.w, binary.LittleEndian, data)
}

type floatStore struct {
	repTyp parquet.FieldRepetitionType

	stats     *floatStats
	pageStats *floatStats

	*ColumnParameters
}

func (f *floatStore) getStats() minMaxValues {
	return f.stats
}

func (f *floatStore) getPageStats() minMaxValues {
	return f.pageStats
}

func (f *floatStore) params() *ColumnParameters {
	if f.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return f.ColumnParameters
}

func (*floatStore) sizeOf(v interface{}) int {
	return 4
}

func (f *floatStore) parquetType() parquet.Type {
	return parquet.Type_FLOAT
}

func (f *floatStore) repetitionType() parquet.FieldRepetitionType {
	return f.repTyp
}

func (f *floatStore) reset(rep parquet.FieldRepetitionType) {
	f.repTyp = rep
	f.stats.reset()
	f.pageStats.reset()
}

func (f *floatStore) setMinMax(j float32) {
	f.stats.setMinMax(j)
	f.pageStats.setMinMax(j)
}

func (f *floatStore) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case float32:
		f.setMinMax(typed)
		vals = []interface{}{typed}
	case []float32:
		if f.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			f.setMinMax(typed[j])
			vals[j] = typed[j]
		}
	default:
		return nil, errors.Errorf("unsupported type for storing in float32 column: %T => %+v", v, v)
	}

	return vals, nil
}

func (*floatStore) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]float32, 0, 1)
	}
	return append(arrayIn.([]float32), value.(float32))
}
