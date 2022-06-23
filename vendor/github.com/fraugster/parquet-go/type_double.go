package goparquet

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

type doublePlainDecoder struct {
	r io.Reader
}

func (d *doublePlainDecoder) init(r io.Reader) error {
	d.r = r

	return nil
}

func (d *doublePlainDecoder) decodeValues(dst []interface{}) (int, error) {
	var data uint64
	for i := range dst {
		if err := binary.Read(d.r, binary.LittleEndian, &data); err != nil {
			return i, err
		}
		dst[i] = math.Float64frombits(data)
	}

	return len(dst), nil
}

type doublePlainEncoder struct {
	w io.Writer
}

func (d *doublePlainEncoder) Close() error {
	return nil
}

func (d *doublePlainEncoder) init(w io.Writer) error {
	d.w = w

	return nil
}

func (d *doublePlainEncoder) encodeValues(values []interface{}) error {
	data := make([]uint64, len(values))
	for i := range values {
		data[i] = math.Float64bits(values[i].(float64))
	}

	return binary.Write(d.w, binary.LittleEndian, data)
}

type doubleStore struct {
	repTyp parquet.FieldRepetitionType

	stats     *doubleStats
	pageStats *doubleStats

	*ColumnParameters
}

func (f *doubleStore) getStats() minMaxValues {
	return f.stats
}

func (f *doubleStore) getPageStats() minMaxValues {
	return f.pageStats
}

func (f *doubleStore) params() *ColumnParameters {
	if f.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return f.ColumnParameters
}

func (*doubleStore) sizeOf(v interface{}) int {
	return 8
}

func (f *doubleStore) parquetType() parquet.Type {
	return parquet.Type_DOUBLE
}

func (f *doubleStore) repetitionType() parquet.FieldRepetitionType {
	return f.repTyp
}

func (f *doubleStore) reset(rep parquet.FieldRepetitionType) {
	f.repTyp = rep
	f.stats.reset()
	f.pageStats.reset()
}

func (f *doubleStore) setMinMax(j float64) {
	f.stats.setMinMax(j)
	f.pageStats.setMinMax(j)
}

func (f *doubleStore) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case float64:
		f.setMinMax(typed)
		vals = []interface{}{typed}
	case []float64:
		if f.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			f.setMinMax(typed[j])
			vals[j] = typed[j]
		}
	default:
		return nil, errors.Errorf("unsupported type for storing in float64 column: %T => %+v", v, v)
	}

	return vals, nil
}

func (*doubleStore) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]float64, 0, 1)
	}
	return append(arrayIn.([]float64), value.(float64))
}
