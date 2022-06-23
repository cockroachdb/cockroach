package goparquet

import (
	"io"

	"github.com/fraugster/parquet-go/parquet"

	"github.com/pkg/errors"
)

type int96PlainDecoder struct {
	r io.Reader
}

func (i *int96PlainDecoder) init(r io.Reader) error {
	i.r = r

	return nil
}

func (i *int96PlainDecoder) decodeValues(dst []interface{}) (int, error) {
	idx := 0
	for range dst {
		var data [12]byte
		// this one is a little tricky do not use ReadFull here
		n, err := i.r.Read(data[:])
		// make sure we handle the read data first then handle the error
		if n == 12 {
			dst[idx] = data
			idx++
		}

		if err != nil && (n == 0 || n == 12) {
			return idx, err
		}

		if err != nil {
			return idx, errors.Wrap(err, "not enough byte to read the Int96")
		}
	}
	return len(dst), nil
}

type int96PlainEncoder struct {
	w io.Writer
}

func (i *int96PlainEncoder) Close() error {
	return nil
}

func (i *int96PlainEncoder) init(w io.Writer) error {
	i.w = w

	return nil
}

func (i *int96PlainEncoder) encodeValues(values []interface{}) error {
	data := make([]byte, len(values)*12)
	for j := range values {
		i96 := values[j].([12]byte)
		copy(data[j*12:], i96[:])
	}

	return writeFull(i.w, data)
}

type int96Store struct {
	byteArrayStore
}

func (*int96Store) sizeOf(v interface{}) int {
	return 12
}

func (is *int96Store) parquetType() parquet.Type {
	return parquet.Type_INT96
}

func (is *int96Store) repetitionType() parquet.FieldRepetitionType {
	return is.repTyp
}

func (is *int96Store) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case [12]byte:
		if err := is.setMinMax(typed[:]); err != nil {
			return nil, err
		}
		vals = []interface{}{typed}
	case [][12]byte:
		if is.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			if err := is.setMinMax(typed[j][:]); err != nil {
				return nil, err
			}
			vals[j] = typed[j]
		}
	default:
		return nil, errors.Errorf("unsupported type for storing in Int96 column: %T => %+v", v, v)
	}

	return vals, nil
}

func (*int96Store) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([][12]byte, 0, 1)
	}
	return append(arrayIn.([][12]byte), value.([12]byte))
}
