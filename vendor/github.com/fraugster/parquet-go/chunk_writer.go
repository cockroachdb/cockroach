package goparquet

import (
	"bytes"
	"context"
	"math"
	"sort"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

func getBooleanValuesEncoder(pageEncoding parquet.Encoding) (valuesEncoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &booleanPlainEncoder{}, nil
	case parquet.Encoding_RLE:
		return &booleanRLEEncoder{}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for boolean", pageEncoding)
	}
}

func getByteArrayValuesEncoder(pageEncoding parquet.Encoding, dictValues []interface{}) (valuesEncoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &byteArrayPlainEncoder{}, nil
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		return &byteArrayDeltaLengthEncoder{}, nil
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		return &byteArrayDeltaEncoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictEncoder{dictValues: dictValues}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for binary", pageEncoding)
	}
}

func getFixedLenByteArrayValuesEncoder(pageEncoding parquet.Encoding, len int, dictValues []interface{}) (valuesEncoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &byteArrayPlainEncoder{length: len}, nil
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		return &byteArrayDeltaEncoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictEncoder{dictValues: dictValues}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for fixed_len_byte_array(%d)", pageEncoding, len)
	}
}

func getInt32ValuesEncoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, dictValues []interface{}) (valuesEncoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &int32PlainEncoder{}, nil
	case parquet.Encoding_DELTA_BINARY_PACKED:
		return &int32DeltaBPEncoder{
			deltaBitPackEncoder32: deltaBitPackEncoder32{
				blockSize:      128,
				miniBlockCount: 4,
			},
		}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictEncoder{dictValues: dictValues}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for int32", pageEncoding)
	}
}

func getInt64ValuesEncoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, dictValues []interface{}) (valuesEncoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &int64PlainEncoder{}, nil
	case parquet.Encoding_DELTA_BINARY_PACKED:
		return &int64DeltaBPEncoder{
			deltaBitPackEncoder64: deltaBitPackEncoder64{
				blockSize:      128,
				miniBlockCount: 4,
			},
		}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictEncoder{
			dictValues: dictValues,
		}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for int64", pageEncoding)
	}
}

func getValuesEncoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, dictValues []interface{}) (valuesEncoder, error) {
	// Change the deprecated value
	if pageEncoding == parquet.Encoding_PLAIN_DICTIONARY {
		pageEncoding = parquet.Encoding_RLE_DICTIONARY
	}

	switch *typ.Type {
	case parquet.Type_BOOLEAN:
		return getBooleanValuesEncoder(pageEncoding)

	case parquet.Type_BYTE_ARRAY:
		return getByteArrayValuesEncoder(pageEncoding, dictValues)

	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ.Type)
		}
		return getFixedLenByteArrayValuesEncoder(pageEncoding, int(*typ.TypeLength), dictValues)

	case parquet.Type_FLOAT:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &floatPlainEncoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictEncoder{
				dictValues: dictValues,
			}, nil
		}

	case parquet.Type_DOUBLE:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &doublePlainEncoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictEncoder{
				dictValues: dictValues,
			}, nil
		}

	case parquet.Type_INT32:
		return getInt32ValuesEncoder(pageEncoding, typ, dictValues)

	case parquet.Type_INT64:
		return getInt64ValuesEncoder(pageEncoding, typ, dictValues)

	case parquet.Type_INT96:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &int96PlainEncoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictEncoder{
				dictValues: dictValues,
			}, nil
		}

	default:
		return nil, errors.Errorf("unsupported type: %s", typ.Type)
	}

	return nil, errors.Errorf("unsupported encoding %s for %s type", pageEncoding, typ.Type)
}

func getDictValuesEncoder(typ *parquet.SchemaElement) (valuesEncoder, error) {
	switch *typ.Type {
	case parquet.Type_BYTE_ARRAY:
		return &byteArrayPlainEncoder{}, nil
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ)
		}
		return &byteArrayPlainEncoder{length: int(*typ.TypeLength)}, nil
	case parquet.Type_FLOAT:
		return &floatPlainEncoder{}, nil
	case parquet.Type_DOUBLE:
		return &doublePlainEncoder{}, nil
	case parquet.Type_INT32:
		return &int32PlainEncoder{}, nil
	case parquet.Type_INT64:
		return &int64PlainEncoder{}, nil
	case parquet.Type_INT96:
		return &int96PlainEncoder{}, nil
	}

	return nil, errors.Errorf("type %s is not supported for dict value encoder", typ)
}

func writeChunk(ctx context.Context, w writePos, sch *schema, col *Column, codec parquet.CompressionCodec, pageFn newDataPageFunc, kvMetaData map[string]string) (*parquet.ColumnChunk, error) {
	pos := w.Pos() // Save the position before writing data
	chunkOffset := pos
	var (
		dictPageOffset *int64
		// NOTE :
		// This is documentation on these two field :
		//  - TotalUncompressedSize: total byte size of all uncompressed pages in this column chunk (including the headers) *
		//  - TotalCompressedSize: total byte size of all compressed pages in this column chunk (including the headers) *
		// the including header part is confusing. for uncompressed size, we can use the position, but for the compressed
		// the only value we have doesn't contain the header
		totalComp   int64
		totalUnComp int64
	)

	// flush final data page before writing dictionary page (if applicable) and all data pages.
	if err := col.data.flushPage(sch, true); err != nil {
		return nil, err
	}

	dictValues := []interface{}{}
	indices := map[interface{}]int32{}

	for _, page := range col.data.dataPages {
		for _, v := range page.values {
			k := mapKey(v)
			if _, ok := indices[k]; !ok {
				idx := int32(len(dictValues))
				indices[k] = idx
				dictValues = append(dictValues, v)
			}
		}
	}

	useDict := true
	if len(dictValues) > math.MaxInt16 {
		useDict = false
	}
	if *col.Type() == parquet.Type_BOOLEAN { // never ever use dictionary encoding on booleans.
		useDict = false
	}
	if !col.data.useDictionary() {
		useDict = false
	}

	if useDict {
		tmp := pos // make a copy, do not use the pos here
		dictPageOffset = &tmp
		dict := &dictPageWriter{}
		if err := dict.init(sch, col, codec, dictValues); err != nil {
			return nil, err
		}
		compSize, unCompSize, err := dict.write(ctx, sch, w)
		if err != nil {
			return nil, err
		}
		totalComp = w.Pos() - pos
		// Header size plus the rLevel and dLevel size
		headerSize := totalComp - int64(compSize)
		totalUnComp = int64(unCompSize) + headerSize
		pos = w.Pos() // Move position for data pos
	}

	var (
		compSize, unCompSize  int
		numValues, nullValues int64
	)

	for _, page := range col.data.dataPages {
		pw := pageFn(useDict, dictValues, page, sch.enableCRC)

		if err := pw.init(sch, col, codec); err != nil {
			return nil, err
		}

		var buf bytes.Buffer

		compressed, uncompressed, err := pw.write(ctx, &buf)
		if err != nil {
			return nil, err
		}

		compSize += compressed
		unCompSize += uncompressed
		numValues += page.numValues
		nullValues += page.nullValues
		if _, err := w.Write(buf.Bytes()); err != nil {
			return nil, err
		}
	}

	col.data.dataPages = nil

	totalComp += w.Pos() - pos
	// Header size plus the rLevel and dLevel size
	headerSize := totalComp - int64(compSize)
	totalUnComp += int64(unCompSize) + headerSize

	encodings := make([]parquet.Encoding, 0, 3)
	encodings = append(encodings,
		parquet.Encoding_RLE,
		col.data.encoding(),
	)
	if useDict {
		encodings[1] = parquet.Encoding_PLAIN // In dictionary we use PLAIN for the data, not the column encoding
		encodings = append(encodings, parquet.Encoding_RLE_DICTIONARY)
	}

	keyValueMetaData := make([]*parquet.KeyValue, 0, len(kvMetaData))
	for k, v := range kvMetaData {
		value := v
		keyValueMetaData = append(keyValueMetaData, &parquet.KeyValue{Key: k, Value: &value})
	}
	sort.Slice(keyValueMetaData, func(i, j int) bool {
		return keyValueMetaData[i].Key < keyValueMetaData[j].Key
	})

	distinctCount := int64(len(dictValues))

	stats := &parquet.Statistics{
		MinValue:      col.data.getStats().minValue(),
		MaxValue:      col.data.getStats().maxValue(),
		NullCount:     &nullValues,
		DistinctCount: &distinctCount,
	}

	ch := &parquet.ColumnChunk{
		FilePath:   nil, // No support for external
		FileOffset: chunkOffset,
		MetaData: &parquet.ColumnMetaData{
			Type:                  col.data.parquetType(),
			Encodings:             encodings,
			PathInSchema:          col.pathArray(),
			Codec:                 codec,
			NumValues:             numValues + nullValues,
			TotalUncompressedSize: totalUnComp,
			TotalCompressedSize:   totalComp,
			KeyValueMetadata:      keyValueMetaData,
			DataPageOffset:        pos,
			IndexPageOffset:       nil,
			DictionaryPageOffset:  dictPageOffset,
			Statistics:            stats,
			EncodingStats:         nil,
		},
		OffsetIndexOffset: nil,
		OffsetIndexLength: nil,
		ColumnIndexOffset: nil,
		ColumnIndexLength: nil,
	}

	return ch, nil
}

func writeRowGroup(ctx context.Context, w writePos, sch *schema, codec parquet.CompressionCodec, pageFn newDataPageFunc, h *flushRowGroupOptionHandle) ([]*parquet.ColumnChunk, error) {
	dataCols := sch.Columns()
	var res = make([]*parquet.ColumnChunk, 0, len(dataCols))
	for _, ci := range dataCols {
		ch, err := writeChunk(ctx, w, sch, ci, codec, pageFn, h.getMetaData(ci.FlatName()))
		if err != nil {
			return nil, err
		}

		res = append(res, ch)
	}

	return res, nil
}
