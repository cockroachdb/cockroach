package goparquet

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"

	"github.com/pkg/errors"

	"github.com/fraugster/parquet-go/parquet"
)

type getValueDecoderFn func(parquet.Encoding) (valuesDecoder, error)
type getLevelDecoder func(parquet.Encoding) (levelDecoder, error)

func getDictValuesDecoder(typ *parquet.SchemaElement) (valuesDecoder, error) {
	switch *typ.Type {
	case parquet.Type_BYTE_ARRAY:
		return &byteArrayPlainDecoder{}, nil
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ)
		}
		return &byteArrayPlainDecoder{length: int(*typ.TypeLength)}, nil
	case parquet.Type_FLOAT:
		return &floatPlainDecoder{}, nil
	case parquet.Type_DOUBLE:
		return &doublePlainDecoder{}, nil
	case parquet.Type_INT32:
		return &int32PlainDecoder{}, nil
	case parquet.Type_INT64:
		return &int64PlainDecoder{}, nil
	case parquet.Type_INT96:
		return &int96PlainDecoder{}, nil
	}

	return nil, errors.Errorf("type %s is not supported for dict value encoder", typ)
}

func getBooleanValuesDecoder(pageEncoding parquet.Encoding) (valuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &booleanPlainDecoder{}, nil
	case parquet.Encoding_RLE:
		return &booleanRLEDecoder{}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for boolean", pageEncoding)
	}
}

func getByteArrayValuesDecoder(pageEncoding parquet.Encoding, dictValues []interface{}) (valuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &byteArrayPlainDecoder{}, nil
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		return &byteArrayDeltaLengthDecoder{}, nil
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		return &byteArrayDeltaDecoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictDecoder{uniqueValues: dictValues}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for binary", pageEncoding)
	}
}

func getFixedLenByteArrayValuesDecoder(pageEncoding parquet.Encoding, len int, dictValues []interface{}) (valuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &byteArrayPlainDecoder{length: len}, nil
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		return &byteArrayDeltaDecoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictDecoder{uniqueValues: dictValues}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for fixed_len_byte_array(%d)", pageEncoding, len)
	}
}

func getInt32ValuesDecoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, dictValues []interface{}) (valuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &int32PlainDecoder{}, nil
	case parquet.Encoding_DELTA_BINARY_PACKED:
		return &int32DeltaBPDecoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictDecoder{uniqueValues: dictValues}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for int32", pageEncoding)
	}
}

func getInt64ValuesDecoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, dictValues []interface{}) (valuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &int64PlainDecoder{}, nil
	case parquet.Encoding_DELTA_BINARY_PACKED:
		return &int64DeltaBPDecoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &dictDecoder{uniqueValues: dictValues}, nil
	default:
		return nil, errors.Errorf("unsupported encoding %s for int64", pageEncoding)
	}
}

func getValuesDecoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, dictValues []interface{}) (valuesDecoder, error) {
	// Change the deprecated value
	if pageEncoding == parquet.Encoding_PLAIN_DICTIONARY {
		pageEncoding = parquet.Encoding_RLE_DICTIONARY
	}

	switch *typ.Type {
	case parquet.Type_BOOLEAN:
		return getBooleanValuesDecoder(pageEncoding)

	case parquet.Type_BYTE_ARRAY:
		return getByteArrayValuesDecoder(pageEncoding, dictValues)

	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ.Type)
		}
		return getFixedLenByteArrayValuesDecoder(pageEncoding, int(*typ.TypeLength), dictValues)
	case parquet.Type_FLOAT:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &floatPlainDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictDecoder{uniqueValues: dictValues}, nil
		}

	case parquet.Type_DOUBLE:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &doublePlainDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictDecoder{uniqueValues: dictValues}, nil
		}

	case parquet.Type_INT32:
		return getInt32ValuesDecoder(pageEncoding, typ, dictValues)

	case parquet.Type_INT64:
		return getInt64ValuesDecoder(pageEncoding, typ, dictValues)

	case parquet.Type_INT96:
		switch pageEncoding {
		case parquet.Encoding_PLAIN:
			return &int96PlainDecoder{}, nil
		case parquet.Encoding_RLE_DICTIONARY:
			return &dictDecoder{uniqueValues: dictValues}, nil
		}

	default:
		return nil, errors.Errorf("unsupported type %s", typ.Type)
	}

	return nil, errors.Errorf("unsupported encoding %s for %s type", pageEncoding, typ.Type)
}

func readPageBlock(r io.Reader, codec parquet.CompressionCodec, compressedSize int32, uncompressedSize int32, validateCRC bool, crc *int32) ([]byte, error) {
	if compressedSize < 0 || uncompressedSize < 0 {
		return nil, errors.New("invalid page data size")
	}

	dataPageBlock, err := io.ReadAll(io.LimitReader(r, int64(compressedSize)))
	if err != nil {
		return nil, errors.Wrap(err, "read failed")
	}

	if validateCRC && crc != nil {
		if sum := crc32.ChecksumIEEE(dataPageBlock); sum != uint32(*crc) {
			return nil, fmt.Errorf("CRC32 check failed: expected CRC32 %x, got %x", sum, uint32(*crc))
		}
	}

	return dataPageBlock, nil
}

func readPages(ctx context.Context, sch *schema, r *offsetReader, col *Column, chunkMeta *parquet.ColumnMetaData, dDecoder, rDecoder getLevelDecoder) (pages []pageReader, useDict bool, err error) {
	var (
		dictPage *dictPageReader
	)

	for {
		if chunkMeta.TotalCompressedSize-r.Count() <= 0 {
			break
		}
		ph := &parquet.PageHeader{}
		if err := readThrift(ctx, ph, r); err != nil {
			return nil, false, err
		}

		if ph.Type == parquet.PageType_DICTIONARY_PAGE {
			if dictPage != nil {
				return nil, false, errors.New("there should be only one dictionary")
			}
			p := &dictPageReader{validateCRC: sch.validateCRC}
			de, err := getDictValuesDecoder(col.Element())
			if err != nil {
				return nil, false, err
			}
			if err := p.init(de); err != nil {
				return nil, false, err
			}

			if err := p.read(r, ph, chunkMeta.Codec); err != nil {
				return nil, false, err
			}

			dictPage = p

			// Go to the next data Page
			// if we have a DictionaryPageOffset we should return to DataPageOffset
			if chunkMeta.DictionaryPageOffset != nil {
				if *chunkMeta.DictionaryPageOffset != r.offset {
					if _, err := r.Seek(chunkMeta.DataPageOffset, io.SeekStart); err != nil {
						return nil, false, err
					}
				}
			}
			continue // go to next page
		}

		var p pageReader
		switch ph.Type {
		case parquet.PageType_DATA_PAGE:
			p = &dataPageReaderV1{
				ph: ph,
			}
		case parquet.PageType_DATA_PAGE_V2:
			p = &dataPageReaderV2{
				ph: ph,
			}
		default:
			return nil, false, errors.Errorf("DATA_PAGE or DATA_PAGE_V2 type supported, but was %s", ph.Type)
		}
		var dictValue []interface{}
		if dictPage != nil {
			dictValue = dictPage.values
		}
		var fn = func(typ parquet.Encoding) (valuesDecoder, error) {
			return getValuesDecoder(typ, col.Element(), clone(dictValue))
		}
		if err := p.init(dDecoder, rDecoder, fn); err != nil {
			return nil, false, err
		}

		if err := p.read(r, ph, chunkMeta.Codec, sch.validateCRC); err != nil {
			return nil, false, err
		}
		pages = append(pages, p)
	}

	return pages, dictPage != nil, nil
}

func clone(in []interface{}) []interface{} {
	out := make([]interface{}, len(in))
	copy(out, in)
	return out
}

func skipChunk(r io.Seeker, col *Column, chunk *parquet.ColumnChunk) error {
	if chunk.FilePath != nil {
		return fmt.Errorf("nyi: data is in another file: '%s'", *chunk.FilePath)
	}

	c := col.Index()
	// chunk.FileOffset is useless so ChunkMetaData is required here
	// as we cannot read it from r
	// see https://issues.apache.org/jira/browse/PARQUET-291
	if chunk.MetaData == nil {
		return errors.Errorf("missing meta data for Column %c", c)
	}

	if typ := *col.Element().Type; chunk.MetaData.Type != typ {
		return errors.Errorf("wrong type in Column chunk metadata, expected %s was %s",
			typ, chunk.MetaData.Type)
	}

	offset := chunk.MetaData.DataPageOffset
	if chunk.MetaData.DictionaryPageOffset != nil {
		offset = *chunk.MetaData.DictionaryPageOffset
	}

	offset += chunk.MetaData.TotalCompressedSize
	_, err := r.Seek(offset, io.SeekStart)
	return err
}

func readChunk(ctx context.Context, sch *schema, r io.ReadSeeker, col *Column, chunk *parquet.ColumnChunk) (pages []pageReader, useDict bool, err error) {
	if chunk.FilePath != nil {
		return nil, false, fmt.Errorf("nyi: data is in another file: '%s'", *chunk.FilePath)
	}

	c := col.Index()
	// chunk.FileOffset is useless so ChunkMetaData is required here
	// as we cannot read it from r
	// see https://issues.apache.org/jira/browse/PARQUET-291
	if chunk.MetaData == nil {
		return nil, false, errors.Errorf("missing meta data for Column %c", c)
	}

	if typ := *col.Element().Type; chunk.MetaData.Type != typ {
		return nil, false, errors.Errorf("wrong type in Column chunk metadata, expected %s was %s",
			typ, chunk.MetaData.Type)
	}

	offset := chunk.MetaData.DataPageOffset
	if chunk.MetaData.DictionaryPageOffset != nil {
		offset = *chunk.MetaData.DictionaryPageOffset
	}
	// Seek to the beginning of the first Page
	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		return nil, false, err
	}

	reader := &offsetReader{
		inner:  r,
		offset: offset,
		count:  0,
	}

	rDecoder := func(enc parquet.Encoding) (levelDecoder, error) {
		if enc != parquet.Encoding_RLE {
			return nil, errors.Errorf("%q is not supported for definition and repetition level", enc)
		}
		dec := newHybridDecoder(bits.Len16(col.MaxRepetitionLevel()))
		dec.buffered = true
		return &levelDecoderWrapper{decoder: dec, max: col.MaxRepetitionLevel()}, nil
	}

	dDecoder := func(enc parquet.Encoding) (levelDecoder, error) {
		if enc != parquet.Encoding_RLE {
			return nil, errors.Errorf("%q is not supported for definition and repetition level", enc)
		}
		dec := newHybridDecoder(bits.Len16(col.MaxDefinitionLevel()))
		dec.buffered = true
		return &levelDecoderWrapper{decoder: dec, max: col.MaxDefinitionLevel()}, nil
	}

	if col.MaxRepetitionLevel() == 0 {
		rDecoder = func(parquet.Encoding) (levelDecoder, error) {
			return &levelDecoderWrapper{decoder: constDecoder(0), max: col.MaxRepetitionLevel()}, nil
		}
	}

	if col.MaxDefinitionLevel() == 0 {
		dDecoder = func(parquet.Encoding) (levelDecoder, error) {
			return &levelDecoderWrapper{decoder: constDecoder(0), max: col.MaxDefinitionLevel()}, nil
		}
	}
	return readPages(ctx, sch, reader, col, chunk.MetaData, dDecoder, rDecoder)
}

func readPageData(col *Column, pages []pageReader, useDict bool) error {
	s := col.getColumnStore()
	s.pageIdx, s.pages = 0, pages
	s.useDict = useDict
	if err := s.readNextPage(); err != nil {
		return nil
	}

	return nil
}

func readRowGroup(ctx context.Context, r io.ReadSeeker, sch *schema, rowGroups *parquet.RowGroup) error {
	dataCols := sch.Columns()
	sch.resetData()
	sch.setNumRecords(rowGroups.NumRows)
	for _, c := range dataCols {
		idx := c.Index()
		if len(rowGroups.Columns) <= idx {
			return fmt.Errorf("column index %d is out of bounds", idx)
		}
		chunk := rowGroups.Columns[c.Index()]
		if !sch.isSelected(c.flatName) {
			if err := skipChunk(r, c, chunk); err != nil {
				return err
			}
			c.data.skipped = true
			continue
		}
		pages, useDict, err := readChunk(ctx, sch, r, c, chunk)
		if err != nil {
			return err
		}
		if err := readPageData(c, pages, useDict); err != nil {
			return err
		}
	}

	return nil
}
