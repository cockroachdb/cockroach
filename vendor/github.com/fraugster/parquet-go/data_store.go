package goparquet

import (
	"math/bits"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

// ColumnStore is the read/write implementation for a column. It buffers a single
// column's data that is to be written to a parquet file, knows how to encode this
// data and will choose an optimal way according to heuristics. It also ensures the
// correct decoding of column data to be read.
type ColumnStore struct {
	typedColumnStore

	repTyp parquet.FieldRepetitionType

	pages   []pageReader
	pageIdx int

	values *dictStore

	dLevels *packedArray
	rLevels *packedArray

	enc     parquet.Encoding
	readPos int

	useDict bool

	skipped bool

	dataPages []*dataPage

	maxPageSize int64

	prevNumRecords int64 // this is just for correctly calculating how many rows are in a data page.
}

type dataPage struct {
	values     []interface{}
	rL         *packedArray
	dL         *packedArray
	numValues  int64
	nullValues int64
	numRows    int64
	stats      *parquet.Statistics
}

// useDictionary is simply a function to decide to use dictionary or not.
func (cs *ColumnStore) useDictionary() bool {
	return cs.useDict
}

func (cs *ColumnStore) encoding() parquet.Encoding {
	return cs.enc
}

func (cs *ColumnStore) repetitionType() parquet.FieldRepetitionType {
	return cs.repTyp
}

func (cs *ColumnStore) reset(rep parquet.FieldRepetitionType, maxR, maxD uint16) {
	if cs.typedColumnStore == nil {
		panic("generic should be used with typed column store")
	}
	cs.repTyp = rep
	if cs.values == nil {
		cs.values = &dictStore{}
		cs.rLevels = &packedArray{}
		cs.dLevels = &packedArray{}
	}
	cs.values.init()
	cs.rLevels.reset(bits.Len16(maxR))
	cs.dLevels.reset(bits.Len16(maxD))
	cs.readPos = 0
	cs.skipped = false
	cs.prevNumRecords = 0

	cs.typedColumnStore.reset(rep)
}

func (cs *ColumnStore) appendRDLevel(rl, dl uint16) {
	cs.rLevels.appendSingle(int32(rl))
	cs.dLevels.appendSingle(int32(dl))
}

// Add One row, if the value is null, call Add() , if the value is repeated, call all value in array
// the second argument s the definition level
// if there is a data the the result should be true, if there is only null (or empty array), the the result should be false
func (cs *ColumnStore) add(v interface{}, dL uint16, maxRL, rL uint16) error {
	// if the current column is repeated, we should increase the maxRL here
	if cs.repTyp == parquet.FieldRepetitionType_REPEATED {
		maxRL++
	}
	if rL > maxRL {
		rL = maxRL
	}
	// the dL is a little tricky. there is some case if the REQUIRED field here are nil (since there is something above
	// them is nil) they can not be the first level, but if they are in the next levels, is actually ok, but the
	// level is one less
	if v == nil {
		cs.appendRDLevel(rL, dL)
		cs.values.addValue(nil, 0)
		return nil
	}
	vals, err := cs.getValues(v)
	if err != nil {
		return err
	}
	if len(vals) == 0 {
		// the MaxRl might be increased in the beginning and increased again in the next call but for nil its not important
		return cs.add(nil, dL, maxRL, rL)
	}

	for i, j := range vals {
		cs.values.addValue(j, cs.sizeOf(j))
		tmp := dL
		if cs.repTyp != parquet.FieldRepetitionType_REQUIRED {
			tmp++
		}

		if i == 0 {
			cs.appendRDLevel(rL, tmp)
		} else {
			cs.appendRDLevel(maxRL, tmp)
		}
	}

	return nil
}

func (cs *ColumnStore) estimateSize() (total int64) {
	dictSize, noDictSize := cs.values.sizes()
	if cs.useDictionary() {
		total += dictSize
	} else {
		total += noDictSize
	}
	total += int64(len(cs.rLevels.data) + len(cs.dLevels.data))
	return total
}

func (cs *ColumnStore) getMaxPageSize() int64 {
	if cs.maxPageSize == 0 {
		return 1024 * 1024
	}
	return cs.maxPageSize
}

func (cs *ColumnStore) flushPage(sch *schema, force bool) error {
	size := cs.estimateSize()

	if !force && size < cs.getMaxPageSize() {
		return nil
	}

	numRows := sch.numRecords - cs.prevNumRecords
	cs.prevNumRecords = sch.numRecords

	cs.dataPages = append(cs.dataPages, &dataPage{
		values:     cs.values.getValues(),
		rL:         cs.rLevels,
		dL:         cs.dLevels,
		numValues:  int64(cs.values.numValues()),
		nullValues: int64(cs.values.nullValueCount()),
		numRows:    numRows,
		stats: &parquet.Statistics{
			NullCount:     int64Ptr(int64(cs.values.nullValueCount())),
			DistinctCount: int64Ptr(cs.values.distinctValueCount()),
			MaxValue:      cs.getPageStats().maxValue(),
			MinValue:      cs.getPageStats().minValue(),
		},
	})

	cs.resetData()

	return nil
}

func int64Ptr(v int64) *int64 {
	return &v
}

// getRDLevelAt return the next rLevel in the read position, if there is no value left, it returns true
// if the position is less than zero, then it returns the current position
// NOTE: make sure always r is before d, in any function
func (cs *ColumnStore) getRDLevelAt(pos int) (int32, int32, bool) {
	if pos < 0 {
		pos = cs.readPos
	}
	if pos >= cs.rLevels.count || pos >= cs.dLevels.count {
		return 0, 0, true
	}
	dl, err := cs.dLevels.at(pos)
	if err != nil {
		return 0, 0, true
	}
	rl, err := cs.rLevels.at(pos)
	if err != nil {
		return 0, 0, true
	}

	return rl, dl, false
}

func (cs *ColumnStore) getNext() (v interface{}, err error) {
	v, err = cs.values.getNextValue()
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (cs *ColumnStore) resetData() {
	cs.readPos = 0
	cs.values = &dictStore{}
	cs.values.init()

	rLevelBitWidth := cs.rLevels.bw
	dLevelBitWidth := cs.dLevels.bw

	cs.rLevels = &packedArray{}
	cs.dLevels = &packedArray{}
	cs.rLevels.reset(rLevelBitWidth)
	cs.dLevels.reset(dLevelBitWidth)

	cs.getPageStats().reset()
}

func (cs *ColumnStore) readNextPage() error {
	if cs.pageIdx >= len(cs.pages) {
		return errors.Errorf("out of range: requested page index = %d total number of pages = %d", cs.pageIdx, len(cs.pages))
	}

	data, dl, rl, err := cs.pages[cs.pageIdx].readValues(int(cs.pages[cs.pageIdx].numValues()))
	if err != nil {
		return err
	}

	cs.pageIdx++

	cs.resetData()

	cs.values.readPos = 0

	for _, v := range data {
		cs.values.addValue(v, cs.sizeOf(v))
	}

	cs.rLevels.appendArray(rl)
	cs.dLevels.appendArray(dl)

	return nil
}

func (cs *ColumnStore) get(maxD, maxR int32) (interface{}, int32, error) {
	if cs.skipped {
		return nil, 0, nil
	}

	if cs.readPos >= cs.rLevels.count || cs.readPos >= cs.dLevels.count {
		if err := cs.readNextPage(); err != nil {
			return nil, 0, err
		}
	}
	_, dl, _ := cs.getRDLevelAt(cs.readPos)
	// this is a null value, increase the read pos, for advancing the rLvl and dLvl but
	// do not touch the dict-store
	if dl < maxD {
		cs.readPos++
		return nil, dl, nil
	}
	v, err := cs.getNext()
	if err != nil {
		return nil, 0, err
	}

	// if this is not repeated just return the value, the result is not an array
	if cs.repTyp != parquet.FieldRepetitionType_REPEATED {
		cs.readPos++
		return v, maxD, err
	}

	// the first rLevel in current object is always less than maxR (only for the repeated values)
	// the next data in this object, should have maxR as the rLevel. the first rLevel less than maxR means the value
	// is from the next object and we should not touch it in this call

	var ret = cs.typedColumnStore.append(nil, v)
	for {
		cs.readPos++
		rl, _, last := cs.getRDLevelAt(cs.readPos)
		if last || rl < maxR {
			// end of this object
			return ret, maxD, nil
		}
		v, err := cs.getNext()
		if err != nil {
			return nil, maxD, err
		}

		ret = cs.typedColumnStore.append(ret, v)
	}
}

func newStore(typed typedColumnStore, enc parquet.Encoding, useDict bool) *ColumnStore {
	return &ColumnStore{
		enc:              enc,
		useDict:          useDict,
		typedColumnStore: typed,
	}
}

func newPlainStore(typed typedColumnStore) *ColumnStore {
	return newStore(typed, parquet.Encoding_PLAIN, true)
}

// getValuesStore is internally used for the reader
func getValuesStore(typ *parquet.SchemaElement) (*ColumnStore, error) {
	params := &ColumnParameters{
		LogicalType:   typ.LogicalType,
		ConvertedType: typ.ConvertedType,
		TypeLength:    typ.TypeLength,
		Scale:         typ.Scale,
		Precision:     typ.Precision,
	}

	switch *typ.Type {
	case parquet.Type_BOOLEAN:
		return newPlainStore(&booleanStore{ColumnParameters: params}), nil
	case parquet.Type_BYTE_ARRAY:
		return newPlainStore(&byteArrayStore{ColumnParameters: params}), nil
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ.Type)
		}

		return newPlainStore(&byteArrayStore{ColumnParameters: params}), nil

	case parquet.Type_FLOAT:
		return newPlainStore(&floatStore{ColumnParameters: params, stats: newFloatStats(), pageStats: newFloatStats()}), nil
	case parquet.Type_DOUBLE:
		return newPlainStore(&doubleStore{ColumnParameters: params, stats: newDoubleStats(), pageStats: newDoubleStats()}), nil

	case parquet.Type_INT32:
		return newPlainStore(&int32Store{ColumnParameters: params, stats: newInt32Stats(), pageStats: newInt32Stats()}), nil
	case parquet.Type_INT64:
		return newPlainStore(&int64Store{ColumnParameters: params, stats: newInt64Stats(), pageStats: newInt64Stats()}), nil
	case parquet.Type_INT96:
		store := &int96Store{}
		store.ColumnParameters = params
		return newPlainStore(store), nil
	default:
		return nil, errors.Errorf("unsupported type: %s", typ.Type)
	}
}

// NewBooleanStore creates new column store to store boolean values.
func NewBooleanStore(enc parquet.Encoding, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_RLE:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&booleanStore{ColumnParameters: params}, enc, false), nil
}

// NewInt32Store create a new column store to store int32 values. If useDict is true,
// then a dictionary is used, otherwise a dictionary will never be used to encode the data.
func NewInt32Store(enc parquet.Encoding, useDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_BINARY_PACKED:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&int32Store{ColumnParameters: params, stats: newInt32Stats(), pageStats: newInt32Stats()}, enc, useDict), nil
}

// NewInt64Store creates a new column store to store int64 values. If useDict is true,
// then a dictionary is used, otherwise a dictionary will never be used to encode the data.
func NewInt64Store(enc parquet.Encoding, useDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_BINARY_PACKED:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&int64Store{ColumnParameters: params, stats: newInt64Stats(), pageStats: newInt64Stats()}, enc, useDict), nil
}

// NewInt96Store creates a new column store to store int96 values. If useDict is true,
// then a dictionary is used, otherwise a dictionary will never be used to encode the data.
func NewInt96Store(enc parquet.Encoding, useDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	store := &int96Store{}
	store.ColumnParameters = params
	return newStore(store, enc, useDict), nil
}

// NewFloatStore creates a new column store to store float (float32) values. If useDict is true,
// then a dictionary is used, otherwise a dictionary will never be used to encode the data.
func NewFloatStore(enc parquet.Encoding, useDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&floatStore{ColumnParameters: params, stats: newFloatStats(), pageStats: newFloatStats()}, enc, useDict), nil
}

// NewDoubleStore creates a new column store to store double (float64) values. If useDict is true,
// then a dictionary is used, otherwise a dictionary will never be used to encode the data.
func NewDoubleStore(enc parquet.Encoding, useDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&doubleStore{ColumnParameters: params, stats: newDoubleStats(), pageStats: newDoubleStats()}, enc, useDict), nil
}

// NewByteArrayStore creates a new column store to store byte arrays. If useDict is true,
// then a dictionary is used, otherwise a dictionary will never be used to encode the data.
func NewByteArrayStore(enc parquet.Encoding, useDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Encoding_DELTA_BYTE_ARRAY:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	return newStore(&byteArrayStore{ColumnParameters: params}, enc, useDict), nil
}

// NewFixedByteArrayStore creates a new column store to store fixed size byte arrays. If useDict is true,
// then a dictionary is used, otherwise a dictionary will never be used to encode the data.
func NewFixedByteArrayStore(enc parquet.Encoding, useDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Encoding_DELTA_BYTE_ARRAY:
	default:
		return nil, errors.Errorf("encoding %q is not supported on this type", enc)
	}
	if params.TypeLength == nil {
		return nil, errors.New("no length provided")
	}

	if *params.TypeLength <= 0 {
		return nil, errors.Errorf("fix length with len %d is not possible", *params.TypeLength)
	}

	return newStore(&byteArrayStore{
		ColumnParameters: params,
	}, enc, useDict), nil
}
