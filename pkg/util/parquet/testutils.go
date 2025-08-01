// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parquet

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

// includeParquestReaderMetadata configures the parquet writer to write metadata
// required for reading parquet files in tests.
var includeParquestReaderMetadata = buildutil.CrdbTestBuild ||
	envutil.EnvOrDefaultBool("COCKROACH_CHANGEFEED_TESTING_INCLUDE_PARQUET_READER_METADATA",
		false)

// ReadFileAndVerifyDatums asserts that a parquet file's metadata matches the
// metadata from the writer and its data matches writtenDatums.
//
// It returns a ReadDatumsMetadata struct with metadata about the file.
// This function will assert the number of rows and columns in the metadata
// match, but the remaining fields are left for the caller to assert.
func ReadFileAndVerifyDatums(
	t *testing.T,
	parquetFile string,
	expectedNumRows int,
	expectedNumCols int,
	writtenDatums [][]tree.Datum,
) ReadDatumsMetadata {
	meta, readDatums, err := ReadFile(parquetFile)

	require.NoError(t, err)
	require.Equal(t, expectedNumRows, meta.NumRows)
	require.Equal(t, expectedNumCols, meta.NumCols)

	for i := 0; i < expectedNumRows; i++ {
		for j := 0; j < expectedNumCols; j++ {
			ValidateDatum(t, writtenDatums[i][j], readDatums[i][j])
		}
	}
	return meta
}

// ReadFile reads a parquet file and returns the contained metadata and datums.
//
// To use this function, the Writer must be configured to write CRDB-specific
// metadata for the reader. See NewWriter() and buildutil.CrdbTestBuild.
//
// NB: The returned datums may not be hydrated or identical to the ones
// which were written. See comment on ValidateDatum for more info.
func ReadFile(parquetFile string) (meta ReadDatumsMetadata, datums [][]tree.Datum, err error) {
	f, err := os.Open(parquetFile)
	if err != nil {
		return ReadDatumsMetadata{}, nil, err
	}

	reader, err := file.NewParquetReader(f)
	if err != nil {
		return ReadDatumsMetadata{}, nil, err
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			err = errors.CombineErrors(err, closeErr)
		}
	}()

	var readDatums [][]tree.Datum

	typFamiliesMeta := reader.MetaData().KeyValueMetadata().FindValue(typeFamilyMetaKey)
	if typFamiliesMeta == nil {
		return ReadDatumsMetadata{}, nil,
			errors.AssertionFailedf("missing type family metadata. ensure the writer is configured" +
				" to write reader metadata. see NewWriter() and buildutil.CrdbTestBuild")
	}
	typFamilies, err := deserializeIntArray(*typFamiliesMeta)
	if err != nil {
		return ReadDatumsMetadata{}, nil, err
	}

	tupleColumnsMeta := reader.MetaData().KeyValueMetadata().FindValue(tupleIndexesMetaKey)
	tupleColumns, err := deserialize2DIntArray(*tupleColumnsMeta)
	if err != nil {
		return ReadDatumsMetadata{}, nil, err
	}

	typOidsMeta := reader.MetaData().KeyValueMetadata().FindValue(typeOidMetaKey)
	if typOidsMeta == nil {
		return ReadDatumsMetadata{}, nil,
			errors.AssertionFailedf("missing type oid metadata. ensure the writer is configured" +
				" to write reader metadata. see NewWriter() and buildutil.CrdbTestBuild)")
	}
	typOids, err := deserializeIntArray(*typOidsMeta)
	if err != nil {
		return ReadDatumsMetadata{}, nil, err
	}

	var colNames []string
	startingRowIdx := 0
	for rg := 0; rg < reader.NumRowGroups(); rg++ {
		rgr := reader.RowGroup(rg)
		rowsInRowGroup := rgr.NumRows()
		for i := int64(0); i < rowsInRowGroup; i++ {
			readDatums = append(readDatums, make([]tree.Datum, rgr.NumColumns()))
		}
		if rg == 0 {
			colNames = make([]string, 0, rgr.NumColumns())
		}

		for colIdx := 0; colIdx < rgr.NumColumns(); colIdx++ {
			col, err := rgr.Column(colIdx)
			if err != nil {
				return ReadDatumsMetadata{}, nil, err
			}

			if rg == 0 {
				colNames = append(colNames, col.Descriptor().Name())
			}

			dec, err := decoderFromFamilyAndType(oid.Oid(typOids[colIdx]), types.Family(typFamilies[colIdx]))
			if err != nil {
				return ReadDatumsMetadata{}, nil, err
			}

			// Based on how we define the schemas for these columns, we can determine if they are arrays or
			// part of tuples. See comments above arrayEntryNonNilDefLevel and tupleFieldNonNilDefLevel for
			// more info.
			isArray := col.Descriptor().MaxDefinitionLevel() == 3
			isTuple := col.Descriptor().MaxDefinitionLevel() == 2

			datumsForColInRowGroup, err := readColInRowGroup(col, dec, rowsInRowGroup, isArray, isTuple)
			if err != nil {
				return ReadDatumsMetadata{}, nil, err
			}
			decodeValuesIntoDatumsHelper(datumsForColInRowGroup, readDatums, colIdx, startingRowIdx)
		}
		startingRowIdx += int(rowsInRowGroup)
	}

	for i := 0; i < len(readDatums); i++ {
		readDatums[i] = squashTuples(readDatums[i], tupleColumns, colNames)
	}

	return makeDatumMeta(reader, readDatums), readDatums, nil
}

// ReadDatumsMetadata contains metadata from the parquet file which was read.
type ReadDatumsMetadata struct {
	// MetaFields is the arbitrary metadata read from the file.
	MetaFields map[string]string
	// NumRows is the number of rows read.
	NumRows int
	// NumCols is the number of datum cols read. Will be 0 if NumRows
	// is 0.
	NumCols int
	// NumRowGroups is the number of row groups in the file.
	NumRowGroups int
}

func makeDatumMeta(reader *file.Reader, readDatums [][]tree.Datum) ReadDatumsMetadata {
	// Copy the reader metadata into a new map since this metadata can be used
	// after the metadata is closed. This is defensive - we should not assume
	// any method or data on the reader is safe to use once it is closed.
	readerMeta := reader.MetaData().KeyValueMetadata()
	kvMeta := make(map[string]string)
	for _, key := range reader.MetaData().KeyValueMetadata().Keys() {
		val := readerMeta.FindValue(key)
		if val != nil {
			kvMeta[key] = *val
		}
	}

	reader.MetaData().Schema.NumColumns()

	// NB: The number of physical columns in the file may be more than
	// the number of datums in each row because duple datums get a physical
	// column for each field.
	// We do not return reader.MetaData().Schema.NumColumns() because that
	// returns the number of physical columns.
	numCols := 0
	if len(readDatums) > 0 {
		numCols = len(readDatums[0])
	}
	return ReadDatumsMetadata{
		MetaFields:   kvMeta,
		NumRows:      len(readDatums),
		NumCols:      numCols,
		NumRowGroups: reader.NumRowGroups(),
	}
}

func readColInRowGroup(
	col file.ColumnChunkReader, dec decoder, rowsInRowGroup int64, isArray bool, isTuple bool,
) ([]tree.Datum, error) {
	switch col.Type() {
	case parquet.Types.Boolean:
		colDatums, err := readRowGroup(col, make([]bool, 1), dec, rowsInRowGroup, isArray, isTuple)
		if err != nil {
			return nil, err
		}
		return colDatums, nil
	case parquet.Types.Int32:
		colDatums, err := readRowGroup(col, make([]int32, 1), dec, rowsInRowGroup, isArray, isTuple)
		if err != nil {
			return nil, err
		}
		return colDatums, nil
	case parquet.Types.Int64:
		colDatums, err := readRowGroup(col, make([]int64, 1), dec, rowsInRowGroup, isArray, isTuple)
		if err != nil {
			return nil, err
		}
		return colDatums, nil
	case parquet.Types.Int96:
		panic("unimplemented")
	case parquet.Types.Float:
		colDatums, err := readRowGroup(col, make([]float32, 1), dec, rowsInRowGroup, isArray, isTuple)
		if err != nil {
			return nil, err
		}
		return colDatums, nil
	case parquet.Types.Double:
		colDatums, err := readRowGroup(col, make([]float64, 1), dec, rowsInRowGroup, isArray, isTuple)
		if err != nil {
			return nil, err
		}
		return colDatums, nil
	case parquet.Types.ByteArray:
		colDatums, err := readRowGroup(col, make([]parquet.ByteArray, 1), dec, rowsInRowGroup, isArray, isTuple)
		if err != nil {
			return nil, err
		}
		return colDatums, nil
	case parquet.Types.FixedLenByteArray:
		colDatums, err := readRowGroup(col, make([]parquet.FixedLenByteArray, 1), dec, rowsInRowGroup, isArray, isTuple)
		if err != nil {
			return nil, err
		}
		return colDatums, nil
	default:
		return nil, errors.AssertionFailedf("unexpected type: %s", col.Type())
	}
}

type batchReader[T parquetDatatypes] interface {
	ReadBatch(batchSize int64, values []T, defLvls []int16, repLvls []int16) (total int64, valuesRead int, err error)
}

// readRowGroup reads all the datums in a row group for a physical column.
func readRowGroup[T parquetDatatypes](
	r file.ColumnChunkReader,
	valueAlloc []T,
	dec decoder,
	expectedRowCount int64,
	isArray bool,
	isTuple bool,
) (tree.Datums, error) {
	br, ok := r.(batchReader[T])
	if !ok {
		return nil, errors.AssertionFailedf("expected batchReader for type %T, but found %T instead", valueAlloc, r)
	}

	result := make([]tree.Datum, 0)
	defLevels := [1]int16{}
	repLevels := [1]int16{}

	for {
		numRowsRead, _, err := br.ReadBatch(1, valueAlloc, defLevels[:], repLevels[:])
		if err != nil {
			return nil, err
		}
		if numRowsRead == 0 {
			break
		}

		if isArray {
			// Replevel 0 indicates the start of a new array.
			if repLevels[0] == 0 {
				// Replevel 0, Deflevel 0 represents a NULL array.
				if defLevels[0] == 0 {
					result = append(result, tree.DNull)
					continue
				}
				arrDatum := &tree.DArray{}
				arrDatum.Array = tree.Datums{}
				result = append(result, arrDatum)
				// Replevel 0, Deflevel 1 represents an array which is empty.
				if defLevels[0] == 1 {
					continue
				}
			}
			currentArrayDatum := result[len(result)-1].(*tree.DArray)
			// Deflevel 2 represents a null value in an array.
			if defLevels[0] == 2 {
				currentArrayDatum.Array = append(currentArrayDatum.Array, tree.DNull)
				continue
			}
			// Deflevel 3 represents a non-null datum in an array.
			d, err := decode(dec, valueAlloc[0])
			if err != nil {
				return nil, err
			}
			currentArrayDatum.Array = append(currentArrayDatum.Array, d)
		} else if isTuple {
			// Deflevel 0 represents a null tuple.
			// Deflevel 1 represents a null value in a non null tuple.
			// Deflevel 2 represents a non-null value in a non-null tuple.
			switch defLevels[0] {
			case 0:
				result = append(result, dNullTuple)
			case 1:
				result = append(result, tree.DNull)
			case 2:
				d, err := decode(dec, valueAlloc[0])
				if err != nil {
					return nil, err
				}
				result = append(result, d)
			}
		} else {
			// Deflevel 0 represents a null value
			// Deflevel 1 represents a non-null value
			d := tree.DNull
			if defLevels[0] != 0 {
				d, err = decode(dec, valueAlloc[0])
				if err != nil {
					return nil, err
				}
			}
			result = append(result, d)
		}
	}
	if int64(len(result)) != expectedRowCount {
		return nil, errors.AssertionFailedf(
			"expected to read %d rows in row group, found %d", expectedRowCount, int64(len(result)))
	}
	return result, nil
}

func decodeValuesIntoDatumsHelper(
	colDatums []tree.Datum, datumRows [][]tree.Datum, colIdx int, startingRowIdx int,
) {
	for rowOffset, datum := range colDatums {
		datumRows[startingRowIdx+rowOffset][colIdx] = datum
	}
}

func unwrapDatum(d tree.Datum) tree.Datum {
	switch t := d.(type) {
	case *tree.DOidWrapper:
		return unwrapDatum(t.Wrapped)
	default:
		return d
	}
}

// ValidateDatum validates that the "contents" of the expected datum matches the
// contents of the actual datum. For example, when validating two arrays, we
// only compare the datums in the arrays. We do not compare CRDB-specific
// metadata fields such as (tree.DArray).HasNulls or (tree.DArray).HasNonNulls.
//
// The reason for this special comparison that the parquet format is presently
// used in an export use case, so we only need to test roundtripability with
// data end users see. We do not need to check roundtripability of internal CRDB
// data.
func ValidateDatum(t *testing.T, expected tree.Datum, actual tree.Datum) {
	// The randgen library may generate datums wrapped in a *tree.DOidWrapper, so
	// we should unwrap them. We unwrap at this stage as opposed to when
	// generating datums to test that the writer can handle wrapped datums.
	expected = unwrapDatum(expected)
	actual = unwrapDatum(actual)

	switch expected.ResolvedType().Family() {
	case types.JsonFamily:
		require.Equal(t, expected.(*tree.DJSON).JSON.String(),
			actual.(*tree.DJSON).JSON.String())
	case types.DateFamily:
		require.Equal(t, expected.(*tree.DDate).Date.UnixEpochDays(),
			actual.(*tree.DDate).Date.UnixEpochDays())
	case types.FloatFamily:
		if expected.ResolvedType().Equal(types.Float4) && expected.(*tree.DFloat).String() != "NaN" {
			// CRDB currently doesn't truncate non NAN float4's correctly, so this
			// test does it manually :(
			// https://github.com/cockroachdb/cockroach/issues/73743
			e := float32(*expected.(*tree.DFloat))
			a := float32(*expected.(*tree.DFloat))
			require.Equal(t, e, a)
		} else {
			require.Equal(t, expected.String(), actual.String())
		}
	case types.ArrayFamily:
		arr1 := expected.(*tree.DArray).Array
		arr2 := actual.(*tree.DArray).Array
		require.Equal(t, len(arr1), len(arr2))
		for i := 0; i < len(arr1); i++ {
			ValidateDatum(t, arr1[i], arr2[i])
		}
	case types.TupleFamily:
		t1 := expected.(*tree.DTuple)
		t2 := actual.(*tree.DTuple)
		require.Equal(t, len(t1.D), len(t2.D))
		for i := 0; i < len(t1.D); i++ {
			ValidateDatum(t, t1.D[i], t2.D[i])
		}
		if t1.ResolvedType().TupleLabels() != nil {
			require.Equal(t, t1.ResolvedType().TupleLabels(), t2.ResolvedType().TupleLabels())
		}
	case types.EnumFamily:
		require.Equal(t, expected.(*tree.DEnum).LogicalRep, actual.(*tree.DEnum).LogicalRep)
	case types.CollatedStringFamily:
		require.Equal(t, expected.(*tree.DCollatedString).Contents, actual.(*tree.DCollatedString).Contents)
	case types.OidFamily:
		require.Equal(t, expected.(*tree.DOid).Oid, actual.(*tree.DOid).Oid)
	default:
		require.Equal(t, expected, actual)
	}
}

func makeTestingEnumType() *types.T {
	enumMembers := []string{"hi", "hello"}
	enumType := types.MakeEnum(catid.TypeIDToOID(500), catid.TypeIDToOID(100500))
	enumType.TypeMeta = types.UserDefinedTypeMetadata{
		Name: &types.UserDefinedTypeName{
			Schema: "test",
			Name:   "greeting",
		},
		EnumData: &types.EnumMetadata{
			LogicalRepresentations: enumMembers,
			PhysicalRepresentations: [][]byte{
				encoding.EncodeUntaggedIntValue(nil, 0),
				encoding.EncodeUntaggedIntValue(nil, 1),
			},
			IsMemberReadOnly: make([]bool, len(enumMembers)),
		},
	}
	return enumType
}

const typeOidMetaKey = `crdbTypeOIDs`
const typeFamilyMetaKey = `crdbTypeFamilies`
const tupleIndexesMetaKey = `crdbTupleIndexes`

// MakeReaderMetadata returns column type metadata that will be written to all
// parquet files. This metadata is useful for roundtrip tests where we construct
// CRDB datums from the raw data in written to parquet files.
func MakeReaderMetadata(sch *SchemaDefinition) map[string]string {
	meta := map[string]string{}
	typOids := make([]uint32, 0, len(sch.cols))
	typFamilies := make([]int32, 0, len(sch.cols))
	var tupleIntervals [][]int
	for _, col := range sch.cols {
		// Tuples get flattened out such that each field in the tuple is
		// its own physical column in the parquet file.
		if col.typ.Family() == types.TupleFamily {
			for _, typ := range col.typ.TupleContents() {
				typOids = append(typOids, uint32(typ.Oid()))
				typFamilies = append(typFamilies, int32(typ.Family()))
			}
			tupleInterval := []int{col.physicalColsStartIdx, col.physicalColsStartIdx + col.numPhysicalCols - 1}
			tupleIntervals = append(tupleIntervals, tupleInterval)
		} else {
			typOids = append(typOids, uint32(col.typ.Oid()))
			typFamilies = append(typFamilies, int32(col.typ.Family()))
		}
	}
	meta[typeOidMetaKey] = serializeIntArray(typOids)
	meta[typeFamilyMetaKey] = serializeIntArray(typFamilies)
	meta[tupleIndexesMetaKey] = serialize2DIntArray(tupleIntervals)
	return meta
}

// serializeIntArray serializes an int array to a string "[23 2 32 43 32]".
func serializeIntArray[I int | int32 | uint32](ints []I) string {
	return fmt.Sprint(ints)
}

// deserializeIntArray deserializes an integer sting in the format "[23 2 32 43
// 32]" to an array of ints.
func deserializeIntArray(s string) ([]int, error) {
	vals := strings.Split(strings.Trim(s, "[]"), " ")

	// If there are no values, strings.Split returns an array of length 1
	// containing the empty string.
	if len(vals) == 0 || (len(vals) == 1 && vals[0] == "") {
		return []int{}, nil
	}

	result := make([]int, 0, len(vals))
	for _, val := range vals {
		intVal, err := strconv.Atoi(val)
		if err != nil {
			return nil, err
		}
		result = append(result, intVal)
	}
	return result, nil
}

// serialize2DIntArray serializes a 2D int array in the format
// "[1 2 3][4 5 6][7 8 9]".
func serialize2DIntArray(ints [][]int) string {
	var buf bytes.Buffer
	for _, colIdxs := range ints {
		buf.WriteString(serializeIntArray(colIdxs))
	}
	return buf.String()
}

// deserialize2DIntArray deserializes an integer sting in the format
// "[1 2 3][4 5 6][7 8 9]".
func deserialize2DIntArray(s string) ([][]int, error) {
	var result [][]int
	found := true
	var currentArray string
	var remaining string
	for found {
		currentArray, remaining, found = strings.Cut(s, "]")
		if !found {
			break
		}
		// currentArray always has the form "[x y" where x, y are integers
		// Drop the leading "["
		currentArray = currentArray[1:]
		tupleInts, err := deserializeIntArray(currentArray)
		if err != nil {
			return nil, err
		}
		result = append(result, tupleInts)
		s = remaining
	}
	return result, nil
}

// dNullTuple is a special null which indicates that a tuple field belongs
// to a tuple which is entirely null. This is used to differentiate
// a tuple which is NULL from a tuple containing all NULL fields.
var dNullTuple = dNullTupleType{tree.DNull}

type dNullTupleType struct {
	tree.Datum
}

// squashTuples takes an array of datums and merges groups of adjacent datums
// into tuples using the supplied intervals. The provided column names will be
// used as tuple labels.
//
// For example:
//
// Input:
//
//	datumRow = ["0", "1", "2", "3", "4", "5", "6"]
//	tupleColIndexes = [[0, 1], [3, 3], [4, 6]]
//	colNames = ["a", "b", "c", "d", "e", "f", "g"]
//
// Output:
//
//	[(a: "0", b: "1"), "2", (d: "3"), (e: "4", f: "5", g: "6")]
//
// Behavior is undefined if the intervals are not sorted, not disjoint,
// not ascending, or out of bounds. The number of elements in datumRow
// should be equal to the number of labels in colNames.
func squashTuples(datumRow []tree.Datum, tupleColIndexes [][]int, colNames []string) []tree.Datum {
	if len(tupleColIndexes) == 0 {
		return datumRow
	}
	tupleIdx := 0
	var updatedDatums []tree.Datum
	var currentTupleDatums []tree.Datum
	var currentTupleTypes []*types.T
	var currentTupleLabels []string
	for i, d := range datumRow {
		if tupleIdx < len(tupleColIndexes) {
			tupleUpperIdx := tupleColIndexes[tupleIdx][1]
			tupleLowerIdx := tupleColIndexes[tupleIdx][0]

			if i >= tupleLowerIdx && i <= tupleUpperIdx {
				currentTupleDatums = append(currentTupleDatums, d)
				currentTupleTypes = append(currentTupleTypes, d.ResolvedType())
				currentTupleLabels = append(currentTupleLabels, colNames[i])
				if i == tupleUpperIdx {
					// Check for marker that indicates the entire tuple is NULL.
					if currentTupleDatums[0] == dNullTuple {
						updatedDatums = append(updatedDatums, tree.DNull)
					} else {
						tupleDatum := tree.MakeDTuple(types.MakeLabeledTuple(currentTupleTypes, currentTupleLabels), currentTupleDatums...)
						updatedDatums = append(updatedDatums, &tupleDatum)
					}

					currentTupleTypes = []*types.T{}
					currentTupleDatums = []tree.Datum{}
					currentTupleLabels = []string{}

					tupleIdx += 1
				}
			} else {
				updatedDatums = append(updatedDatums, d)
			}
		} else {
			updatedDatums = append(updatedDatums, d)
		}
	}
	return updatedDatums
}
