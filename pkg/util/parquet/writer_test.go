// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parquet

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type colSchema struct {
	columnNames []string
	columnTypes []*types.T
}

func newColSchema(numCols int) *colSchema {
	return &colSchema{
		columnNames: make([]string, numCols),
		columnTypes: make([]*types.T, numCols),
	}
}

// typSupported filters out types which can be returned by randgen.RandType
// that are not supported by the writer.
func typSupported(typ *types.T) bool {
	switch typ.Family() {
	case types.AnyFamily, types.TSQueryFamily, types.TSVectorFamily, types.PGVectorFamily, types.VoidFamily, types.JsonpathFamily:
		return false
	case types.ArrayFamily:
		if typ.ArrayContents().Family() == types.ArrayFamily || typ.ArrayContents().Family() == types.TupleFamily {
			return false
		}
		return typSupported(typ.ArrayContents())
	case types.TupleFamily:
		supported := true
		if len(typ.TupleContents()) == 0 {
			return false
		}
		for _, typleFieldTyp := range typ.TupleContents() {
			if typleFieldTyp.Family() == types.ArrayFamily || typleFieldTyp.Family() == types.TupleFamily {
				return false
			}
			supported = supported && typSupported(typleFieldTyp)
		}
		return supported
	default:
		// It is better to let an unexpected type pass the filter and fail the test
		// because we can observe and document such failures.
		return true
	}
}

// randTestingType returns a random type for testing.
func randTestingType(rng *rand.Rand) *types.T {
	supported := false
	var typ *types.T
	for !supported {
		typ = randgen.RandType(rng)
		supported = typSupported(typ)
	}
	return typ
}

func makeRandDatums(numRows int, sch *colSchema, rng *rand.Rand, nullsAllowed bool) [][]tree.Datum {
	datums := make([][]tree.Datum, numRows)
	for i := 0; i < numRows; i++ {
		datums[i] = make([]tree.Datum, len(sch.columnTypes))
		for j := 0; j < len(sch.columnTypes); j++ {
			datums[i][j] = randgen.RandDatum(rng, sch.columnTypes[j], nullsAllowed)
		}
	}
	return datums
}

func makeRandSchema(
	numCols int, randType func(rng *rand.Rand) *types.T, rng *rand.Rand,
) *colSchema {
	sch := newColSchema(numCols)
	for i := 0; i < numCols; i++ {
		sch.columnTypes[i] = randType(rng)
		sch.columnNames[i] = fmt.Sprintf("%s%d", sch.columnTypes[i].Name(), i)
	}
	return sch
}

func TestRandomDatums(t *testing.T) {
	seed := rand.NewSource(timeutil.Now().UnixNano())
	rng := rand.New(seed)
	t.Logf("random seed %d", seed.Int63())

	numRows := 256
	numCols := 128

	sch := makeRandSchema(numCols, randTestingType, rng)
	datums := makeRandDatums(numRows, sch, rng, true)

	fileName := "TestRandomDatums.parquet"
	f, err := os.CreateTemp("", fileName)
	require.NoError(t, err)

	schemaDef, err := NewSchema(sch.columnNames, sch.columnTypes)
	require.NoError(t, err)

	writer, err := NewWriter(schemaDef, f, WithMaxRowGroupLength(20))
	require.NoError(t, err)

	var numExplicitFlushes int
	for _, row := range datums {
		err = writer.AddRow(row)
		require.NoError(t, err)
		// Flush every 10 datums on average.
		if rng.Float32() < 0.1 {
			err = writer.Flush()
			require.NoError(t, err)
			numExplicitFlushes += 1
		}
	}

	err = writer.Close()
	require.NoError(t, err)

	ReadFileAndVerifyDatums(t, f.Name(), numRows, numCols, datums)
}

// TestBasicDatums tests roundtripability for all supported scalar data types
// and one simple array type.
func TestBasicDatums(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sch    *colSchema
		datums func() ([][]tree.Datum, error)
	}{
		{
			name: "bool",
			sch: &colSchema{
				columnTypes: []*types.T{types.Bool, types.Bool, types.Bool},
				columnNames: []string{"a", "b", "c"},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{tree.DBoolFalse, tree.DBoolTrue, tree.DNull},
				}, nil
			},
		},
		{
			name: "string",
			sch: &colSchema{
				columnTypes: []*types.T{types.String, types.String, types.String},
				columnNames: []string{"a", "b", "c"},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{tree.NewDString("a"), tree.NewDString(""), tree.DNull}}, nil
			},
		},
		{
			name: "timestamp",
			sch: &colSchema{
				columnTypes: []*types.T{types.Timestamp, types.Timestamp, types.Timestamp},
				columnNames: []string{"a", "b", "c"},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{
						tree.MustMakeDTimestamp(timeutil.Now(), time.Microsecond),
						tree.MustMakeDTimestamp(timeutil.Now(), time.Microsecond),
						tree.DNull,
					},
				}, nil
			},
		},
		{
			name: "timestamptz",
			sch: &colSchema{
				columnTypes: []*types.T{types.TimestampTZ, types.TimestampTZ, types.TimestampTZ},
				columnNames: []string{"a", "b", "c"},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{
						tree.MustMakeDTimestampTZ(timeutil.Now(), time.Microsecond),
						tree.MustMakeDTimestampTZ(timeutil.Now(), time.Microsecond),
						tree.DNull,
					},
				}, nil
			},
		},
		{
			name: "int",
			sch: &colSchema{
				columnTypes: []*types.T{types.Int4, types.Int, types.Int, types.Int2, types.Int2},
				columnNames: []string{"a", "b", "c", "d", "e"},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{tree.NewDInt(1 << 16), tree.NewDInt(1 << 32),
						tree.NewDInt(-1 * (1 << 32)), tree.NewDInt(12), tree.DNull},
				}, nil
			},
		},
		{
			name: "decimal",
			sch: &colSchema{
				columnTypes: []*types.T{types.Decimal, types.Decimal, types.Decimal, types.Decimal},
				columnNames: []string{"a", "b", "c", "d"},
			},
			datums: func() ([][]tree.Datum, error) {
				var err error
				datums := make([]tree.Datum, 4)
				if datums[0], err = tree.ParseDDecimal("-1.222"); err != nil {
					return nil, err
				}
				if datums[1], err = tree.ParseDDecimal("-inf"); err != nil {
					return nil, err
				}
				if datums[2], err = tree.ParseDDecimal("inf"); err != nil {
					return nil, err
				}
				if datums[3], err = tree.ParseDDecimal("nan"); err != nil {
					return nil, err
				}
				return [][]tree.Datum{datums}, nil
			},
		},
		{
			name: "uuid",
			sch: &colSchema{
				columnTypes: []*types.T{types.Uuid, types.Uuid},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				uid, err := uuid.FromString("acde070d-8c4c-4f0d-9d8a-162843c10333")
				if err != nil {
					return nil, err
				}
				return [][]tree.Datum{
					{tree.NewDUuid(tree.DUuid{UUID: uid}), tree.DNull},
				}, nil
			},
		},
		{
			name: "array",
			sch: &colSchema{
				columnTypes: []*types.T{types.IntArray, types.IntArray},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				da := tree.NewDArray(types.Int)
				da.Array = tree.Datums{tree.NewDInt(0), tree.NewDInt(1)}
				da2 := tree.NewDArray(types.Int)
				da2.Array = tree.Datums{tree.NewDInt(2), tree.DNull}
				da3 := tree.NewDArray(types.Int)
				da3.Array = tree.Datums{}
				return [][]tree.Datum{
					{da, da2}, {da3, tree.DNull},
				}, nil
			},
		},
		{
			name: "inet",
			sch: &colSchema{
				columnTypes: []*types.T{types.INet, types.INet},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				var ipa ipaddr.IPAddr
				err := ipaddr.ParseINet("192.168.2.1", &ipa)
				require.NoError(t, err)

				return [][]tree.Datum{
					{&tree.DIPAddr{IPAddr: ipa}, tree.DNull},
				}, nil
			},
		},
		{
			name: "json",
			sch: &colSchema{
				columnTypes: []*types.T{types.Json, types.Jsonb},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				j, err := tree.ParseDJSON("[{\"a\": 1}]")
				require.NoError(t, err)

				return [][]tree.Datum{
					{j, tree.DNull},
				}, nil
			},
		},
		{
			name: "bitarray",
			sch: &colSchema{
				columnTypes: []*types.T{types.VarBit, types.VarBit},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				ba, err := bitarray.Parse(string("101001"))
				if err != nil {
					return nil, err
				}

				return [][]tree.Datum{
					{&tree.DBitArray{BitArray: ba}, tree.DNull},
				}, nil
			},
		},
		{
			name: "bytes",
			sch: &colSchema{
				columnTypes: []*types.T{types.Bytes, types.Bytes},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{tree.NewDBytes("bytes"), tree.DNull},
				}, nil
			},
		},
		{
			name: "enum",
			sch: &colSchema{
				columnTypes: []*types.T{makeTestingEnumType(), makeTestingEnumType()},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				d, err := tree.MakeDEnumFromLogicalRepresentation(makeTestingEnumType(), "hi")
				require.NoError(t, err)
				return [][]tree.Datum{
					{&d, tree.DNull},
				}, nil
			},
		},
		{
			name: "date",
			sch: &colSchema{
				columnTypes: []*types.T{types.Date, types.Date},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				d, err := pgdate.MakeDateFromTime(timeutil.Now())
				require.NoError(t, err)
				date := tree.MakeDDate(d)
				return [][]tree.Datum{
					{&date, tree.DNull},
				}, nil
			},
		},
		{
			name: "box2d",
			sch: &colSchema{
				columnTypes: []*types.T{types.Box2D, types.Box2D},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				b, err := geo.ParseCartesianBoundingBox("BOX(-0.4088850532348978 -0.19224841029808887,0.9334155753101069 0.7180433951296195)")
				require.NoError(t, err)
				return [][]tree.Datum{
					{tree.NewDBox2D(b), tree.DNull},
				}, nil
			},
		},
		{
			name: "geography",
			sch: &colSchema{
				columnTypes: []*types.T{types.Geography, types.Geography},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				g, err := geo.ParseGeographyFromEWKB([]byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"))
				require.NoError(t, err)
				return [][]tree.Datum{
					{&tree.DGeography{Geography: g}, tree.DNull},
				}, nil
			},
		},
		{
			name: "geometry",
			sch: &colSchema{
				columnTypes: []*types.T{types.Geometry, types.Geometry},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				g, err := geo.ParseGeometryFromEWKB([]byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"))
				require.NoError(t, err)
				return [][]tree.Datum{
					{&tree.DGeometry{Geometry: g}, tree.DNull},
				}, nil
			},
		},
		{
			name: "interval",
			sch: &colSchema{
				columnTypes: []*types.T{types.Interval, types.Interval},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{&tree.DInterval{Duration: duration.MakeDuration(0, 10, 15)}, tree.DNull},
				}, nil
			},
		},
		{
			name: "time",
			sch: &colSchema{
				columnTypes: []*types.T{types.Time, types.Time},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				dt := tree.DTime(12345)
				return [][]tree.Datum{
					{&dt, tree.DNull},
				}, nil
			},
		},
		{
			name: "timetz",
			sch: &colSchema{
				columnTypes: []*types.T{types.TimeTZ, types.TimeTZ},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				dt := tree.NewDTimeTZFromTime(timeutil.Now())
				return [][]tree.Datum{
					{dt, tree.DNull},
				}, nil
			},
		},
		{
			name: "float",
			sch: &colSchema{
				columnTypes: []*types.T{types.Float, types.Float, types.Float, types.Float, types.Float4, types.Float4, types.Float4},
				columnNames: []string{"a", "b", "c", "d", "e", "f", "g"},
			},
			datums: func() ([][]tree.Datum, error) {
				d1 := tree.DFloat(math.MaxFloat64)
				d2 := tree.DFloat(math.SmallestNonzeroFloat64)
				d3 := tree.DFloat(math.NaN())
				d4 := tree.DFloat(math.MaxFloat32)
				d5 := tree.DFloat(math.SmallestNonzeroFloat32)
				return [][]tree.Datum{
					{&d1, &d2, &d3, tree.DNull, &d4, &d5, tree.DNull},
				}, nil
			},
		},
		{
			name: "tuple",
			sch: &colSchema{
				columnTypes: []*types.T{
					types.Int,
					types.MakeTuple([]*types.T{types.Int, types.Int, types.Int, types.Int}),
					types.Int,
					types.MakeTuple([]*types.T{types.Int}),
					types.Int,
					types.MakeTuple([]*types.T{types.Int, types.Int}),
					types.Int,
					types.MakeLabeledTuple([]*types.T{types.Int, types.Int}, []string{"a", "b"}),
				},
				columnNames: []string{"a", "b", "c", "d", "e", "f", "g", "h"},
			},
			datums: func() ([][]tree.Datum, error) {
				dt1 := tree.MakeDTuple(types.MakeTuple([]*types.T{types.Int, types.Int, types.Int, types.Int}), tree.NewDInt(2), tree.NewDInt(3), tree.NewDInt(4), tree.NewDInt(5))
				dt2 := tree.MakeDTuple(types.MakeTuple([]*types.T{types.Int}), tree.NewDInt(7))
				dt3 := tree.MakeDTuple(types.MakeLabeledTuple([]*types.T{types.Int, types.Int}, []string{"a", "b"}), tree.DNull, tree.DNull)
				return [][]tree.Datum{
					{tree.NewDInt(1), &dt1, tree.NewDInt(6), &dt2, tree.NewDInt(8), tree.DNull, tree.NewDInt(9), &dt3},
				}, nil
			},
		},
		{
			name: "refcursor",
			sch: &colSchema{
				columnTypes: []*types.T{types.RefCursor, types.RefCursor, types.RefCursor},
				columnNames: []string{"a", "b", "c"},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{tree.NewDRefCursor("a"), tree.NewDRefCursor(""), tree.DNull}}, nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			datums, err := tc.datums()
			require.NoError(t, err)
			numRows := len(datums)
			numCols := len(datums[0])
			maxRowGroupSize := int64(2)

			fileName := "TestBasicDatums.parquet"
			f, err := os.CreateTemp("", fileName)
			require.NoError(t, err)

			schemaDef, err := NewSchema(tc.sch.columnNames, tc.sch.columnTypes)
			require.NoError(t, err)

			writer, err := NewWriter(schemaDef, f, WithMaxRowGroupLength(maxRowGroupSize))
			require.NoError(t, err)

			for _, row := range datums {
				err = writer.AddRow(row)
				require.NoError(t, err)
			}

			err = writer.Close()
			require.NoError(t, err)

			meta := ReadFileAndVerifyDatums(t, f.Name(), numRows, numCols, datums)
			expectedNumRowGroups := int(math.Ceil(float64(numRows) / float64(maxRowGroupSize)))
			require.EqualValues(t, expectedNumRowGroups, meta.NumRowGroups)
		})
	}
}

func TestInvalidWriterUsage(t *testing.T) {
	colNames := []string{"col1", "col2"}
	colTypes := []*types.T{types.Bool, types.Bool}
	datum := tree.DBoolTrue

	schemaDef, err := NewSchema(colNames, colTypes)
	require.NoError(t, err)

	t.Run("cannot write an invalid number of columns outside of row size", func(t *testing.T) {
		buf := bytes.Buffer{}
		writer, err := NewWriter(schemaDef, &buf)
		require.NoError(t, err)

		err = writer.AddRow([]tree.Datum{datum, datum, datum})
		require.ErrorContains(t, err, "expected 2 datums in row, got 3 datums")

		_ = writer.Close()
	})

	t.Run("cannot write datum of wrong type", func(t *testing.T) {
		buf := bytes.Buffer{}
		writer, err := NewWriter(schemaDef, &buf)
		require.NoError(t, err)

		err = writer.AddRow([]tree.Datum{tree.NewDInt(0), datum})
		require.ErrorContains(t, err, "expected DBool")

		_ = writer.Close()
	})
}

func TestVersions(t *testing.T) {
	for version := range allowedVersions {
		opt := WithVersion(version)
		optionsTest(t, opt, func(t *testing.T, reader *file.Reader) {
			require.Equal(t, reader.MetaData().Version(), allowedVersions[version])
		})
	}

	schemaDef, err := NewSchema([]string{}, []*types.T{})
	require.NoError(t, err)
	buf := bytes.Buffer{}
	_, err = NewWriter(schemaDef, &buf, WithVersion("invalid"))
	require.Error(t, err)
}

func TestCompressionCodecs(t *testing.T) {
	for compression := range compressionCodecToParquet {
		opt := WithCompressionCodec(compression)
		optionsTest(t, opt, func(t *testing.T, reader *file.Reader) {
			colChunk, err := reader.MetaData().RowGroup(0).ColumnChunk(0)
			require.NoError(t, err)
			require.Equal(t, colChunk.Compression(), compressionCodecToParquet[compression])
		})
	}
}

// TestMetadata tests writing arbitrary kv metadata to parquet files.
func TestMetadata(t *testing.T) {
	meta := map[string]string{}
	meta["testKey1"] = "testValue1"
	meta["testKey2"] = "testValue2"
	opt := WithMetadata(meta)
	optionsTest(t, opt, func(t *testing.T, reader *file.Reader) {
		val := reader.MetaData().KeyValueMetadata().FindValue("testKey1")
		require.NotNil(t, reader.MetaData().KeyValueMetadata().FindValue("testKey1"))
		require.Equal(t, *val, "testValue1")

		val = reader.MetaData().KeyValueMetadata().FindValue("testKey2")
		require.NotNil(t, reader.MetaData().KeyValueMetadata().FindValue("testKey2"))
		require.Equal(t, *val, "testValue2")
	})
}

// optionsTest can be used to assert the behavior of an Option. It creates a
// writer using the supplied Option and writes a parquet file with sample data.
// Then it calls the provided test function with the reader and subsequently
// closes it.
func optionsTest(t *testing.T, opt Option, testFn func(t *testing.T, reader *file.Reader)) {
	schemaDef, err := NewSchema([]string{"a"}, []*types.T{types.Int})
	require.NoError(t, err)

	fileName := "OptionsTest.parquet"
	f, err := os.CreateTemp("", fileName)
	require.NoError(t, err)

	writer, err := NewWriter(schemaDef, f, opt)
	require.NoError(t, err)

	err = writer.AddRow([]tree.Datum{tree.NewDInt(0)})
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	f, err = os.Open(f.Name())
	require.NoError(t, err)

	reader, err := file.NewParquetReader(f)
	require.NoError(t, err)

	testFn(t, reader)

	err = reader.Close()
	require.NoError(t, err)
}

func TestSquashTuples(t *testing.T) {
	datums := []tree.Datum{
		tree.NewDInt(1),
		tree.NewDString("string"),
		tree.NewDBytes("bytes"),
		tree.NewDUuid(tree.DUuid{UUID: uuid.FromStringOrNil("52fdfc07-2182-454f-963f-5f0f9a621d72")}),
		tree.NewDJSON(json.FromInt(1)),
		tree.NewDFloat(0.1),
		tree.NewDJSON(json.FromBool(false)),
		tree.NewDInt(0),
	}
	labels := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	for _, tc := range []struct {
		tupleIntervals [][]int
		tupleOutput    string
	}{
		{
			tupleIntervals: [][]int{},
			tupleOutput:    "[1 'string' '\\x6279746573' '52fdfc07-2182-454f-963f-5f0f9a621d72' '1' 0.1 'false' 0]",
		},
		{
			tupleIntervals: [][]int{{0, 1}, {2, 4}},
			tupleOutput:    "[((1, 'string') AS a, b) (('\\x6279746573', '52fdfc07-2182-454f-963f-5f0f9a621d72', '1') AS c, d, e) 0.1 'false' 0]",
		},
		{
			tupleIntervals: [][]int{{0, 2}, {3, 3}},
			tupleOutput:    "[((1, 'string', '\\x6279746573') AS a, b, c) (('52fdfc07-2182-454f-963f-5f0f9a621d72',) AS d) '1' 0.1 'false' 0]",
		},
		{
			tupleIntervals: [][]int{{0, 7}},
			tupleOutput:    "[((1, 'string', '\\x6279746573', '52fdfc07-2182-454f-963f-5f0f9a621d72', '1', 0.1, 'false', 0) AS a, b, c, d, e, f, g, h)]",
		},
	} {
		squashedDatums := squashTuples(datums, tc.tupleIntervals, labels)
		require.Equal(t, tc.tupleOutput, fmt.Sprint(squashedDatums))
	}
}

// TestBufferedBytes asserts that the `BufferedBytesEstimate` method
// on the Writer is somewhat accurate (it increases every time a few rows are
// added, it resets after flushing, it is updated for each physical column etc.)
func TestBufferedBytes(t *testing.T) {
	// Common schema for all subtests below.
	tupleTyp := types.MakeTuple([]*types.T{types.Int, types.Int, types.String})
	sch, err := NewSchema([]string{"a", "b", "c", "d"}, []*types.T{types.Int, types.String, tupleTyp, types.IntArray})
	require.NoError(t, err)
	makeArray := func(vals ...tree.Datum) *tree.DArray {
		da := tree.NewDArray(types.Int)
		da.Array = vals
		return da
	}

	for _, tc := range []struct {
		name              string
		datums            [][]tree.Datum
		numInsertsPerStep int
		checkEstimate     func(lastEst int64, newEst int64, idx int)
	}{
		{
			name: "null datums",
			datums: [][]tree.Datum{
				{
					tree.DNull,
					tree.DNull,
					tree.DNull,
					tree.DNull,
				},
			},
			numInsertsPerStep: 10,
			checkEstimate: func(lastEst int64, newEst int64, idx int) {
				if idx > 0 {
					require.Equal(t, newEst, lastEst)
				} else {
					require.Greater(t, newEst, lastEst)
				}
			},
		},
		{
			name: "non null mixed",
			datums: [][]tree.Datum{
				{
					tree.NewDInt(1),
					tree.NewDString("string"),
					tree.NewDTuple(tupleTyp, tree.NewDInt(1), tree.NewDInt(2), tree.NewDString("test")),
					makeArray(tree.NewDInt(1)),
				}, {
					tree.NewDInt(2),
					tree.NewDString("asdf"),
					tree.NewDTuple(tupleTyp, tree.NewDInt(5), tree.NewDInt(6), tree.NewDString("lkjh")),
					makeArray(tree.NewDInt(2)),
				}, {
					tree.NewDInt(3),
					tree.NewDString("qwer"),
					tree.NewDTuple(tupleTyp, tree.NewDInt(8), tree.NewDInt(9), tree.NewDString("uiop")),
					makeArray(tree.NewDInt(3)),
				},
				{
					tree.NewDInt(4),
					tree.NewDString("lmno"),
					tree.NewDTuple(tupleTyp, tree.NewDInt(10), tree.NewDInt(11), tree.NewDString("pqrs")),
					makeArray(tree.NewDInt(4)),
				},
			},
			numInsertsPerStep: 2,
		},
		{
			name: "non null same",
			datums: [][]tree.Datum{
				{
					tree.NewDInt(1),
					tree.NewDString("string"),
					tree.NewDTuple(tupleTyp, tree.NewDInt(1), tree.NewDInt(2), tree.NewDString("test")),
					makeArray(tree.NewDInt(1)),
				},
			},
			numInsertsPerStep: 10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Create the writer.
			var b bytes.Buffer
			writer, err := NewWriter(sch, &b)
			require.NoError(t, err)
			require.Equal(t, int64(0), writer.BufferedBytesEstimate())

			// Add a testing knob which we can use to assert that we consider every column when computing estimates.
			var callbackCount int
			columnsSeen := make(map[string]struct{})
			testingCountEstimatedBytesCallback = func(writer file.ColumnChunkWriter, i int64) error {
				if i <= int64(0) {
					return errors.AssertionFailedf("expected byte estimate to by positive")
				}
				callbackCount += 1
				columnsSeen[writer.Descr().Name()] = struct{}{}
				return nil
			}
			verifyEstimates := func() {
				// There are 4 columns but 6 physical columns because the tuple has 3 fields.
				// Assert that every time we add a row each one of these physical columns
				// contributes to the byte estimate.
				require.Equal(t, 6, callbackCount)
				require.Equal(t, 6, len(columnsSeen))

				columnsSeen = make(map[string]struct{})
				callbackCount = 0
			}

			addDatums := func(writer *Writer) {
				// The number of times to insert all the datums into the writer before checking
				// the byte estimate. Sometimes the estimate does not change by just inserting one
				// row because the writer may amortize allocation and/or bit pack efficiently.
				for i := 0; i < tc.numInsertsPerStep; i++ {
					for _, d := range tc.datums {
						err := writer.AddRow(d)
						require.NoError(t, err)
						verifyEstimates()
					}
				}
			}

			var lastEst int64
			for i := 0; i < 10; i++ {
				addDatums(writer)
				newEst := writer.BufferedBytesEstimate()

				if tc.checkEstimate != nil {
					tc.checkEstimate(lastEst, newEst, i)
				} else {
					require.Greater(t, newEst, lastEst)
				}
				lastEst = newEst
			}

			// Flush the writer and assert the estimate goes back to 0
			require.NoError(t, writer.Flush())
			require.Equal(t, int64(0), writer.BufferedBytesEstimate())

			// Add datums again and assert the estimate goes up.
			addDatums(writer)
			require.Greater(t, writer.BufferedBytesEstimate(), int64(0))

			require.NoError(t, writer.Close())
			require.Equal(t, writer.BufferedBytesEstimate(), int64(0))
		})
	}
}
