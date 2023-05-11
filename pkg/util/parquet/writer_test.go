// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

// supportedTypes contains all types supported by the writer,
// which is all types that pass randomized testing below.
var supportedTypes []*types.T

func init() {
	for _, typ := range randgen.SeedTypes {
		switch typ.Family() {
		// The types below are unsupported. They will fail randomized tests.
		case types.AnyFamily:
		case types.TSQueryFamily, types.TSVectorFamily:
		case types.VoidFamily:
		case types.TupleFamily:
		case types.ArrayFamily:
			// We will manually add array types which are supported below.
			// Excluding types.TupleFamily and types.ArrayFamily leaves us with only
			// scalar types so far.
		default:
			supportedTypes = append(supportedTypes, typ)
		}
	}

	// randgen.SeedTypes does not include types.Json, so we add it manually here.
	supportedTypes = append(supportedTypes, types.Json)

	// Include all array types which are arrays of the scalar types above.
	var arrayTypes []*types.T
	for oid := range types.ArrayOids {
		arrayTyp := types.OidToType[oid]
		for _, typ := range supportedTypes {
			if arrayTyp.InternalType.ArrayContents == typ {
				arrayTypes = append(arrayTypes, arrayTyp)
			}
		}
	}
	supportedTypes = append(supportedTypes, arrayTypes...)
}

func makeRandDatums(numRows int, sch *colSchema, rng *rand.Rand) [][]tree.Datum {
	datums := make([][]tree.Datum, numRows)
	for i := 0; i < numRows; i++ {
		datums[i] = make([]tree.Datum, len(sch.columnTypes))
		for j := 0; j < len(sch.columnTypes); j++ {
			datums[i][j] = randgen.RandDatum(rng, sch.columnTypes[j], true)
		}
	}
	return datums
}

func makeRandSchema(numCols int, allowedTypes []*types.T, rng *rand.Rand) *colSchema {
	sch := newColSchema(numCols)
	for i := 0; i < numCols; i++ {
		sch.columnTypes[i] = allowedTypes[rng.Intn(len(allowedTypes))]
		sch.columnNames[i] = fmt.Sprintf("%s%d", sch.columnTypes[i].Name(), i)
	}
	return sch
}

func TestRandomDatums(t *testing.T) {
	seed := rand.NewSource(timeutil.Now().UnixNano())
	rng := rand.New(seed)
	t.Logf("random seed %d", seed.Int63())

	numRows := 64
	numCols := 128
	maxRowGroupSize := int64(8)

	sch := makeRandSchema(numCols, supportedTypes, rng)
	datums := makeRandDatums(numRows, sch, rng)

	fileName := "TestRandomDatums.parquet"
	f, err := os.CreateTemp("", fileName)
	require.NoError(t, err)

	schemaDef, err := NewSchema(sch.columnNames, sch.columnTypes)
	require.NoError(t, err)

	writer, err := NewWriter(schemaDef, f, WithMaxRowGroupLength(maxRowGroupSize))
	require.NoError(t, err)

	for _, row := range datums {
		err = writer.AddRow(row)
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	ReadFileAndVerifyDatums(t, f.Name(), numRows, numCols, writer, datums)
}

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

			ReadFileAndVerifyDatums(t, f.Name(), numRows, numCols, writer, datums)
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

// optionsTest can be used to assert the behavior of an Option.

// It creates a writer using the supplied Option and writes a parquet file
// containing 1 column & 1 datum. Then, it creates a reader for the file and
// calls testFn with the reader.
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
