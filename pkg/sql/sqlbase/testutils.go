// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"math/rand"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// This file contains utility functions for tests (in other packages).

// TestingGetMutableExistingTableDescriptor retrieves a MutableTableDescriptor
// directly from the KV layer.
func TestingGetMutableExistingTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *MutableTableDescriptor {
	return NewMutableExistingTableDescriptor(*TestingGetTableDescriptor(kvDB, codec, database, table))
}

// TestingGetTableDescriptor retrieves a table descriptor directly from the KV
// layer.
//
// TODO(ajwerner): Move this to catalogkv and/or question the very existence of
// this function. Consider renaming to TestingGetTableDescriptorByName or
// removing it altogether.
func TestingGetTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *TableDescriptor {
	// log.VEventf(context.TODO(), 2, "TestingGetTableDescriptor %q %q", database, table)
	// testutil, so we pass settings as nil for both database and table name keys.
	dKey := NewDatabaseKey(database)
	ctx := context.TODO()
	gr, err := kvDB.Get(ctx, dKey.Key(codec))
	if err != nil {
		panic(err)
	}
	if !gr.Exists() {
		panic("database missing")
	}
	dbDescID := ID(gr.ValueInt())

	tKey := NewPublicTableKey(dbDescID, table)
	gr, err = kvDB.Get(ctx, tKey.Key(codec))
	if err != nil {
		panic(err)
	}
	if !gr.Exists() {
		panic("table missing")
	}

	descKey := MakeDescMetadataKey(codec, ID(gr.ValueInt()))
	desc := &Descriptor{}
	ts, err := kvDB.GetProtoTs(ctx, descKey, desc)
	if err != nil || desc.Equal(Descriptor{}) {
		log.Fatalf(ctx, "proto with id %d missing. err: %v", gr.ValueInt(), err)
	}
	tableDesc := desc.Table(ts)
	if tableDesc == nil {
		return nil
	}
	err = tableDesc.MaybeFillInDescriptor(ctx, kvDB, codec)
	if err != nil {
		log.Fatalf(ctx, "failure to fill in descriptor. err: %v", err)
	}
	return tableDesc
}

// TestingGetImmutableTableDescriptor retrieves an immutable table descriptor
// directly from the KV layer.
func TestingGetImmutableTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *ImmutableTableDescriptor {
	return NewImmutableTableDescriptor(*TestingGetTableDescriptor(kvDB, codec, database, table))
}

// RandDatum generates a random Datum of the given type.
// If nullOk is true, the datum can be DNull.
// Note that if typ.Family is UNKNOWN, the datum will always be DNull,
// regardless of the null flag.
func RandDatum(rng *rand.Rand, typ *types.T, nullOk bool) tree.Datum {
	nullDenominator := 10
	if !nullOk {
		nullDenominator = 0
	}
	return RandDatumWithNullChance(rng, typ, nullDenominator)
}

// RandDatumWithNullChance generates a random Datum of the given type.
// nullChance is the chance of returning null, expressed as a fraction
// denominator. For example, a nullChance of 5 means that there's a 1/5 chance
// that DNull will be returned. A nullChance of 0 means that DNull will not
// be returned.
// Note that if typ.Family is UNKNOWN, the datum will always be
// DNull, regardless of the null flag.
func RandDatumWithNullChance(rng *rand.Rand, typ *types.T, nullChance int) tree.Datum {
	if nullChance != 0 && rng.Intn(nullChance) == 0 {
		return tree.DNull
	}
	// Sometimes pick from a predetermined list of known interesting datums.
	if rng.Intn(10) == 0 {
		if special := randInterestingDatum(rng, typ); special != nil {
			return special
		}
	}
	switch typ.Family() {
	case types.BoolFamily:
		return tree.MakeDBool(rng.Intn(2) == 1)
	case types.IntFamily:
		switch typ.Width() {
		case 64:
			// int64(rng.Uint64()) to get negative numbers, too
			return tree.NewDInt(tree.DInt(int64(rng.Uint64())))
		case 32:
			// int32(rng.Uint64()) to get negative numbers, too
			return tree.NewDInt(tree.DInt(int32(rng.Uint64())))
		case 16:
			// int16(rng.Uint64()) to get negative numbers, too
			return tree.NewDInt(tree.DInt(int16(rng.Uint64())))
		case 8:
			// int8(rng.Uint64()) to get negative numbers, too
			return tree.NewDInt(tree.DInt(int8(rng.Uint64())))
		default:
			panic(fmt.Sprintf("int with an unexpected width %d", typ.Width()))
		}
	case types.FloatFamily:
		switch typ.Width() {
		case 64:
			return tree.NewDFloat(tree.DFloat(rng.NormFloat64()))
		case 32:
			return tree.NewDFloat(tree.DFloat(float32(rng.NormFloat64())))
		default:
			panic(fmt.Sprintf("float with an unexpected width %d", typ.Width()))
		}
	case types.GeographyFamily:
		// TODO(otan): generate fake data properly.
		return tree.NewDGeography(
			geo.MustParseGeographyFromEWKB([]byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f")),
		)
	case types.GeometryFamily:
		// TODO(otan): generate fake data properly.
		return tree.NewDGeometry(
			geo.MustParseGeometryFromEWKB([]byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f")),
		)
	case types.DecimalFamily:
		d := &tree.DDecimal{}
		// int64(rng.Uint64()) to get negative numbers, too
		d.Decimal.SetFinite(int64(rng.Uint64()), int32(rng.Intn(40)-20))
		return d
	case types.DateFamily:
		d, err := pgdate.MakeDateFromUnixEpoch(int64(rng.Intn(10000)))
		if err != nil {
			return nil
		}
		return tree.NewDDate(d)
	case types.TimeFamily:
		return tree.MakeDTime(timeofday.Random(rng)).Round(tree.TimeFamilyPrecisionToRoundDuration(typ.Precision()))
	case types.TimeTZFamily:
		return tree.NewDTimeTZFromOffset(
			timeofday.Random(rng),
			// We cannot randomize seconds, because lib/pq does NOT print the
			// second offsets making some tests break when comparing
			// results in == results out using string comparison.
			(rng.Int31n(28*60+59)-(14*60+59))*60,
		).Round(tree.TimeFamilyPrecisionToRoundDuration(typ.Precision()))
	case types.TimestampFamily:
		return tree.MustMakeDTimestamp(
			timeutil.Unix(rng.Int63n(1000000), rng.Int63n(1000000)),
			tree.TimeFamilyPrecisionToRoundDuration(typ.Precision()),
		)
	case types.IntervalFamily:
		sign := 1 - rng.Int63n(2)*2
		return &tree.DInterval{Duration: duration.MakeDuration(
			sign*rng.Int63n(25*3600*int64(1000000000)),
			sign*rng.Int63n(1000),
			sign*rng.Int63n(1000),
		)}
	case types.UuidFamily:
		gen := uuid.NewGenWithReader(rng)
		return tree.NewDUuid(tree.DUuid{UUID: uuid.Must(gen.NewV4())})
	case types.INetFamily:
		ipAddr := ipaddr.RandIPAddr(rng)
		return tree.NewDIPAddr(tree.DIPAddr{IPAddr: ipAddr})
	case types.JsonFamily:
		j, err := json.Random(20, rng)
		if err != nil {
			return nil
		}
		return &tree.DJSON{JSON: j}
	case types.TupleFamily:
		tuple := tree.DTuple{D: make(tree.Datums, len(typ.TupleContents()))}
		for i := range typ.TupleContents() {
			tuple.D[i] = RandDatum(rng, typ.TupleContents()[i], true)
		}
		return &tuple
	case types.BitFamily:
		width := typ.Width()
		if width == 0 {
			width = rng.Int31n(100)
		}
		r := bitarray.Rand(rng, uint(width))
		return &tree.DBitArray{BitArray: r}
	case types.StringFamily:
		// Generate a random ASCII string.
		p := make([]byte, rng.Intn(10))
		for i := range p {
			p[i] = byte(1 + rng.Intn(127))
		}
		if typ.Oid() == oid.T_name {
			return tree.NewDName(string(p))
		}
		return tree.NewDString(string(p))
	case types.BytesFamily:
		p := make([]byte, rng.Intn(10))
		_, _ = rng.Read(p)
		return tree.NewDBytes(tree.DBytes(p))
	case types.TimestampTZFamily:
		return tree.MustMakeDTimestampTZ(
			timeutil.Unix(rng.Int63n(1000000), rng.Int63n(1000000)),
			tree.TimeFamilyPrecisionToRoundDuration(typ.Precision()),
		)
	case types.CollatedStringFamily:
		// Generate a random Unicode string.
		var buf bytes.Buffer
		n := rng.Intn(10)
		for i := 0; i < n; i++ {
			var r rune
			for {
				r = rune(rng.Intn(unicode.MaxRune + 1))
				if !unicode.Is(unicode.C, r) {
					break
				}
			}
			buf.WriteRune(r)
		}
		d, err := tree.NewDCollatedString(buf.String(), typ.Locale(), &tree.CollationEnvironment{})
		if err != nil {
			panic(err)
		}
		return d
	case types.OidFamily:
		return tree.NewDOid(tree.DInt(rng.Uint32()))
	case types.UnknownFamily:
		return tree.DNull
	case types.ArrayFamily:
		return RandArray(rng, typ, 0)
	case types.AnyFamily:
		return RandDatumWithNullChance(rng, RandType(rng), nullChance)
	case types.EnumFamily:
		// If the input type is not hydrated with metadata, or doesn't contain
		// any enum values, then return NULL.
		if typ.TypeMeta.EnumData == nil {
			return tree.DNull
		}
		reps := typ.TypeMeta.EnumData.LogicalRepresentations
		if len(reps) == 0 {
			return tree.DNull
		}
		// Otherwise, pick a random enum value.
		d, err := tree.MakeDEnumFromLogicalRepresentation(typ, reps[rng.Intn(len(reps))])
		if err != nil {
			panic(err)
		}
		return d
	default:
		panic(fmt.Sprintf("invalid type %v", typ.DebugString()))
	}
}

// RandArray generates a random DArray where the contents have nullChance
// of being null.
func RandArray(rng *rand.Rand, typ *types.T, nullChance int) tree.Datum {
	contents := typ.ArrayContents()
	if contents.Family() == types.AnyFamily {
		contents = RandArrayContentsType(rng)
	}
	arr := tree.NewDArray(contents)
	for i := 0; i < rng.Intn(10); i++ {
		if err := arr.Append(RandDatumWithNullChance(rng, contents, nullChance)); err != nil {
			panic(err)
		}
	}
	return arr
}

const simpleRange = 10

// RandDatumSimple generates a random Datum of the given type. The generated
// datums will be simple (i.e., only one character or an integer between 0
// and 9), such that repeated calls to this function will regularly return a
// previously generated datum.
func RandDatumSimple(rng *rand.Rand, typ *types.T) tree.Datum {
	datum := tree.DNull
	switch typ.Family() {
	case types.BitFamily:
		datum, _ = tree.NewDBitArrayFromInt(rng.Int63n(simpleRange), uint(bits.Len(simpleRange)))
	case types.BoolFamily:
		if rng.Intn(2) == 1 {
			datum = tree.DBoolTrue
		} else {
			datum = tree.DBoolFalse
		}
	case types.BytesFamily:
		datum = tree.NewDBytes(tree.DBytes(randStringSimple(rng)))
	case types.DateFamily:
		date, _ := pgdate.MakeDateFromPGEpoch(rng.Int31n(simpleRange))
		datum = tree.NewDDate(date)
	case types.DecimalFamily:
		datum = &tree.DDecimal{
			Decimal: apd.Decimal{
				Coeff: *big.NewInt(rng.Int63n(simpleRange)),
			},
		}
	case types.IntFamily:
		datum = tree.NewDInt(tree.DInt(rng.Intn(simpleRange)))
	case types.IntervalFamily:
		datum = &tree.DInterval{Duration: duration.MakeDuration(
			rng.Int63n(simpleRange)*1e9,
			0,
			0,
		)}
	case types.FloatFamily:
		datum = tree.NewDFloat(tree.DFloat(rng.Intn(simpleRange)))
	case types.INetFamily:
		datum = tree.NewDIPAddr(tree.DIPAddr{
			IPAddr: ipaddr.IPAddr{
				Addr: ipaddr.Addr(uint128.FromInts(0, uint64(rng.Intn(simpleRange)))),
			},
		})
	case types.JsonFamily:
		datum = tree.NewDJSON(randJSONSimple(rng))
	case types.OidFamily:
		datum = tree.NewDOid(tree.DInt(rng.Intn(simpleRange)))
	case types.StringFamily:
		datum = tree.NewDString(randStringSimple(rng))
	case types.TimeFamily:
		datum = tree.MakeDTime(timeofday.New(0, rng.Intn(simpleRange), 0, 0))
	case types.TimestampFamily:
		datum = tree.MustMakeDTimestamp(time.Date(2000, 1, 1, rng.Intn(simpleRange), 0, 0, 0, time.UTC), time.Microsecond)
	case types.TimestampTZFamily:
		datum = tree.MustMakeDTimestampTZ(time.Date(2000, 1, 1, rng.Intn(simpleRange), 0, 0, 0, time.UTC), time.Microsecond)
	case types.UuidFamily:
		datum = tree.NewDUuid(tree.DUuid{
			UUID: uuid.FromUint128(uint128.FromInts(0, uint64(rng.Intn(simpleRange)))),
		})
	}
	return datum
}

func randStringSimple(rng *rand.Rand) string {
	return string('A' + rng.Intn(simpleRange))
}

func randJSONSimple(rng *rand.Rand) json.JSON {
	switch rng.Intn(10) {
	case 0:
		return json.NullJSONValue
	case 1:
		return json.FalseJSONValue
	case 2:
		return json.TrueJSONValue
	case 3:
		return json.FromInt(rng.Intn(simpleRange))
	case 4:
		return json.FromString(randStringSimple(rng))
	case 5:
		a := json.NewArrayBuilder(0)
		for i := rng.Intn(3); i >= 0; i-- {
			a.Add(randJSONSimple(rng))
		}
		return a.Build()
	default:
		a := json.NewObjectBuilder(0)
		for i := rng.Intn(3); i >= 0; i-- {
			a.Add(randStringSimple(rng), randJSONSimple(rng))
		}
		return a.Build()
	}
}

// GenerateRandInterestingTable takes a gosql.DB connection and creates
// a table with all the types in randInterestingDatums and rows of the
// interesting datums.
func GenerateRandInterestingTable(db *gosql.DB, dbName, tableName string) error {
	var (
		randTypes []*types.T
		colNames  []string
	)
	numRows := 0
	for _, v := range randInterestingDatums {
		colTyp := v[0].ResolvedType()
		randTypes = append(randTypes, colTyp)
		colNames = append(colNames, colTyp.Name())
		if len(v) > numRows {
			numRows = len(v)
		}
	}

	var columns strings.Builder
	comma := ""
	for i, typ := range randTypes {
		columns.WriteString(comma)
		columns.WriteString(colNames[i])
		columns.WriteString(" ")
		columns.WriteString(typ.SQLString())
		comma = ", "
	}

	createStatement := fmt.Sprintf("CREATE TABLE %s.%s (%s)", dbName, tableName, columns.String())
	if _, err := db.Exec(createStatement); err != nil {
		return err
	}

	row := make([]string, len(randTypes))
	for i := 0; i < numRows; i++ {
		for j, typ := range randTypes {
			datums := randInterestingDatums[typ.Family()]
			var d tree.Datum
			if i < len(datums) {
				d = datums[i]
			} else {
				d = tree.DNull
			}
			row[j] = tree.AsStringWithFlags(d, tree.FmtParsable)
		}
		var builder strings.Builder
		comma := ""
		for _, d := range row {
			builder.WriteString(comma)
			builder.WriteString(d)
			comma = ", "
		}
		insertStmt := fmt.Sprintf("INSERT INTO %s.%s VALUES (%s)", dbName, tableName, builder.String())
		if _, err := db.Exec(insertStmt); err != nil {
			return err
		}
	}
	return nil
}

var (
	// randInterestingDatums is a collection of interesting datums that can be
	// used for random testing.
	randInterestingDatums = map[types.Family][]tree.Datum{
		types.BoolFamily: {
			tree.DBoolTrue,
			tree.DBoolFalse,
		},
		types.IntFamily: {
			tree.NewDInt(tree.DInt(0)),
			tree.NewDInt(tree.DInt(-1)),
			tree.NewDInt(tree.DInt(1)),
			tree.NewDInt(tree.DInt(math.MaxInt8)),
			tree.NewDInt(tree.DInt(math.MinInt8)),
			tree.NewDInt(tree.DInt(math.MaxInt16)),
			tree.NewDInt(tree.DInt(math.MinInt16)),
			tree.NewDInt(tree.DInt(math.MaxInt32)),
			tree.NewDInt(tree.DInt(math.MinInt32)),
			tree.NewDInt(tree.DInt(math.MaxInt64)),
			// Use +1 because that's the SQL range.
			tree.NewDInt(tree.DInt(math.MinInt64 + 1)),
		},
		types.FloatFamily: {
			tree.NewDFloat(tree.DFloat(0)),
			tree.NewDFloat(tree.DFloat(1)),
			tree.NewDFloat(tree.DFloat(-1)),
			tree.NewDFloat(tree.DFloat(math.SmallestNonzeroFloat32)),
			tree.NewDFloat(tree.DFloat(math.MaxFloat32)),
			tree.NewDFloat(tree.DFloat(math.SmallestNonzeroFloat64)),
			tree.NewDFloat(tree.DFloat(math.MaxFloat64)),
			tree.NewDFloat(tree.DFloat(math.Inf(1))),
			tree.NewDFloat(tree.DFloat(math.Inf(-1))),
			tree.NewDFloat(tree.DFloat(math.NaN())),
		},
		types.DecimalFamily: func() []tree.Datum {
			var res []tree.Datum
			for _, s := range []string{
				"0",
				"1",
				"-1",
				"Inf",
				"-Inf",
				"NaN",
				"-12.34e400",
			} {
				d, err := tree.ParseDDecimal(s)
				if err != nil {
					panic(err)
				}
				res = append(res, d)
			}
			return res
		}(),
		types.DateFamily: {
			tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(0)),
			tree.NewDDate(pgdate.LowDate),
			tree.NewDDate(pgdate.HighDate),
			tree.NewDDate(pgdate.PosInfDate),
			tree.NewDDate(pgdate.NegInfDate),
		},
		types.TimeFamily: {
			tree.MakeDTime(timeofday.Min),
			tree.MakeDTime(timeofday.Max),
			tree.MakeDTime(timeofday.Time2400),
		},
		types.TimeTZFamily: {
			tree.DMinTimeTZ,
			tree.DMaxTimeTZ,
		},
		types.TimestampFamily: func() []tree.Datum {
			res := make([]tree.Datum, len(randTimestampSpecials))
			for i, t := range randTimestampSpecials {
				res[i] = tree.MustMakeDTimestamp(t, time.Microsecond)
			}
			return res
		}(),
		types.TimestampTZFamily: func() []tree.Datum {
			res := make([]tree.Datum, len(randTimestampSpecials))
			for i, t := range randTimestampSpecials {
				res[i] = tree.MustMakeDTimestampTZ(t, time.Microsecond)
			}
			return res
		}(),
		types.IntervalFamily: {
			&tree.DInterval{Duration: duration.MakeDuration(0, 0, 0)},
			&tree.DInterval{Duration: duration.MakeDuration(0, 1, 0)},
			&tree.DInterval{Duration: duration.MakeDuration(1, 0, 0)},
			&tree.DInterval{Duration: duration.MakeDuration(1, 1, 1)},
			// TODO(mjibson): fix intervals to stop overflowing then this can be larger.
			&tree.DInterval{Duration: duration.MakeDuration(0, 0, 290*12)},
		},
		types.GeographyFamily: {
			// NOTE(otan): we cannot use WKT here because roachtests do not have geos uploaded.
			// If we parse WKT ourselves or upload GEOS on every roachtest, we may be able to avoid this.
			// POINT(1.0 1.0)
			&tree.DGeography{Geography: geo.MustParseGeography("0101000000000000000000F03F000000000000F03F")},
			// LINESTRING(1.0 1.0, 2.0 2.0)
			&tree.DGeography{Geography: geo.MustParseGeography("010200000002000000000000000000F03F000000000000F03F00000000000000400000000000000040")},
			// POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))
			&tree.DGeography{Geography: geo.MustParseGeography("0103000000010000000500000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F0000000000000000000000000000F03F00000000000000000000000000000000")},
			// POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))
			&tree.DGeography{Geography: geo.MustParseGeography("0103000000020000000500000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F0000000000000000000000000000F03F00000000000000000000000000000000050000009A9999999999C93F9A9999999999C93F9A9999999999C93F9A9999999999D93F9A9999999999D93F9A9999999999D93F9A9999999999D93F9A9999999999C93F9A9999999999C93F9A9999999999C93F")},
			// MULTIPOINT ((10 40), (40 30), (20 20), (30 10))
			&tree.DGeography{Geography: geo.MustParseGeography("010400000004000000010100000000000000000024400000000000004440010100000000000000000044400000000000003E4001010000000000000000003440000000000000344001010000000000000000003E400000000000002440")},
			// MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))
			&tree.DGeography{Geography: geo.MustParseGeography("010500000002000000010200000003000000000000000000244000000000000024400000000000003440000000000000344000000000000024400000000000004440010200000004000000000000000000444000000000000044400000000000003E400000000000003E40000000000000444000000000000034400000000000003E400000000000002440")},
			// MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))
			&tree.DGeography{Geography: geo.MustParseGeography("01060000000200000001030000000100000004000000000000000000444000000000000044400000000000003440000000000080464000000000008046400000000000003E4000000000000044400000000000004440010300000002000000060000000000000000003440000000000080414000000000000024400000000000003E40000000000000244000000000000024400000000000003E4000000000000014400000000000804640000000000000344000000000000034400000000000804140040000000000000000003E40000000000000344000000000000034400000000000002E40000000000000344000000000000039400000000000003E400000000000003440")},
			// GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))
			&tree.DGeography{Geography: geo.MustParseGeography("01070000000300000001010000000000000000004440000000000000244001020000000300000000000000000024400000000000002440000000000000344000000000000034400000000000002440000000000000444001030000000100000004000000000000000000444000000000000044400000000000003440000000000080464000000000008046400000000000003E4000000000000044400000000000004440")},
			// POINT EMPTY
			&tree.DGeography{Geography: geo.MustParseGeography("0101000000000000000000F87F000000000000F87F")},
			// LINESTRING EMPTY
			&tree.DGeography{Geography: geo.MustParseGeography("010200000000000000")},
			// POLYGON EMPTY
			&tree.DGeography{Geography: geo.MustParseGeography("010300000000000000")},
			// MULTIPOINT EMPTY
			&tree.DGeography{Geography: geo.MustParseGeography("010400000000000000")},
			// MULTILINESTRING EMPTY
			&tree.DGeography{Geography: geo.MustParseGeography("010500000000000000")},
			// MULTIPOLYGON EMPTY
			&tree.DGeography{Geography: geo.MustParseGeography("010600000000000000")},
			// GEOMETRYCOLLECTION EMPTY
			&tree.DGeography{Geography: geo.MustParseGeography("010700000000000000")},
		},
		types.GeometryFamily: {
			// NOTE(otan): we cannot use WKT here because roachtests do not have geos uploaded.
			// If we parse WKT ourselves or upload GEOS on every roachtest, we may be able to avoid this.
			// POINT(1.0 1.0)
			&tree.DGeometry{Geometry: geo.MustParseGeometry("0101000000000000000000F03F000000000000F03F")},
			// LINESTRING(1.0 1.0, 2.0 2.0)
			&tree.DGeometry{Geometry: geo.MustParseGeometry("010200000002000000000000000000F03F000000000000F03F00000000000000400000000000000040")},
			// POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))
			&tree.DGeometry{Geometry: geo.MustParseGeometry("0103000000010000000500000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F0000000000000000000000000000F03F00000000000000000000000000000000")},
			// POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))
			&tree.DGeometry{Geometry: geo.MustParseGeometry("0103000000020000000500000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F0000000000000000000000000000F03F00000000000000000000000000000000050000009A9999999999C93F9A9999999999C93F9A9999999999C93F9A9999999999D93F9A9999999999D93F9A9999999999D93F9A9999999999D93F9A9999999999C93F9A9999999999C93F9A9999999999C93F")},
			// MULTIPOINT ((10 40), (40 30), (20 20), (30 10))
			&tree.DGeometry{Geometry: geo.MustParseGeometry("010400000004000000010100000000000000000024400000000000004440010100000000000000000044400000000000003E4001010000000000000000003440000000000000344001010000000000000000003E400000000000002440")},
			// MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))
			&tree.DGeometry{Geometry: geo.MustParseGeometry("010500000002000000010200000003000000000000000000244000000000000024400000000000003440000000000000344000000000000024400000000000004440010200000004000000000000000000444000000000000044400000000000003E400000000000003E40000000000000444000000000000034400000000000003E400000000000002440")},
			// MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))
			&tree.DGeometry{Geometry: geo.MustParseGeometry("01060000000200000001030000000100000004000000000000000000444000000000000044400000000000003440000000000080464000000000008046400000000000003E4000000000000044400000000000004440010300000002000000060000000000000000003440000000000080414000000000000024400000000000003E40000000000000244000000000000024400000000000003E4000000000000014400000000000804640000000000000344000000000000034400000000000804140040000000000000000003E40000000000000344000000000000034400000000000002E40000000000000344000000000000039400000000000003E400000000000003440")},
			// GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))
			&tree.DGeometry{Geometry: geo.MustParseGeometry("01070000000300000001010000000000000000004440000000000000244001020000000300000000000000000024400000000000002440000000000000344000000000000034400000000000002440000000000000444001030000000100000004000000000000000000444000000000000044400000000000003440000000000080464000000000008046400000000000003E4000000000000044400000000000004440")},
			// POINT EMPTY
			&tree.DGeometry{Geometry: geo.MustParseGeometry("0101000000000000000000F87F000000000000F87F")},
			// LINESTRING EMPTY
			&tree.DGeometry{Geometry: geo.MustParseGeometry("010200000000000000")},
			// POLYGON EMPTY
			&tree.DGeometry{Geometry: geo.MustParseGeometry("010300000000000000")},
			// MULTIPOINT EMPTY
			&tree.DGeometry{Geometry: geo.MustParseGeometry("010400000000000000")},
			// MULTILINESTRING EMPTY
			&tree.DGeometry{Geometry: geo.MustParseGeometry("010500000000000000")},
			// MULTIPOLYGON EMPTY
			&tree.DGeometry{Geometry: geo.MustParseGeometry("010600000000000000")},
			// GEOMETRYCOLLECTION EMPTY
			&tree.DGeometry{Geometry: geo.MustParseGeometry("010700000000000000")},
		},
		types.StringFamily: {
			tree.NewDString(""),
			tree.NewDString("X"),
			tree.NewDString(`"`),
			tree.NewDString(`'`),
			tree.NewDString("\x00"),
			tree.NewDString("\u2603"), // unicode snowman
		},
		types.BytesFamily: {
			tree.NewDBytes(""),
			tree.NewDBytes("X"),
			tree.NewDBytes(`"`),
			tree.NewDBytes(`'`),
			tree.NewDBytes("\x00"),
			tree.NewDBytes("\u2603"), // unicode snowman
			tree.NewDBytes("\xFF"),   // invalid utf-8 sequence, but a valid bytes
		},
		types.OidFamily: {
			tree.NewDOid(0),
		},
		types.UuidFamily: {
			tree.DMinUUID,
			tree.DMaxUUID,
		},
		types.INetFamily: {
			tree.DMinIPAddr,
			tree.DMaxIPAddr,
		},
		types.JsonFamily: func() []tree.Datum {
			var res []tree.Datum
			for _, s := range []string{
				`{}`,
				`1`,
				`{"test": "json"}`,
			} {
				d, err := tree.ParseDJSON(s)
				if err != nil {
					panic(err)
				}
				res = append(res, d)
			}
			return res
		}(),
		types.BitFamily: func() []tree.Datum {
			var res []tree.Datum
			for _, i := range []int64{
				0,
				1<<63 - 1,
			} {
				d, err := tree.NewDBitArrayFromInt(i, 64)
				if err != nil {
					panic(err)
				}
				res = append(res, d)
			}
			return res
		}(),
	}
	randTimestampSpecials = []time.Time{
		{},
		time.Date(-2000, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC),
		// NOTE(otan): we cannot support this as it does not work with colexec in tests.
		tree.MinSupportedTime,
		tree.MaxSupportedTime,
	}
)

var (
	// SeedTypes includes the following types that form the basis of randomly
	// generated types:
	//   - All scalar types, except UNKNOWN and ANY
	//   - ARRAY of ANY, where the ANY will be replaced with one of the legal
	//     array element types in RandType
	//   - OIDVECTOR and INT2VECTOR types
	SeedTypes []*types.T

	// arrayContentsTypes contains all of the types that are valid to store within
	// an array.
	arrayContentsTypes []*types.T
	collationLocales   = [...]string{"da", "de", "en"}
)

func init() {
	for _, typ := range types.OidToType {
		switch typ.Oid() {
		case oid.T_unknown, oid.T_anyelement:
			// Don't include these.
		case oid.T_anyarray, oid.T_oidvector, oid.T_int2vector:
			// Include these.
			SeedTypes = append(SeedTypes, typ)
		default:
			// Only include scalar types.
			if typ.Family() != types.ArrayFamily {
				SeedTypes = append(SeedTypes, typ)
			}
		}
	}

	for _, typ := range types.OidToType {
		// Don't include un-encodable types.
		encTyp, err := datumTypeToArrayElementEncodingType(typ)
		if err != nil || encTyp == 0 {
			continue
		}

		// Don't include reg types, since parser currently doesn't allow them to
		// be declared as array element types.
		if typ.Family() == types.OidFamily && typ.Oid() != oid.T_oid {
			continue
		}

		arrayContentsTypes = append(arrayContentsTypes, typ)
	}

	// Sort these so randomly chosen indexes always point to the same element.
	sort.Slice(SeedTypes, func(i, j int) bool {
		return SeedTypes[i].String() < SeedTypes[j].String()
	})
	sort.Slice(arrayContentsTypes, func(i, j int) bool {
		return arrayContentsTypes[i].String() < arrayContentsTypes[j].String()
	})
}

// randInterestingDatum returns an interesting Datum of type typ.
// If there are no such Datums for a scalar type, it panics. Otherwise,
// it returns nil if there are no such Datums. Note that it pays attention
// to the width of the requested type for Int and Float type families.
func randInterestingDatum(rng *rand.Rand, typ *types.T) tree.Datum {
	specials, ok := randInterestingDatums[typ.Family()]
	if !ok || len(specials) == 0 {
		for _, sc := range types.Scalar {
			// Panic if a scalar type doesn't have an interesting datum.
			if sc == typ {
				panic(fmt.Sprintf("no interesting datum for type %s found", typ.String()))
			}
		}
		return nil
	}

	special := specials[rng.Intn(len(specials))]
	switch typ.Family() {
	case types.IntFamily:
		switch typ.Width() {
		case 64:
			return special
		case 32:
			return tree.NewDInt(tree.DInt(int32(tree.MustBeDInt(special))))
		case 16:
			return tree.NewDInt(tree.DInt(int16(tree.MustBeDInt(special))))
		case 8:
			return tree.NewDInt(tree.DInt(int8(tree.MustBeDInt(special))))
		default:
			panic(fmt.Sprintf("int with an unexpected width %d", typ.Width()))
		}
	case types.FloatFamily:
		switch typ.Width() {
		case 64:
			return special
		case 32:
			return tree.NewDFloat(tree.DFloat(float32(*special.(*tree.DFloat))))
		default:
			panic(fmt.Sprintf("float with an unexpected width %d", typ.Width()))
		}
	default:
		return special
	}
}

// RandCollationLocale returns a random element of collationLocales.
func RandCollationLocale(rng *rand.Rand) *string {
	return &collationLocales[rng.Intn(len(collationLocales))]
}

// RandType returns a random type value.
func RandType(rng *rand.Rand) *types.T {
	return RandTypeFromSlice(rng, SeedTypes)
}

// RandArrayContentsType returns a random type that's guaranteed to be valid to
// use as the contents of an array.
func RandArrayContentsType(rng *rand.Rand) *types.T {
	return RandTypeFromSlice(rng, arrayContentsTypes)
}

// RandTypeFromSlice returns a random type from the input slice of types.
func RandTypeFromSlice(rng *rand.Rand, typs []*types.T) *types.T {
	typ := typs[rng.Intn(len(typs))]
	switch typ.Family() {
	case types.BitFamily:
		return types.MakeBit(int32(rng.Intn(50)))
	case types.CollatedStringFamily:
		return types.MakeCollatedString(types.String, *RandCollationLocale(rng))
	case types.ArrayFamily:
		if typ.ArrayContents().Family() == types.AnyFamily {
			inner := RandArrayContentsType(rng)
			if inner.Family() == types.CollatedStringFamily {
				// TODO(justin): change this when collated arrays are supported.
				inner = types.String
			}
			return types.MakeArray(inner)
		}
	case types.TupleFamily:
		// Generate tuples between 0 and 4 datums in length
		len := rng.Intn(5)
		contents := make([]*types.T, len)
		for i := range contents {
			contents[i] = RandType(rng)
		}
		return types.MakeTuple(contents)
	}
	return typ
}

// RandColumnType returns a random type that is a legal column type (e.g. no
// nested arrays or tuples).
func RandColumnType(rng *rand.Rand) *types.T {
	for {
		typ := RandType(rng)
		if err := ValidateColumnDefType(typ); err == nil {
			return typ
		}
	}
}

// RandArrayType generates a random array type.
func RandArrayType(rng *rand.Rand) *types.T {
	for {
		typ := RandColumnType(rng)
		resTyp := types.MakeArray(typ)
		if err := ValidateColumnDefType(resTyp); err == nil {
			return resTyp
		}
	}
}

// RandColumnTypes returns a slice of numCols random types. These types must be
// legal table column types.
func RandColumnTypes(rng *rand.Rand, numCols int) []*types.T {
	types := make([]*types.T, numCols)
	for i := range types {
		types[i] = RandColumnType(rng)
	}
	return types
}

// RandSortingType returns a column type which can be key-encoded.
func RandSortingType(rng *rand.Rand) *types.T {
	typ := RandType(rng)
	for MustBeValueEncoded(typ) {
		typ = RandType(rng)
	}
	return typ
}

// RandSortingTypes returns a slice of numCols random ColumnType values
// which are key-encodable.
func RandSortingTypes(rng *rand.Rand, numCols int) []*types.T {
	types := make([]*types.T, numCols)
	for i := range types {
		types[i] = RandSortingType(rng)
	}
	return types
}

// RandDatumEncoding returns a random DatumEncoding value.
func RandDatumEncoding(rng *rand.Rand) DatumEncoding {
	return DatumEncoding(rng.Intn(len(DatumEncoding_value)))
}

// RandEncodableType wraps RandType in order to workaround #36736, which fails
// when name[] (or other type using DTypeWrapper) is encoded.
//
// TODO(andyk): Remove this workaround once #36736 is resolved. Also, RandDatum
// really should be extended to create DTypeWrapper datums with alternate OIDs
// like oid.T_varchar for better testing.
func RandEncodableType(rng *rand.Rand) *types.T {
	var isEncodableType func(t *types.T) bool
	isEncodableType = func(t *types.T) bool {
		switch t.Family() {
		case types.ArrayFamily:
			// Due to #36736, any type returned by RandType that gets turned into
			// a DTypeWrapper random datum will not work. Currently, that's just
			// types.Name.
			if t.ArrayContents().Oid() == oid.T_name {
				return false
			}
			return isEncodableType(t.ArrayContents())

		case types.TupleFamily:
			for i := range t.TupleContents() {
				if !isEncodableType(t.TupleContents()[i]) {
					return false
				}
			}
		}
		return true
	}

	for {
		typ := RandType(rng)
		if isEncodableType(typ) {
			return typ
		}
	}
}

// RandEncodableColumnTypes works around #36736, which fails when name[] (or
// other type using DTypeWrapper) is encoded.
//
// TODO(andyk): Remove this workaround once #36736 is resolved. Replace calls to
// it with calls to RandColumnTypes.
func RandEncodableColumnTypes(rng *rand.Rand, numCols int) []*types.T {
	types := make([]*types.T, numCols)
	for i := range types {
		for {
			types[i] = RandEncodableType(rng)
			if err := ValidateColumnDefType(types[i]); err == nil {
				break
			}
		}
	}
	return types
}

// RandEncDatum generates a random EncDatum (of a random type).
func RandEncDatum(rng *rand.Rand) (EncDatum, *types.T) {
	typ := RandEncodableType(rng)
	datum := RandDatum(rng, typ, true /* nullOk */)
	return DatumToEncDatum(typ, datum), typ
}

// RandSortingEncDatumSlice generates a slice of random EncDatum values of the
// same random type which is key-encodable.
func RandSortingEncDatumSlice(rng *rand.Rand, numVals int) ([]EncDatum, *types.T) {
	typ := RandSortingType(rng)
	vals := make([]EncDatum, numVals)
	for i := range vals {
		vals[i] = DatumToEncDatum(typ, RandDatum(rng, typ, true))
	}
	return vals, typ
}

// RandSortingEncDatumSlices generates EncDatum slices, each slice with values of the same
// random type which is key-encodable.
func RandSortingEncDatumSlices(
	rng *rand.Rand, numSets, numValsPerSet int,
) ([][]EncDatum, []*types.T) {
	vals := make([][]EncDatum, numSets)
	types := make([]*types.T, numSets)
	for i := range vals {
		val, typ := RandSortingEncDatumSlice(rng, numValsPerSet)
		vals[i], types[i] = val, typ
	}
	return vals, types
}

// RandEncDatumRowOfTypes generates a slice of random EncDatum values for the
// corresponding type in types.
func RandEncDatumRowOfTypes(rng *rand.Rand, types []*types.T) EncDatumRow {
	vals := make([]EncDatum, len(types))
	for i := range types {
		vals[i] = DatumToEncDatum(types[i], RandDatum(rng, types[i], true))
	}
	return vals
}

// RandEncDatumRows generates EncDatumRows where all rows follow the same random
// []ColumnType structure.
func RandEncDatumRows(rng *rand.Rand, numRows, numCols int) (EncDatumRows, []*types.T) {
	types := RandEncodableColumnTypes(rng, numCols)
	return RandEncDatumRowsOfTypes(rng, numRows, types), types
}

// RandEncDatumRowsOfTypes generates EncDatumRows, each row with values of the
// corresponding type in types.
func RandEncDatumRowsOfTypes(rng *rand.Rand, numRows int, types []*types.T) EncDatumRows {
	vals := make(EncDatumRows, numRows)
	for i := range vals {
		vals[i] = RandEncDatumRowOfTypes(rng, types)
	}
	return vals
}

// TestingMakePrimaryIndexKey creates a key prefix that corresponds to
// a table row (in the primary index); it is intended for tests.
//
// It is exported because it is used by tests outside of this package.
//
// The value types must match the primary key columns (or a prefix of them);
// supported types are: - Datum
//  - bool (converts to DBool)
//  - int (converts to DInt)
//  - string (converts to DString)
func TestingMakePrimaryIndexKey(desc *TableDescriptor, vals ...interface{}) (roachpb.Key, error) {
	index := &desc.PrimaryIndex
	if len(vals) > len(index.ColumnIDs) {
		return nil, errors.Errorf("got %d values, PK has %d columns", len(vals), len(index.ColumnIDs))
	}
	datums := make([]tree.Datum, len(vals))
	for i, v := range vals {
		switch v := v.(type) {
		case bool:
			datums[i] = tree.MakeDBool(tree.DBool(v))
		case int:
			datums[i] = tree.NewDInt(tree.DInt(v))
		case string:
			datums[i] = tree.NewDString(v)
		case tree.Datum:
			datums[i] = v
		default:
			return nil, errors.Errorf("unexpected value type %T", v)
		}
		// Check that the value type matches.
		colID := index.ColumnIDs[i]
		for i := range desc.Columns {
			c := &desc.Columns[i]
			if c.ID == colID {
				colTyp := datums[i].ResolvedType()
				if t := colTyp.Family(); t != c.Type.Family() {
					return nil, errors.Errorf("column %d of type %s, got value of type %s", i, c.Type.Family(), t)
				}
				break
			}
		}
	}
	// Create the ColumnID to index in datums slice map needed by
	// MakeIndexKeyPrefix.
	colIDToRowIndex := make(map[ColumnID]int)
	for i := range vals {
		colIDToRowIndex[index.ColumnIDs[i]] = i
	}

	keyPrefix := MakeIndexKeyPrefix(keys.SystemSQLCodec, desc, index.ID)
	key, _, err := EncodeIndexKey(desc, index, colIDToRowIndex, datums, keyPrefix)
	if err != nil {
		return nil, err
	}
	return roachpb.Key(key), nil
}

// Mutator defines a method that can mutate or add SQL statements. See the
// sql/mutations package. This interface is defined here to avoid cyclic
// dependencies.
type Mutator interface {
	Mutate(rng *rand.Rand, stmts []tree.Statement) (mutated []tree.Statement, changed bool)
}

// RandCreateTables creates random table definitions.
func RandCreateTables(
	rng *rand.Rand, prefix string, num int, mutators ...Mutator,
) []tree.Statement {
	if num < 1 {
		panic("at least one table required")
	}

	// Make some random tables.
	tables := make([]tree.Statement, num)
	for i := 0; i < num; i++ {
		var interleave *tree.CreateTable
		// 50% chance of interleaving past the first table. Interleaving doesn't
		// make anything harder to do for tests - so prefer to do it a lot.
		if i > 0 && rng.Intn(2) == 0 {
			interleave = tables[rng.Intn(i)].(*tree.CreateTable)
		}
		t := RandCreateTableWithInterleave(rng, prefix, i+1, interleave)
		tables[i] = t
	}

	for _, m := range mutators {
		tables, _ = m.Mutate(rng, tables)
	}

	return tables
}

// RandCreateTable creates a random CreateTable definition.
func RandCreateTable(rng *rand.Rand, prefix string, tableIdx int) *tree.CreateTable {
	return RandCreateTableWithInterleave(rng, prefix, tableIdx, nil)
}

// RandCreateTableWithInterleave creates a random CreateTable definition,
// interleaved into the given other CreateTable definition.
func RandCreateTableWithInterleave(
	rng *rand.Rand, prefix string, tableIdx int, interleaveInto *tree.CreateTable,
) *tree.CreateTable {
	// columnDefs contains the list of Columns we'll add to our table.
	nColumns := randutil.RandIntInRange(rng, 1, 20)
	columnDefs := make([]*tree.ColumnTableDef, 0, nColumns)
	// defs contains the list of Columns and other attributes (indexes, column
	// families, etc) we'll add to our table.
	defs := make(tree.TableDefs, 0, len(columnDefs))

	// Find columnDefs from previous create table.
	interleaveIntoColumnDefs := make(map[tree.Name]*tree.ColumnTableDef)
	var interleaveIntoPK *tree.UniqueConstraintTableDef
	if interleaveInto != nil {
		for i := range interleaveInto.Defs {
			switch d := interleaveInto.Defs[i].(type) {
			case *tree.ColumnTableDef:
				interleaveIntoColumnDefs[d.Name] = d
			case *tree.UniqueConstraintTableDef:
				if d.PrimaryKey {
					interleaveIntoPK = d
				}
			}
		}
	}
	var interleaveDef *tree.InterleaveDef
	if interleaveIntoPK != nil && len(interleaveIntoPK.Columns) > 0 {
		// Make the interleave prefix, which has to be exactly the columns in the
		// parent's primary index.
		prefixLength := len(interleaveIntoPK.Columns)
		fields := make(tree.NameList, prefixLength)
		for i := range interleaveIntoPK.Columns[:prefixLength] {
			def := interleaveIntoColumnDefs[interleaveIntoPK.Columns[i].Column]
			columnDefs = append(columnDefs, def)
			defs = append(defs, def)
			fields[i] = def.Name
		}

		extraCols := make([]*tree.ColumnTableDef, nColumns)
		// Add more columns to the table.
		for i := range extraCols {
			// Loop until we generate an indexable column type.
			var extraCol *tree.ColumnTableDef
			for {
				extraCol = randColumnTableDef(rng, tableIdx, i+prefixLength)
				extraColType := tree.MustBeStaticallyKnownType(extraCol.Type)
				if ColumnTypeIsIndexable(extraColType) {
					break
				}
			}
			extraCols[i] = extraCol
			columnDefs = append(columnDefs, extraCol)
			defs = append(defs, extraCol)
		}

		rng.Shuffle(nColumns, func(i, j int) {
			extraCols[i], extraCols[j] = extraCols[j], extraCols[i]
		})

		// Create the primary key to interleave, maybe add some new columns to the
		// one we're interleaving.
		pk := &tree.UniqueConstraintTableDef{
			PrimaryKey: true,
			IndexTableDef: tree.IndexTableDef{
				Columns: interleaveIntoPK.Columns[:prefixLength:prefixLength],
			},
		}
		for i := range extraCols[:rng.Intn(len(extraCols))] {
			pk.Columns = append(pk.Columns, tree.IndexElem{
				Column:    extraCols[i].Name,
				Direction: tree.Direction(rng.Intn(int(tree.Descending) + 1)),
			})
		}
		defs = append(defs, pk)
		interleaveDef = &tree.InterleaveDef{
			Parent: interleaveInto.Table,
			Fields: fields,
		}
	} else {
		// Make new defs from scratch.
		for i := 0; i < nColumns; i++ {
			columnDef := randColumnTableDef(rng, tableIdx, i)
			columnDefs = append(columnDefs, columnDef)
			defs = append(defs, columnDef)
		}

		// Make a random primary key with high likelihood.
		if rng.Intn(8) != 0 {
			indexDef := randIndexTableDefFromCols(rng, columnDefs)
			if len(indexDef.Columns) > 0 {
				defs = append(defs, &tree.UniqueConstraintTableDef{
					PrimaryKey:    true,
					IndexTableDef: indexDef,
				})
			}
			// Although not necessary for Cockroach to function correctly,
			// but for ease of use for any code that introspects on the
			// AST data structure (instead of the descriptor which doesn't
			// exist yet), explicitly set all PK cols as NOT NULL.
			for _, col := range columnDefs {
				for _, elem := range indexDef.Columns {
					if col.Name == elem.Column {
						col.Nullable.Nullability = tree.NotNull
					}
				}
			}
		}
	}

	// Make indexes.
	nIdxs := rng.Intn(10)
	for i := 0; i < nIdxs; i++ {
		indexDef := randIndexTableDefFromCols(rng, columnDefs)
		if len(indexDef.Columns) == 0 {
			continue
		}
		unique := rng.Intn(2) == 0
		if unique {
			defs = append(defs, &tree.UniqueConstraintTableDef{
				IndexTableDef: indexDef,
			})
		} else {
			defs = append(defs, &indexDef)
		}
	}

	ret := &tree.CreateTable{
		Table:      tree.MakeUnqualifiedTableName(tree.Name(fmt.Sprintf("%s%d", prefix, tableIdx))),
		Defs:       defs,
		Interleave: interleaveDef,
	}

	// Create some random column families.
	if rng.Intn(2) == 0 {
		ColumnFamilyMutator(rng, ret)
	}

	// Maybe add some storing columns.
	res, _ := IndexStoringMutator(rng, []tree.Statement{ret})
	return res[0].(*tree.CreateTable)
}

// ColumnFamilyMutator is mutations.StatementMutator, but lives here to prevent
// dependency cycles with RandCreateTable.
func ColumnFamilyMutator(rng *rand.Rand, stmt tree.Statement) (changed bool) {
	ast, ok := stmt.(*tree.CreateTable)
	if !ok {
		return false
	}

	var columns []tree.Name
	for _, def := range ast.Defs {
		switch def := def.(type) {
		case *tree.FamilyTableDef:
			return false
		case *tree.ColumnTableDef:
			if def.HasColumnFamily() {
				return false
			}
			columns = append(columns, def.Name)
		}
	}

	if len(columns) <= 1 {
		return false
	}

	// Any columns not specified in column families
	// are auto assigned to the first family, so
	// there's no requirement to exhaust columns here.

	rng.Shuffle(len(columns), func(i, j int) {
		columns[i], columns[j] = columns[j], columns[i]
	})
	fd := &tree.FamilyTableDef{}
	for {
		if len(columns) == 0 {
			if len(fd.Columns) > 0 {
				ast.Defs = append(ast.Defs, fd)
			}
			break
		}
		fd.Columns = append(fd.Columns, columns[0])
		columns = columns[1:]
		// 50% chance to make a new column family.
		if rng.Intn(2) != 0 {
			ast.Defs = append(ast.Defs, fd)
			fd = &tree.FamilyTableDef{}
		}
	}
	return true
}

type tableInfo struct {
	columnNames []tree.Name
	pkCols      []tree.Name
}

// IndexStoringMutator is mutations.StatementMutator, but lives here to prevent
// dependency cycles with RandCreateTable.
func IndexStoringMutator(rng *rand.Rand, stmts []tree.Statement) ([]tree.Statement, bool) {
	changed := false
	tables := map[tree.Name]tableInfo{}
	getTableInfoFromCreateStatement := func(ct *tree.CreateTable) tableInfo {
		var columnNames []tree.Name
		var pkCols []tree.Name
		for _, def := range ct.Defs {
			switch ast := def.(type) {
			case *tree.ColumnTableDef:
				columnNames = append(columnNames, ast.Name)
				if ast.PrimaryKey.IsPrimaryKey {
					pkCols = []tree.Name{ast.Name}
				}
			case *tree.UniqueConstraintTableDef:
				if ast.PrimaryKey {
					for _, elem := range ast.Columns {
						pkCols = append(pkCols, elem.Column)
					}
				}
			}
		}
		return tableInfo{columnNames: columnNames, pkCols: pkCols}
	}
	mapFromIndexCols := func(cols []tree.Name) map[tree.Name]struct{} {
		colMap := map[tree.Name]struct{}{}
		for _, col := range cols {
			colMap[col] = struct{}{}
		}
		return colMap
	}
	generateStoringCols := func(rng *rand.Rand, tableCols []tree.Name, indexCols map[tree.Name]struct{}) []tree.Name {
		var storingCols []tree.Name
		for _, col := range tableCols {
			_, ok := indexCols[col]
			if ok {
				continue
			}
			if rng.Intn(2) == 0 {
				storingCols = append(storingCols, col)
			}
		}
		return storingCols
	}
	for _, stmt := range stmts {
		switch ast := stmt.(type) {
		case *tree.CreateIndex:
			if ast.Inverted {
				continue
			}
			tableInfo, ok := tables[ast.Table.ObjectName]
			if !ok {
				continue
			}
			// If we don't have a storing list, make one with 50% chance.
			if ast.Storing == nil && rng.Intn(2) == 0 {
				indexCols := mapFromIndexCols(tableInfo.pkCols)
				for _, elem := range ast.Columns {
					indexCols[elem.Column] = struct{}{}
				}
				ast.Storing = generateStoringCols(rng, tableInfo.columnNames, indexCols)
				changed = true
			}
		case *tree.CreateTable:
			// Write down this table for later.
			tableInfo := getTableInfoFromCreateStatement(ast)
			tables[ast.Table.ObjectName] = tableInfo
			for _, def := range ast.Defs {
				var idx *tree.IndexTableDef
				switch defType := def.(type) {
				case *tree.IndexTableDef:
					idx = defType
				case *tree.UniqueConstraintTableDef:
					if !defType.PrimaryKey {
						idx = &defType.IndexTableDef
					}
				}
				if idx == nil || idx.Inverted {
					continue
				}
				// If we don't have a storing list, make one with 50% chance.
				if idx.Storing == nil && rng.Intn(2) == 0 {
					indexCols := mapFromIndexCols(tableInfo.pkCols)
					for _, elem := range idx.Columns {
						indexCols[elem.Column] = struct{}{}
					}
					idx.Storing = generateStoringCols(rng, tableInfo.columnNames, indexCols)
					changed = true
				}
			}
		}
	}
	return stmts, changed
}

// randColumnTableDef produces a random ColumnTableDef, with a random type and
// nullability.
func randColumnTableDef(rand *rand.Rand, tableIdx int, colIdx int) *tree.ColumnTableDef {
	columnDef := &tree.ColumnTableDef{
		// We make a unique name for all columns by prefixing them with the table
		// index to make it easier to reference columns from different tables.
		Name: tree.Name(fmt.Sprintf("col%d_%d", tableIdx, colIdx)),
		Type: RandSortingType(rand),
	}
	columnDef.Nullable.Nullability = tree.Nullability(rand.Intn(int(tree.SilentNull) + 1))
	return columnDef
}

func randIndexTableDefFromCols(
	rng *rand.Rand, columnTableDefs []*tree.ColumnTableDef,
) tree.IndexTableDef {
	cpy := make([]*tree.ColumnTableDef, len(columnTableDefs))
	copy(cpy, columnTableDefs)
	rng.Shuffle(len(cpy), func(i, j int) { cpy[i], cpy[j] = cpy[j], cpy[i] })
	nCols := rng.Intn(len(cpy)) + 1

	cols := cpy[:nCols]

	indexElemList := make(tree.IndexElemList, 0, len(cols))
	for i := range cols {
		semType := tree.MustBeStaticallyKnownType(cols[i].Type)
		if !ColumnTypeIsIndexable(semType) {
			continue
		}
		indexElemList = append(indexElemList, tree.IndexElem{
			Column:    cols[i].Name,
			Direction: tree.Direction(rng.Intn(int(tree.Descending) + 1)),
		})
	}
	return tree.IndexTableDef{Columns: indexElemList}
}

// The following variables are useful for testing.
var (
	// OneIntCol is a slice of one IntType.
	OneIntCol = []*types.T{types.Int}
	// TwoIntCols is a slice of two IntTypes.
	TwoIntCols = []*types.T{types.Int, types.Int}
	// ThreeIntCols is a slice of three IntTypes.
	ThreeIntCols = []*types.T{types.Int, types.Int, types.Int}
	// FourIntCols is a slice of four IntTypes.
	FourIntCols = []*types.T{types.Int, types.Int, types.Int, types.Int}
)

// MakeIntCols makes a slice of numCols IntTypes.
func MakeIntCols(numCols int) []*types.T {
	ret := make([]*types.T, numCols)
	for i := 0; i < numCols; i++ {
		ret[i] = types.Int
	}
	return ret
}

// IntEncDatum returns an EncDatum representation of DInt(i).
func IntEncDatum(i int) EncDatum {
	return EncDatum{Datum: tree.NewDInt(tree.DInt(i))}
}

// StrEncDatum returns an EncDatum representation of DString(s).
func StrEncDatum(s string) EncDatum {
	return EncDatum{Datum: tree.NewDString(s)}
}

// NullEncDatum returns and EncDatum representation of tree.DNull.
func NullEncDatum() EncDatum {
	return EncDatum{Datum: tree.DNull}
}

// GenEncDatumRowsInt converts rows of ints to rows of EncDatum DInts.
// If an int is negative, the corresponding value is NULL.
func GenEncDatumRowsInt(inputRows [][]int) EncDatumRows {
	rows := make(EncDatumRows, len(inputRows))
	for i, inputRow := range inputRows {
		for _, x := range inputRow {
			if x < 0 {
				rows[i] = append(rows[i], NullEncDatum())
			} else {
				rows[i] = append(rows[i], IntEncDatum(x))
			}
		}
	}
	return rows
}

// MakeIntRows constructs a numRows x numCols table where rows[i][j] = i + j.
func MakeIntRows(numRows, numCols int) EncDatumRows {
	rows := make(EncDatumRows, numRows)
	for i := range rows {
		rows[i] = make(EncDatumRow, numCols)
		for j := 0; j < numCols; j++ {
			rows[i][j] = IntEncDatum(i + j)
		}
	}
	return rows
}

// MakeRandIntRows constructs a numRows x numCols table where the values are random.
func MakeRandIntRows(rng *rand.Rand, numRows int, numCols int) EncDatumRows {
	rows := make(EncDatumRows, numRows)
	for i := range rows {
		rows[i] = make(EncDatumRow, numCols)
		for j := 0; j < numCols; j++ {
			rows[i][j] = IntEncDatum(rng.Int())
		}
	}
	return rows
}

// MakeRandIntRowsInRange constructs a numRows * numCols table where the values
// are random integers in the range [0, maxNum).
func MakeRandIntRowsInRange(
	rng *rand.Rand, numRows int, numCols int, maxNum int, nullProbability float64,
) EncDatumRows {
	rows := make(EncDatumRows, numRows)
	for i := range rows {
		rows[i] = make(EncDatumRow, numCols)
		for j := 0; j < numCols; j++ {
			rows[i][j] = IntEncDatum(rng.Intn(maxNum))
			if rng.Float64() < nullProbability {
				rows[i][j] = NullEncDatum()
			}
		}
	}
	return rows
}

// MakeRepeatedIntRows constructs a numRows x numCols table where blocks of n
// consecutive rows have the same value.
func MakeRepeatedIntRows(n int, numRows int, numCols int) EncDatumRows {
	rows := make(EncDatumRows, numRows)
	for i := range rows {
		rows[i] = make(EncDatumRow, numCols)
		for j := 0; j < numCols; j++ {
			rows[i][j] = IntEncDatum(i/n + j)
		}
	}
	return rows
}

// RandString generates a random string of the desired length from the
// input alphaget.
func RandString(rng *rand.Rand, length int, alphabet string) string {
	buf := make([]byte, length)
	for i := range buf {
		buf[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(buf)
}

// RandCreateType creates a random CREATE TYPE statement. The resulting
// type's name will be name, and if the type is an enum, the members will
// be random strings generated from alphabet.
func RandCreateType(rng *rand.Rand, name, alphabet string) tree.Statement {
	numLabels := rng.Intn(6) + 1
	labels := make([]string, numLabels)
	labelsMap := make(map[string]struct{})
	i := 0
	for i < numLabels {
		s := RandString(rng, rng.Intn(6)+1, alphabet)
		if _, ok := labelsMap[s]; !ok {
			labels[i] = s
			labelsMap[s] = struct{}{}
			i++
		}
	}
	un, err := tree.NewUnresolvedObjectName(1, [3]string{name}, 0)
	if err != nil {
		panic(err)
	}
	return &tree.CreateType{
		TypeName:   un,
		Variety:    tree.Enum,
		EnumLabels: labels,
	}
}
