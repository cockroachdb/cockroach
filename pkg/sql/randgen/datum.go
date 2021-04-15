// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randgen

import (
	"bytes"
	"math"
	"math/big"
	"math/bits"
	"math/rand"
	"time"
	"unicode"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geogen"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

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
			panic(errors.AssertionFailedf("int with an unexpected width %d", typ.Width()))
		}
	case types.FloatFamily:
		switch typ.Width() {
		case 64:
			return tree.NewDFloat(tree.DFloat(rng.NormFloat64()))
		case 32:
			return tree.NewDFloat(tree.DFloat(float32(rng.NormFloat64())))
		default:
			panic(errors.AssertionFailedf("float with an unexpected width %d", typ.Width()))
		}
	case types.Box2DFamily:
		b := geo.NewCartesianBoundingBox().AddPoint(rng.NormFloat64(), rng.NormFloat64()).AddPoint(rng.NormFloat64(), rng.NormFloat64())
		return tree.NewDBox2D(*b)
	case types.GeographyFamily:
		gm, err := typ.GeoMetadata()
		if err != nil {
			panic(err)
		}
		srid := gm.SRID
		if srid == 0 {
			srid = geopb.DefaultGeographySRID
		}
		return tree.NewDGeography(geogen.RandomGeography(rng, srid))
	case types.GeometryFamily:
		gm, err := typ.GeoMetadata()
		if err != nil {
			panic(err)
		}
		return tree.NewDGeometry(geogen.RandomGeometry(rng, gm.SRID))
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
			timeutil.Unix(rng.Int63n(2000000000), rng.Int63n(1000000)),
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
		// Calling ResolvedType causes the internal TupleContents types to be
		// populated.
		tuple.ResolvedType()
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
		var length int
		if typ.Oid() == oid.T_char || typ.Oid() == oid.T_bpchar {
			length = 1
		} else {
			length = rng.Intn(10)
		}
		p := make([]byte, length)
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
			timeutil.Unix(rng.Int63n(2000000000), rng.Int63n(1000000)),
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
		panic(errors.AssertionFailedf("invalid type %v", typ.DebugString()))
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
				panic(errors.AssertionFailedf("no interesting datum for type %s found", typ.String()))
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
			panic(errors.AssertionFailedf("int with an unexpected width %d", typ.Width()))
		}
	case types.FloatFamily:
		switch typ.Width() {
		case 64:
			return special
		case 32:
			return tree.NewDFloat(tree.DFloat(float32(*special.(*tree.DFloat))))
		default:
			panic(errors.AssertionFailedf("float with an unexpected width %d", typ.Width()))
		}
	case types.BitFamily:
		// A width of 64 is used by all special BitFamily datums in randInterestingDatums.
		// If the provided bit type, typ, has a width of 0 (representing an arbitrary width) or 64 exactly,
		// then the special datum will be valid for the provided type. Otherwise, the special type
		// must be resized to match the width of the provided type.
		if typ.Width() == 0 || typ.Width() == 64 {
			return special
		}
		return &tree.DBitArray{BitArray: special.(*tree.DBitArray).ToWidth(uint(typ.Width()))}

	default:
		return special
	}
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
	return string(rune('A' + rng.Intn(simpleRange)))
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
		types.Box2DFamily: {
			&tree.DBox2D{CartesianBoundingBox: geo.CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: -10, HiX: 10, LoY: -10, HiY: 10}}},
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
