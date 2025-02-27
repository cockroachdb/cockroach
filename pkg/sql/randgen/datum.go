// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geogen"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/lsn"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	// TODO(normanchenn): temporarily import the parser here to ensure that
	// init() is called.
	_ "github.com/cockroachdb/cockroach/pkg/util/jsonpath/parser"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/tsearch"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
	"github.com/twpayne/go-geom"
)

// RandDatum generates a random Datum of the given type.
// If nullOk is true, the datum can be DNull.
// Note that if typ.Family is UNKNOWN, the datum will always be DNull,
// regardless of the null flag.
func RandDatum(rng *rand.Rand, typ *types.T, nullOk bool) tree.Datum {
	nullChance := NullChance(nullOk)
	return RandDatumWithNullChance(rng, typ, nullChance,
		false /* favorCommonData */, false /* targetColumnIsUnique */)
}

// NullChance returns `n` representing a 1 out of `n` probability of generating
// nulls, depending on whether `nullOk` is true.
func NullChance(nullOk bool) (nullChance int) {
	nullChance = 10
	if !nullOk {
		nullChance = 0
	}
	return nullChance
}

// RandDatumWithNullChance generates a random Datum of the given type.
// nullChance is the chance of returning null, expressed as a fraction
// denominator. For example, a nullChance of 5 means that there's a 1/5 chance
// that DNull will be returned. A nullChance of 0 means that DNull will not
// be returned.
// Note that if typ.Family is UNKNOWN, the datum will always be DNull,
// regardless of the null flag. If favorCommonData is true, selection of data
// values from a pre-determined set of values as opposed to purely random values
// will occur 40% of the time, unless targetColumnIsUnique is also true, in
// which case the common data set is used 10% of the time. Selection of edge
// case, or "interesting" data occurs 10% of the time.
func RandDatumWithNullChance(
	rng *rand.Rand, typ *types.T, nullChance int, favorCommonData, targetColumnIsUnique bool,
) tree.Datum {
	if nullChance != 0 && rng.Intn(nullChance) == 0 {
		return tree.DNull
	}
	// Sometimes pick from a predetermined list of known interesting datums.
	randomInt := rng.Intn(10)
	// OIDs must be valid defined objects for their specific type, so random data
	// errors out most of the time. Always pick interesting OIDs if common
	// data is favored.
	commonDataCutoff := 5
	if targetColumnIsUnique {
		commonDataCutoff = 8
	}
	if favorCommonData && (randomInt > commonDataCutoff || typ.Family() == types.OidFamily) {
		if special := randCommonDatum(rng, typ); special != nil {
			return special
		}
	} else if randomInt == 0 {
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
	case types.PGLSNFamily:
		return tree.NewDPGLSN(lsn.LSN(rng.Uint64()))
	case types.RefCursorFamily:
		p := make([]byte, rng.Intn(10))
		for i := range p {
			p[i] = byte(1 + rng.Intn(127))
		}
		return tree.NewDRefCursor(string(p))
	case types.GeographyFamily:
		gm, err := typ.GeoMetadata()
		if err != nil {
			panic(err)
		}
		srid := gm.SRID
		if srid == 0 {
			srid = geopb.DefaultGeographySRID
		}
		// Limit the maximum geometry size to 16 megabytes.
		const maxSize = uintptr(1024 * 1024 * 16)
		maxRetries := 5
		// Retry until we get a geography below the target size.
		for maxRetries > 0 {
			newGeo := tree.NewDGeography(geogen.RandomGeography(rng, srid))
			if newGeo.Size() < maxSize {
				return newGeo
			}
			maxRetries -= 1
		}
		// Otherwise, pick a simple random polygon.
		geog, err := geo.MakeGeographyFromGeomT(geogen.RandomPolygon(rng, geogen.MakeRandomGeomBoundsForGeography(), gm.SRID, geom.NoLayout))
		if err != nil {
			panic(err)
		}
		dgm := tree.NewDGeography(geog)
		return dgm
	case types.GeometryFamily:
		gm, err := typ.GeoMetadata()
		if err != nil {
			panic(err)
		}
		// Limit the maximum geometry size to 16 megabytes.
		const maxSize = uintptr(1024 * 1024 * 16)
		maxRetries := 5
		// Retry until we get a geography below the target size.
		for maxRetries > 0 {
			newGeom := tree.NewDGeometry(geogen.RandomGeometry(rng, gm.SRID))
			if newGeom.Size() < maxSize {
				return newGeom
			}
			maxRetries -= 1
		}
		// Otherwise, pick a simple random polygon.
		geom, err := geo.MakeGeometryFromGeomT(geogen.RandomPolygon(rng, geogen.MakeRandomGeomBounds(), gm.SRID, geom.NoLayout))
		if err != nil {
			panic(err)
		}
		dgm := tree.NewDGeometry(geom)
		return dgm
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
		gen := uuid.NewGenWithRand(rng.Uint64)
		return tree.NewDUuid(tree.DUuid{UUID: gen.NewV4()})
	case types.INetFamily:
		ipAddr := ipaddr.RandIPAddr(rng)
		return tree.NewDIPAddr(tree.DIPAddr{IPAddr: ipAddr})
	case types.JsonFamily:
		j, err := json.Random(20, rng)
		if err != nil {
			return nil
		}
		return &tree.DJSON{JSON: j}
	case types.JsonpathFamily:
		jsonpath := randJsonpath(rng)
		return tree.NewDJsonpath(jsonpath)
	case types.TupleFamily:
		tuple := tree.DTuple{D: make(tree.Datums, len(typ.TupleContents()))}
		if nullChance == 0 {
			nullChance = 10
		}
		for i := range typ.TupleContents() {
			tuple.D[i] = RandDatumWithNullChance(
				rng, typ.TupleContents()[i], nullChance, favorCommonData, targetColumnIsUnique,
			)
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
		if typ.Oid() == oid.T_bpchar {
			return tree.NewDString(strings.TrimRight(string(p), " "))
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
		return tree.NewDOidWithType(oid.Oid(rng.Uint32()), typ)
	case types.UnknownFamily:
		return tree.DNull
	case types.ArrayFamily:
		return RandArrayWithCommonDataChance(rng, typ, 0, /* nullChance */
			favorCommonData, targetColumnIsUnique)
	case types.AnyFamily:
		return RandDatumWithNullChance(rng, RandType(rng), nullChance,
			favorCommonData, targetColumnIsUnique,
		)
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
		return tree.NewDEnum(d)
	case types.VoidFamily:
		return tree.DVoidDatum
	case types.TSVectorFamily:
		return tree.NewDTSVector(tsearch.RandomTSVector(rng))
	case types.TSQueryFamily:
		return tree.NewDTSQuery(tsearch.RandomTSQuery(rng))
	case types.PGVectorFamily:
		var maxDim = 1000
		if util.RaceEnabled {
			// Some tests might be significantly slower under race, so we reduce
			// the dimensionality.
			maxDim = 50
		}
		return tree.NewDPGVector(vector.Random(rng, maxDim))
	default:
		panic(errors.AssertionFailedf("invalid type %v", typ.DebugString()))
	}
}

// RandArray generates a random DArray where the contents have nullChance
// of being null.
func RandArray(rng *rand.Rand, typ *types.T, nullChance int) tree.Datum {
	return RandArrayWithCommonDataChance(rng, typ, nullChance,
		false /* favorCommonData */, false /* targetColumnIsUnique */)
}

// RandArrayWithCommonDataChance generates a random DArray where the contents
// have a 1 in `nullChance` chance of being null, plus it favors generation of
// non-random data if favorCommonData is true. If both favorCommonData and
// targetColumnIsUnique are true, the non-random data set is used only 10% of
// the time.
func RandArrayWithCommonDataChance(
	rng *rand.Rand, typ *types.T, nullChance int, favorCommonData, targetColumnIsUnique bool,
) tree.Datum {
	contents := typ.ArrayContents()
	if contents.Family() == types.AnyFamily {
		contents = RandArrayContentsType(rng)
	}
	arr := tree.NewDArray(contents)
	for i := 0; i < rng.Intn(10); i++ {
		if err :=
			arr.Append(
				RandDatumWithNullChance(rng, contents, nullChance, favorCommonData, targetColumnIsUnique)); err != nil {
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
	specials, ok := getRandInterestingDatums(typ.Family())
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
	return adjustDatum(special, typ)
}

func adjustDatum(datum tree.Datum, typ *types.T) tree.Datum {
	switch typ.Family() {
	case types.IntFamily:
		switch typ.Width() {
		case 64:
			return datum
		case 32:
			return tree.NewDInt(tree.DInt(int32(tree.MustBeDInt(datum))))
		case 16:
			return tree.NewDInt(tree.DInt(int16(tree.MustBeDInt(datum))))
		case 8:
			return tree.NewDInt(tree.DInt(int8(tree.MustBeDInt(datum))))
		default:
			panic(errors.AssertionFailedf("int with an unexpected width %d", typ.Width()))
		}
	case types.FloatFamily:
		switch typ.Width() {
		case 64:
			return datum
		case 32:
			return tree.NewDFloat(tree.DFloat(float32(*datum.(*tree.DFloat))))
		default:
			panic(errors.AssertionFailedf("float with an unexpected width %d", typ.Width()))
		}
	case types.BitFamily:
		// A width of 64 is used by all special BitFamily datums in randInterestingDatums.
		// If the provided bit type, typ, has a width of 0 (representing an arbitrary width) or 64 exactly,
		// then the special datum will be valid for the provided type. Otherwise, the special type
		// must be resized to match the width of the provided type.
		if typ.Width() == 0 || typ.Width() == 64 {
			return datum
		}
		return &tree.DBitArray{BitArray: datum.(*tree.DBitArray).ToWidth(uint(typ.Width()))}

	case types.StringFamily:
		if typ.Oid() == oid.T_name {
			datum = tree.NewDName(string(*datum.(*tree.DString)))
		}
		return datum

	default:
		return datum
	}
}

// randCommonDatum returns a random Datum of type typ from a common set of
// predetermined values. If there are no such Datums for a scalar type, it
// panics. Otherwise, it returns nil if there are no such Datums. Note that it
// pays attention to the width of the requested type for Int and Float type
// families.
func randCommonDatum(rng *rand.Rand, typ *types.T) tree.Datum {
	var dataSet []tree.Datum
	var ok bool
	typeFamily := typ.Family()
	switch typeFamily {
	case types.GeographyFamily, types.GeometryFamily:
		dataSet, ok = getRandInterestingDatums(typeFamily)
	default:
		dataSet, ok = randCommonDatums[typeFamily]
	}

	if !ok || len(dataSet) == 0 {
		for _, sc := range types.Scalar {
			// Panic if a scalar type doesn't have a common datum.
			if sc == typ {
				panic(errors.AssertionFailedf("no common datum for type %s found", typ.String()))
			}
		}
		return nil
	}

	datum := dataSet[rng.Intn(len(dataSet))]
	return adjustDatum(datum, typ)
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
		dd := &tree.DDecimal{}
		dd.SetInt64(rng.Int63n(simpleRange))
		datum = dd
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
		datum = tree.NewDOidWithType(oid.Oid(rng.Intn(simpleRange)), typ)
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
	case types.TSQueryFamily:
		datum = tree.NewDTSQuery(tsearch.RandomTSQuery(rng))
	case types.TSVectorFamily:
		datum = tree.NewDTSVector(tsearch.RandomTSVector(rng))
	}
	return datum
}

func randStringSimple(rng *rand.Rand) string {
	return string(rune('A' + rng.Intn(simpleRange)))
}

func randJSONSimple(rng *rand.Rand) json.JSON {
	return randJSONSimpleDepth(rng, 0)
}

func randJSONSimpleDepth(rng *rand.Rand, depth int) json.JSON {
	depth++
	// Prevents timeouts during stress runs.
	if depth > 100 {
		return json.NullJSONValue
	}
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
			a.Add(randJSONSimpleDepth(rng, depth))
		}
		return a.Build()
	default:
		a := json.NewObjectBuilder(0)
		for i := rng.Intn(3); i >= 0; i-- {
			a.Add(randStringSimple(rng), randJSONSimpleDepth(rng, depth))
		}
		return a.Build()
	}
}

const charSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// TODO(normanchenn): Add support for more complex jsonpath queries.
func randJsonpath(rng *rand.Rand) string {
	var parts []string
	depth := 1 + rng.Intn(20)

	for range depth {
		p := make([]byte, 1+rng.Intn(20))
		for i := range p {
			p[i] = charSet[rng.Intn(len(charSet))]
		}
		if rng.Intn(5) == 0 {
			p = append(p, []byte("[*]")...)
		}
		parts = append(parts, string(p))
	}
	return "$." + strings.Join(parts, ".")
}

var once sync.Once

// randInterestingDatumskl contains interesting datums for each type.  Note that
// this map should never be assessed directly. Use getRandInterestingDatums
// instead.
var randInterestingDatums map[types.Family][]tree.Datum

// getRandInterestingDatums is equivalent to getting the values out of the
// randInterestingDatums map. This function is used to prevent
// randInterestingDatums from getting initialized unless it is needed.
// Preventing it's initialization significantly speeds up the tests by reducing
// heap allocation.
func getRandInterestingDatums(typ types.Family) ([]tree.Datum, bool) {
	once.Do(func() {

		randTimestampSpecials := []time.Time{
			{},
			time.Date(-2000, time.January, 1, 0, 0, 0, 0, time.UTC),
			time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC),
			// NOTE(otan): we cannot support this as it does not work with colexec in tests.
			tree.MinSupportedTime,
			tree.MaxSupportedTime,
		}

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
				tree.NewDFloat(tree.DFloat(math.Copysign(0, -1))), // -0
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
					"-0",
					"0",
					"1",
					"1.0",
					"-1",
					"-1.0",
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
			types.PGLSNFamily: {
				tree.NewDPGLSN(0),
				tree.NewDPGLSN(math.MaxInt64),
				tree.NewDPGLSN(math.MaxInt64 + 1),
				tree.NewDPGLSN(math.MaxUint64),
			},
			types.RefCursorFamily: {
				tree.NewDRefCursor(""),
				tree.NewDRefCursor("X"),
				tree.NewDRefCursor(`"`),
				tree.NewDRefCursor(`'`),
				tree.NewDRefCursor("\x00"),
				tree.NewDRefCursor("\u2603"), // unicode snowman
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
			types.JsonpathFamily: func() []tree.Datum {
				var res []tree.Datum
				for _, s := range []string{
					"$",
					"strict $",
					"lax $",
					"$.a1[*]",
					// "$.*",
					// "$.1a[*]",
					// "$.a ? (@.b == 1)",
					// "$.a ? (@.b == 1).b",
					// "$.a ? (@.b == 'true').c",
					// "$.a ? (@.b == 1).c ? (@.d == 2)",
					// "$.a?(@.b==1).c?(@.d==2)",
					// "$  .  a  ?  (  @  .  b  ==  1  )  .  c  ?  (  @  .  d  ==  2  )  ",
					// "$.a.type()",
				} {
					d, err := tree.ParseDJsonpath(s)
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

	})
	datums, ok := randInterestingDatums[typ]
	return datums, ok
}

var commonTimestampSpecials = func() []time.Time {
	const secondsPerDay = 24 * 60 * 60
	var res []time.Time
	for _, i := range []int{
		// Number of days since unix epoch
		-10000, -1000, -100, -10, 0, 10, 100, 1000, 10000,
	} {
		for _, j := range []int{
			// Adjustment in number of seconds
			-81364, -10000, 0, 500, 12345, 100000,
		} {
			t := timeutil.Unix(int64(i*secondsPerDay+j), 0)
			res = append(res, t)
		}
	}
	return res
}()

// randCommonDatums is a collection of datums that can be sampled for cases
// where it's useful to pull data from a common, relatively small data set.
// For example, an inner equijoin between two tables on an integer column will
// only return rows if the join columns share some of the same data values.
var randCommonDatums = map[types.Family][]tree.Datum{
	types.BoolFamily: {
		tree.DBoolTrue,
		tree.DBoolFalse,
	},
	types.IntFamily: func() []tree.Datum {
		var dataSet []tree.Datum
		dataSet = make([]tree.Datum, 0, 256)
		for i := -128; i < 128; i++ {
			dataSet = append(dataSet, tree.NewDInt(tree.DInt(i)))
		}
		return dataSet
	}(),
	types.FloatFamily: func() []tree.Datum {
		// 17 digits of precision
		val := float64(1.2345678901234567e-50)
		dataSet := make([]tree.Datum, 0, 256)
		for i := 0; i < 100; i++ {
			dataSet = append(dataSet, tree.NewDFloat(tree.DFloat(val)))
			val *= 10
		}
		return dataSet
	}(),
	types.DecimalFamily: func() []tree.Datum {
		var res []tree.Datum
		for _, s := range []string{
			"0",
			"1",
			"1.1",
			"1.11",
			"1.2",
			"-10.1",
			".111",
			"9.9",
			"9.8",
			"-9.8",
			"99.8",
			"12.01e10",
			"-10.1e-10",
			"1e3",
		} {
			d, err := tree.ParseDDecimal(s)
			if err != nil {
				panic(err)
			}
			res = append(res, d)
		}
		return res
	}(),
	types.DateFamily: func() []tree.Datum {
		var res []tree.Datum
		for _, i := range []int64{
			0, -2400000, 2145000000, -100, -1000, -10000, 100, 1000, 1000, 10000,
			10001, 10002, 10010, 10020, 10030, 11000, 11001, 12000, 20000, 20030,
		} {
			postgresDate, err := pgdate.MakeDateFromUnixEpoch(i)
			if err == nil {
				d := tree.NewDDate(postgresDate)
				res = append(res, d)
			}
		}
		return res
	}(),
	types.TimeFamily: func() []tree.Datum {
		var res []tree.Datum
		for _, i := range []int{
			85400000000, 80400000000, 76400000000, 60000000000, 50000000000, 40000000000, 30000000000,
			20000000000, 10000000000, 1000000000, 100000000, 99999999, 10000000, 100000010, 100000001,
			100000100, 100001000, 100010000, 100100000, 101000000, 110000000,
		} {
			d := tree.MakeDTime(timeofday.TimeOfDay(i))
			res = append(res, d)
		}
		return res
	}(),
	types.TimeTZFamily: func() []tree.Datum {
		var res []tree.Datum
		for _, i := range []int{
			85400000000, 76400000000, 50000000000, 30000000000, 10000000000, 100000000, 99999999,
			10000000, 100000010, 100000100, 100001000, 100010000, 100100000, 101000000, 110000000,
		} {
			for _, j := range []int32{
				-15, -9, -6, 0, 8, 15,
			} {
				timeTZOffsetSecs := int32((time.Duration(j) * time.Hour) / time.Second)
				d := tree.NewDTimeTZFromOffset(timeofday.TimeOfDay(i), timeTZOffsetSecs)
				res = append(res, d)
			}
		}
		return res
	}(),
	types.TimestampFamily: func() []tree.Datum {
		res := make([]tree.Datum, len(commonTimestampSpecials))
		for i, t := range commonTimestampSpecials {
			res[i] = tree.MustMakeDTimestamp(t, time.Microsecond)
		}
		return res
	}(),
	types.TimestampTZFamily: func() []tree.Datum {
		res := make([]tree.Datum, len(commonTimestampSpecials))
		for i, t := range commonTimestampSpecials {
			res[i] = tree.MustMakeDTimestampTZ(t, time.Microsecond)
		}
		return res
	}(),
	types.PGLSNFamily: {
		tree.NewDPGLSN(0x1000),
	},
	types.RefCursorFamily: {
		tree.NewDRefCursor("a"),
		tree.NewDRefCursor("a\n"),
		tree.NewDRefCursor("aa"),
		tree.NewDRefCursor(`Aa`),
		tree.NewDRefCursor(`aab`),
		tree.NewDRefCursor(`aaaaaa`),
		tree.NewDRefCursor("a "),
		tree.NewDRefCursor(" a"),
		tree.NewDRefCursor("	a"),
		tree.NewDRefCursor("a	"),
		tree.NewDRefCursor("a	"),
		tree.NewDRefCursor("\u0001"),
		tree.NewDRefCursor("\ufffd"),
		tree.NewDRefCursor("\u00e1"),
		tree.NewDRefCursor("À"),
		tree.NewDRefCursor("à"),
		tree.NewDRefCursor("àá"),
		tree.NewDRefCursor("À1                à\n"),
	},
	types.IntervalFamily: func() []tree.Datum {
		var res []tree.Datum
		for _, nanos := range []int64{
			0, 999, 987654000, 10000000111, 1000000000000111000,
		} {
			for _, days := range []int64{
				0, 1, 3, 5, 8,
			} {
				for _, months := range []int64{
					0, 1, 12, 200 * 12,
				} {
					d := &tree.DInterval{Duration: duration.MakeDuration(nanos, days, months)}
					res = append(res, d)
				}
			}
		}
		return res
	}(),
	types.Box2DFamily: func() []tree.Datum {
		var res []tree.Datum
		for _, lowX := range []float64{
			-1, 0.44, 10.999, 100000,
		} {
			for _, lowY := range []float64{
				-9.99, 1.1, 15,
			} {
				for _, deltas := range []struct {
					xDelta float64
					yDelta float64
				}{
					{1.234e-10, 9.234e-10},
					{1.0, 1.0},
					{10.00000001, 100.000001},
					{9.99e10, 8.88e10},
					{300, 400},
				} {
					d := &tree.DBox2D{CartesianBoundingBox: geo.CartesianBoundingBox{
						BoundingBox: geopb.BoundingBox{LoX: lowX, HiX: lowX + deltas.xDelta, LoY: lowY, HiY: lowY + deltas.yDelta}}}
					res = append(res, d)
				}
			}
		}
		return res
	}(),
	types.StringFamily: {
		tree.NewDString("a"),
		tree.NewDString("a\n"),
		tree.NewDString("aa"),
		tree.NewDString(`Aa`),
		tree.NewDString(`aab`),
		tree.NewDString(`aaaaaa`),
		tree.NewDString("a "),
		tree.NewDString(" a"),
		tree.NewDString("	a"),
		tree.NewDString("a	"),
		tree.NewDString("a	"),
		tree.NewDString("\u0001"),
		tree.NewDString("\ufffd"),
		tree.NewDString("\u00e1"),
		tree.NewDString("À"),
		tree.NewDString("à"),
		tree.NewDString("àá"),
		tree.NewDString("À1                à\n"),
	},
	types.BytesFamily: {
		tree.NewDBytes("a"),
		tree.NewDBytes("a\n"),
		tree.NewDBytes("aa"),
		tree.NewDBytes(`Aa`),
		tree.NewDBytes(`aab`),
		tree.NewDBytes(`aaaaaa`),
		tree.NewDBytes("a "),
		tree.NewDBytes(" a"),
		tree.NewDBytes("	a"),
		tree.NewDBytes("a	"),
		tree.NewDBytes("a	"),
		tree.NewDBytes("\u0001"),
		tree.NewDBytes("\ufffd"),
		tree.NewDBytes("\u00e1"),
		tree.NewDBytes("À"),
		tree.NewDBytes("à"),
		tree.NewDBytes("àá"),
		tree.NewDBytes("À1                à\n"),
	},
	types.OidFamily: {
		// TODO(msirek): Look up valid OIDs based on the type of OID. Need to
		//               match on specific type instead of just type family.
		tree.NewDOid(0),
	},
	types.UuidFamily: func() []tree.Datum {
		var res []tree.Datum
		for _, i := range []byte{
			0x01, 0x0a, 0x2b, 0x3c, 0xfe,
		} {
			for _, j := range []byte{
				0x0e, 0xa0, 0x4d, 0x5e, 0xef,
			} {
				d := tree.NewDUuid(tree.DUuid{UUID: uuid.UUID{i, j, i, j, i, j, i, j,
					i, j, i, j, i, j, i, j}})
				res = append(res, d)
			}
		}
		return res
	}(),
	types.INetFamily: func() []tree.Datum {
		var res []tree.Datum
		for _, i := range []byte{
			0x01, 0x0a, 0x2b, 0x3c, 0xfe,
		} {
			for _, j := range []byte{
				0x0e, 0xa0, 0x4d, 0x5e, 0xef,
			} {
				ipv4AddrString := fmt.Sprintf("%d.%d.%d.%d", i, j, j, i)
				ipv4Addr := tree.NewDIPAddr(tree.DIPAddr{IPAddr: ipaddr.IPAddr{Family: ipaddr.IPv4family,
					Addr: ipaddr.Addr(uint128.FromBytes(ipaddr.ParseIP(ipv4AddrString)))}})
				ipv6AddrString := fmt.Sprintf("%x:%x:%x:%x:%x:%x:%x:%x", i, j, j, i, j, i, i, j)
				ipv6Addr := tree.NewDIPAddr(tree.DIPAddr{IPAddr: ipaddr.IPAddr{Family: ipaddr.IPv6family,
					Addr: ipaddr.Addr(uint128.FromBytes(ipaddr.ParseIP(ipv6AddrString)))}})
				res = append(res, ipv4Addr)
				res = append(res, ipv6Addr)
			}
		}
		return res
	}(),
	types.JsonFamily: func() []tree.Datum {
		var res []tree.Datum
		for _, s := range []string{
			`{}`,
			`1`,
			`{"test": "json"}`,
			`{"a": 5, "b": [1, 2]}`,
			`{"a": 5, "c": [1, 2]}`,
			`[1, 2]`,
			`{"cars": [
						{"make": "Volkswagen", "model": "Rabbit", "trim": "S", "year": "2009"},
						{"make": "Toyota", "model": "Camry", "trim": "LE", "year": "2002"},
						{"make": "Ford", "model": "Focus", "trim": "SE", "year": "2011"},
						{"make": "Buick", "model": "Grand National", "trim": "T-Type", "year": "1987"},
						{"make": "Buick", "model": "Skylark", "trim": "Gran Sport", "year": "1966"},
						{"make": "Porsche", "model": "911", "trim": "Turbo S", "year": "2022"},
						{"make": "Chevrolet", "model": "Corvette", "trim": "C8", "year": "2022"}
						]}`,
		} {
			d, err := tree.ParseDJSON(s)
			if err != nil {
				panic(err)
			}
			res = append(res, d)
		}
		return res
	}(),
	types.JsonpathFamily: func() []tree.Datum {
		var res []tree.Datum
		for _, s := range []string{
			"$.a",
			"$.b[*]",
		} {
			d, err := tree.ParseDJsonpath(s)
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
			-123456789, -938456, -100, -1, 0, 1, 100, 2000, 100000, 10000000, 1000000000,
			3, 4, 5, 6, 7, 8, 9,
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
