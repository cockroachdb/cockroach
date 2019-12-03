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
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"math/rand"
	"sort"
	"time"
	"unicode"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
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
	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
)

// This file contains utility functions for tests (in other packages).

// GetTableDescriptor retrieves a table descriptor directly from the KV layer.
func GetTableDescriptor(kvDB *client.DB, database string, table string) *TableDescriptor {
	// log.VEventf(context.TODO(), 2, "GetTableDescriptor %q %q", database, table)
	// testutil, so we pass settings as nil for both database and table name keys.
	dKey := NewDatabaseKey(database)
	ctx := context.TODO()
	gr, err := kvDB.Get(ctx, dKey.Key())
	if err != nil {
		panic(err)
	}
	if !gr.Exists() {
		panic("database missing")
	}
	dbDescID := ID(gr.ValueInt())

	tKey := NewPublicTableKey(dbDescID, table)
	gr, err = kvDB.Get(ctx, tKey.Key())
	if err != nil {
		panic(err)
	}
	if !gr.Exists() {
		panic("table missing")
	}

	descKey := MakeDescMetadataKey(ID(gr.ValueInt()))
	desc := &Descriptor{}
	ts, err := kvDB.GetProtoTs(ctx, descKey, desc)
	if err != nil || (*desc == Descriptor{}) {
		log.Fatalf(ctx, "proto with id %d missing. err: %v", gr.ValueInt(), err)
	}
	tableDesc := desc.Table(ts)
	if tableDesc == nil {
		return nil
	}
	err = tableDesc.MaybeFillInDescriptor(ctx, kvDB)
	if err != nil {
		log.Fatalf(ctx, "failure to fill in descriptor. err: %v", err)
	}
	return tableDesc
}

// GetImmutableTableDescriptor retrieves an immutable table descriptor directly from the KV layer.
func GetImmutableTableDescriptor(
	kvDB *client.DB, database string, table string,
) *ImmutableTableDescriptor {
	return NewImmutableTableDescriptor(*GetTableDescriptor(kvDB, database, table))
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
		return tree.MakeDTime(timeofday.Random(rng))
	case types.TimeTZFamily:
		return tree.NewDTimeTZFromOffset(
			timeofday.Random(rng),
			// We cannot randomize seconds, because lib/pq does NOT print the
			// second offsets making some tests break when comparing
			// results in == results out using string comparison.
			(rng.Int31n(28*60+59)-(14*60+59))*60,
		)
	case types.TimestampFamily:
		return tree.MakeDTimestamp(timeutil.Unix(rng.Int63n(1000000), rng.Int63n(1000000)), time.Microsecond)
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
			tuple.D[i] = RandDatum(rng, &typ.TupleContents()[i], true)
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
		return &tree.DTimestampTZ{Time: timeutil.Unix(rng.Int63n(1000000), rng.Int63n(1000000))}
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
		return tree.NewDCollatedString(buf.String(), typ.Locale(), &tree.CollationEnvironment{})
	case types.OidFamily:
		return tree.NewDOid(tree.DInt(rng.Uint32()))
	case types.UnknownFamily:
		return tree.DNull
	case types.ArrayFamily:
		contents := typ.ArrayContents()
		if contents.Family() == types.AnyFamily {
			contents = RandArrayContentsType(rng)
		}
		arr := tree.NewDArray(contents)
		for i := 0; i < rng.Intn(10); i++ {
			if err := arr.Append(RandDatumWithNullChance(rng, contents, 0)); err != nil {
				panic(err)
			}
		}
		return arr
	case types.AnyFamily:
		return RandDatumWithNullChance(rng, RandType(rng), nullChance)
	default:
		panic(fmt.Sprintf("invalid type %v", typ.DebugString()))
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
		datum = tree.MakeDTimestamp(time.Date(2000, 1, 1, rng.Intn(simpleRange), 0, 0, 0, time.UTC), time.Microsecond)
	case types.TimestampTZFamily:
		datum = tree.MakeDTimestampTZ(time.Date(2000, 1, 1, rng.Intn(simpleRange), 0, 0, 0, time.UTC), time.Microsecond)
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

var (
	// randInterestingDatums is a collection of interesting datums that can be
	// used for random testing.
	randInterestingDatums = map[types.Family][]tree.Datum{
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
		},
		types.TimestampFamily: func() []tree.Datum {
			res := make([]tree.Datum, len(randTimestampSpecials))
			for i, t := range randTimestampSpecials {
				res[i] = tree.MakeDTimestamp(t, time.Microsecond)
			}
			return res
		}(),
		types.TimestampTZFamily: func() []tree.Datum {
			res := make([]tree.Datum, len(randTimestampSpecials))
			for i, t := range randTimestampSpecials {
				res[i] = tree.MakeDTimestampTZ(t, time.Microsecond)
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
	}
	randTimestampSpecials = []time.Time{
		{},
		time.Date(-2000, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC),
	}
)

var (
	// seedTypes includes the following types that form the basis of randomly
	// generated types:
	//   - All scalar types, except UNKNOWN and ANY
	//   - ARRAY of ANY, where the ANY will be replaced with one of the legal
	//     array element types in RandType
	//   - OIDVECTOR and INT2VECTOR types
	seedTypes []*types.T

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
			seedTypes = append(seedTypes, typ)
		default:
			// Only include scalar types.
			if typ.Family() != types.ArrayFamily {
				seedTypes = append(seedTypes, typ)
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
	sort.Slice(seedTypes, func(i, j int) bool {
		return seedTypes[i].String() < seedTypes[j].String()
	})
	sort.Slice(arrayContentsTypes, func(i, j int) bool {
		return arrayContentsTypes[i].String() < arrayContentsTypes[j].String()
	})
}

// randInterestingDatum returns an interesting Datum of type typ. If there are
// no such Datums, it returns nil. Note that it pays attention to the width of
// the requested type for Int and Float type families.
func randInterestingDatum(rng *rand.Rand, typ *types.T) tree.Datum {
	specials := randInterestingDatums[typ.Family()]
	if len(specials) == 0 {
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
	return randType(rng, seedTypes)
}

// RandScalarType returns a random type value that is not an array or tuple.
func RandScalarType(rng *rand.Rand) *types.T {
	return randType(rng, types.Scalar)
}

// RandArrayContentsType returns a random type that's guaranteed to be valid to
// use as the contents of an array.
func RandArrayContentsType(rng *rand.Rand) *types.T {
	return randType(rng, arrayContentsTypes)
}

func randType(rng *rand.Rand, typs []*types.T) *types.T {
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
		contents := make([]types.T, len)
		for i := range contents {
			contents[i] = *RandType(rng)
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

// RandColumnTypes returns a slice of numCols random types. These types must be
// legal table column types.
func RandColumnTypes(rng *rand.Rand, numCols int) []types.T {
	types := make([]types.T, numCols)
	for i := range types {
		types[i] = *RandColumnType(rng)
	}
	return types
}

// RandSortingType returns a column type which can be key-encoded.
func RandSortingType(rng *rand.Rand) *types.T {
	typ := RandType(rng)
	for MustBeValueEncoded(typ.Family()) {
		typ = RandType(rng)
	}
	return typ
}

// RandSortingTypes returns a slice of numCols random ColumnType values
// which are key-encodable.
func RandSortingTypes(rng *rand.Rand, numCols int) []types.T {
	types := make([]types.T, numCols)
	for i := range types {
		types[i] = *RandSortingType(rng)
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
				if !isEncodableType(&t.TupleContents()[i]) {
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
func RandEncodableColumnTypes(rng *rand.Rand, numCols int) []types.T {
	types := make([]types.T, numCols)
	for i := range types {
		for {
			types[i] = *RandEncodableType(rng)
			if err := ValidateColumnDefType(&types[i]); err == nil {
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
) ([][]EncDatum, []types.T) {
	vals := make([][]EncDatum, numSets)
	types := make([]types.T, numSets)
	for i := range vals {
		val, typ := RandSortingEncDatumSlice(rng, numValsPerSet)
		vals[i], types[i] = val, *typ
	}
	return vals, types
}

// RandEncDatumRowOfTypes generates a slice of random EncDatum values for the
// corresponding type in types.
func RandEncDatumRowOfTypes(rng *rand.Rand, types []types.T) EncDatumRow {
	vals := make([]EncDatum, len(types))
	for i := range types {
		vals[i] = DatumToEncDatum(&types[i], RandDatum(rng, &types[i], true))
	}
	return vals
}

// RandEncDatumRows generates EncDatumRows where all rows follow the same random
// []ColumnType structure.
func RandEncDatumRows(rng *rand.Rand, numRows, numCols int) (EncDatumRows, []types.T) {
	types := RandEncodableColumnTypes(rng, numCols)
	return RandEncDatumRowsOfTypes(rng, numRows, types), types
}

// RandEncDatumRowsOfTypes generates EncDatumRows, each row with values of the
// corresponding type in types.
func RandEncDatumRowsOfTypes(rng *rand.Rand, numRows int, types []types.T) EncDatumRows {
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

	keyPrefix := MakeIndexKeyPrefix(desc, index.ID)
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
	for i := 1; i <= num; i++ {
		t := RandCreateTable(rng, prefix, i)
		tables[i-1] = t
	}

	for _, m := range mutators {
		tables, _ = m.Mutate(rng, tables)
	}

	return tables
}

// RandCreateTable creates a random CreateTable definition.
func RandCreateTable(rng *rand.Rand, prefix string, tableIdx int) *tree.CreateTable {
	// columnDefs contains the list of Columns we'll add to our table.
	columnDefs := make([]*tree.ColumnTableDef, randutil.RandIntInRange(rng, 1, 20))
	// defs contains the list of Columns and other attributes (indexes, column
	// families, etc) we'll add to our table.
	defs := make(tree.TableDefs, len(columnDefs))

	for i := range columnDefs {
		columnDef := randColumnTableDef(rng, i)
		columnDefs[i] = columnDef
		defs[i] = columnDef
	}

	// Shuffle our column definitions.
	rng.Shuffle(len(columnDefs), func(i, j int) {
		columnDefs[i], columnDefs[j] = columnDefs[j], columnDefs[i]
	})

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
		Table: tree.MakeUnqualifiedTableName(tree.Name(fmt.Sprintf("%s%d", prefix, tableIdx))),
		Defs:  defs,
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
	isPKCol := map[tree.Name]bool{}
	for _, def := range ast.Defs {
		switch def := def.(type) {
		case *tree.FamilyTableDef:
			return false
		case *tree.ColumnTableDef:
			if def.HasColumnFamily() {
				return false
			}
			// Primary keys must be in the first
			// column family, so don't add them to
			// the list.
			if def.PrimaryKey.IsPrimaryKey {
				continue
			}
			columns = append(columns, def.Name)
		case *tree.UniqueConstraintTableDef:
			// If there's an explicit PK index
			// definition, save the columns from it
			// and remove them later.
			if def.PrimaryKey {
				for _, col := range def.Columns {
					isPKCol[col.Column] = true
				}
			}
		}
	}

	if len(columns) <= 1 {
		return false
	}

	// Any columns not specified in column families
	// are auto assigned to the first family, so
	// there's no requirement to exhaust columns here.

	// Remove columns specified in PK index
	// definitions. We need to do this here because
	// index defs and columns can appear in any
	// order in the CREATE TABLE.
	{
		n := 0
		for _, x := range columns {
			if !isPKCol[x] {
				columns[n] = x
				n++
			}
		}
		columns = columns[:n]
	}
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
			tableInfo, ok := tables[ast.Table.TableName]
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
			tables[ast.Table.TableName] = tableInfo
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
func randColumnTableDef(rand *rand.Rand, colIdx int) *tree.ColumnTableDef {
	columnDef := &tree.ColumnTableDef{
		Name: tree.Name(fmt.Sprintf("col%d", colIdx)),
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
		semType := cols[i].Type.Family()
		if MustBeValueEncoded(semType) {
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
	OneIntCol = []types.T{*types.Int}
	// TwoIntCols is a slice of two IntTypes.
	TwoIntCols = []types.T{*types.Int, *types.Int}
	// ThreeIntCols is a slice of three IntTypes.
	ThreeIntCols = []types.T{*types.Int, *types.Int, *types.Int}
	// FourIntCols is a slice of four IntTypes.
	FourIntCols = []types.T{*types.Int, *types.Int, *types.Int, *types.Int}
)

// MakeIntCols makes a slice of numCols IntTypes.
func MakeIntCols(numCols int) []types.T {
	ret := make([]types.T, numCols)
	for i := 0; i < numCols; i++ {
		ret[i] = *types.Int
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
