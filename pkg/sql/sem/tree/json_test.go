// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	jsonb "github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func BenchmarkAsJSON(b *testing.B) {
	// Use fixed seed so that each invocation of this benchmark
	// produces exactly the same types, and datums streams.
	// This number can be changed to an arbitrary value; doing so
	// would result in new types/datums being produced.
	rng := randutil.NewTestRandWithSeed(-4365865412074131521)

	const numDatums = 1024
	makeDatums := func(typ *types.T) tree.Datums {
		const allowNulls = true
		res := make(tree.Datums, numDatums)
		for i := 0; i < numDatums; i++ {
			res[i] = randgen.RandDatum(rng, typ, allowNulls)
		}
		return res
	}

	bench := func(b *testing.B, typ *types.T) {
		b.ReportAllocs()
		b.StopTimer()
		datums := makeDatums(typ)
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			_, err := tree.AsJSON(datums[i%numDatums], sessiondatapb.DataConversionConfig{}, time.UTC)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	for _, typ := range testTypes(rng) {
		b.Run(typ.String(), func(b *testing.B) {
			bench(b, typ)
		})

		if randgen.IsAllowedForArray(typ) {
			typ = types.MakeArray(typ)
			b.Run(typ.String(), func(b *testing.B) {
				bench(b, typ)
			})
		}
	}
}

func BenchmarkParseJSON(b *testing.B) {
	// Use fixed seed so that each invocation of this benchmark
	// produces exactly the same types, and datums streams.
	// This number can be changed to an arbitrary value; doing so
	// would result in new types/datums being produced.
	rng := randutil.NewTestRandWithSeed(-4365865412074131521)

	const numJSON = 4096
	makeTestData := func(opts ...jsonb.RandOption) (res []string) {
		res = make([]string, numJSON)
		for i := 0; i < numJSON; i++ {
			j, err := jsonb.RandGen(rng, opts...)
			if err != nil {
				b.Fatal(err)
			}
			res[i] = j.String()
		}
		return res
	}

	type testInput struct {
		strLen     int
		escapeProb float32
		data       []string
	}

	stringEscapeChance := []float32{0, 0.03, 0.4}
	strMaxLengths := []int{1 << 7, 1 << 9, 1 << 12, 1 << 14, 1 << 18, 1 << 19}

	start := timeutil.Now()
	benchCases := func() (res []testInput) {
		for _, escapeProb := range stringEscapeChance {
			for _, maxStrLen := range strMaxLengths {
				res = append(res, testInput{
					strLen:     maxStrLen,
					escapeProb: escapeProb,
					data:       makeTestData(jsonb.WithMaxStrLen(maxStrLen), jsonb.WithEscapeProb(escapeProb)),
				})
			}
		}
		return res
	}()
	log.Infof(context.Background(), "test data generation took %s", timeutil.Since(start))
	b.ResetTimer()

	parseOpts := []jsonb.ParseOption{jsonb.WithFastJSONParser(), jsonb.WithUnorderedObjectKeys()}

	for _, bc := range benchCases {
		b.Run(fmt.Sprintf("escape=%.02f%%/%d", 100*bc.escapeProb, bc.strLen), func(b *testing.B) {
			b.ReportAllocs()
			var totalBytes int64
			for i := 0; i < b.N; i++ {
				str := bc.data[i%numJSON]

				_, err := jsonb.ParseJSON(str, parseOpts...)
				if err != nil {
					b.Fatal(err)
				}
				totalBytes += int64(len(str))
			}
			b.SetBytes(int64(float64(totalBytes) / (float64(b.N))))
		})
	}
}

// testTypes returns list of types to test against.
func testTypes(rng *rand.Rand) (typs []*types.T) {
	for _, typ := range randgen.SeedTypes {
		switch typ {
		case types.AnyTuple:
		// Ignore AnyTuple -- it's not very interesting; we'll generate test tuples below.
		case types.RegClass, types.RegNamespace, types.RegProc, types.RegProcedure, types.RegRole, types.RegType:
		// Ignore a bunch of pseudo-OID types (just want regular OID).
		case types.Geometry, types.Geography:
		// Ignore geometry/geography: these types are insanely inefficient;
		// AsJson(Geo) -> MarshalGeo -> go JSON bytes ->  ParseJSON -> Go native -> json.JSON
		// Benchmarking this generates too much noise.
		// TODO(yevgeniy): fix this.
		default:
			typs = append(typs, typ)
		}
	}

	// Add tuple types.
	var tupleTypes []*types.T
	makeTupleType := func() *types.T {
		contents := make([]*types.T, rng.Intn(6)) // Up to 6 fields
		for i := range contents {
			contents[i] = randgen.RandTypeFromSlice(rng, typs)
		}
		candidateTuple := types.MakeTuple(contents)
		// Ensure tuple type is unique.
		for _, t := range tupleTypes {
			if t.Equal(candidateTuple) {
				return nil
			}
		}
		tupleTypes = append(tupleTypes, candidateTuple)
		return candidateTuple
	}

	const numTupleTypes = 5
	for i := 0; i < numTupleTypes; i++ {
		var typ *types.T
		for typ == nil {
			typ = makeTupleType()
		}
		typs = append(typs, typ)
	}

	return typs
}
