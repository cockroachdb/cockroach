// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// maxVarLen specifies a length limit for variable length types (e.g. byte slices).
const maxVarLen = 64

var locations []*time.Location

func init() {
	// Load some random time zones.
	for _, locationName := range []string{
		"Africa/Addis_Ababa",
		"America/Anchorage",
		"Antarctica/Davis",
		"Asia/Ashkhabad",
		"Australia/Sydney",
		"Europe/Minsk",
		"Pacific/Palau",
	} {
		loc, err := timeutil.LoadLocation(locationName)
		if err == nil {
			locations = append(locations, loc)
		}
	}
}

// RandomVec populates vec with n random values of typ, setting each value to
// null with a probability of nullProbability. It is assumed that n is in bounds
// of the given vec.
// bytesFixedLength (when greater than zero) specifies the fixed length of the
// bytes slice to be generated. It is used only if typ == coltypes.Bytes.
func RandomVec(
	rng *rand.Rand, typ coltypes.T, bytesFixedLength int, vec Vec, n int, nullProbability float64,
) {
	switch typ {
	case coltypes.Bool:
		bools := vec.Bool()
		for i := 0; i < n; i++ {
			if rng.Float64() < 0.5 {
				bools[i] = true
			} else {
				bools[i] = false
			}
		}
	case coltypes.Bytes:
		bytes := vec.Bytes()
		for i := 0; i < n; i++ {
			bytesLen := bytesFixedLength
			if bytesLen <= 0 {
				bytesLen = rng.Intn(maxVarLen)
			}
			randBytes := make([]byte, bytesLen)
			// Read always returns len(bytes[i]) and nil.
			_, _ = rand.Read(randBytes)
			bytes.Set(i, randBytes)
		}
	case coltypes.Decimal:
		decs := vec.Decimal()
		for i := 0; i < n; i++ {
			// int64(rng.Uint64()) to get negative numbers, too
			decs[i].SetFinite(int64(rng.Uint64()), int32(rng.Intn(40)-20))
		}
	case coltypes.Int16:
		ints := vec.Int16()
		for i := 0; i < n; i++ {
			ints[i] = int16(rng.Uint64())
		}
	case coltypes.Int32:
		ints := vec.Int32()
		for i := 0; i < n; i++ {
			ints[i] = int32(rng.Uint64())
		}
	case coltypes.Int64:
		ints := vec.Int64()
		for i := 0; i < n; i++ {
			ints[i] = int64(rng.Uint64())
		}
	case coltypes.Float64:
		floats := vec.Float64()
		for i := 0; i < n; i++ {
			floats[i] = rng.Float64()
		}
	case coltypes.Timestamp:
		timestamps := vec.Timestamp()
		for i := 0; i < n; i++ {
			timestamps[i] = timeutil.Unix(rng.Int63n(1000000), rng.Int63n(1000000))
			loc := locations[rng.Intn(len(locations))]
			timestamps[i] = timestamps[i].In(loc)
		}
	case coltypes.Interval:
		intervals := vec.Interval()
		for i := 0; i < n; i++ {
			intervals[i] = duration.FromFloat64(rng.Float64())
		}
	default:
		panic(fmt.Sprintf("unhandled type %s", typ))
	}
	vec.Nulls().UnsetNulls()
	if nullProbability == 0 {
		return
	}

	for i := 0; i < n; i++ {
		if rng.Float64() < nullProbability {
			vec.Nulls().SetNull(i)
		}
	}
}
