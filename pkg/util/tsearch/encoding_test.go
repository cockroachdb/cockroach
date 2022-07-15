// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tsearch

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
)

func TestRoundtripRandomTSVector(t *testing.T) {
	// We test TSVector encoding roundtripping in the tsvector_test.go file for
	// hardcoded test cases; this file uses a random one.
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 1000; i++ {
		v := RandomTSVector(rng)
		encoded, err := EncodeTSVector(nil, v)
		assert.NoError(t, err)
		roundtripped, err := DecodeTSVector(encoded)
		assert.NoError(t, err)
		assert.Equal(t, v.String(), roundtripped.String())
		reEncoded, err := EncodeTSVector(nil, roundtripped)
		assert.NoError(t, err)
		assert.Equal(t, encoded, reEncoded)

		encoded, err = EncodeTSVectorPGBinary(nil, v)
		assert.NoError(t, err)
		roundtripped, err = DecodeTSVectorPGBinary(encoded)
		assert.NoError(t, err)
		assert.Equal(t, v.String(), roundtripped.String())
		reEncoded, err = EncodeTSVectorPGBinary(nil, roundtripped)
		assert.NoError(t, err)
		assert.Equal(t, encoded, reEncoded)
	}
}

func TestRoundtripRandomTSQuery(t *testing.T) {
	// We test TSVector encoding roundtripping in the tsvector_test.go file for
	// hardcoded test cases; this file uses a random one.
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 1000; i++ {
		q := RandomTSQuery(rng)
		encoded, err := EncodeTSQuery(nil, q)
		assert.NoError(t, err)
		roundtripped, err := DecodeTSQuery(encoded)
		assert.NoError(t, err)
		assert.Equal(t, q.String(), roundtripped.String())
		reEncoded, err := EncodeTSQuery(nil, roundtripped)
		assert.NoError(t, err)
		assert.Equal(t, encoded, reEncoded)

		encoded = EncodeTSQueryPGBinary(nil, q)
		roundtripped, err = DecodeTSQueryPGBinary(encoded)
		assert.NoError(t, err)
		assert.Equal(t, q.String(), roundtripped.String())
		reEncoded = EncodeTSQueryPGBinary(nil, roundtripped)
		assert.Equal(t, encoded, reEncoded)
	}
}
