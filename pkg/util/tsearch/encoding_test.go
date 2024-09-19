// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tsearch

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
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

func TestEncodeTSQueryInvertedIndexSpans(t *testing.T) {
	testCases := []struct {
		vector   string
		query    string
		expected bool
		tight    bool
		unique   bool
	}{
		// This test uses EncodeInvertedIndexKeys and
		// GetInvertedExpr to determine whether the tsquery matches the tsvector. If
		// the vector @@ query, expected is true. Otherwise expected is false. If
		// the spans produced for contains are tight, tight is true. Otherwise tight
		// is false.
		//
		// If GetInvertedExpr produces spans that are guaranteed not to
		// contain duplicate primary keys, unique is true. Otherwise it is false.
		{`a:2`, `a`, true, true, true},
		{`b:2`, `a`, false, true, true},

		{`'foo'`, `'foo'`, true, true, true},

		{`a:2`, `a & b`, false, true, true},
		{`a:1 b:2`, `a & b`, true, true, true},

		{`a:2`, `a | b`, true, true, false},
		{`a:1 b:2`, `a | b`, true, true, false},
		{`c:1`, `a | b`, false, true, false},

		{`a:1`, `a <-> b`, false, false, true},
		{`a:1 b:2`, `a <-> b`, true, false, true},
		{`a:1 b:3`, `a <-> b`, false, false, true},

		{`a:1 b:2`, `a <-> (b|c)`, true, false, false},
		{`a:1 c:2`, `a <-> (b|c)`, true, false, false},
		{`a:1 d:2`, `a <-> (b|c)`, false, false, false},
		{`a:1 b:2`, `a <-> (!b|c)`, false, false, true},
		{`a:1 c:2`, `a <-> (!b|c)`, true, false, true},
		{`a:1 d:2`, `a <-> (!b|c)`, true, false, true},
		{`a:1 b:2`, `a <-> (b|!c)`, true, false, true},
		{`a:1 c:2`, `a <-> (b|!c)`, false, false, true},
		{`a:1 d:2`, `a <-> (b|!c)`, true, false, true},
		{`a:1 b:2`, `a <-> (!b|!c)`, true, false, true},
		{`a:1 c:2`, `a <-> (!b|!c)`, true, false, true},
		{`a:1 d:2`, `a <-> (!b|!c)`, true, false, true},
		{`a:1 b:2 c:3 d:4`, `a <-> ((b <-> c) | d)`, true, false, true},
		{`a:1 b:2 c:3 d:4`, `a <-> (b | (c <-> d))`, true, false, true},
	}

	// runTest checks that evaluating `left @@ right` using keys from
	// EncodeInvertedIndexKeys and spans from GetInvertedExpr
	// produces the expected result.
	// returns tight=true if the spans from GetInvertedExpr
	// were tight, and tight=false otherwise.
	runTest := func(left TSVector, right TSQuery, expected, expectUnique bool) (tight bool) {
		keys, err := EncodeInvertedIndexKeys(nil, left)
		assert.NoError(t, err)

		invertedExpr, err := right.GetInvertedExpr()
		assert.NoError(t, err)

		spanExpr, ok := invertedExpr.(*inverted.SpanExpression)
		assert.True(t, ok)

		if spanExpr.Unique != expectUnique {
			t.Errorf("For %s, expected unique=%v, but got %v", right, expectUnique, spanExpr.Unique)
		}

		actual, err := spanExpr.ContainsKeys(keys)
		assert.NoError(t, err)

		// There may be some false positives, so filter those out.
		if actual && !spanExpr.Tight {
			actual, err = EvalTSQuery(right, left)
			assert.NoError(t, err)
		}

		if actual != expected {
			if expected {
				t.Errorf("expected %s to match %s but it did not", left.String(), right.String())
			} else {
				t.Errorf("expected %s not to match %s but it did", left.String(), right.String())
			}
		}

		return spanExpr.Tight
	}

	// Run pre-defined test cases from above.
	for _, c := range testCases {
		indexedValue, err := ParseTSVector(c.vector)
		assert.NoError(t, err)
		query, err := ParseTSQuery(c.query)
		assert.NoError(t, err)

		// First check that evaluating `indexedValue @@ query` matches the expected
		// result.
		res, err := EvalTSQuery(query, indexedValue)
		assert.NoError(t, err)
		if res != c.expected {
			t.Fatalf(
				"expected value of %s @@ %s did not match actual value. Expected: %v. Got: %v",
				c.vector, c.query, c.expected, res,
			)
		}

		// Now check that we get the same result with the inverted index spans.
		tight := runTest(indexedValue, query, c.expected, c.unique)

		// And check that the tightness matches the expected value.
		if tight != c.tight {
			if c.tight {
				t.Errorf("expected spans for %s to be tight but they were not", c.query)
			} else {
				t.Errorf("expected spans for %s not to be tight but they were", c.query)
			}
		}
	}

	// Run a set of randomly generated test cases.
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 100; i++ {
		// Generate a random query and vector and evaluate the result of `left @@ right`.
		query := RandomTSQuery(rng)
		vector := RandomTSVector(rng)

		res, err := EvalTSQuery(query, vector)
		assert.NoError(t, err)

		invertedExpr, err := query.GetInvertedExpr()
		if err != nil {
			// We can't generate an inverted expression for this query, so there's
			// nothing to test here.
			continue
		}
		expectedUnique := invertedExpr.(*inverted.SpanExpression).Unique

		// Now check that we get the same result with the inverted index spans.
		runTest(vector, query, res, expectedUnique)
	}
}
