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
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

// testingT is a private interface that mirrors the testing.TB methods used.
// testing.TB cannot be used directly since testing is an illegal import.
// TODO(asubiotto): Remove AssertEquivalentBatches' dependency on testing.TB by
//  checking for equality and returning a diff string instead of operating on
//  testing.TB.
type testingT interface {
	Helper()
	Errorf(format string, args ...interface{})
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	FailNow()
}

// AssertEquivalentBatches is a testing function that asserts that expected and
// actual are equivalent.
func AssertEquivalentBatches(t testingT, expected, actual Batch) {
	t.Helper()

	if actual.Selection() != nil {
		t.Fatal("violated invariant that batches have no selection vectors")
	}
	require.Equal(t, expected.Length(), actual.Length())
	if expected.Length() == 0 {
		// The schema of a zero-length batch is undefined, so the rest of the check
		// is not required.
		return
	}
	require.Equal(t, expected.Width(), actual.Width())
	for colIdx := 0; colIdx < expected.Width(); colIdx++ {
		// Verify equality of ColVecs (this includes nulls). Since the coldata.Vec
		// backing array is always of coldata.BatchSize() due to the scratch batch
		// that the converter keeps around, the coldata.Vec needs to be sliced to
		// the first length elements to match on length, otherwise the check will
		// fail.
		expectedVec := expected.ColVec(colIdx)
		actualVec := actual.ColVec(colIdx)
		require.Equal(t, expectedVec.Type(), actualVec.Type())
		// Check whether the nulls bitmaps are the same. Note that we don't
		// track precisely the fact whether nulls are present or not in
		// 'maybeHasNulls' field, so we override it manually to be 'true' for
		// both nulls vectors if it is 'true' for at least one of them. This is
		// acceptable since we still check the bitmaps precisely.
		expectedNulls := expectedVec.Nulls()
		actualNulls := actualVec.Nulls()
		oldExpMaybeHasNulls, oldActMaybeHasNulls := expectedNulls.maybeHasNulls, actualNulls.maybeHasNulls
		defer func() {
			expectedNulls.maybeHasNulls, actualNulls.maybeHasNulls = oldExpMaybeHasNulls, oldActMaybeHasNulls
		}()
		expectedNulls.maybeHasNulls = expectedNulls.maybeHasNulls || actualNulls.maybeHasNulls
		actualNulls.maybeHasNulls = expectedNulls.maybeHasNulls || actualNulls.maybeHasNulls
		require.Equal(t, expectedNulls.Slice(0, expected.Length()), actualNulls.Slice(0, actual.Length()))

		canonicalTypeFamily := expectedVec.CanonicalTypeFamily()
		if canonicalTypeFamily == types.BytesFamily {
			expectedBytes := expectedVec.Bytes().Window(0, expected.Length())
			resultBytes := actualVec.Bytes().Window(0, actual.Length())
			require.Equal(t, expectedBytes.Len(), resultBytes.Len())
			for i := 0; i < expectedBytes.Len(); i++ {
				if !expectedNulls.NullAt(i) {
					if !bytes.Equal(expectedBytes.Get(i), resultBytes.Get(i)) {
						t.Fatalf("bytes mismatch at index %d:\nexpected:\n%s\nactual:\n%s", i, expectedBytes, resultBytes)
					}
				}
			}
		} else if canonicalTypeFamily == types.DecimalFamily {
			expectedDecimal := expectedVec.Decimal()[0:expected.Length()]
			resultDecimal := actualVec.Decimal()[0:actual.Length()]
			require.Equal(t, len(expectedDecimal), len(resultDecimal))
			for i := range expectedDecimal {
				if !expectedNulls.NullAt(i) {
					if expectedDecimal[i].Cmp(&resultDecimal[i]) != 0 {
						t.Fatalf("Decimal mismatch at index %d:\nexpected:\n%s\nactual:\n%s", i, &expectedDecimal[i], &resultDecimal[i])
					}
				}
			}
		} else if canonicalTypeFamily == types.TimestampTZFamily {
			expectedTimestamp := expectedVec.Timestamp()[0:expected.Length()]
			resultTimestamp := actualVec.Timestamp()[0:actual.Length()]
			require.Equal(t, len(expectedTimestamp), len(resultTimestamp))
			for i := range expectedTimestamp {
				if !expectedNulls.NullAt(i) {
					if !expectedTimestamp[i].Equal(resultTimestamp[i]) {
						t.Fatalf("Timestamp mismatch at index %d:\nexpected:\n%s\nactual:\n%s", i, expectedTimestamp[i], resultTimestamp[i])
					}
				}
			}
		} else if canonicalTypeFamily == types.IntervalFamily {
			expectedInterval := expectedVec.Interval()[0:expected.Length()]
			resultInterval := actualVec.Interval()[0:actual.Length()]
			require.Equal(t, len(expectedInterval), len(resultInterval))
			for i := range expectedInterval {
				if !expectedNulls.NullAt(i) {
					if expectedInterval[i].Compare(resultInterval[i]) != 0 {
						t.Fatalf("Interval mismatch at index %d:\nexpected:\n%s\nactual:\n%s", i, expectedInterval[i], resultInterval[i])
					}
				}
			}
		} else if canonicalTypeFamily == types.JsonFamily {
			expectedJSON := expectedVec.JSON().Window(0, expected.Length())
			resultJSON := actualVec.JSON().Window(0, actual.Length())
			require.Equal(t, expectedJSON.Len(), resultJSON.Len())
			for i := 0; i < expectedJSON.Len(); i++ {
				if !expectedNulls.NullAt(i) {
					cmp, err := expectedJSON.Get(i).Compare(resultJSON.Get(i))
					if err != nil {
						t.Fatal(err)
					}
					if cmp != 0 {
						t.Fatalf("json mismatch at index %d:\nexpected:\n%s\nactual:\n%s", i, expectedJSON, resultJSON)
					}
				}
			}
		} else if canonicalTypeFamily == typeconv.DatumVecCanonicalTypeFamily {
			expectedDatum := expectedVec.Datum().Window(0 /* start */, expected.Length())
			resultDatum := actualVec.Datum().Window(0 /* start */, actual.Length())
			require.Equal(t, expectedDatum.Len(), resultDatum.Len())
			for i := 0; i < expectedDatum.Len(); i++ {
				if !expectedNulls.NullAt(i) {
					expected := expectedDatum.Get(i).(fmt.Stringer).String()
					actual := resultDatum.Get(i).(fmt.Stringer).String()
					if expected != actual {
						t.Fatalf("Datum mismatch at index %d:\nexpected:\n%s\nactual:\n%s", i, expectedDatum.Get(i), resultDatum.Get(i))
					}
				}
			}
		} else {
			require.Equal(
				t,
				expectedVec.Window(0, expected.Length()),
				actualVec.Window(0, actual.Length()),
			)
		}
	}
}
