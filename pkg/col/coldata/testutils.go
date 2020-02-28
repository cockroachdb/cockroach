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

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
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
		typ := expectedVec.Type()
		require.Equal(t, typ, actualVec.Type())
		require.Equal(
			t,
			expectedVec.Nulls().Slice(0, expected.Length()),
			actualVec.Nulls().Slice(0, actual.Length()),
		)
		if typ == coltypes.Bytes {
			// Cannot use require.Equal for this type.
			// TODO(asubiotto): Again, why not?
			expectedBytes := expectedVec.Bytes().Window(0, expected.Length())
			resultBytes := actualVec.Bytes().Window(0, actual.Length())
			require.Equal(t, expectedBytes.Len(), resultBytes.Len())
			for i := 0; i < expectedBytes.Len(); i++ {
				if !bytes.Equal(expectedBytes.Get(i), resultBytes.Get(i)) {
					t.Fatalf("bytes mismatch at index %d:\nexpected:\n%sactual:\n%s", i, expectedBytes, resultBytes)
				}
			}
		} else if typ == coltypes.Timestamp {
			// Cannot use require.Equal for this type.
			// TODO(yuzefovich): Again, why not?
			expectedTimestamp := expectedVec.Timestamp()[0:expected.Length()]
			resultTimestamp := actualVec.Timestamp()[0:actual.Length()]
			require.Equal(t, len(expectedTimestamp), len(resultTimestamp))
			for i := range expectedTimestamp {
				if !expectedTimestamp[i].Equal(resultTimestamp[i]) {
					t.Fatalf("Timestamp mismatch at index %d:\nexpected:\n%sactual:\n%s", i, expectedTimestamp[i], resultTimestamp[i])
				}
			}
		} else if typ == coltypes.Interval {
			// Cannot use require.Equal for this type.
			// TODO(yuzefovich): Again, why not?
			expectedInterval := expectedVec.Interval()[0:expected.Length()]
			resultInterval := actualVec.Interval()[0:actual.Length()]
			require.Equal(t, len(expectedInterval), len(resultInterval))
			for i := range expectedInterval {
				if expectedInterval[i].Compare(resultInterval[i]) != 0 {
					t.Fatalf("Interval mismatch at index %d:\nexpected:\n%sactual:\n%s", i, expectedInterval[i], resultInterval[i])
				}
			}
		} else {
			require.Equal(
				t,
				expectedVec.Window(expectedVec.Type(), 0, expected.Length()),
				actualVec.Window(actualVec.Type(), 0, actual.Length()),
			)
		}
	}
}
