// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecutils

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestBoolVecToSelOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tcs := []struct {
		boolCol  uint32
		tuples   colexectestutils.Tuples
		expected colexectestutils.Tuples
	}{
		{
			boolCol:  0,
			tuples:   colexectestutils.Tuples{{true}, {false}, {true}},
			expected: colexectestutils.Tuples{{true}, {true}},
		},
	}
	for _, tc := range tcs {
		colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, tc.expected, colexectestutils.OrderedVerifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
			return NewBoolVecToSelOp(input[0], 0), nil
		})
	}
}
