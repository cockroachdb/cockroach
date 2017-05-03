// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsqlrun

import (
	"math"
	"sort"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func TestHashJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(i)))
	}
	null := sqlbase.EncDatum{Datum: parser.DNull}

	testCases := []struct {
		spec     HashJoinerSpec
		outCols  []uint32
		inputs   []sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           JoinType_INNER,
				// Implicit @1 = @3 constraint.
			},
			outCols: []uint32{0, 3, 4},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], v[4]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
					{v[5], v[5]},
				},
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
		},
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           JoinType_INNER,
				// Implicit @1 = @3 constraint.
			},
			outCols: []uint32{0, 1, 3},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[0], v[1]},
				},
				{
					{v[0], v[4]},
					{v[0], v[1]},
					{v[0], v[0]},
					{v[0], v[5]},
					{v[0], v[4]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[0], v[0], v[1]},
				{v[0], v[0], v[0]},
				{v[0], v[0], v[5]},
				{v[0], v[0], v[4]},
				{v[0], v[1], v[4]},
				{v[0], v[1], v[1]},
				{v[0], v[1], v[0]},
				{v[0], v[1], v[5]},
				{v[0], v[1], v[4]},
			},
		},
		// Test that inner joins work with filter expressions.
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           JoinType_INNER,
				OnExpr:         Expression{Expr: "@4 >= 4"},
				// Implicit AND @1 = @3 constraint.
			},
			outCols: []uint32{0, 1, 3},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[0], v[1]},
					{v[1], v[0]},
					{v[1], v[1]},
				},
				{
					{v[0], v[4]},
					{v[0], v[1]},
					{v[0], v[0]},
					{v[0], v[5]},
					{v[0], v[4]},
					{v[1], v[4]},
					{v[1], v[1]},
					{v[1], v[0]},
					{v[1], v[5]},
					{v[1], v[4]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[0], v[0], v[5]},
				{v[0], v[0], v[4]},
				{v[0], v[1], v[4]},
				{v[0], v[1], v[5]},
				{v[0], v[1], v[4]},
				{v[1], v[0], v[4]},
				{v[1], v[0], v[5]},
				{v[1], v[0], v[4]},
				{v[1], v[1], v[4]},
				{v[1], v[1], v[5]},
				{v[1], v[1], v[4]},
			},
		},
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           JoinType_LEFT_OUTER,
				// Implicit @1 = @3 constraint.
			},
			outCols: []uint32{0, 3, 4},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], v[4]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
					{v[5], v[5]},
				},
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], null, null},
				{v[1], v[0], v[4]},
				{v[2], null, null},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
				{v[5], null, null},
			},
		},
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           JoinType_RIGHT_OUTER,
				// Implicit @1 = @3 constraint.
			},
			outCols: []uint32{3, 1, 2},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
				},
				{
					{v[0], v[0]},
					{v[1], v[4]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
					{v[5], v[5]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], null, null},
				{v[1], v[0], v[4]},
				{v[2], null, null},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
				{v[5], null, null},
			},
		},
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           JoinType_FULL_OUTER,
				// Implicit @1 = @3 constraint.
			},
			outCols: []uint32{0, 3, 4},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], v[4]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
				},
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
					{v[5], v[5], v[1]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], null, null},
				{v[1], v[0], v[4]},
				{v[2], null, null},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
				{null, v[5], v[1]},
			},
		},
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           JoinType_INNER,
				// Implicit @1 = @3 constraint.
			},
			outCols: []uint32{0, 3, 4},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
					{v[5], v[5]},
				},
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
		},
		// Test that left outer joins work with filters as expected.
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           JoinType_LEFT_OUTER,
				OnExpr:         Expression{Expr: "@2 > 1"},
			},
			outCols: []uint32{0, 1},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0]},
					{v[1]},
					{v[2]},
				},
				{
					{v[1]},
					{v[2]},
					{v[3]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], null},
				{v[1], null},
				{v[2], v[2]},
			},
		},
		// Test that right outer joins work with filters as expected.
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           JoinType_RIGHT_OUTER,
				OnExpr:         Expression{Expr: "@2 > 1"},
			},
			outCols: []uint32{0, 1},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0]},
					{v[1]},
					{v[2]},
				},
				{
					{v[1]},
					{v[2]},
					{v[3]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{null, v[1]},
				{v[2], v[2]},
				{null, v[3]},
			},
		},
		// Test that full outer joins work with filters as expected.
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0},
				RightEqColumns: []uint32{0},
				Type:           JoinType_FULL_OUTER,
				OnExpr:         Expression{Expr: "@2 > 1"},
			},
			outCols: []uint32{0, 1},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0]},
					{v[1]},
					{v[2]},
				},
				{
					{v[1]},
					{v[2]},
					{v[3]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], null},
				{null, v[1]},
				{v[1], null},
				{v[2], v[2]},
				{null, v[3]},
			},
		},

		// Tests for behavior when input contains NULLs.
		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0, 1},
				RightEqColumns: []uint32{0, 1},
				Type:           JoinType_INNER,
				// Implicit @1,@2 = @3,@4 constraint.
			},
			outCols: []uint32{0, 1, 2, 3, 4},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], null},
					{null, v[2]},
					{null, null},
				},
				{
					{v[0], v[0], v[4]},
					{v[1], null, v[5]},
					{null, v[2], v[6]},
					{null, null, v[7]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0], v[0], v[4]},
			},
		},

		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0, 1},
				RightEqColumns: []uint32{0, 1},
				Type:           JoinType_LEFT_OUTER,
				// Implicit @1,@2 = @3,@4 constraint.
			},
			outCols: []uint32{0, 1, 2, 3, 4},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], null},
					{null, v[2]},
					{null, null},
				},
				{
					{v[0], v[0], v[4]},
					{v[1], null, v[5]},
					{null, v[2], v[6]},
					{null, null, v[7]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0], v[0], v[4]},
				{v[1], null, null, null, null},
				{null, v[2], null, null, null},
				{null, null, null, null, null},
			},
		},

		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0, 1},
				RightEqColumns: []uint32{0, 1},
				Type:           JoinType_RIGHT_OUTER,
				// Implicit @1,@2 = @3,@4 constraint.
			},
			outCols: []uint32{0, 1, 2, 3, 4},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], null},
					{null, v[2]},
					{null, null},
				},
				{
					{v[0], v[0], v[4]},
					{v[1], null, v[5]},
					{null, v[2], v[6]},
					{null, null, v[7]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0], v[0], v[4]},
				{null, null, v[1], null, v[5]},
				{null, null, null, v[2], v[6]},
				{null, null, null, null, v[7]},
			},
		},

		{
			spec: HashJoinerSpec{
				LeftEqColumns:  []uint32{0, 1},
				RightEqColumns: []uint32{0, 1},
				Type:           JoinType_FULL_OUTER,
				// Implicit @1,@2 = @3,@4 constraint.
			},
			outCols: []uint32{0, 1, 2, 3, 4},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], null},
					{null, v[2]},
					{null, null},
				},
				{
					{v[0], v[0], v[4]},
					{v[1], null, v[5]},
					{null, v[2], v[6]},
					{null, null, v[7]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0], v[0], v[4]},
				{null, null, v[1], null, v[5]},
				{null, null, null, v[2], v[6]},
				{null, null, null, null, v[7]},
				{v[1], null, null, null, null},
				{null, v[2], null, null, null},
				{null, null, null, null, null},
			},
		},
	}

	monitor := mon.MakeUnlimitedMonitor(context.Background(), "test", nil, nil, math.MaxInt64)
	defer monitor.Stop(context.Background())
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			hs := c.spec
			leftInput := NewRowBuffer(nil /* types */, c.inputs[0], RowBufferArgs{})
			rightInput := NewRowBuffer(nil /* types */, c.inputs[1], RowBufferArgs{})
			out := &RowBuffer{}
			flowCtx := FlowCtx{evalCtx: parser.EvalContext{Mon: &monitor}}

			post := PostProcessSpec{Projection: true, OutputColumns: c.outCols}
			h, err := newHashJoiner(&flowCtx, &hs, leftInput, rightInput, &post, out)
			if err != nil {
				t.Fatal(err)
			}

			h.Run(context.Background(), nil)

			if !out.ProducerClosed {
				t.Fatalf("output RowReceiver not closed")
			}

			if err := checkExpectedRows(c.expected, out); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func checkExpectedRows(expectedRows sqlbase.EncDatumRows, results *RowBuffer) error {
	var expected []string
	for _, row := range expectedRows {
		expected = append(expected, row.String())
	}
	sort.Strings(expected)
	expStr := strings.Join(expected, "")

	var rets []string
	for {
		row, meta := results.Next()
		if !meta.Empty() {
			return errors.Errorf("unexpected metadata: %v", meta)
		}
		if row == nil {
			break
		}
		rets = append(rets, row.String())
	}
	sort.Strings(rets)
	retStr := strings.Join(rets, "")

	if expStr != retStr {
		return errors.Errorf("invalid results; expected:\n   %s\ngot:\n   %s",
			expStr, retStr)
	}
	return nil
}

// TestDrain tests that, if the consumer starts draining, the hashJoiner informs
// the producers and drains them.
//
// Concretely, the HashJoiner reads the right input fully before starting to
// produce rows, so only the left input will be ask to drain if the consumer is
// draining.
func TestHashJoinerDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	columnTypeInt := sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(i)))
	}
	spec := HashJoinerSpec{
		LeftEqColumns:  []uint32{0},
		RightEqColumns: []uint32{0},
		Type:           JoinType_INNER,
		// Implicit @1 = @2 constraint.
	}
	outCols := []uint32{0}
	inputs := []sqlbase.EncDatumRows{
		{
			{v[0]},
			{v[1]},
		},
		{
			{v[0]},
			{v[1]},
		},
	}
	expected := sqlbase.EncDatumRows{
		{v[0]},
	}
	leftInputDrainNotification := make(chan error, 1)
	leftInputConsumerDone := func(rb *RowBuffer) {
		// Check that draining occurs before the left input has been consumer, not
		// at the end.
		// The left input started with 2 rows and 1 was consumed to find out that we
		// need to drain. So we expect 1 to be left.
		rb.mu.Lock()
		defer rb.mu.Unlock()
		if len(rb.mu.records) != 1 {
			leftInputDrainNotification <- errors.Errorf(
				"expected 1 row left, got: %d", len(rb.mu.records))
			return
		}
		leftInputDrainNotification <- nil
	}
	leftInput := NewRowBuffer(
		nil, /* types */
		inputs[0],
		RowBufferArgs{OnConsumerDone: leftInputConsumerDone})
	rightInput := NewRowBuffer(nil /* types */, inputs[1], RowBufferArgs{})
	out := NewRowBuffer(
		nil /* types */, nil, /* rows */
		RowBufferArgs{AccumulateRowsWhileDraining: true})
	monitor := mon.MakeUnlimitedMonitor(context.Background(), "test", nil, nil, math.MaxInt64)
	defer monitor.Stop(context.Background())
	flowCtx := FlowCtx{evalCtx: parser.EvalContext{Mon: &monitor}}

	post := PostProcessSpec{Projection: true, OutputColumns: outCols}
	h, err := newHashJoiner(&flowCtx, &spec, leftInput, rightInput, &post, out)
	if err != nil {
		t.Fatal(err)
	}

	out.ConsumerDone()
	h.Run(context.Background(), nil)

	if !out.ProducerClosed {
		t.Fatalf("output RowReceiver not closed")
	}

	callbackErr := <-leftInputDrainNotification
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}

	leftInput.mu.Lock()
	defer leftInput.mu.Unlock()
	if len(leftInput.mu.records) != 0 {
		t.Fatalf("left input not drained; still %d rows in it", len(leftInput.mu.records))
	}

	if err := checkExpectedRows(expected, out); err != nil {
		t.Fatal(err)
	}
}

// TestHashJoinerDrainAfterBuildPhaseError tests that, if the HashJoiner
// encounters an error in the "build phase" (reading of the right input), the
// joiner will drain both inputs.
func TestHashJoinerDrainAfterBuildPhaseError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(i)))
	}
	spec := HashJoinerSpec{
		LeftEqColumns:  []uint32{0},
		RightEqColumns: []uint32{0},
		Type:           JoinType_INNER,
		// Implicit @1 = @2 constraint.
	}
	outCols := []uint32{0}
	inputs := []sqlbase.EncDatumRows{
		{
			{v[0]},
			{v[1]},
		},
		{
			{v[0]},
			{v[1]},
		},
	}
	leftInputDrainNotification := make(chan error, 1)
	leftInputConsumerDone := func(rb *RowBuffer) {
		// Check that draining occurs before the left input has been consumed, not
		// at the end.
		rb.mu.Lock()
		defer rb.mu.Unlock()
		if len(rb.mu.records) != 2 {
			leftInputDrainNotification <- errors.Errorf(
				"expected 2 rows left in the left input, got: %d", len(rb.mu.records))
			return
		}
		leftInputDrainNotification <- nil
	}
	rightInputDrainNotification := make(chan error, 1)
	rightInputConsumerDone := func(rb *RowBuffer) {
		// Check that draining occurs before the right input has been consumed, not
		// at the end.
		rb.mu.Lock()
		defer rb.mu.Unlock()
		if len(rb.mu.records) != 2 {
			rightInputDrainNotification <- errors.Errorf(
				"expected 2 rows left in the right input, got: %d", len(rb.mu.records))
			return
		}
		rightInputDrainNotification <- nil
	}
	rightErrorReturned := false
	rightInputNext := func(rb *RowBuffer) (sqlbase.EncDatumRow, ProducerMetadata) {
		if !rightErrorReturned {
			rightErrorReturned = true
			// The right input is going to return an error as the first thing.
			return nil, ProducerMetadata{Err: errors.Errorf("Test error. Please drain.")}
		}
		// Let RowBuffer.Next() do its usual thing.
		return nil, ProducerMetadata{}
	}
	leftInput := NewRowBuffer(
		nil, /* types */
		inputs[0],
		RowBufferArgs{OnConsumerDone: leftInputConsumerDone})
	rightInput := NewRowBuffer(
		nil /* types */, inputs[1],
		RowBufferArgs{
			OnConsumerDone: rightInputConsumerDone,
			OnNext:         rightInputNext})
	out := NewRowBuffer(
		nil /* types */, nil, /* rows */
		RowBufferArgs{})
	monitor := mon.MakeUnlimitedMonitor(context.Background(), "test", nil, nil, math.MaxInt64)
	defer monitor.Stop(context.Background())
	flowCtx := FlowCtx{evalCtx: parser.EvalContext{Mon: &monitor}}

	post := PostProcessSpec{Projection: true, OutputColumns: outCols}
	h, err := newHashJoiner(&flowCtx, &spec, leftInput, rightInput, &post, out)
	if err != nil {
		t.Fatal(err)
	}

	h.Run(context.Background(), nil)

	if !out.ProducerClosed {
		t.Fatalf("output RowReceiver not closed")
	}

	callbackErr := <-leftInputDrainNotification
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}

	leftInput.mu.Lock()
	defer leftInput.mu.Unlock()
	if len(leftInput.mu.records) != 0 {
		t.Fatalf("left input not drained; still %d rows in it", len(leftInput.mu.records))
	}

	out.mu.Lock()
	defer out.mu.Unlock()
	if len(out.mu.records) != 1 {
		t.Fatalf("expected 1 record, got: %d", len(out.mu.records))
	}
	if !testutils.IsError(out.mu.records[0].Meta.Err, "Test error. Please drain.") {
		t.Fatalf("expected %q, got: %v", "Test error", out.mu.records[0].Meta.Err)
	}
}
