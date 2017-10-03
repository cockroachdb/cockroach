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

package distsqlrun

import (
	"fmt"
	math "math"
	"sort"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func TestHashJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
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
				// Implicit @1 = @4 constraint.
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
				OnExpr:         Expression{Expr: "@3 = 9"},
			},
			outCols: []uint32{0, 1},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[1]},
					{v[2]},
					{v[3]},
					{v[5]},
					{v[6]},
					{v[7]},
				},
				{
					{v[2], v[8]},
					{v[3], v[9]},
					{v[4], v[9]},

					// Rows that match v[5].
					{v[5], v[9]},
					{v[5], v[9]},

					// Rows that match v[6] but the ON condition fails.
					{v[6], v[8]},
					{v[6], v[8]},

					// Rows that match v[7], ON condition fails for one.
					{v[7], v[8]},
					{v[7], v[9]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], null},
				{v[2], null},
				{v[3], v[3]},
				{v[5], v[5]},
				{v[5], v[5]},
				{v[6], null},
				{v[7], v[7]},
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

	ctx := context.Background()
	tempEngine, err := engine.NewTempEngine(ctx, base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	evalCtx := parser.MakeTestingEvalContext()
	defer evalCtx.Stop(ctx)
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	for _, c := range testCases {
		// testFunc is a helper function that runs a hashJoin with the current
		// test case after running the provided setup function.
		testFunc := func(t *testing.T, setup func(h *hashJoiner)) error {
			leftInput := NewRowBuffer(nil /* types */, c.inputs[0], RowBufferArgs{})
			rightInput := NewRowBuffer(nil /* types */, c.inputs[1], RowBufferArgs{})
			out := &RowBuffer{}
			flowCtx := FlowCtx{
				Settings:    cluster.MakeTestingClusterSettings(),
				EvalCtx:     evalCtx,
				TempStorage: tempEngine,
				diskMonitor: &diskMonitor,
			}

			post := PostProcessSpec{Projection: true, OutputColumns: c.outCols}
			h, err := newHashJoiner(&flowCtx, &c.spec, leftInput, rightInput, &post, out)
			if err != nil {
				return err
			}
			setup(h)
			h.Run(ctx, nil)

			if !out.ProducerClosed {
				return errors.New("output RowReceiver not closed")
			}

			return checkExpectedRows(c.expected, out)
		}

		// Run test with a variety of initial buffer sizes.
		for _, initialBuffer := range []int64{0, 32, 64, 128, 1024 * 1024} {
			t.Run(fmt.Sprintf("InitialBuffer=%d", initialBuffer), func(t *testing.T) {
				if err := testFunc(t, func(h *hashJoiner) {
					h.initialBufferSize = initialBuffer
				}); err != nil {
					t.Fatal(err)
				}
			})
		}

		// Run tests with a probability of the run failing with a memory error.
		// These verify that the hashJoiner falls back to disk correctly in all
		// cases.
		for _, memFailPoint := range []hashJoinPhase{buffer, build} {
			for i := 0; i < 5; i++ {
				t.Run(fmt.Sprintf("MemFailPoint=%s", memFailPoint), func(t *testing.T) {
					if err := testFunc(t, func(h *hashJoiner) {
						h.testingKnobMemFailPoint = memFailPoint
						h.testingKnobFailProbability = 0.5
					}); err != nil {
						t.Fatal(err)
					}
				})
			}
		}

		// Run test with a variety of memory limits.
		for _, memLimit := range []int64{1, 256, 512, 1024, 2048} {
			t.Run(fmt.Sprintf("MemLimit=%d", memLimit), func(t *testing.T) {
				if err := testFunc(t, func(h *hashJoiner) {
					h.flowCtx.testingKnobs.MemoryLimitBytes = memLimit
				}); err != nil {
					t.Fatal(err)
				}
			})
		}
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
// Concretely, the HashJoiner is set up to read the right input fully before
// starting to produce rows, so only the left input will be asked to drain if
// the consumer is draining.
func TestHashJoinerDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
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
		// Check that draining occurs before the left input has been consumed,
		// not at the end.
		// The left input started with 2 rows and 1 was consumed to find out
		// that we need to drain. So we expect 1 to be left.
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
	evalCtx := parser.MakeTestingEvalContext()
	ctx := context.Background()
	defer evalCtx.Stop(ctx)

	// Since the use of external storage overrides h.initialBufferSize, disable
	// it for this test.
	settings := cluster.MakeTestingClusterSettings()
	settingUseTempStorageJoins.Override(&settings.SV, false)
	flowCtx := FlowCtx{Settings: settings, EvalCtx: evalCtx}

	post := PostProcessSpec{Projection: true, OutputColumns: outCols}
	h, err := newHashJoiner(&flowCtx, &spec, leftInput, rightInput, &post, out)
	if err != nil {
		t.Fatal(err)
	}
	// Disable initial buffering. We always store the right stream in this case.
	// If not disabled, both streams will be fully consumed before outputting
	// any rows.
	h.initialBufferSize = 0

	out.ConsumerDone()
	h.Run(ctx, nil)

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

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
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
	evalCtx := parser.MakeTestingEvalContext()
	defer evalCtx.Stop(context.Background())
	flowCtx := FlowCtx{Settings: cluster.MakeTestingClusterSettings(), EvalCtx: evalCtx}

	post := PostProcessSpec{Projection: true, OutputColumns: outCols}
	h, err := newHashJoiner(&flowCtx, &spec, leftInput, rightInput, &post, out)
	if err != nil {
		t.Fatal(err)
	}
	// Disable initial buffering. We always store the right stream in this case.
	h.initialBufferSize = 0

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

// BenchmarkHashJoiner times how long it takes to join two tables of the same
// variable size. There is a 1:1 relationship between the rows of each table.
// TODO(asubiotto): More complex benchmarks.
func BenchmarkHashJoiner(b *testing.B) {
	ctx := context.Background()
	evalCtx := parser.MakeTestingEvalContext()
	defer evalCtx.Stop(ctx)
	flowCtx := FlowCtx{
		Settings: cluster.MakeTestingClusterSettings(),
		EvalCtx:  evalCtx,
	}

	spec := HashJoinerSpec{
		LeftEqColumns:  []uint32{0},
		RightEqColumns: []uint32{0},
		Type:           JoinType_INNER,
	}
	post := PostProcessSpec{Projection: true, OutputColumns: []uint32{0}}

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	numCols := 4
	for _, inputSize := range []int{0, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		b.Run(fmt.Sprintf("InputSize=%d", inputSize), func(b *testing.B) {
			types := make([]sqlbase.ColumnType, numCols)
			for i := 0; i < numCols; i++ {
				types[i] = columnTypeInt
			}
			// makeInputTable constructs an inputSize x numCols table where
			// input[i][j] = i + j.
			makeInputTable := func() sqlbase.EncDatumRows {
				input := make(sqlbase.EncDatumRows, inputSize)
				for i := range input {
					input[i] = make(sqlbase.EncDatumRow, numCols)
					for j := 0; j < numCols; j++ {
						input[i][j] = sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(i+j)))
					}
				}
				return input
			}
			leftInput := NewRepeatableRowSource(types, makeInputTable())
			rightInput := NewRepeatableRowSource(types, makeInputTable())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// TODO(asubiotto): Get rid of uncleared state between
				// hashJoiner Run()s to omit instantiation time from benchmarks.
				h, err := newHashJoiner(&flowCtx, &spec, leftInput, rightInput, &post, &RowDisposer{})
				if err != nil {
					b.Fatal(err)
				}
				h.Run(ctx, nil)
				leftInput.Reset()
				rightInput.Reset()
			}
		})
	}
}
