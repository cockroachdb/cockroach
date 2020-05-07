// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

func TestHashJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := joinerTestCases()

	// Add INTERSECT ALL cases with HashJoinerSpecs.
	for _, tc := range intersectAllTestCases() {
		testCases = append(testCases, setOpTestCaseToJoinerTestCase(tc))
	}

	// Add EXCEPT ALL cases with HashJoinerSpecs.
	for _, tc := range exceptAllTestCases() {
		testCases = append(testCases, setOpTestCaseToJoinerTestCase(tc))
	}

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, storage.DefaultStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	for _, c := range testCases {
		// testFunc is a helper function that runs a hashJoin with the current
		// test case.
		// flowCtxSetup can optionally be provided to set up additional testing
		// knobs in the flowCtx before instantiating a hashJoiner and hjSetup can
		// optionally be provided to modify the hashJoiner after instantiation but
		// before Run().
		testFunc := func(t *testing.T, flowCtxSetup func(f *execinfra.FlowCtx), hjSetup func(h *hashJoiner)) error {
			side := rightSide
			for i := 0; i < 2; i++ {
				leftInput := distsqlutils.NewRowBuffer(c.leftTypes, c.leftInput, distsqlutils.RowBufferArgs{})
				rightInput := distsqlutils.NewRowBuffer(c.rightTypes, c.rightInput, distsqlutils.RowBufferArgs{})
				out := &distsqlutils.RowBuffer{}
				flowCtx := execinfra.FlowCtx{
					EvalCtx: &evalCtx,
					Cfg: &execinfra.ServerConfig{
						Settings:    st,
						TempStorage: tempEngine,
						DiskMonitor: &diskMonitor,
					},
				}
				if flowCtxSetup != nil {
					flowCtxSetup(&flowCtx)
				}
				post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: c.outCols}
				spec := &execinfrapb.HashJoinerSpec{
					LeftEqColumns:  c.leftEqCols,
					RightEqColumns: c.rightEqCols,
					Type:           c.joinType,
					OnExpr:         c.onExpr,
				}
				h, err := newHashJoiner(
					&flowCtx, 0 /* processorID */, spec, leftInput,
					rightInput, &post, out, false, /* disableTempStorage */
				)
				if err != nil {
					return err
				}
				outTypes := h.OutputTypes()
				if hjSetup != nil {
					hjSetup(h)
				}
				// Only force the other side after running the buffering logic once.
				if i == 1 {
					h.forcedStoredSide = &side
				}
				h.Run(context.Background())
				side = otherSide(h.storedSide)

				if !out.ProducerClosed() {
					return errors.New("output RowReceiver not closed")
				}

				if err := checkExpectedRows(outTypes, c.expected, out); err != nil {
					return err
				}
			}
			return nil
		}

		// Run test with a variety of initial buffer sizes.
		for _, initialBuffer := range []int64{0, 32, 64, 128, 1024 * 1024} {
			t.Run(fmt.Sprintf("InitialBuffer=%d", initialBuffer), func(t *testing.T) {
				if err := testFunc(t, nil, func(h *hashJoiner) {
					h.initialBufferSize = initialBuffer
				}); err != nil {
					t.Fatal(err)
				}
			})
		}

		// Run test with a variety of memory limits.
		for _, memLimit := range []int64{1, 256, 512, 1024, 2048} {
			t.Run(fmt.Sprintf("MemLimit=%d", memLimit), func(t *testing.T) {
				if err := testFunc(t, func(f *execinfra.FlowCtx) {
					f.Cfg.TestingKnobs.MemoryLimitBytes = memLimit
				}, nil); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestHashJoinerError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	testCases := joinerErrorTestCases()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, storage.DefaultStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	for _, c := range testCases {
		// testFunc is a helper function that runs a hashJoin with the current
		// test case after running the provided setup function.
		testFunc := func(t *testing.T, setup func(h *hashJoiner)) error {
			leftInput := distsqlutils.NewRowBuffer(c.leftTypes, c.leftInput, distsqlutils.RowBufferArgs{})
			rightInput := distsqlutils.NewRowBuffer(c.rightTypes, c.rightInput, distsqlutils.RowBufferArgs{})
			out := &distsqlutils.RowBuffer{}
			flowCtx := execinfra.FlowCtx{
				EvalCtx: &evalCtx,
				Cfg: &execinfra.ServerConfig{
					Settings:    st,
					TempStorage: tempEngine,
					DiskMonitor: &diskMonitor,
				},
			}

			post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: c.outCols}
			spec := &execinfrapb.HashJoinerSpec{
				LeftEqColumns:  c.leftEqCols,
				RightEqColumns: c.rightEqCols,
				Type:           c.joinType,
				OnExpr:         c.onExpr,
			}
			h, err := newHashJoiner(
				&flowCtx, 0 /* processorID */, spec, leftInput, rightInput,
				&post, out, false, /* disableTempStorage */
			)
			if err != nil {
				return err
			}
			outTypes := h.OutputTypes()
			setup(h)
			h.Run(context.Background())

			if !out.ProducerClosed() {
				return errors.New("output RowReceiver not closed")
			}

			return checkExpectedRows(outTypes, nil, out)
		}

		t.Run(c.description, func(t *testing.T) {
			if err := testFunc(t, func(h *hashJoiner) {
				h.initialBufferSize = 1024 * 32
			}); err == nil {
				t.Errorf("Expected an error:%s, but found nil", c.expectedErr)
			} else if err.Error() != c.expectedErr.Error() {
				t.Errorf("HashJoinerErrorTest: expected\n%s, but found\n%v", c.expectedErr, err)
			}
		})
	}
}

func checkExpectedRows(
	types []*types.T, expectedRows sqlbase.EncDatumRows, results *distsqlutils.RowBuffer,
) error {
	var expected []string
	for _, row := range expectedRows {
		expected = append(expected, row.String(types))
	}
	sort.Strings(expected)
	expStr := strings.Join(expected, "")

	var rets []string
	for {
		row, meta := results.Next()
		if meta != nil {
			return errors.Errorf("unexpected metadata: %v", meta)
		}
		if row == nil {
			break
		}
		rets = append(rets, row.String(types))
	}
	sort.Strings(rets)
	retStr := strings.Join(rets, "")

	if expStr != retStr {
		return errors.Errorf("invalid results; expected:\n   %s\ngot:\n   %s",
			expStr, retStr)
	}
	return nil
}

// TestHashJoinerDrain tests that, if the consumer starts draining, the
// hashJoiner informs the producers and drains them.
//
// Concretely, the HashJoiner is set up to read the right input fully before
// starting to produce rows, so only the left input will be asked to drain if
// the consumer is draining.
func TestHashJoinerDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}
	spec := execinfrapb.HashJoinerSpec{
		LeftEqColumns:  []uint32{0},
		RightEqColumns: []uint32{0},
		Type:           sqlbase.InnerJoin,
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
	leftInputConsumerDone := func(rb *distsqlutils.RowBuffer) {
		// Check that draining occurs before the left input has been consumed,
		// not at the end.
		// The left input started with 2 rows and 1 was consumed to find out
		// that we need to drain. So we expect 1 to be left.
		rb.Mu.Lock()
		defer rb.Mu.Unlock()
		if len(rb.Mu.Records) != 1 {
			leftInputDrainNotification <- errors.Errorf(
				"expected 1 row left, got: %d", len(rb.Mu.Records))
			return
		}
		leftInputDrainNotification <- nil
	}
	leftInput := distsqlutils.NewRowBuffer(
		sqlbase.OneIntCol,
		inputs[0],
		distsqlutils.RowBufferArgs{OnConsumerDone: leftInputConsumerDone},
	)
	rightInput := distsqlutils.NewRowBuffer(sqlbase.OneIntCol, inputs[1], distsqlutils.RowBufferArgs{})
	out := distsqlutils.NewRowBuffer(
		sqlbase.OneIntCol,
		nil, /* rows */
		distsqlutils.RowBufferArgs{AccumulateRowsWhileDraining: true},
	)

	settings := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(settings)
	ctx := context.Background()
	defer evalCtx.Stop(ctx)
	flowCtx := execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: settings},
		EvalCtx: &evalCtx,
	}

	post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: outCols}
	// Since the use of external storage overrides h.initialBufferSize, disable
	// it for this test.
	h, err := newHashJoiner(
		&flowCtx, 0 /* processorID */, &spec, leftInput, rightInput,
		&post, out, true, /* disableTempStorage */
	)
	if err != nil {
		t.Fatal(err)
	}
	// Disable initial buffering. We always store the right stream in this case.
	// If not disabled, both streams will be fully consumed before outputting
	// any rows.
	h.initialBufferSize = 0

	out.ConsumerDone()
	h.Run(context.Background())

	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}

	callbackErr := <-leftInputDrainNotification
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}

	leftInput.Mu.Lock()
	defer leftInput.Mu.Unlock()
	if len(leftInput.Mu.Records) != 0 {
		t.Fatalf("left input not drained; still %d rows in it", len(leftInput.Mu.Records))
	}

	if err := checkExpectedRows(sqlbase.OneIntCol, expected, out); err != nil {
		t.Fatal(err)
	}
}

// TestHashJoinerDrainAfterBuildPhaseError tests that, if the HashJoiner
// encounters an error in the "build phase" (reading of the right input), the
// joiner will drain both inputs.
func TestHashJoinerDrainAfterBuildPhaseError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}
	spec := execinfrapb.HashJoinerSpec{
		LeftEqColumns:  []uint32{0},
		RightEqColumns: []uint32{0},
		Type:           sqlbase.InnerJoin,
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
	leftInputConsumerDone := func(rb *distsqlutils.RowBuffer) {
		// Check that draining occurs before the left input has been consumed, not
		// at the end.
		rb.Mu.Lock()
		defer rb.Mu.Unlock()
		if len(rb.Mu.Records) != 2 {
			leftInputDrainNotification <- errors.Errorf(
				"expected 2 rows left in the left input, got: %d", len(rb.Mu.Records))
			return
		}
		leftInputDrainNotification <- nil
	}
	rightInputDrainNotification := make(chan error, 1)
	rightInputConsumerDone := func(rb *distsqlutils.RowBuffer) {
		// Check that draining occurs before the right input has been consumed, not
		// at the end.
		rb.Mu.Lock()
		defer rb.Mu.Unlock()
		if len(rb.Mu.Records) != 2 {
			rightInputDrainNotification <- errors.Errorf(
				"expected 2 rows left in the right input, got: %d", len(rb.Mu.Records))
			return
		}
		rightInputDrainNotification <- nil
	}
	rightErrorReturned := false
	rightInputNext := func(rb *distsqlutils.RowBuffer) (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
		if !rightErrorReturned {
			rightErrorReturned = true
			// The right input is going to return an error as the first thing.
			return nil, &execinfrapb.ProducerMetadata{Err: errors.Errorf("Test error. Please drain.")}
		}
		// Let RowBuffer.Next() do its usual thing.
		return nil, nil
	}
	leftInput := distsqlutils.NewRowBuffer(
		sqlbase.OneIntCol,
		inputs[0],
		distsqlutils.RowBufferArgs{OnConsumerDone: leftInputConsumerDone},
	)
	rightInput := distsqlutils.NewRowBuffer(
		sqlbase.OneIntCol,
		inputs[1],
		distsqlutils.RowBufferArgs{
			OnConsumerDone: rightInputConsumerDone,
			OnNext:         rightInputNext,
		},
	)
	out := distsqlutils.NewRowBuffer(
		sqlbase.OneIntCol,
		nil, /* rows */
		distsqlutils.RowBufferArgs{},
	)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	// Disable external storage for this test to avoid initializing temp storage
	// infrastructure.
	post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: outCols}
	h, err := newHashJoiner(
		&flowCtx, 0 /* processorID */, &spec, leftInput, rightInput,
		&post, out, true, /* disableTempStorage */
	)
	if err != nil {
		t.Fatal(err)
	}
	// Disable initial buffering. We always store the right stream in this case.
	h.initialBufferSize = 0

	h.Run(context.Background())

	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}

	callbackErr := <-leftInputDrainNotification
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}

	leftInput.Mu.Lock()
	defer leftInput.Mu.Unlock()
	if len(leftInput.Mu.Records) != 0 {
		t.Fatalf("left input not drained; still %d rows in it", len(leftInput.Mu.Records))
	}

	out.Mu.Lock()
	defer out.Mu.Unlock()
	if len(out.Mu.Records) != 1 {
		t.Fatalf("expected 1 record, got: %d", len(out.Mu.Records))
	}
	if !testutils.IsError(out.Mu.Records[0].Meta.Err, "Test error. Please drain.") {
		t.Fatalf("expected %q, got: %v", "Test error", out.Mu.Records[0].Meta.Err)
	}
}

// BenchmarkHashJoiner times how long it takes to join two tables of the same
// variable size. There is a 1:1 relationship between the rows of each table.
// TODO(asubiotto): More complex benchmarks.
func BenchmarkHashJoiner(b *testing.B) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DiskMonitor: diskMonitor,
		},
	}
	tempEngine, _, err := storage.NewTempEngine(ctx, storage.DefaultStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		b.Fatal(err)
	}
	defer tempEngine.Close()
	flowCtx.Cfg.TempStorage = tempEngine

	spec := &execinfrapb.HashJoinerSpec{
		LeftEqColumns:  []uint32{0},
		RightEqColumns: []uint32{0},
		Type:           sqlbase.InnerJoin,
		// Implicit @1 = @2 constraint.
	}
	post := &execinfrapb.PostProcessSpec{}

	const numCols = 1
	for _, spill := range []bool{true, false} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spill
		b.Run(fmt.Sprintf("spill=%t", spill), func(b *testing.B) {
			for _, numRows := range []int{0, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
				if spill && numRows < 1<<8 {
					// The benchmark takes a long time with a small number of rows and
					// spilling, since the times change wildly. Disable for now.
					continue
				}
				b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
					rows := sqlbase.MakeIntRows(numRows, numCols)
					leftInput := execinfra.NewRepeatableRowSource(sqlbase.OneIntCol, rows)
					rightInput := execinfra.NewRepeatableRowSource(sqlbase.OneIntCol, rows)
					b.SetBytes(int64(8 * numRows * numCols * 2))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						// TODO(asubiotto): Get rid of uncleared state between
						// hashJoiner Run()s to omit instantiation time from benchmarks.
						h, err := newHashJoiner(
							flowCtx, 0 /* processorID */, spec, leftInput, rightInput,
							post, &rowDisposer{}, false, /* disableTempStorage */
						)
						if err != nil {
							b.Fatal(err)
						}
						h.Run(context.Background())
						leftInput.Reset()
						rightInput.Reset()
					}
				})
			}
		})
	}
}
