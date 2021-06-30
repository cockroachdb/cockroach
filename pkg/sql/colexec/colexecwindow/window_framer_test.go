// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecwindow

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const probabilityOfNewNumber = 0.5
const orderColIdx = 0
const peersColIdx = 1

// TestWindowFramer runs the various windowFramer operators against the
// WindowFrameRunner used by the row engine.
func TestWindowFramer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewPseudoRand()
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	memAcc := testMemMonitor.MakeBoundAccount()
	defer memAcc.Close(evalCtx.Ctx())

	factory := coldataext.NewExtendedColumnFactory(evalCtx)
	allocator := colmem.NewAllocator(evalCtx.Ctx(), &memAcc, factory)
	queueCfg.CacheMode = colcontainer.DiskQueueCacheModeClearAndReuseCache
	queueCfg.SetDefaultBufferSizeBytesForCacheMode()

	testCfg := &testConfig{
		rng:       rng,
		evalCtx:   evalCtx,
		factory:   factory,
		allocator: allocator,
		queueCfg:  queueCfg,
	}

	const randTypeProbability = 0.5
	var randTypes = []*types.T{
		types.Float, types.Decimal, types.Interval, types.TimestampTZ, types.Date, types.TimeTZ,
	}

	const randExcludeProbability = 0.5
	var randExclusions = []tree.WindowFrameExclusion{
		tree.ExcludeCurrentRow, tree.ExcludeGroup, tree.ExcludeTies,
	}

	for _, count := range []int{1, 17, 42, 91} {
		testCfg.count = count
		for _, asc := range []bool{true, false} {
			typ := types.Int
			if rng.Float64() < randTypeProbability {
				typ = randTypes[rng.Intn(len(randTypes))]
			}
			exclusion := tree.NoExclusion
			if rng.Float64() < randExcludeProbability {
				exclusion = randExclusions[rng.Intn(len(randExclusions))]
			}
			testCfg.asc = asc
			testCfg.typ = typ
			testCfg.exclusion = exclusion
			for _, mode := range []tree.WindowFrameMode{tree.ROWS, tree.GROUPS, tree.RANGE} {
				testCfg.mode = mode
				for _, bound := range []tree.WindowFrameBoundType{
					tree.UnboundedPreceding, tree.OffsetPreceding, tree.CurrentRow,
					tree.OffsetFollowing, tree.UnboundedFollowing,
				} {
					if validForStart(bound) {
						testCfg.startBound = bound
						testCfg.endBound = getEndBound(rng, bound)
						testWindowFramer(t, testCfg)
					}
					if validForEnd(bound) {
						testCfg.startBound = getStartBound(rng, bound)
						testCfg.endBound = bound
						testWindowFramer(t, testCfg)
					}
				}
			}
		}
	}
}

type testConfig struct {
	rng        *rand.Rand
	evalCtx    *tree.EvalContext
	factory    coldata.ColumnFactory
	allocator  *colmem.Allocator
	queueCfg   colcontainer.DiskQueueCfg
	typ        *types.T
	count      int
	asc        bool
	mode       tree.WindowFrameMode
	startBound tree.WindowFrameBoundType
	endBound   tree.WindowFrameBoundType
	exclusion  tree.WindowFrameExclusion
}

func testWindowFramer(t *testing.T, testCfg *testConfig) {
	errorMsg := func(assertion string) string {
		return fmt.Sprintf("Assertion: %s\nOrder Column Type: %v\nRow Count: %d\n"+
			"Ascending: %v\nMode: %s\nStart Bound: %s\nEnd Bound: %s\nExclude: %s\n",
			assertion, testCfg.typ, testCfg.count, testCfg.asc, testCfg.mode,
			testCfg.startBound, testCfg.endBound, testCfg.exclusion)
	}

	colWindowFramer, rowWindowFramer, partition := initWindowFramers(t, testCfg)

	for i := 0; i < testCfg.count; i++ {
		// Advance the columnar framer.
		colWindowFramer.next(testCfg.evalCtx.Ctx())

		// Advance the row-wise framer.
		batch, batchIdx := partition.GetBatchWithTuple(testCfg.evalCtx.Ctx(), i)
		if i > 0 && batch.ColVec(peersColIdx).Bool()[batchIdx] {
			rowWindowFramer.CurRowPeerGroupNum++
			err := rowWindowFramer.PeerHelper.Update(rowWindowFramer)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}
		rowWindowFramer.RowIdx = i
		rowStartIdx, err := rowWindowFramer.FrameStartIdx(testCfg.evalCtx.Ctx(), testCfg.evalCtx)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		rowEndIdx, err := rowWindowFramer.FrameEndIdx(testCfg.evalCtx.Ctx(), testCfg.evalCtx)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Validate that the columnar window framer describes the same window
		// frame as the row-wise one.
		var colFrameIdx, firstIdx, lastIdx int
		var foundRow bool
		for j := rowStartIdx; j < rowEndIdx; j++ {
			skipped, err := rowWindowFramer.IsRowSkipped(testCfg.evalCtx.Ctx(), j)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if skipped {
				continue
			}
			if !foundRow {
				firstIdx = j
				foundRow = true
			}
			lastIdx = j
			colFrameIdx++
			require.Equal(t, j, colWindowFramer.frameNthIdx(colFrameIdx), errorMsg("NthIdx"))
		}
		if !foundRow {
			// The columnar window framer functions return -1 to signify an empty
			// window frame.
			require.Equal(t, -1, colWindowFramer.frameFirstIdx(), errorMsg("No FirstIdx"))
			require.Equal(t, -1, colWindowFramer.frameLastIdx(), errorMsg("No LastIdx"))
			require.Equal(t, -1, colWindowFramer.frameNthIdx(1 /* n */), errorMsg("No NthIdx"))
			continue
		}
		require.Equal(t, firstIdx, colWindowFramer.frameFirstIdx(), errorMsg("FirstIdx"))
		require.Equal(t, lastIdx, colWindowFramer.frameLastIdx(), errorMsg("LastIdx"))
	}

	if err := partition.Close(testCfg.evalCtx.Ctx()); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func validForStart(bound tree.WindowFrameBoundType) bool {
	return bound != tree.UnboundedFollowing
}

func validForEnd(bound tree.WindowFrameBoundType) bool {
	return bound != tree.UnboundedPreceding
}

func getStartBound(rng *rand.Rand, endBound tree.WindowFrameBoundType) tree.WindowFrameBoundType {
	startBoundTypes := []tree.WindowFrameBoundType{
		tree.UnboundedPreceding, tree.OffsetPreceding, tree.CurrentRow, tree.OffsetFollowing,
	}
	for {
		startBound := startBoundTypes[rng.Intn(len(startBoundTypes))]
		if startBound <= endBound {
			return startBound
		}
	}
}

func getEndBound(rng *rand.Rand, startBound tree.WindowFrameBoundType) tree.WindowFrameBoundType {
	endBoundTypes := []tree.WindowFrameBoundType{
		tree.OffsetPreceding, tree.CurrentRow, tree.OffsetFollowing, tree.UnboundedFollowing,
	}
	for {
		endBound := endBoundTypes[rng.Intn(len(endBoundTypes))]
		if endBound >= startBound {
			return endBound
		}
	}
}

type datumRows struct {
	rows tree.Datums
	asc  bool
	ctx  *tree.EvalContext
}

func (r *datumRows) Len() int {
	return len(r.rows)
}

func (r *datumRows) Less(i, j int) bool {
	cmp := r.rows[i].Compare(r.ctx, r.rows[j])
	if r.asc {
		return cmp < 0
	}
	return cmp > 0
}

func (r *datumRows) Swap(i, j int) {
	r.rows[i], r.rows[j] = r.rows[j], r.rows[i]
}

func makeSortedPartition(testCfg *testConfig) (tree.Datums, *colexecutils.SpillingBuffer) {
	datums := &datumRows{
		rows: make(tree.Datums, testCfg.count),
		asc:  testCfg.asc,
		ctx:  testCfg.evalCtx,
	}
	const nullChance = 0.2
	for i := range datums.rows {
		if testCfg.rng.Float64() < nullChance {
			datums.rows[i] = tree.DNull
			continue
		}
		if i == 0 || testCfg.rng.Float64() < probabilityOfNewNumber {
			datums.rows[i] = randgen.RandDatumSimple(testCfg.rng, testCfg.typ)
			continue
		}
		datums.rows[i] = datums.rows[i-1]
	}
	sort.Sort(datums)

	partition := colexecutils.NewSpillingBuffer(
		testCfg.allocator, math.MaxInt64, testCfg.queueCfg,
		colexecop.NewTestingSemaphore(2), []*types.T{testCfg.typ, types.Bool}, testDiskAcc,
	)
	insertBatch := coldata.NewMemBatchWithCapacity(
		[]*types.T{testCfg.typ, types.Bool},
		1, /* capacity */
		testCfg.factory,
	)
	var last tree.Datum
	for i, val := range datums.rows {
		insertBatch.ColVec(orderColIdx).Nulls().UnsetNulls()
		insertBatch.ColVec(peersColIdx).Bool()[0] = false
		if i == 0 || (last != nil && val.Compare(testCfg.evalCtx, last) != 0) {
			insertBatch.ColVec(peersColIdx).Bool()[0] = true
		}
		last = val
		vec := insertBatch.ColVec(orderColIdx)
		if val == tree.DNull {
			vec.Nulls().SetNull(0)
		} else {
			switch t := val.(type) {
			case *tree.DInt:
				vec.Int64()[0] = int64(*t)
			case *tree.DFloat:
				vec.Float64()[0] = float64(*t)
			case *tree.DDecimal:
				vec.Decimal()[0] = t.Decimal
			case *tree.DInterval:
				vec.Interval()[0] = t.Duration
			case *tree.DTimestampTZ:
				vec.Timestamp()[0] = t.Time
			case *tree.DDate:
				vec.Int64()[0] = t.UnixEpochDaysWithOrig()
			case *tree.DTimeTZ:
				vec.Datum().Set(0, t)
			default:
				panic(errors.AssertionFailedf("unsupported datum: %v", t))
			}
		}
		insertBatch.SetLength(1)
		partition.AppendTuples(
			testCfg.evalCtx.Ctx(), insertBatch, 0 /* startIdx */, insertBatch.Length())
	}
	return datums.rows, partition
}

func initWindowFramers(
	t *testing.T, testCfg *testConfig,
) (windowFramer, *tree.WindowFrameRun, *colexecutils.SpillingBuffer) {
	offsetType := types.Int
	if testCfg.mode == tree.RANGE {
		offsetType = getOffsetType(testCfg.typ)
	}
	startOffset := makeRandOffset(testCfg.rng, offsetType)
	endOffset := makeRandOffset(testCfg.rng, offsetType)

	datums, colBuffer := makeSortedPartition(testCfg)

	datumEncoding := descpb.DatumEncoding_ASCENDING_KEY
	if !testCfg.asc {
		datumEncoding = descpb.DatumEncoding_DESCENDING_KEY
	}
	frame := &execinfrapb.WindowerSpec_Frame{
		Mode: modeToExecinfrapb(testCfg.mode),
		Bounds: execinfrapb.WindowerSpec_Frame_Bounds{
			Start: execinfrapb.WindowerSpec_Frame_Bound{
				BoundType:   boundToExecinfrapb(testCfg.startBound),
				TypedOffset: encodeOffset(startOffset),
				OffsetType: execinfrapb.DatumInfo{
					Type:     testCfg.typ,
					Encoding: datumEncoding,
				},
			},
			End: &execinfrapb.WindowerSpec_Frame_Bound{
				BoundType:   boundToExecinfrapb(testCfg.endBound),
				TypedOffset: encodeOffset(endOffset),
				OffsetType: execinfrapb.DatumInfo{
					Type:     testCfg.typ,
					Encoding: datumEncoding,
				},
			},
		},
		Exclusion: exclusionToExecinfrapb(testCfg.exclusion),
	}
	if testCfg.mode != tree.RANGE {
		frame.Bounds.Start.IntOffset = uint64(*(startOffset.(*tree.DInt)))
		frame.Bounds.End.IntOffset = uint64(*(endOffset.(*tree.DInt)))
	}
	dir := execinfrapb.Ordering_Column_ASC
	if !testCfg.asc {
		dir = execinfrapb.Ordering_Column_DESC
	}
	ordering := &execinfrapb.Ordering{
		Columns: []execinfrapb.Ordering_Column{{
			ColIdx:    orderColIdx,
			Direction: dir,
		}},
	}
	colWindowFramer := newWindowFramer(
		testCfg.evalCtx, frame, ordering, []*types.T{testCfg.typ, types.Bool}, peersColIdx)
	colWindowFramer.startPartition(testCfg.evalCtx.Ctx(), colBuffer.Length(), colBuffer)

	rowDir := encoding.Ascending
	if !testCfg.asc {
		rowDir = encoding.Descending
	}
	rowWindowFramer := &tree.WindowFrameRun{
		Rows:         &indexedRows{partition: datums, orderColType: testCfg.typ},
		ArgsIdxs:     []uint32{0},
		FilterColIdx: tree.NoColumnIdx,
		Frame: &tree.WindowFrame{
			Mode: testCfg.mode,
			Bounds: tree.WindowFrameBounds{
				StartBound: &tree.WindowFrameBound{
					BoundType:  testCfg.startBound,
					OffsetExpr: startOffset,
				},
				EndBound: &tree.WindowFrameBound{
					BoundType:  testCfg.endBound,
					OffsetExpr: endOffset,
				},
			},
			Exclusion: testCfg.exclusion,
		},
		StartBoundOffset: startOffset,
		EndBoundOffset:   endOffset,
		OrdColIdx:        orderColIdx,
		OrdDirection:     rowDir,
	}
	rowWindowFramer.PlusOp, rowWindowFramer.MinusOp, _ = tree.WindowFrameRangeOps{}.LookupImpl(testCfg.typ, offsetType)
	err := rowWindowFramer.PeerHelper.Init(rowWindowFramer, &peerGroupChecker{partition: datums})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	return colWindowFramer, rowWindowFramer, colBuffer
}

type indexedRows struct {
	partition    tree.Datums
	orderColType *types.T
}

var _ tree.IndexedRows = &indexedRows{}

func (ir indexedRows) Len() int {
	return len(ir.partition)
}

func (ir indexedRows) GetRow(ctx context.Context, idx int) (tree.IndexedRow, error) {
	return indexedRow{row: tree.Datums{ir.partition[idx]}}, nil
}

type indexedRow struct {
	idx int
	row tree.Datums
}

var _ tree.IndexedRow = &indexedRow{}

func (ir indexedRow) GetIdx() int {
	return ir.idx
}

func (ir indexedRow) GetDatum(colIdx int) (tree.Datum, error) {
	return ir.row[colIdx], nil
}

func (ir indexedRow) GetDatums(firstColIdx, lastColIdx int) (tree.Datums, error) {
	return ir.row[firstColIdx:lastColIdx], nil
}

type peerGroupChecker struct {
	evalCtx   tree.EvalContext
	partition tree.Datums
}

var _ tree.PeerGroupChecker = &peerGroupChecker{}

func (c *peerGroupChecker) InSameGroup(i, j int) (bool, error) {
	return c.partition[i].Compare(&c.evalCtx, c.partition[j]) == 0, nil
}

func modeToExecinfrapb(mode tree.WindowFrameMode) execinfrapb.WindowerSpec_Frame_Mode {
	switch mode {
	case tree.RANGE:
		return execinfrapb.WindowerSpec_Frame_RANGE
	case tree.ROWS:
		return execinfrapb.WindowerSpec_Frame_ROWS
	case tree.GROUPS:
		return execinfrapb.WindowerSpec_Frame_GROUPS
	}
	panic(errors.AssertionFailedf("boundType out of range"))
}

func boundToExecinfrapb(bound tree.WindowFrameBoundType) execinfrapb.WindowerSpec_Frame_BoundType {
	switch bound {
	case tree.UnboundedPreceding:
		return execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING
	case tree.OffsetPreceding:
		return execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING
	case tree.CurrentRow:
		return execinfrapb.WindowerSpec_Frame_CURRENT_ROW
	case tree.OffsetFollowing:
		return execinfrapb.WindowerSpec_Frame_OFFSET_FOLLOWING
	case tree.UnboundedFollowing:
		return execinfrapb.WindowerSpec_Frame_UNBOUNDED_FOLLOWING
	}
	panic(errors.AssertionFailedf("boundType out of range"))
}

func exclusionToExecinfrapb(
	exclusion tree.WindowFrameExclusion,
) execinfrapb.WindowerSpec_Frame_Exclusion {
	switch exclusion {
	case tree.NoExclusion:
		return execinfrapb.WindowerSpec_Frame_NO_EXCLUSION
	case tree.ExcludeCurrentRow:
		return execinfrapb.WindowerSpec_Frame_EXCLUDE_CURRENT_ROW
	case tree.ExcludeGroup:
		return execinfrapb.WindowerSpec_Frame_EXCLUDE_GROUP
	case tree.ExcludeTies:
		return execinfrapb.WindowerSpec_Frame_EXCLUDE_TIES
	}
	panic(errors.AssertionFailedf("exclusion out of range"))
}

func encodeOffset(offset tree.Datum) []byte {
	var encoded, scratch []byte
	encoded, err := rowenc.EncodeTableValue(
		encoded, descpb.ColumnID(encoding.NoColumnID), offset, scratch)
	if err != nil {
		panic(err)
	}
	return encoded
}

func makeRandOffset(rng *rand.Rand, typ *types.T) tree.Datum {
	isNegative := func(val tree.Datum) bool {
		switch t := val.(type) {
		case *tree.DInt:
			return int64(*t) < 0
		case *tree.DFloat:
			return float64(*t) < 0
		case *tree.DDecimal:
			return t.Negative
		case *tree.DInterval, *tree.DTimestampTZ, *tree.DDate, *tree.DTimeTZ:
			return false
		default:
			panic(errors.AssertionFailedf("unsupported datum: %v", t))
		}
	}

	for {
		val := randgen.RandDatumSimple(rng, typ)
		if isNegative(val) {
			// Offsets must be non-null and non-negative.
			continue
		}
		return val
	}
}
