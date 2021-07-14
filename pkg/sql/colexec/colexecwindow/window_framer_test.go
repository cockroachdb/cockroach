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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
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
	for _, mode := range []tree.WindowFrameMode{tree.ROWS, tree.GROUPS, tree.RANGE} {
		testCfg.mode = mode
		t.Run(fmt.Sprintf("mode=%v", mode.Name()), func(t *testing.T) {
			for _, bound := range []tree.WindowFrameBoundType{
				tree.UnboundedPreceding, tree.OffsetPreceding, tree.CurrentRow,
				tree.OffsetFollowing, tree.UnboundedFollowing,
			} {
				for _, bounds := range [][2]tree.WindowFrameBoundType{
					{bound, getEndBound(rng, bound)},
					{getStartBound(rng, bound), bound},
				} {
					start, end := bounds[0], bounds[1]
					if !validForStart(start) || !validForEnd(end) {
						// These bounds would entail a syntactic error.
						continue
					}
					testCfg.startBound, testCfg.endBound = start, end
					t.Run(fmt.Sprintf("start=%v/end=%v", start.Name(), end.Name()), func(t *testing.T) {
						for _, count := range []int{1, 17, 42, 91} {
							testCfg.count = count
							testCfg.ordered = true
							typ := types.Int
							if rng.Float64() < randTypeProbability {
								typ = randTypes[rng.Intn(len(randTypes))]
							}
							testCfg.typ = typ
							t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
								for _, asc := range []bool{true, false} {
									testCfg.asc = asc
									t.Run(fmt.Sprintf("ordered/asc=%v/typ=%v", asc, typ), func(t *testing.T) {
										testWindowFramer(t, testCfg)
									})
								}
								if mode == tree.ROWS {
									// An ORDER BY clause is required for RANGE and GROUPS modes.
									testCfg.ordered = false
									t.Run("unordered", func(t *testing.T) {
										testWindowFramer(t, testCfg)
									})
								}
							})
						}
					})
				}
			}
		})
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
	ordered    bool
	asc        bool
	mode       tree.WindowFrameMode
	startBound tree.WindowFrameBoundType
	endBound   tree.WindowFrameBoundType
}

func testWindowFramer(t *testing.T, testCfg *testConfig) {
	colWindowFramer, rowWindowFramer, partition := initWindowFramers(t, testCfg)

	for i := 0; i < testCfg.count; i++ {
		// Advance the columnar framer.
		colWindowFramer.next(testCfg.evalCtx.Ctx())

		// Advance the row-wise framer.
		if testCfg.ordered {
			vec, vecIdx, _ := partition.GetVecWithTuple(testCfg.evalCtx.Ctx(), peersColIdx, i)
			if i > 0 && vec.Bool()[vecIdx] {
				rowWindowFramer.CurRowPeerGroupNum++
				require.NoError(t, rowWindowFramer.PeerHelper.Update(rowWindowFramer))
			}
		}
		rowWindowFramer.RowIdx = i
		rowStartIdx, err := rowWindowFramer.FrameStartIdx(testCfg.evalCtx.Ctx(), testCfg.evalCtx)
		require.NoError(t, err)
		rowEndIdx, err := rowWindowFramer.FrameEndIdx(testCfg.evalCtx.Ctx(), testCfg.evalCtx)
		require.NoError(t, err)

		// Validate that the columnar window framer describes the same window
		// frame as the row-wise one.
		var colFrameIdx int
		frameIsEmpty := true
		for j := rowStartIdx; j < rowEndIdx; j++ {
			skipped, err := rowWindowFramer.IsRowSkipped(testCfg.evalCtx.Ctx(), j)
			require.NoError(t, err)
			if skipped {
				continue
			}
			colFrameIdx++
			frameIsEmpty = false
			require.Equal(t, j, colWindowFramer.frameNthIdx(colFrameIdx), "NthIdx")
		}
		if frameIsEmpty {
			// The columnar window framer functions return -1 to signify an empty
			// window frame.
			require.Equal(t, -1, colWindowFramer.frameFirstIdx(), "Empty FirstIdx")
			require.Equal(t, -1, colWindowFramer.frameLastIdx(), "Empty LastIdx")
			require.Equal(t, -1, colWindowFramer.frameNthIdx(0 /* n */), "Empty NthIdx")
			continue
		}
		require.Equal(t, rowStartIdx, colWindowFramer.frameFirstIdx(), "FirstIdx")
		require.Equal(t, rowEndIdx, colWindowFramer.frameLastIdx()+1, "LastIdx")
	}

	partition.Close(testCfg.evalCtx.Ctx())
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
	insertBatch := testCfg.allocator.NewMemBatchWithFixedCapacity(
		[]*types.T{testCfg.typ, types.Bool},
		1, /* capacity */
	)
	var last tree.Datum
	for i, val := range datums.rows {
		insertBatch.ColVec(orderColIdx).Nulls().UnsetNulls()
		insertBatch.ColVec(peersColIdx).Bool()[0] = false
		if i == 0 || val.Compare(testCfg.evalCtx, last) != 0 {
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
				colexecerror.InternalError(errors.AssertionFailedf("unsupported datum: %v", t))
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

	peersCol, orderCol := tree.NoColumnIdx, tree.NoColumnIdx
	if testCfg.ordered {
		peersCol, orderCol = peersColIdx, orderColIdx
	}

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
	}
	if testCfg.mode != tree.RANGE {
		frame.Bounds.Start.IntOffset = uint64(*(startOffset.(*tree.DInt)))
		frame.Bounds.End.IntOffset = uint64(*(endOffset.(*tree.DInt)))
	}
	ordering := &execinfrapb.Ordering{}
	if testCfg.ordered {
		dir := execinfrapb.Ordering_Column_ASC
		if !testCfg.asc {
			dir = execinfrapb.Ordering_Column_DESC
		}
		ordering.Columns = []execinfrapb.Ordering_Column{{
			ColIdx:    uint32(orderCol),
			Direction: dir,
		}}
	}
	colWindowFramer := newWindowFramer(
		testCfg.evalCtx, frame, ordering, []*types.T{testCfg.typ, types.Bool}, peersCol)
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
		},
		StartBoundOffset: startOffset,
		EndBoundOffset:   endOffset,
		OrdColIdx:        orderCol,
		OrdDirection:     rowDir,
	}
	rowWindowFramer.PlusOp, rowWindowFramer.MinusOp, _ = tree.WindowFrameRangeOps{}.LookupImpl(testCfg.typ, offsetType)
	require.NoError(t, rowWindowFramer.PeerHelper.Init(
		rowWindowFramer,
		&peerGroupChecker{partition: datums, ordered: testCfg.ordered}),
	)

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
	ordered   bool
}

var _ tree.PeerGroupChecker = &peerGroupChecker{}

func (c *peerGroupChecker) InSameGroup(i, j int) (bool, error) {
	if !c.ordered {
		// All rows are in the same peer group.
		return true, nil
	}
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
	return 0
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
	return 0
}

func encodeOffset(offset tree.Datum) []byte {
	var encoded, scratch []byte
	encoded, err := rowenc.EncodeTableValue(
		encoded, descpb.ColumnID(encoding.NoColumnID), offset, scratch)
	if err != nil {
		colexecerror.InternalError(err)
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
			colexecerror.InternalError(errors.AssertionFailedf("unsupported datum: %v", t))
			return false
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
