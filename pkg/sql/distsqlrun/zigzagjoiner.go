// Copyright 2018 The Cockroach Authors.
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
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// zigzagJoiner performs a zigzag join, as inputs its spec contains an array of
// indexes to join, equality columns to join on, and optionally some columns
// may be fixed to given EncDatums.
// The concatentaiton of the fixed columns and the equality columns must be a
// prefix of the index columns. Additionally each index must be sorted by the
// equality columns.
//
// # Zigzag Join Algorithm #
// The algorithm can be visualized as follows, which is joining two secondary
// indexes where (a) is the primary key, and 'X' indicates a match.
// This query may be executed for: SELECT * FROM abc WHERE b = 0 AND c = 1;
//
// CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT, INDEX b (b), INDEX c (c));
//
//       b | a      c | a
//     =========  ==========
// START
// |
// +---> 0   1 ---> 1   1 --+ X
//                          | (Next())
// +--------------- 1   4 <-+
// |                1   5
// |                1   6
// |                1   7
// +---> 0   14 -+
//               |
//               +->1   15 --+
//                           |
// +-------------------------+
// |
// +-> 0   16 ----> 1   16 --+ X
//                           | (Next())
//                  Done <---+ (row is nil)
//
//
// Every transition from one side to the other is an index lookup.
// The entries are sorted by primary key, since the prefixes have been
// fixed. In this example, the equality columns are those in the primary key
// since we are searching a single table.
//
// In general, the algorithm between 2 indexes (LEFT and RIGHT) is as follows:
// 1) Get first row from the LEFT, let this be the base row. This is done once.
// ---- LOOP START
// 2) Check if there are rows in the container of the current side. If there
//    is, emit the cartesian product which match the onExpr.
// 3) Construct a key based on this row to search the RIGHT, based on the
//    equality columns of the join.
// 4) Find first row greater than or equal to this key on the RIGHT side.
// 5) If the equality columns match: get the next row from that side,
//    otherwise do nothing.
// 6) Set the most recent row fetched from the RIGHT as the base row.
// 7) If there was a match, collect all matches from both sides and store.
// 8) If the base row is nil, all the rows on that side have been exhausted
//    so there cannot be any more matches. Break out of loop and finish.
// 9) Loop back to step 2 with the new base row and flip the sides.
// ---- LOOP END
//
type zigzagJoiner struct {
	joinerBase

	evalCtx       *tree.EvalContext
	cancelChecker *sqlbase.CancelChecker

	// numTables stored the number of tables involved in the join.
	numTables int
	// side keeps track of which side is being processed.
	side int

	// Stores relevant information for each side of the join including table
	// descriptors, index IDs, rowFetchers, and more. See zigzagJoinInfo for
	// more information.
	infos []zigzagJoinerInfo

	// Base row stores the that the algorithm is compared against and is updated
	// with every change of side.
	baseRow sqlbase.EncDatumRow

	// Batch size is a parameter which determines how many rows should be fetched
	// at a time. Increasing this will improve performance for when matched rows
	// are grouped together, but increasing this too much will result in fetching
	// too many rows and therefore skipping less rows.
	batchSize int64

	rowAlloc sqlbase.EncDatumRowAlloc

	// TODO(andrei): get rid of this field and move the actions it gates into the
	// Start() method.
	started bool
}

var _ Processor = &zigzagJoiner{}
var _ RowSource = &zigzagJoiner{}

const zigzagJoinerProcName = "zigzagJoiner"

// newZigzagJoiner creates a new zigzag joiner given a spec and an EncDatumRow
// holding the values of the prefix columns of the index specified in the spec.
func newZigzagJoiner(
	flowCtx *FlowCtx,
	spec *ZigzagJoinerSpec,
	fixedValues []sqlbase.EncDatumRow,
	post *PostProcessSpec,
	output RowReceiver,
) (*zigzagJoiner, error) {
	z := &zigzagJoiner{}

	leftColumnTypes := spec.Tables[0].ColumnTypes()
	rightColumnTypes := spec.Tables[1].ColumnTypes()
	leftEqCols := make([]uint32, 0, len(spec.EqColumns[0].Columns))
	rightEqCols := make([]uint32, 0, len(spec.EqColumns[1].Columns))
	err := z.joinerBase.init(flowCtx, leftColumnTypes, rightColumnTypes, spec.Type, spec.OnExpr, leftEqCols, rightEqCols,
		0 /* numMerged */, post, output)
	if err != nil {
		return nil, err
	}

	z.numTables = len(spec.Tables)
	z.infos = make([]zigzagJoinerInfo, z.numTables)

	for i := 0; i < z.numTables; i++ {
		if i < len(fixedValues) {
			z.infos[i].fixedValues = fixedValues[i]
		}
		z.side = i
		if err := z.setupInfo(spec); err != nil {
			return nil, err
		}
	}
	z.side = 0
	return z, nil
}

// Run is part of the processor interface.
func (z *zigzagJoiner) Run(ctx context.Context, wg *sync.WaitGroup) {
	if z.out.output == nil {
		panic("zigzagJoiner output not initialized for emitting rows")
	}
	z.Start(ctx)
	Run(z.ctx, z, z.out.output)
	if wg != nil {
		wg.Done()
	}
}

// Start is part of the RowSource interface.
func (z *zigzagJoiner) Start(ctx context.Context) context.Context {
	ctx = z.startInternal(ctx, zigzagJoinerProcName)
	z.evalCtx = z.flowCtx.NewEvalCtx()
	z.cancelChecker = sqlbase.NewCancelChecker(ctx)
	log.VEventf(ctx, 2, "starting zigzag joiner run")
	return ctx
}

// zigzagJoinerInfo contains all the information that needs to be
// stored for each side of the join.
type zigzagJoinerInfo struct {
	fetcher sqlbase.RowFetcher
	alloc   *sqlbase.DatumAlloc
	table   *sqlbase.TableDescriptor
	index   *sqlbase.IndexDescriptor

	// Stores one batch of matches at a time. When all the rows are collected
	// the cartesian product of the containers will be emitted.
	container    []sqlbase.EncDatumRow
	containerIdx int

	eqColumnIDs columns

	// Prefix of the index key that has fixed values.
	fixedValues sqlbase.EncDatumRow

	// The current key being fetched by this side.
	key roachpb.Key
	// The prefix of the key which includes the table and index IDs.
	prefix []byte
	// endKey marks where this side should stop fetching, taking into account the
	// fixedValues.
	endKey roachpb.Key
}

// Returns the info struct of the current side.
func (z *zigzagJoiner) curInfo() *zigzagJoinerInfo {
	return &z.infos[z.side]
}

// Setup the curInfo struct for the current z.side, which specifies the side
// number of the curInfo to set up.
func (z *zigzagJoiner) setupInfo(spec *ZigzagJoinerSpec) error {
	z.curInfo().table = &spec.Tables[z.side]
	z.curInfo().eqColumnIDs = spec.EqColumns[z.side].Columns
	indexID := spec.IndexIds[z.side]
	if indexID == 0 {
		z.curInfo().index = &z.curInfo().table.PrimaryIndex
	} else {
		z.curInfo().index = &z.curInfo().table.Indexes[indexID-1]
	}

	z.curInfo().containerIdx = -1

	// Add all columns that appear in the key of the index.
	allIndexCols := util.MakeFastIntSet()
	for _, id := range z.curInfo().index.ColumnIDs {
		allIndexCols.Add(int(id) - 1)
	}
	for _, id := range z.curInfo().index.ExtraColumnIDs {
		allIndexCols.Add(int(id) - 1)
	}
	_, _, err := initRowFetcher(
		&(z.curInfo().fetcher),
		z.curInfo().table,
		int(z.curInfo().index.ID)-1,
		false, /* reverse */
		allIndexCols,
		false, /* check */
		z.curInfo().alloc,
	)
	if err != nil {
		return err
	}

	z.curInfo().prefix = sqlbase.MakeIndexKeyPrefix(z.curInfo().table, z.curInfo().index.ID)
	z.curInfo().key, err = z.produceKeyFromBaseRow()

	if err != nil {
		return err
	}
	z.curInfo().endKey = z.curInfo().key.PrefixEnd()
	return nil
}

func (z *zigzagJoiner) close() {
	if !z.closed {
		log.VEventf(z.ctx, 2, "exiting zigzag joiner run")
	}
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error. It is ok for err to be
// nil indicating that we're done producing rows even though no error occurred.
func (z *zigzagJoiner) producerMeta(err error) *ProducerMetadata {
	var meta *ProducerMetadata
	if !z.closed {
		if err != nil {
			meta = &ProducerMetadata{Err: err}
		} else if trace := getTraceData(z.ctx); trace != nil {
			meta = &ProducerMetadata{TraceData: trace}
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		z.close()
	}
	return meta
}

func findColumnID(s []sqlbase.ColumnID, t sqlbase.ColumnID) int {
	for i := range s {
		if s[i] == t {
			return i
		}
	}
	return -1
}

// Return the datums from the equality columns from a given non-empty row
// from the specified side.
func (z *zigzagJoiner) extractEqDatums(row sqlbase.EncDatumRow, side int) sqlbase.EncDatumRow {
	eqColIDs := z.infos[side].eqColumnIDs
	eqCols := make(sqlbase.EncDatumRow, 0, len(eqColIDs))
	for _, id := range eqColIDs {
		eqCols = append(eqCols, row[id])
	}
	return eqCols
}

// separateTypes partitions the column types based on whether the column is
// an explicit or implicit part of the index.
func (z *zigzagJoiner) separateTypes() ([]sqlbase.ColumnType, []sqlbase.ColumnType) {
	indexDescriptor := z.curInfo().index
	allTypes := z.curInfo().table.ColumnTypes()
	explicitTypes := make([]sqlbase.ColumnType, 0, len(indexDescriptor.ColumnIDs))
	implicitTypes := make([]sqlbase.ColumnType, 0, len(indexDescriptor.ExtraColumnIDs))
	for _, id := range indexDescriptor.ColumnIDs {
		explicitTypes = append(explicitTypes, allTypes[id-1])
	}
	for _, id := range indexDescriptor.ExtraColumnIDs {
		implicitTypes = append(implicitTypes, allTypes[id-1])
	}
	return explicitTypes, implicitTypes
}

// separateDatums partitions the column datums based on whether the column is
// an explicit or implicit part of the index.
func (z *zigzagJoiner) separateDatums() (sqlbase.EncDatumRow, sqlbase.EncDatumRow) {
	fixedDatums := z.curInfo().fixedValues
	neededDatums := fixedDatums
	if z.baseRow != nil {
		eqDatums := z.extractEqDatums(z.baseRow, z.side)
		neededDatums = append(neededDatums, eqDatums...)
	}
	indexDescriptor := z.curInfo().index
	explicitDatums := make(sqlbase.EncDatumRow, 0, len(indexDescriptor.ColumnIDs))
	implicitDatums := make(sqlbase.EncDatumRow, 0, len(indexDescriptor.ExtraColumnIDs))
	for i := range neededDatums {
		if i < cap(explicitDatums) {
			explicitDatums = append(explicitDatums, neededDatums[i])
		} else {
			implicitDatums = append(implicitDatums, neededDatums[i])
		}
	}
	return explicitDatums, implicitDatums
}

// Generates a Key, corresponding to the current `z.baseRow` in
// the index on the current side.
func (z *zigzagJoiner) produceKeyFromBaseRow() (roachpb.Key, error) {
	explicitDatums, implicitDatums := z.separateDatums()
	explicitTypes, _ := z.separateTypes()
	explicitTypes = explicitTypes[:len(explicitDatums)]

	// Construct correct row by concatenating right fixed datums with
	// primary key extracted from `row`.
	key, err := sqlbase.MakeExtendedKeyFromEncDatums(
		explicitTypes,
		explicitDatums,
		implicitDatums,
		z.curInfo().table,
		z.curInfo().index,
		z.curInfo().prefix,
		z.curInfo().alloc,
	)
	return key, err
}

// Returns the column types of the equality columns.
func (zi *zigzagJoinerInfo) eqColTypes() []sqlbase.ColumnType {
	eqColIDs := zi.eqColumnIDs
	eqColTypes := make([]sqlbase.ColumnType, 0, len(eqColIDs))
	for _, id := range eqColIDs {
		eqColTypes = append(eqColTypes, zi.table.ColumnTypes()[id])
	}
	return eqColTypes
}

// Returns the ordering of the equality columns.
func (zi *zigzagJoinerInfo) eqOrdering() (sqlbase.ColumnOrdering, error) {
	ordering := make(sqlbase.ColumnOrdering, len(zi.eqColumnIDs))
	for i, colID := range zi.eqColumnIDs {
		direction := encoding.Ascending
		// Search the index columns, then the primary keys to find an ordering for
		// the current column, 'colID'.
		if idx := findColumnID(zi.index.ColumnIDs, sqlbase.ColumnID(colID+1)); idx != -1 {
			if zi.index.ColumnDirections[idx] == sqlbase.IndexDescriptor_DESC {
				direction = encoding.Descending
			}
		} else if idx = findColumnID(zi.table.PrimaryIndex.ColumnIDs, sqlbase.ColumnID(colID+1)); idx != -1 {
			if zi.table.PrimaryIndex.ColumnDirections[idx] == sqlbase.IndexDescriptor_DESC {
				direction = encoding.Descending
			}
		} else {
			return nil, errors.New("ordering of equality column not found in index or primary key")
		}
		ordering[i] = sqlbase.ColumnOrderInfo{ColIdx: i, Direction: direction}
	}
	return ordering, nil
}

// matchBase compares the equality columns of the current row to `z.baseRow`,
// which is the previously fetched row. Returns whether or not the rows match
// on the equality columns.
func (z *zigzagJoiner) matchBase(curRow sqlbase.EncDatumRow) (bool, error) {
	if len(curRow) == 0 {
		return false, nil
	}

	prevSide := z.prevSide()
	prevEqDatums := z.extractEqDatums(z.baseRow, prevSide)
	curEqDatums := z.extractEqDatums(curRow, z.side)

	eqColTypes := z.curInfo().eqColTypes()
	ordering, err := z.curInfo().eqOrdering()
	if err != nil {
		return false, err
	}

	// Compare the equality columns of the baseRow to that of the curRow.
	cmp, err := prevEqDatums.Compare(eqColTypes, &sqlbase.DatumAlloc{}, ordering, &z.flowCtx.EvalCtx, curEqDatums)
	if err != nil {
		return false, err
	}
	return cmp == 0, nil
}

// emitFromContainers returns the next row that is to be emitted from those
// already stored in the containers.
// Since this is called after the side has been incremented, it produces the
// cartesian product of the previous side's container and the side before that
// one. These are the `matchSize` and `baseSide` respectively.
// Iterates through every matchSide row for each baseSide row.
func (z *zigzagJoiner) emitFromContainers() (sqlbase.EncDatumRow, error) {
	matchSide := z.prevSide()
	baseSide := z.sideBefore(matchSide)
	for z.infos[baseSide].containerIdx >= 0 {
		baseIdx := z.infos[baseSide].containerIdx
		matchIdx := z.infos[matchSide].containerIdx

		baseRow := z.infos[baseSide].container[baseIdx]
		matchedRow := z.infos[matchSide].container[matchIdx]
		leftRow := baseRow
		rightRow := matchedRow

		if baseSide == 1 {
			leftRow = matchedRow
			rightRow = baseRow
		}
		renderedRow, err := z.render(leftRow, rightRow)
		if err != nil {
			return nil, err
		}
		// Iterate through every matchSide row for each baseSide row.
		// After both sides have been exhausted, the indexes should end up as -1.
		// Since a pair between the matchSide and baseSide was just emitted,
		// decrement the matchSide index to create a new pair.
		z.infos[matchSide].containerIdx--
		if z.infos[matchSide].containerIdx < 0 {
			// If the matchSize index becomes negative then all of the rows from the
			// matchSide for this baseRow have been emitted, so decrement the
			// baseSide index to get a new baseRow.
			z.infos[baseSide].containerIdx--
			if z.infos[baseSide].containerIdx >= 0 {
				// If there are still more rows on the baseSide, reset the matchSize
				// index to the size of the matchSide.
				z.infos[matchSide].containerIdx = len(z.infos[matchSide].container) - 1
			}
		}
		if renderedRow != nil {
			// The pair satisfied the onExpr.
			return renderedRow, nil
		}
	}

	// All matches have been returned since the baseSide index is negative.
	// Empty the containers to reset their contents.
	z.infos[baseSide].container = z.infos[baseSide].container[:0]
	z.infos[matchSide].container = z.infos[matchSide].container[:0]

	return nil, nil
}

// nextRow fetches the nextRow to emit from the join. It iterates through all
// sides until a match is found then emits the results of the match one result
// at a time.
func (z *zigzagJoiner) nextRow(
	ctx context.Context, txn *client.Txn,
) (sqlbase.EncDatumRow, *ProducerMetadata) {
	for {
		if err := z.cancelChecker.Check(); err != nil {
			return nil, &ProducerMetadata{Err: err}
		}

		// Check if there are any rows built up in the containers that need to be
		// emitted.
		if rowToEmit, err := z.emitFromContainers(); err != nil {
			return nil, z.producerMeta(err)
		} else if rowToEmit != nil {
			return rowToEmit, nil
		}

		// If the baseRow is nil, the last fetched row was nil. That means that
		// that there does not exist any more matches in the join so we break and
		// return nil to indicate that we are done to the caller.
		if len(z.baseRow) == 0 {
			break
		}

		var err error
		// Generate a key from the last row seen from the last side. We're about to
		// use it to jump to the next possible match on the current side.
		z.curInfo().key, err = z.produceKeyFromBaseRow()
		if err != nil {
			return nil, z.producerMeta(err)
		}

		err = z.curInfo().fetcher.StartScan(
			ctx,
			txn,
			roachpb.Spans{roachpb.Span{Key: z.curInfo().key, EndKey: z.curInfo().endKey}},
			true, /* batch limit */
			z.batchSize,
			false, /* traceKV */
		)
		if err != nil {
			return nil, z.producerMeta(err)
		}

		fetchedRow, _, _, err := z.curInfo().fetcher.NextRow(ctx)
		if err != nil {
			return nil, z.producerMeta(err)
		}
		// If the next possible match on the current side that matches the previous
		// row is `nil`, that means that there does not exist any more matches in
		// the join so we break and return nil to indicate that to the caller.
		if fetchedRow == nil {
			break
		}

		matched, err := z.matchBase(fetchedRow)
		if err != nil {
			return nil, z.producerMeta(err)
		}
		if matched {
			// We've detected a match! Now, we collect all subsequent matches on both
			// sides for the current equality column values and add them to our
			// list of rows to emit.
			prevSide := z.prevSide()

			// Store the matched rows in the appropriate container to emit.
			prevRow := z.rowAlloc.AllocRow(len(z.baseRow))
			copy(prevRow, z.baseRow)
			z.infos[prevSide].container = append(z.infos[prevSide].container, prevRow)
			curRow := z.rowAlloc.AllocRow(len(fetchedRow))
			copy(curRow, fetchedRow)
			z.infos[z.side].container = append(z.infos[z.side].container, curRow)

			z.infos[prevSide].containerIdx++
			z.infos[z.side].containerIdx++

			// After collecting all matches from each side, the first unmatched
			// row from each side is returned. We want the new baseRow to be
			// the latest of these rows since no match can occur before considering
			// this row.
			prevNext, err := z.collectAllMatches(ctx, prevSide)
			if err != nil {
				return nil, z.producerMeta(err)
			}
			curNext, err := z.collectAllMatches(ctx, z.side)
			if err != nil {
				return nil, z.producerMeta(err)
			}

			// No more matches, so set the baseRow to nil to indicate that we should
			// terminate after emitting all the rows stored in the container.
			if len(prevNext) == 0 || len(curNext) == 0 {
				z.baseRow = nil
				continue
			}

			prevEqCols := z.extractEqDatums(prevNext, prevSide)
			currentEqCols := z.extractEqDatums(curNext, z.side)
			eqColTypes := z.curInfo().eqColTypes()
			ordering, err := z.curInfo().eqOrdering()
			if err != nil {
				return nil, z.producerMeta(err)
			}
			cmp, err := prevEqCols.Compare(eqColTypes, &sqlbase.DatumAlloc{}, ordering, &z.flowCtx.EvalCtx, currentEqCols)
			if err != nil {
				return nil, z.producerMeta(err)
			}
			// We want the new current side to be the one that has the latest key
			// since we know that this key will not be able to match any previous
			// key. The current side should be the side after the baseRow's side.
			if cmp < 0 {
				// The current side had the later row, so increment the side.
				z.side = z.nextSide()
				z.baseRow = curNext
			} else {
				// The previous side had the later row so the side doesn't change.
				z.baseRow = prevNext
			}
		} else {
			// The current row doesn't match the base row, so update the base row to
			// the current row and increment the side to repeat the process.
			z.baseRow = fetchedRow
			z.baseRow = z.rowAlloc.AllocRow(len(fetchedRow))
			copy(z.baseRow, fetchedRow)
			z.side = z.nextSide()
		}
	}
	return nil, nil
}

// nextSide the side after the current side.
func (z *zigzagJoiner) nextSide() int {
	return (z.side + 1) % z.numTables
}

// prevSide returns the side before the current side.
func (z *zigzagJoiner) prevSide() int {
	return z.sideBefore(z.side)
}

// sideBefore returns the side before the given side.
func (z *zigzagJoiner) sideBefore(side int) int {
	return (side + z.numTables - 1) % z.numTables
}

// Adds all rows that match the current base row from the specified side into
// the appropriate container.
// Returns the first row that doesn't match.
func (z *zigzagJoiner) collectAllMatches(
	ctx context.Context, side int,
) (sqlbase.EncDatumRow, error) {
	matched := true
	var row sqlbase.EncDatumRow
	for matched {
		var err error
		fetchedRow, _, _, err := z.infos[side].fetcher.NextRow(ctx)
		row = z.rowAlloc.AllocRow(len(fetchedRow))
		copy(row, fetchedRow)
		if err != nil {
			return nil, err
		}
		matched, err = z.matchBase(row)
		if err != nil {
			return nil, err
		}
		if matched {
			z.infos[side].container = append(z.infos[side].container, row)
			z.infos[side].containerIdx++
		}
	}
	return row, nil
}

// Next is part of the RowSource interface.
func (z *zigzagJoiner) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	txn := z.flowCtx.txn

	if !z.started {
		z.started = true

		// Fetch initial batch.
		z.batchSize = 5
		// TODO(pbardea): set the traceKV flag when requested by the session.
		err := z.curInfo().fetcher.StartScan(
			z.ctx,
			txn,
			roachpb.Spans{roachpb.Span{Key: z.curInfo().key, EndKey: z.curInfo().endKey}},
			true, /* batch limit */
			z.batchSize,
			false, /* traceKV */
		)
		if err != nil {
			log.Errorf(z.ctx, "scan error: %s", err)
			return nil, z.producerMeta(err)
		}
		fetchedRow, _, _, err := z.infos[0].fetcher.NextRow(z.ctx)
		if err != nil {
			err = scrub.UnwrapScrubError(err)
			return nil, z.producerMeta(err)
		}
		z.baseRow = z.rowAlloc.AllocRow(len(fetchedRow))
		copy(z.baseRow, fetchedRow)
		z.side = z.nextSide()
	}

	if z.closed {
		return nil, z.producerMeta(nil /* err */)
	}

	for {
		row, meta := z.nextRow(z.ctx, txn)
		if z.closed || meta != nil {
			return nil, meta
		}
		if row == nil {
			return nil, z.producerMeta(nil /* err */)
		}

		outRow, status, err := z.out.ProcessRow(z.ctx, row)
		if err != nil {
			return nil, z.producerMeta(err)
		}
		switch status {
		case NeedMoreRows:
			if outRow == nil && err == nil {
				continue
			}
		case DrainRequested:
			continue
		}
		return outRow, nil
	}
}

// ConsumerDone is part of the RowSource interface.
func (z *zigzagJoiner) ConsumerDone() {
}

// ConsumerClosed is part of the RowSource interface.
func (z *zigzagJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	z.close()
}
