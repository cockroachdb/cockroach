// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Consider the schema:
//
// CREATE TABLE abcd (a INT, b INT, c INT, d INT, PRIMARY KEY (a, b),
// INDEX c_idx (c), INDEX d_idx (d));
//
// and the query:
//
// SELECT * FROM abcd@c_idx WHERE c = 2 AND d = 3;
//
//
// Without a zigzag joiner, this query would previously execute: index scan on
// `c_idx`, followed by an index join on the primary index, then filter out rows
// where `d ≠ 3`.
// This plan scans through all values in `c_idx` where `c = 2`, however if among
// these rows there are not many where `d = 3` a lot of rows are unnecessarily
// scanned. A zigzag join allows us to skip many of these rows and many times
// will also render the index join unnecessary, by making use of `d_idx`.
//
// To see how this query would be executed, consider the equivalent query:
//
// SELECT t1.* FROM abcd@c_idx AS t1 JOIN abcd@d_idx ON t1.a = t2.a AND
// t1.b = t2.b WHERE t1.c = 2 AND t2.d = 3;
//
// A zigzag joiner takes 2 sides as input. In the example above, the join would
// be between `c_idx` and `d_idx`. Both sides will have the same equality
// columns: (a, b) since that is the primary key of the table. The `c_idx` side
// fixes a prefix (c) to a specific value (2), as does the `d_idx` side (d = 3).
// This can be summarized as:
// Side 1:
//
// - Index: `abcd@c_idx`, with columns (c | a, b)
// - Equality columns: (a, b)
// - Fixed columns: (c)
// - Fixed values: (2)
//
// Side 2:
//
// - Index: `abcd@d_idx`, with columns (d | a, b)
// - Equality columns: (a, b)
// - Fixed columns: (d)
// - Fixed values: (3)
//
// The actual execution can be visualized below :
//
//   c_idx         d_idx
// c | a, b       d | a, b
// ============= ============
// --> 2   1  1 ----> 3   1  1 ---+ X
//                                |
// +----------------- 3   4  2 <--+
// |                  3   4  3
// |                  3   5  6
// |                  3   7  2
// +--> 2  8  2 -------------------+
//                                 |
// +----------------- 3   8  3 ----+
// |
// +-> 2  9  3 -----> 3   9  3 --+ X
//                               |
//                 nil (Done) <--+
//
//
// - The execution starts by fetching the (2, 1, 1) row from c_idx. This is the
// first row fetched when an index lookup in `c_idx` where `c = 2`. Let this be
// the `baseRow`. This is the current contender for other rows to match.
// - An index lookup is performed on `d_idx` for the first row where `d = 3`
// that has equality column (a, b) values greater than or equal to (1, 1), which
// are the values of the equality columns of the `baseRow`.
// - The index lookup on `d_idx` retrieves the row (3, 1, 1)
// - The equality columns of the (3, 1, 1) row are compared against the equality
// columns of the base row (2, 1, 1). They are found to match.
// - Since both indexes are sorted, once a match is found it is guaranteed that
// all rows which match the `baseRow` will follow the two that were matched. All
// of the possible matches are found and put into a container that is maintained
// by each side. Since the equality columns is the primary key, only one match
// can be produced in this example. The left container now contains (2, 1, 1)
// and the right container now contains (3, 1, 1).
// - The cross-product of the containers is emitted. In this case, just the row
// (1, 1, 2, 3).
// - The side with the latest match, (3, 1, 1) in this case, will fetch the next
// row in the index (3, 4, 2). This becomes the new `baseRow`.
// - As before, an index lookup is performed on the other side `c_idx` for the
// first row where `c = 2` that has the equality column (a, b) values greater
// than or equal to (4, 2), which are the values of the equality columns of the
// `baseRow`. In this example, the processor can skip a group of rows that are
// guaranteed to not be in the output of the join.
// - The first row found is (2, 8, 2). Since the equality columns do not match
// to the base row ((8, 2) ≠ (4, 2)), this row becomes the new base row and the
// process is repeated.
// - We are done when the index lookup returns `nil`. There were no more rows in
// this index that could satisfy the join.
//
//
// When Can a Zigzag Join Be Planned:
//
// Every side of a zigzag join has fixed columns, equality columns, and index
// columns.
//
// A zigzag join can be used when for each side, there exists an index with the
// prefix (fixed columns + equality columns). This guarantees that the rows on
// both sides of the join, when iterating through the index, will have both
// sides of the join sorted by its equality columns.
//
// When Should a Zigzag Join Be Planned:
//
// The intuition behind when a zigzag join should be used is when the carnality
// of the output is much smaller than the size of either side of the join. If
// this is not the case, it may end up being slower than other joins because it
// is constantly alternating between sides of the join. Alternatively, the
// zigzag join should be used in cases where an index scan would be used with a
// filter on the results. Examples would be inverted index JSON queries and
// queries such as the `SELECT * FROM abcd@c_idx WHERE c = 2 AND d = 3;` example
// above.
//
// For a description of index columns, refer to Appendix A.
//
// Additional Cases
//
// Normal Joins
// This algorithm can also be applied to normal joins such as:
//
// SELECT t1.a, t1.b FROM abcd t1 JOIN abcd t2 ON t1.b = t2.a WHERE t1.a = 3;
//
// (Using the same schema as above).
//
// The sides of this zigzag join would be:
// Side 1:
//
// - Index: `abcd@primary`
// - Equality columns: (b)
// - Fixed columns: (a)
// - Fixed values: (3)
//
// Side 2:
//
// - Index: `abcd@primary`
// - Equality columns: (a)
// - Fixed columns: None
//- Fixed values: None
//
// Note: If the query were to `SELECT *` instead of `SELECT a, b` a further
// index join would be needed, but this index join would only be applied on the
// necessary rows.
//
// No Fixed Columns
// As shown above, a side can have no fixed columns. This means that the
// equality columns will be a prefix of the index. Specifically this means that
// all rows in the index will be considered rather than doing a lookup on a
// specific prefix.
//
// Multi-Way Join [not implemented]:
// Also note that this algorithm can be extended to support a multi-way join by
// performing index lookups in a round-robin fashion iterating through all of
// the sides until a match is found on all sides of the join. It is expected
// that a zigzag join’s utility will increase as the number of sides increases
// because more rows will be able to be skipped.
//
//
// Appendix A: Indexes
//
// The zigzag joins makes use of multiple indexes. Each index is composed of a
// set of explicit columns, and a set of implicit columns. The union of these
// sets will be referred to as index columns.
//
// The purpose of implicit columns in indexes is to provide unique keys for
// RocksDB as well as to be able to relate the specified row back to the primary
// index where the full row is stored.
//
// Consider the schema:
//
// CREATE TABLE abcd (a INT, b INT, c INT, d INT, (a, b) PRIMARY KEY,
// INDEX c_idx (c), INDEX da_idx (d, a), INDEX db_idx (d, b));
//
// The following three indexes are created:
//
// - Primary Index: (Key format: `/Table/abcd/primary/<a_val>/<b_val>/`)
// - Explicit columns: (a, b)
// - Implicit columns: None
// - Index columns: (a, b)
// - c_idx: (Key format: `/Table/abcd/c_idx/<c_val>/<a_val>/<b_val>/`)
// - Explicit columns: (c)
// - Implicit columns: (a, b)
// - Index columns (c, a, b)
// - da_idx: (Key format: `/Table/abcd/d_idx/<d_val>/<a_val>/<b_val>/`)
// - Explicit columns: (d, a)
// - Implicit columns (b)
// - Index columns: (d, a, b)
// - db_idx: (Key format: `/Table/abcd/d_idx/<d_val>/<b_val>/<a_val>/`)
// - Explicit columns: (d, b)
// - Implicit columns (a)
// - Index columns: (d, b, a)
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
	infos []*zigzagJoinerInfo

	// Base row stores the that the algorithm is compared against and is updated
	// with every change of side.
	baseRow sqlbase.EncDatumRow

	rowAlloc sqlbase.EncDatumRowAlloc

	// TODO(andrei): get rid of this field and move the actions it gates into the
	// Start() method.
	started bool

	// returnedMeta contains all the metadata that zigzag joiner has emitted.
	returnedMeta []execinfrapb.ProducerMetadata
}

// Batch size is a parameter which determines how many rows should be fetched
// at a time. Increasing this will improve performance for when matched rows
// are grouped together, but increasing this too much will result in fetching
// too many rows and therefore skipping less rows.
const zigzagJoinerBatchSize = 5

var _ execinfra.Processor = &zigzagJoiner{}
var _ execinfra.RowSource = &zigzagJoiner{}
var _ execinfrapb.MetadataSource = &zigzagJoiner{}
var _ execinfra.OpNode = &zigzagJoiner{}

const zigzagJoinerProcName = "zigzagJoiner"

// newZigzagJoiner creates a new zigzag joiner given a spec and an EncDatumRow
// holding the values of the prefix columns of the index specified in the spec.
func newZigzagJoiner(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.ZigzagJoinerSpec,
	fixedValues []sqlbase.EncDatumRow,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*zigzagJoiner, error) {
	z := &zigzagJoiner{}

	leftColumnTypes := spec.Tables[0].ColumnTypes()
	rightColumnTypes := spec.Tables[1].ColumnTypes()
	leftEqCols := make([]uint32, 0, len(spec.EqColumns[0].Columns))
	rightEqCols := make([]uint32, 0, len(spec.EqColumns[1].Columns))
	err := z.joinerBase.init(
		z, /* self */
		flowCtx,
		processorID,
		leftColumnTypes,
		rightColumnTypes,
		spec.Type,
		spec.OnExpr,
		leftEqCols,
		rightEqCols,
		0, /* numMerged */
		post,
		output,
		execinfra.ProcStateOpts{}, // zigzagJoiner doesn't have any inputs to drain.
	)
	if err != nil {
		return nil, err
	}

	z.numTables = len(spec.Tables)
	z.infos = make([]*zigzagJoinerInfo, z.numTables)
	z.returnedMeta = make([]execinfrapb.ProducerMetadata, 0, 1)

	for i := range z.infos {
		z.infos[i] = &zigzagJoinerInfo{}
	}

	colOffset := 0
	for i := 0; i < z.numTables; i++ {
		if fixedValues != nil && i < len(fixedValues) {
			// Useful for testing. In cases where we plan a zigzagJoin in
			// the planner, we specify fixed values as ValuesCoreSpecs in
			// the spec itself.
			z.infos[i].fixedValues = fixedValues[i]
		} else if i < len(spec.FixedValues) {
			z.infos[i].fixedValues, err = valuesSpecToEncDatum(spec.FixedValues[i])
			if err != nil {
				return nil, err
			}
		}
		if err := z.setupInfo(flowCtx, spec, i, colOffset); err != nil {
			return nil, err
		}
		colOffset += len(z.infos[i].table.Columns)
	}
	z.side = 0
	return z, nil
}

// Helper function to convert a values spec containing one tuple into EncDatums for
// each cell. Note that this function assumes that there is only one tuple in the
// ValuesSpec (i.e. the way fixed values are encoded in the ZigzagJoinSpec).
func valuesSpecToEncDatum(
	valuesSpec *execinfrapb.ValuesCoreSpec,
) (res []sqlbase.EncDatum, err error) {
	res = make([]sqlbase.EncDatum, len(valuesSpec.Columns))
	rem := valuesSpec.RawBytes[0]
	for i, colInfo := range valuesSpec.Columns {
		res[i], rem, err = sqlbase.EncDatumFromBuffer(colInfo.Type, colInfo.Encoding, rem)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// Start is part of the RowSource interface.
func (z *zigzagJoiner) Start(ctx context.Context) context.Context {
	ctx = z.StartInternal(ctx, zigzagJoinerProcName)
	z.evalCtx = z.FlowCtx.NewEvalCtx()
	z.cancelChecker = sqlbase.NewCancelChecker(ctx)
	log.VEventf(ctx, 2, "starting zigzag joiner run")
	return ctx
}

// zigzagJoinerInfo contains all the information that needs to be
// stored for each side of the join.
type zigzagJoinerInfo struct {
	fetcher    row.Fetcher
	alloc      *sqlbase.DatumAlloc
	table      *sqlbase.TableDescriptor
	index      *sqlbase.IndexDescriptor
	indexTypes []*types.T
	indexDirs  []sqlbase.IndexDescriptor_Direction

	// Stores one batch of matches at a time. When all the rows are collected
	// the cartesian product of the containers will be emitted.
	container sqlbase.EncDatumRowContainer

	// eqColumns is the ordinal positions of the equality columns.
	eqColumns []uint32

	// Prefix of the index key that has fixed values.
	fixedValues sqlbase.EncDatumRow

	// The current key being fetched by this side.
	key roachpb.Key
	// The prefix of the key which includes the table and index IDs.
	prefix []byte
	// endKey marks where this side should stop fetching, taking into account the
	// fixedValues.
	endKey roachpb.Key

	spanBuilder *span.Builder
}

// Setup the curInfo struct for the current z.side, which specifies the side
// number of the curInfo to set up.
// Side specifies which the spec is associated with.
// colOffset is specified to determine the appropriate range of output columns
// to process. It is the number of columns in the tables of all previous sides
// of the join.
func (z *zigzagJoiner) setupInfo(
	flowCtx *execinfra.FlowCtx, spec *execinfrapb.ZigzagJoinerSpec, side int, colOffset int,
) error {
	z.side = side
	info := z.infos[side]

	info.alloc = &sqlbase.DatumAlloc{}
	info.table = &spec.Tables[side]
	info.eqColumns = spec.EqColumns[side].Columns
	indexOrdinal := spec.IndexOrdinals[side]
	if indexOrdinal == 0 {
		info.index = &info.table.PrimaryIndex
	} else {
		info.index = &info.table.Indexes[indexOrdinal-1]
	}

	var columnIDs []sqlbase.ColumnID
	columnIDs, info.indexDirs = info.index.FullColumnIDs()
	info.indexTypes = make([]*types.T, len(columnIDs))
	columnTypes := info.table.ColumnTypes()
	colIdxMap := info.table.ColumnIdxMap()
	for i, columnID := range columnIDs {
		info.indexTypes[i] = columnTypes[colIdxMap[columnID]]
	}

	// Add the outputted columns.
	neededCols := util.MakeFastIntSet()
	outCols := z.Out.NeededColumns()
	maxCol := colOffset + len(info.table.Columns)
	for i, ok := outCols.Next(colOffset); ok && i < maxCol; i, ok = outCols.Next(i + 1) {
		neededCols.Add(i - colOffset)
	}

	// Add the fixed columns.
	for i := 0; i < len(info.fixedValues); i++ {
		neededCols.Add(colIdxMap[columnIDs[i]])
	}

	// Add the equality columns.
	for _, col := range info.eqColumns {
		neededCols.Add(int(col))
	}

	// Setup the RowContainers.
	info.container.Reset()

	info.spanBuilder = span.MakeBuilder(flowCtx.Codec(), info.table, info.index)

	// Setup the Fetcher.
	_, _, err := initRowFetcher(
		flowCtx,
		&info.fetcher,
		info.table,
		int(indexOrdinal),
		info.table.ColumnIdxMap(),
		false, /* reverse */
		neededCols,
		false, /* check */
		info.alloc,
		execinfra.ScanVisibilityPublic,
		// NB: zigzag joins are disabled when a row-level locking clause is
		// supplied, so there is no locking strength on *ZigzagJoinerSpec.
		sqlbase.ScanLockingStrength_FOR_NONE,
	)
	if err != nil {
		return err
	}

	info.prefix = sqlbase.MakeIndexKeyPrefix(flowCtx.Codec(), info.table, info.index.ID)
	span, err := z.produceSpanFromBaseRow()

	if err != nil {
		return err
	}
	info.key = span.Key
	info.endKey = span.EndKey
	return nil
}

func (z *zigzagJoiner) close() {
	if z.InternalClose() {
		log.VEventf(z.Ctx, 2, "exiting zigzag joiner run")
	}
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error. It is ok for err to be
// nil indicating that we're done producing rows even though no error occurred.
func (z *zigzagJoiner) producerMeta(err error) *execinfrapb.ProducerMetadata {
	var meta *execinfrapb.ProducerMetadata
	if !z.Closed {
		if err != nil {
			meta = &execinfrapb.ProducerMetadata{Err: err}
		} else if trace := execinfra.GetTraceData(z.Ctx); trace != nil {
			meta = &execinfrapb.ProducerMetadata{TraceData: trace}
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		z.close()
	}
	if meta != nil {
		z.returnedMeta = append(z.returnedMeta, *meta)
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

// Fetches the first row from the current rowFetcher that does not have any of
// the equality columns set to null.
func (z *zigzagJoiner) fetchRow(ctx context.Context) (sqlbase.EncDatumRow, error) {
	return z.fetchRowFromSide(ctx, z.side)
}

func (z *zigzagJoiner) fetchRowFromSide(
	ctx context.Context, side int,
) (fetchedRow sqlbase.EncDatumRow, err error) {
	// Keep fetching until a row is found that does not have null in an equality
	// column.
	hasNull := func(row sqlbase.EncDatumRow) bool {
		for _, c := range z.infos[side].eqColumns {
			if row[c].IsNull() {
				return true
			}
		}
		return false
	}
	for {
		fetchedRow, _, _, err = z.infos[side].fetcher.NextRow(ctx)
		if fetchedRow == nil || err != nil {
			return fetchedRow, err
		}
		if !hasNull(fetchedRow) {
			break
		}
	}
	return fetchedRow, nil
}

// Return the datums from the equality columns from a given non-empty row
// from the specified side.
func (z *zigzagJoiner) extractEqDatums(row sqlbase.EncDatumRow, side int) sqlbase.EncDatumRow {
	eqCols := z.infos[side].eqColumns
	eqDatums := make(sqlbase.EncDatumRow, len(eqCols))
	for i, col := range eqCols {
		eqDatums[i] = row[col]
	}
	return eqDatums
}

// Generates a Key for an inverted index from the passed datums and side
// info. Used by produceKeyFromBaseRow.
func (z *zigzagJoiner) produceInvertedIndexKey(
	info *zigzagJoinerInfo, datums sqlbase.EncDatumRow,
) (roachpb.Span, error) {
	// For inverted indexes, the JSON field (first column in the index) is
	// encoded a little differently. We need to explicitly call
	// EncodeInvertedIndexKeys to generate the prefix. The rest of the
	// index key containing the remaining neededDatums can be generated
	// and appended using EncodeColumns.
	colMap := make(map[sqlbase.ColumnID]int)
	decodedDatums := make([]tree.Datum, len(datums))

	// Ensure all EncDatums have been decoded.
	for i, encDatum := range datums {
		err := encDatum.EnsureDecoded(info.indexTypes[i], info.alloc)
		if err != nil {
			return roachpb.Span{}, err
		}

		decodedDatums[i] = encDatum.Datum
		if i < len(info.index.ColumnIDs) {
			colMap[info.index.ColumnIDs[i]] = i
		} else {
			// This column's value will be encoded in the second part (i.e.
			// EncodeColumns).
			colMap[info.index.ExtraColumnIDs[i-len(info.index.ColumnIDs)]] = i
		}
	}

	keys, err := sqlbase.EncodeInvertedIndexKeys(
		info.index,
		colMap,
		decodedDatums,
		info.prefix,
	)
	if err != nil {
		return roachpb.Span{}, err
	}
	if len(keys) != 1 {
		return roachpb.Span{}, errors.Errorf("%d fixed values passed in for inverted index", len(keys))
	}

	// Append remaining (non-JSON) datums to the key.
	keyBytes, _, err := sqlbase.EncodeColumns(
		info.index.ExtraColumnIDs[:len(datums)-1],
		info.indexDirs[1:],
		colMap,
		decodedDatums,
		keys[0],
	)
	key := roachpb.Key(keyBytes)
	return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}, err
}

// Generates a Key, corresponding to the current `z.baseRow` in
// the index on the current side.
func (z *zigzagJoiner) produceSpanFromBaseRow() (roachpb.Span, error) {
	info := z.infos[z.side]
	neededDatums := info.fixedValues
	if z.baseRow != nil {
		eqDatums := z.extractEqDatums(z.baseRow, z.prevSide())
		neededDatums = append(neededDatums, eqDatums...)
	}

	// Construct correct row by concatenating right fixed datums with
	// primary key extracted from `row`.
	if info.index.Type == sqlbase.IndexDescriptor_INVERTED {
		return z.produceInvertedIndexKey(info, neededDatums)
	}

	s, _, err := info.spanBuilder.SpanFromEncDatums(neededDatums, len(neededDatums))
	return s, err
}

// Returns the column types of the equality columns.
func (zi *zigzagJoinerInfo) eqColTypes() []*types.T {
	eqColTypes := make([]*types.T, len(zi.eqColumns))
	colTypes := zi.table.ColumnTypes()
	for i := range eqColTypes {
		eqColTypes[i] = colTypes[zi.eqColumns[i]]
	}
	return eqColTypes
}

// Returns the ordering of the equality columns.
func (zi *zigzagJoinerInfo) eqOrdering() (sqlbase.ColumnOrdering, error) {
	ordering := make(sqlbase.ColumnOrdering, len(zi.eqColumns))
	for i := range zi.eqColumns {
		colID := zi.table.Columns[zi.eqColumns[i]].ID
		// Search the index columns, then the primary keys to find an ordering for
		// the current column, 'colID'.
		var direction encoding.Direction
		var err error
		if idx := findColumnID(zi.index.ColumnIDs, colID); idx != -1 {
			direction, err = zi.index.ColumnDirections[idx].ToEncodingDirection()
			if err != nil {
				return nil, err
			}
		} else if idx := findColumnID(zi.table.PrimaryIndex.ColumnIDs, colID); idx != -1 {
			direction, err = zi.table.PrimaryIndex.ColumnDirections[idx].ToEncodingDirection()
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errors.New("ordering of equality column not found in index or primary key")
		}
		ordering[i] = sqlbase.ColumnOrderInfo{ColIdx: i, Direction: direction}
	}
	return ordering, nil
}

// matchBase compares the equality columns of the given row to `z.baseRow`,
// which is the previously fetched row. Returns whether or not the rows match
// on the equality columns. The given row is from the specified `side`.
func (z *zigzagJoiner) matchBase(curRow sqlbase.EncDatumRow, side int) (bool, error) {
	if len(curRow) == 0 {
		return false, nil
	}

	prevEqDatums := z.extractEqDatums(z.baseRow, z.prevSide())
	curEqDatums := z.extractEqDatums(curRow, side)

	eqColTypes := z.infos[side].eqColTypes()
	ordering, err := z.infos[side].eqOrdering()
	if err != nil {
		return false, err
	}

	// Compare the equality columns of the baseRow to that of the curRow.
	da := &sqlbase.DatumAlloc{}
	cmp, err := prevEqDatums.Compare(eqColTypes, da, ordering, z.FlowCtx.EvalCtx, curEqDatums)
	if err != nil {
		return false, err
	}
	return cmp == 0, nil
}

// emitFromContainers returns the next row that is to be emitted from those
// already stored in the containers.
// Since this is called after the side has been incremented, it produces the
// cartesian product of the previous side's container and the side before that
// one.
func (z *zigzagJoiner) emitFromContainers() (sqlbase.EncDatumRow, error) {
	right := z.prevSide()
	left := z.sideBefore(right)
	for !z.infos[right].container.IsEmpty() {
		leftRow := z.infos[left].container.Pop()
		rightRow := z.infos[right].container.Peek()

		// TODO(pbardea): Extend this logic to support multi-way joins.
		if left == int(rightSide) {
			leftRow, rightRow = rightRow, leftRow
		}
		renderedRow, err := z.render(leftRow, rightRow)
		if err != nil {
			return nil, err
		}
		if z.infos[left].container.IsEmpty() {
			z.infos[right].container.Pop()
		}
		if renderedRow != nil {
			// The pair satisfied the onExpr.
			return renderedRow, nil
		}
	}

	// All matches have been returned since the left index is negative.
	// Empty the containers to reset their contents.
	z.infos[left].container.Reset()
	z.infos[right].container.Reset()

	return nil, nil
}

// nextRow fetches the nextRow to emit from the join. It iterates through all
// sides until a match is found then emits the results of the match one result
// at a time.
func (z *zigzagJoiner) nextRow(
	ctx context.Context, txn *kv.Txn,
) (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for {
		if err := z.cancelChecker.Check(); err != nil {
			return nil, &execinfrapb.ProducerMetadata{Err: err}
		}

		// Check if there are any rows built up in the containers that need to be
		// emitted.
		if rowToEmit, err := z.emitFromContainers(); err != nil {
			return nil, z.producerMeta(err)
		} else if rowToEmit != nil {
			return rowToEmit, nil
		}

		// If the baseRow is nil, the last fetched row was nil. That means that
		// that there are no more matches in the join so we break and return nil
		// to indicate that we are done to the caller.
		if len(z.baseRow) == 0 {
			return nil, nil
		}

		curInfo := z.infos[z.side]

		// Generate a key from the last row seen from the last side. We're about to
		// use it to jump to the next possible match on the current side.
		span, err := z.produceSpanFromBaseRow()
		if err != nil {
			return nil, z.producerMeta(err)
		}
		curInfo.key = span.Key

		err = curInfo.fetcher.StartScan(
			ctx,
			txn,
			roachpb.Spans{roachpb.Span{Key: curInfo.key, EndKey: curInfo.endKey}},
			true, /* batch limit */
			zigzagJoinerBatchSize,
			z.FlowCtx.TraceKV,
		)
		if err != nil {
			return nil, z.producerMeta(err)
		}

		fetchedRow, err := z.fetchRow(ctx)
		if err != nil {
			return nil, z.producerMeta(err)
		}
		// If the next possible match on the current side that matches the previous
		// row is `nil`, that means that there are no more matches in the join so
		// we return nil to indicate that to the caller.
		if fetchedRow == nil {
			return nil, nil
		}

		matched, err := z.matchBase(fetchedRow, z.side)
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
			z.infos[prevSide].container.Push(prevRow)
			curRow := z.rowAlloc.AllocRow(len(fetchedRow))
			copy(curRow, fetchedRow)
			curInfo.container.Push(curRow)

			// After collecting all matches from each side, the first unmatched
			// row from each side is returned. We want the new baseRow to be
			// the latest of these rows since no match can occur before the latter
			// of the two rows.
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
			eqColTypes := curInfo.eqColTypes()
			ordering, err := curInfo.eqOrdering()
			if err != nil {
				return nil, z.producerMeta(err)
			}
			da := &sqlbase.DatumAlloc{}
			cmp, err := prevEqCols.Compare(eqColTypes, da, ordering, z.FlowCtx.EvalCtx, currentEqCols)
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
}

// nextSide returns the side after the current side.
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
		fetchedRow, err := z.fetchRowFromSide(ctx, side)
		row = z.rowAlloc.AllocRow(len(fetchedRow))
		copy(row, fetchedRow)
		if err != nil {
			return nil, err
		}
		matched, err = z.matchBase(row, side)
		if err != nil {
			return nil, err
		}
		if matched {
			z.infos[side].container.Push(row)
		}
	}
	return row, nil
}

// Next is part of the RowSource interface.
func (z *zigzagJoiner) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	txn := z.FlowCtx.Txn

	if !z.started {
		z.started = true

		curInfo := z.infos[z.side]
		// Fetch initial batch.
		err := curInfo.fetcher.StartScan(
			z.Ctx,
			txn,
			roachpb.Spans{roachpb.Span{Key: curInfo.key, EndKey: curInfo.endKey}},
			true, /* batch limit */
			zigzagJoinerBatchSize,
			z.FlowCtx.TraceKV,
		)
		if err != nil {
			log.Errorf(z.Ctx, "scan error: %s", err)
			return nil, z.producerMeta(err)
		}
		fetchedRow, err := z.fetchRow(z.Ctx)
		if err != nil {
			err = scrub.UnwrapScrubError(err)
			return nil, z.producerMeta(err)
		}
		z.baseRow = z.rowAlloc.AllocRow(len(fetchedRow))
		copy(z.baseRow, fetchedRow)
		z.side = z.nextSide()
	}

	if z.Closed {
		return nil, z.producerMeta(nil /* err */)
	}

	for {
		row, meta := z.nextRow(z.Ctx, txn)
		if z.Closed || meta != nil {
			if meta != nil {
				z.returnedMeta = append(z.returnedMeta, *meta)
			}
			return nil, meta
		}
		if row == nil {
			z.MoveToDraining(nil /* err */)
			break
		}

		outRow := z.ProcessRowHelper(row)
		if outRow == nil {
			continue
		}
		return outRow, nil
	}
	meta := z.DrainHelper()
	if meta != nil {
		z.returnedMeta = append(z.returnedMeta, *meta)
	}
	return nil, meta
}

// ConsumerClosed is part of the RowSource interface.
func (z *zigzagJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	z.close()
}

// DrainMeta is part of the MetadataSource interface.
func (z *zigzagJoiner) DrainMeta(_ context.Context) []execinfrapb.ProducerMetadata {
	return z.returnedMeta
}

// ChildCount is part of the execinfra.OpNode interface.
func (z *zigzagJoiner) ChildCount(verbose bool) int {
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (z *zigzagJoiner) Child(nth int, verbose bool) execinfra.OpNode {
	panic(fmt.Sprintf("invalid index %d", nth))
}
