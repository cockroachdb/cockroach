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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
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
// The intuition behind when a zigzag join should be used is when the cardinality
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

	cancelChecker *cancelchecker.CancelChecker

	// numTables stores the number of tables involved in the join.
	numTables int
	// side keeps track of which side is being processed.
	side int

	// Stores relevant information for each side of the join including table
	// descriptors, index IDs, rowFetchers, and more. See zigzagJoinInfo for
	// more information.
	infos []*zigzagJoinerInfo

	// baseRow stores the row that the algorithm is compared against and is
	// updated with every change of side.
	baseRow rowenc.EncDatumRow

	rowAlloc           rowenc.EncDatumRowAlloc
	fetchedInititalRow bool
}

// zigzagJoinerBatchSize is a parameter which determines how many rows should
// be fetched at a time. Increasing this will improve performance for when
// matched rows are grouped together, but increasing this too much will result
// in fetching too many rows and therefore skipping less rows.
var zigzagJoinerBatchSize = int64(util.ConstantWithMetamorphicTestValue(
	"zig-zag-joiner-batch-size",
	5, /* defaultValue */
	1, /* metamorphicValue */
))

var _ execinfra.Processor = &zigzagJoiner{}
var _ execinfra.RowSource = &zigzagJoiner{}
var _ execinfra.OpNode = &zigzagJoiner{}

const zigzagJoinerProcName = "zigzagJoiner"

// newZigzagJoiner creates a new zigzag joiner given a spec and an EncDatumRow
// holding the values of the prefix columns of the index specified in the spec.
func newZigzagJoiner(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.ZigzagJoinerSpec,
	fixedValues []rowenc.EncDatumRow,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*zigzagJoiner, error) {
	// TODO(ajwerner): Utilize a cached copy of these tables.
	tables := spec.BuildTableDescriptors()
	if len(tables) != 2 {
		return nil, errors.AssertionFailedf("zigzag joins only of two tables (or indexes) are supported, %d requested", len(tables))
	}
	if spec.Type != descpb.InnerJoin {
		return nil, errors.AssertionFailedf("only inner zigzag joins are supported, %s requested", spec.Type)
	}
	z := &zigzagJoiner{}

	leftColumnTypes := catalog.ColumnTypes(tables[0].PublicColumns())
	rightColumnTypes := catalog.ColumnTypes(tables[1].PublicColumns())
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
		false, /* outputContinuationColumn */
		post,
		output,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				// We need to generate metadata before closing the processor
				// because InternalClose() updates z.Ctx to the "original"
				// context.
				trailingMeta := z.generateMeta()
				z.close()
				return trailingMeta
			},
		},
	)
	if err != nil {
		return nil, err
	}

	z.numTables = len(tables)
	z.infos = make([]*zigzagJoinerInfo, z.numTables)
	for i := range z.infos {
		z.infos[i] = &zigzagJoinerInfo{}
	}

	collectingStats := false
	if execinfra.ShouldCollectStats(flowCtx.EvalCtx.Ctx(), flowCtx) {
		collectingStats = true
		z.ExecStatsForTrace = z.execStatsForTrace
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
		if err := z.setupInfo(flowCtx, spec, i, colOffset, tables, collectingStats); err != nil {
			return nil, err
		}
		colOffset += len(z.infos[i].table.PublicColumns())
	}
	z.side = 0
	return z, nil
}

// Helper function to convert a values spec containing one tuple into EncDatums for
// each cell. Note that this function assumes that there is only one tuple in the
// ValuesSpec (i.e. the way fixed values are encoded in the ZigzagJoinSpec).
func valuesSpecToEncDatum(
	valuesSpec *execinfrapb.ValuesCoreSpec,
) (res []rowenc.EncDatum, err error) {
	res = make([]rowenc.EncDatum, len(valuesSpec.Columns))
	rem := valuesSpec.RawBytes[0]
	for i, colInfo := range valuesSpec.Columns {
		res[i], rem, err = rowenc.EncDatumFromBuffer(colInfo.Type, colInfo.Encoding, rem)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// Start is part of the RowSource interface.
func (z *zigzagJoiner) Start(ctx context.Context) {
	ctx = z.StartInternal(ctx, zigzagJoinerProcName)
	z.cancelChecker = cancelchecker.NewCancelChecker(ctx)
	log.VEventf(ctx, 2, "starting zigzag joiner run")
}

// zigzagJoinerInfo contains all the information that needs to be
// stored for each side of the join.
type zigzagJoinerInfo struct {
	fetcher rowFetcher
	// rowsRead is the total number of rows that this fetcher read from disk.
	rowsRead   int64
	alloc      *rowenc.DatumAlloc
	table      catalog.TableDescriptor
	index      catalog.Index
	indexTypes []*types.T
	indexDirs  []descpb.IndexDescriptor_Direction

	// Stores one batch of matches at a time. When all the rows are collected
	// the cartesian product of the containers will be emitted.
	container rowenc.EncDatumRowContainer

	// eqColumns is the ordinal positions of the equality columns.
	eqColumns []uint32

	// Prefix of the index key that has fixed values.
	fixedValues rowenc.EncDatumRow

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
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ZigzagJoinerSpec,
	side int,
	colOffset int,
	tables []catalog.TableDescriptor,
	collectingStats bool,
) error {
	z.side = side
	info := z.infos[side]

	info.alloc = &rowenc.DatumAlloc{}
	info.table = tables[side]
	info.eqColumns = spec.EqColumns[side].Columns
	indexOrdinal := spec.IndexOrdinals[side]
	info.index = info.table.ActiveIndexes()[indexOrdinal]

	var columnIDs []descpb.ColumnID
	columnIDs, info.indexDirs = catalog.FullIndexColumnIDs(info.index)
	info.indexTypes = make([]*types.T, len(columnIDs))
	columnTypes := catalog.ColumnTypes(info.table.PublicColumns())
	colIdxMap := catalog.ColumnIDToOrdinalMap(info.table.PublicColumns())
	for i, columnID := range columnIDs {
		if info.index.GetType() == descpb.IndexDescriptor_INVERTED &&
			columnID == info.index.InvertedColumnID() {
			// Inverted key columns have type Bytes.
			info.indexTypes[i] = types.Bytes
		} else {
			info.indexTypes[i] = columnTypes[colIdxMap.GetDefault(columnID)]
		}
	}

	// Add the outputted columns.
	neededCols := util.MakeFastIntSet()
	outCols := z.OutputHelper.NeededColumns()
	maxCol := colOffset + len(info.table.PublicColumns())
	for i, ok := outCols.Next(colOffset); ok && i < maxCol; i, ok = outCols.Next(i + 1) {
		neededCols.Add(i - colOffset)
	}

	// Add the fixed columns.
	for i := 0; i < len(info.fixedValues); i++ {
		neededCols.Add(colIdxMap.GetDefault(columnIDs[i]))
	}

	// Add the equality columns.
	for _, col := range info.eqColumns {
		neededCols.Add(int(col))
	}

	// Setup the RowContainers.
	info.container.Reset()

	info.spanBuilder = span.MakeBuilder(flowCtx.EvalCtx, flowCtx.Codec(), info.table, info.index)

	// Setup the Fetcher.
	var fetcher row.Fetcher
	_, _, err := initRowFetcher(
		flowCtx,
		&fetcher,
		info.table,
		int(indexOrdinal),
		catalog.ColumnIDToOrdinalMap(info.table.PublicColumns()),
		false, /* reverse */
		neededCols,
		false, /* check */
		flowCtx.EvalCtx.Mon,
		info.alloc,
		execinfra.ScanVisibilityPublic,
		// NB: zigzag joins are disabled when a row-level locking clause is
		// supplied, so there is no locking strength on *ZigzagJoinerSpec.
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		false, /* withSystemColumns */
		nil,   /* virtualColumn */
	)
	if err != nil {
		return err
	}

	if collectingStats {
		info.fetcher = newRowFetcherStatCollector(&fetcher)
	} else {
		info.fetcher = &fetcher
	}

	info.prefix = rowenc.MakeIndexKeyPrefix(flowCtx.Codec(), info.table, info.index.GetID())
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
		for i := range z.infos {
			z.infos[i].fetcher.Close(z.Ctx)
		}
		log.VEventf(z.Ctx, 2, "exiting zigzag joiner run")
	}
}

func findColumnOrdinalInIndex(index catalog.Index, t descpb.ColumnID) int {
	for i := 0; i < index.NumKeyColumns(); i++ {
		if index.GetKeyColumnID(i) == t {
			return i
		}
	}
	return -1
}

// Fetches the first row from the current rowFetcher that does not have any of
// the equality columns set to null.
func (z *zigzagJoiner) fetchRow(ctx context.Context) (rowenc.EncDatumRow, error) {
	return z.fetchRowFromSide(ctx, z.side)
}

func (z *zigzagJoiner) fetchRowFromSide(
	ctx context.Context, side int,
) (fetchedRow rowenc.EncDatumRow, err error) {
	// Keep fetching until a row is found that does not have null in an equality
	// column.
	hasNull := func(row rowenc.EncDatumRow) bool {
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
		z.infos[side].rowsRead++
		if !hasNull(fetchedRow) {
			break
		}
	}
	return fetchedRow, nil
}

// Return the datums from the equality columns from a given non-empty row
// from the specified side.
func (z *zigzagJoiner) extractEqDatums(row rowenc.EncDatumRow, side int) rowenc.EncDatumRow {
	eqCols := z.infos[side].eqColumns
	eqDatums := make(rowenc.EncDatumRow, len(eqCols))
	for i, col := range eqCols {
		eqDatums[i] = row[col]
	}
	return eqDatums
}

// Generates a Key for an inverted index from the passed datums and side
// info. Used by produceKeyFromBaseRow.
func (z *zigzagJoiner) produceInvertedIndexKey(
	info *zigzagJoinerInfo, datums rowenc.EncDatumRow,
) (roachpb.Span, error) {
	// For inverted indexes, the JSON field (first column in the index) is
	// encoded a little differently. We need to explicitly call
	// EncodeInvertedIndexKeys to generate the prefix. The rest of the
	// index key containing the remaining neededDatums can be generated
	// and appended using EncodeColumns.
	var colMap catalog.TableColMap
	decodedDatums := make([]tree.Datum, len(datums))

	// Ensure all EncDatums have been decoded.
	for i, encDatum := range datums {
		err := encDatum.EnsureDecoded(info.indexTypes[i], info.alloc)
		if err != nil {
			return roachpb.Span{}, err
		}

		decodedDatums[i] = encDatum.Datum
		if i < info.index.NumKeyColumns() {
			colMap.Set(info.index.GetKeyColumnID(i), i)
		} else {
			// This column's value will be encoded in the second part (i.e.
			// EncodeColumns).
			colMap.Set(info.index.GetKeySuffixColumnID(i-info.index.NumKeyColumns()), i)
		}
	}

	// First encode datums for any non-inverted prefix columns.
	keyPrefix, err := rowenc.EncodeInvertedIndexPrefixKeys(
		info.index,
		colMap,
		decodedDatums,
		info.prefix,
	)
	if err != nil {
		return roachpb.Span{}, err
	}

	// Add the inverted key, which is already encoded as a DBytes.
	invOrd, ok := colMap.Get(info.index.InvertedColumnID())
	if !ok {
		return roachpb.Span{}, errors.AssertionFailedf("inverted column not found in colMap")
	}
	invertedKey, ok := decodedDatums[invOrd].(*tree.DBytes)
	if !ok {
		return roachpb.Span{}, errors.AssertionFailedf("inverted key must be type DBytes")
	}
	keyPrefix = append(keyPrefix, []byte(*invertedKey)...)

	// Append remaining (non-JSON) datums to the key.
	keyBytes, _, err := rowenc.EncodeColumns(
		info.index.IndexDesc().KeySuffixColumnIDs[:len(datums)-1],
		info.indexDirs[1:],
		colMap,
		decodedDatums,
		keyPrefix,
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
	if info.index.GetType() == descpb.IndexDescriptor_INVERTED {
		return z.produceInvertedIndexKey(info, neededDatums)
	}

	s, _, err := info.spanBuilder.SpanFromEncDatums(neededDatums, len(neededDatums))
	return s, err
}

// Returns the column types of the equality columns.
func (zi *zigzagJoinerInfo) eqColTypes() []*types.T {
	eqColTypes := make([]*types.T, len(zi.eqColumns))
	colTypes := catalog.ColumnTypes(zi.table.PublicColumns())
	for i := range eqColTypes {
		eqColTypes[i] = colTypes[zi.eqColumns[i]]
	}
	return eqColTypes
}

// Returns the ordering of the equality columns.
func (zi *zigzagJoinerInfo) eqOrdering() (colinfo.ColumnOrdering, error) {
	ordering := make(colinfo.ColumnOrdering, len(zi.eqColumns))
	for i := range zi.eqColumns {
		colID := zi.table.PublicColumns()[zi.eqColumns[i]].GetID()
		// Search the index columns, then the primary keys to find an ordering for
		// the current column, 'colID'.
		var direction encoding.Direction
		var err error
		if idx := findColumnOrdinalInIndex(zi.index, colID); idx != -1 {
			direction, err = zi.index.GetKeyColumnDirection(idx).ToEncodingDirection()
			if err != nil {
				return nil, err
			}
		} else if idx := findColumnOrdinalInIndex(zi.table.GetPrimaryIndex(), colID); idx != -1 {
			direction, err = zi.table.GetPrimaryIndex().GetKeyColumnDirection(idx).ToEncodingDirection()
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errors.New("ordering of equality column not found in index or primary key")
		}
		ordering[i] = colinfo.ColumnOrderInfo{ColIdx: i, Direction: direction}
	}
	return ordering, nil
}

// matchBase compares the equality columns of the given row to `z.baseRow`,
// which is the previously fetched row. Returns whether or not the rows match
// on the equality columns. The given row is from the specified `side`.
func (z *zigzagJoiner) matchBase(curRow rowenc.EncDatumRow, side int) (bool, error) {
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
	da := &rowenc.DatumAlloc{}
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
func (z *zigzagJoiner) emitFromContainers() (rowenc.EncDatumRow, error) {
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
func (z *zigzagJoiner) nextRow(ctx context.Context, txn *kv.Txn) (rowenc.EncDatumRow, error) {
	for {
		if err := z.cancelChecker.Check(); err != nil {
			return nil, err
		}

		// Check if there are any rows built up in the containers that need to be
		// emitted.
		if rowToEmit, err := z.emitFromContainers(); err != nil {
			return nil, err
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
			return nil, err
		}
		curInfo.key = span.Key

		err = curInfo.fetcher.StartScan(
			ctx,
			txn,
			roachpb.Spans{roachpb.Span{Key: curInfo.key, EndKey: curInfo.endKey}},
			true, /* batch limit */
			zigzagJoinerBatchSize,
			z.FlowCtx.TraceKV,
			z.EvalCtx.TestingKnobs.ForceProductionBatchSizes,
		)
		if err != nil {
			return nil, err
		}

		fetchedRow, err := z.fetchRow(ctx)
		if err != nil {
			return nil, err
		}
		// If the next possible match on the current side that matches the previous
		// row is `nil`, that means that there are no more matches in the join so
		// we return nil to indicate that to the caller.
		if fetchedRow == nil {
			return nil, nil
		}

		matched, err := z.matchBase(fetchedRow, z.side)
		if err != nil {
			return nil, err
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
				return nil, err
			}
			curNext, err := z.collectAllMatches(ctx, z.side)
			if err != nil {
				return nil, err
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
				return nil, err
			}
			da := &rowenc.DatumAlloc{}
			cmp, err := prevEqCols.Compare(eqColTypes, da, ordering, z.FlowCtx.EvalCtx, currentEqCols)
			if err != nil {
				return nil, err
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
) (rowenc.EncDatumRow, error) {
	matched := true
	var row rowenc.EncDatumRow
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

// maybeFetchInitialRow checks whether we have already fetched an initial row
// from one of the inputs and does so if we haven't.
func (z *zigzagJoiner) maybeFetchInitialRow() error {
	if !z.fetchedInititalRow {
		z.fetchedInititalRow = true

		curInfo := z.infos[z.side]
		err := curInfo.fetcher.StartScan(
			z.Ctx,
			z.FlowCtx.Txn,
			roachpb.Spans{roachpb.Span{Key: curInfo.key, EndKey: curInfo.endKey}},
			true, /* batch limit */
			zigzagJoinerBatchSize,
			z.FlowCtx.TraceKV,
			z.EvalCtx.TestingKnobs.ForceProductionBatchSizes,
		)
		if err != nil {
			log.Errorf(z.Ctx, "scan error: %s", err)
			return err
		}
		fetchedRow, err := z.fetchRow(z.Ctx)
		if err != nil {
			return scrub.UnwrapScrubError(err)
		}
		z.baseRow = z.rowAlloc.AllocRow(len(fetchedRow))
		copy(z.baseRow, fetchedRow)
		z.side = z.nextSide()
	}
	return nil
}

// Next is part of the RowSource interface.
func (z *zigzagJoiner) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for z.State == execinfra.StateRunning {
		if err := z.maybeFetchInitialRow(); err != nil {
			z.MoveToDraining(err)
			break
		}
		row, err := z.nextRow(z.Ctx, z.FlowCtx.Txn)
		if err != nil {
			z.MoveToDraining(err)
			break
		}
		if row == nil {
			z.MoveToDraining(nil /* err */)
			break
		}

		if outRow := z.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}

	return nil, z.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (z *zigzagJoiner) ConsumerClosed() {
	z.close()
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (z *zigzagJoiner) execStatsForTrace() *execinfrapb.ComponentStats {
	kvStats := execinfrapb.KVStats{
		BytesRead:      optional.MakeUint(uint64(z.getBytesRead())),
		ContentionTime: optional.MakeTimeValue(execinfra.GetCumulativeContentionTime(z.Ctx)),
	}
	for i := range z.infos {
		fis, ok := getFetcherInputStats(z.infos[i].fetcher)
		if !ok {
			return nil
		}
		kvStats.TuplesRead.MaybeAdd(fis.NumTuples)
		kvStats.KVTime.MaybeAdd(fis.WaitTime)
	}
	return &execinfrapb.ComponentStats{
		KV:     kvStats,
		Output: z.OutputHelper.Stats(),
	}
}

func (z *zigzagJoiner) getBytesRead() int64 {
	var bytesRead int64
	for i := range z.infos {
		bytesRead += z.infos[i].fetcher.GetBytesRead()
	}
	return bytesRead
}

func (z *zigzagJoiner) getRowsRead() int64 {
	var rowsRead int64
	for i := range z.infos {
		rowsRead += z.infos[i].rowsRead
	}
	return rowsRead
}

func (z *zigzagJoiner) generateMeta() []execinfrapb.ProducerMetadata {
	trailingMeta := make([]execinfrapb.ProducerMetadata, 1, 2)
	meta := &trailingMeta[0]
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.BytesRead = z.getBytesRead()
	meta.Metrics.RowsRead = z.getRowsRead()
	if tfs := execinfra.GetLeafTxnFinalState(z.Ctx, z.FlowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}
	return trailingMeta
}

// ChildCount is part of the execinfra.OpNode interface.
func (z *zigzagJoiner) ChildCount(verbose bool) int {
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (z *zigzagJoiner) Child(nth int, verbose bool) execinfra.OpNode {
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
