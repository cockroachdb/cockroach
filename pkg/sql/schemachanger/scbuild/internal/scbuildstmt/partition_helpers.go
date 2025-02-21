// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// TODO(annie): This is unused for now.
var _ = configureIndexDescForNewIndexPartitioning

// configureIndexDescForNewIndexPartitioning returns a new copy of an index
// descriptor containing modifications needed if partitioning is configured.
func configureIndexDescForNewIndexPartitioning(
	b BuildCtx,
	tableID catid.DescID,
	indexDesc descpb.IndexDescriptor,
	partitionByIndex *tree.PartitionByIndex,
) (descpb.IndexDescriptor, error) {
	var err error
	partitionAllBy := b.QueryByID(tableID).FilterTablePartitioning().MustHaveZeroOrOne()
	if partitionByIndex.ContainsPartitioningClause() || partitionAllBy != nil {
		var partitionBy *tree.PartitionBy
		if partitionAllBy == nil {
			if partitionByIndex.ContainsPartitions() {
				partitionBy = partitionByIndex.PartitionBy
			}
		} else if partitionByIndex.ContainsPartitioningClause() {
			return indexDesc, pgerror.New(
				pgcode.FeatureNotSupported,
				"cannot define PARTITION BY on an index if the table has a PARTITION ALL BY definition",
			)
		} else {
			partitionBy, err = partitionByFromTableID(b, tableID)
			if err != nil {
				return indexDesc, err
			}
		}
		localityRBR := b.QueryByID(tableID).FilterTableLocalityRegionalByRow().
			MustGetZeroOrOneElement()
		allowImplicitPartitioning := b.EvalCtx().SessionData().ImplicitColumnPartitioningEnabled ||
			localityRBR != nil
		if partitionBy != nil {
			newImplicitCols, newPartitioning, err := createPartitioning(
				b,
				tableID,
				partitionBy,
				int(indexDesc.Partitioning.NumImplicitColumns),
				indexDesc.KeyColumnNames,
				nil, /* allowedNewColumnNames */
				allowImplicitPartitioning,
			)
			if err != nil {
				return indexDesc, err
			}
			updateIndexPartitioning(&indexDesc, false /* isIndexPrimary */, newImplicitCols, newPartitioning)
		}
	}
	return indexDesc, nil
}

// updateIndexPartitioning applies the new partition and adjusts the column info
// for the specified index descriptor. Returns false iff this was a no-op.
func updateIndexPartitioning(
	idx *descpb.IndexDescriptor,
	isIndexPrimary bool,
	newImplicitCols []*scpb.ColumnName,
	newPartitioning catpb.PartitioningDescriptor,
) bool {
	oldNumImplicitCols := int(idx.Partitioning.NumImplicitColumns)
	isNoOp := oldNumImplicitCols == len(newImplicitCols) && idx.Partitioning.Equal(newPartitioning)
	numCols := len(idx.KeyColumnIDs)
	newCap := numCols + len(newImplicitCols) - oldNumImplicitCols
	newColumnIDs := make([]descpb.ColumnID, len(newImplicitCols), newCap)
	newColumnNames := make([]string, len(newImplicitCols), newCap)
	newColumnDirections := make([]catenumpb.IndexColumn_Direction, len(newImplicitCols), newCap)
	for i, col := range newImplicitCols {
		newColumnIDs[i] = col.ColumnID
		newColumnNames[i] = col.Name
		newColumnDirections[i] = catenumpb.IndexColumn_ASC
		if isNoOp &&
			(idx.KeyColumnIDs[i] != newColumnIDs[i] ||
				idx.KeyColumnNames[i] != newColumnNames[i] ||
				idx.KeyColumnDirections[i] != newColumnDirections[i]) {
			isNoOp = false
		}
	}
	if isNoOp {
		return false
	}
	idx.KeyColumnIDs = append(newColumnIDs, idx.KeyColumnIDs[oldNumImplicitCols:]...)
	idx.KeyColumnNames = append(newColumnNames, idx.KeyColumnNames[oldNumImplicitCols:]...)
	idx.KeyColumnDirections = append(newColumnDirections, idx.KeyColumnDirections[oldNumImplicitCols:]...)
	idx.Partitioning = newPartitioning
	if !isIndexPrimary {
		return true
	}

	newStoreColumnIDs := make([]descpb.ColumnID, 0, len(idx.StoreColumnIDs))
	newStoreColumnNames := make([]string, 0, len(idx.StoreColumnNames))
	for i := range idx.StoreColumnIDs {
		id := idx.StoreColumnIDs[i]
		name := idx.StoreColumnNames[i]
		found := false
		for _, newColumnName := range newColumnNames {
			if newColumnName == name {
				found = true
				break
			}
		}
		if !found {
			newStoreColumnIDs = append(newStoreColumnIDs, id)
			newStoreColumnNames = append(newStoreColumnNames, name)
		}
	}
	idx.StoreColumnIDs = newStoreColumnIDs
	idx.StoreColumnNames = newStoreColumnNames
	if len(idx.StoreColumnNames) == 0 {
		idx.StoreColumnIDs = nil
		idx.StoreColumnNames = nil
	}
	return true
}

// partitionByFromTableID constructs a PartitionBy clause from a tableID.
func partitionByFromTableID(b BuildCtx, tableID catid.DescID) (*tree.PartitionBy, error) {
	idx := getLatestPrimaryIndex(b, tableID)
	partitioning := mustRetrievePartitioningFromIndexPartitioning(b, tableID, idx.IndexID)
	return partitionByFromTableIDImpl(b, tableID, idx.IndexID, partitioning, 0)
}

// partitionByFromTableIDImpl contains the inner logic of partitionByFromTableID.
// We derive the Fields, LIST and RANGE clauses from the table, recursing into
// the subpartitions as required for LIST partitions.
func partitionByFromTableIDImpl(
	b BuildCtx, tableID catid.DescID, indexID catid.IndexID, part catalog.Partitioning, colOffset int,
) (*tree.PartitionBy, error) {
	if part.NumColumns() == 0 {
		return nil, nil
	}

	// We don't need real prefixes in the DecodePartitionTuple calls because we
	// only use the tree.Datums part of the output.
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	partitionBy := &tree.PartitionBy{
		Fields: make(tree.NameList, part.NumColumns()),
		List:   make([]tree.ListPartition, 0, part.NumLists()),
		Range:  make([]tree.RangePartition, 0, part.NumRanges()),
	}
	keyCols, _, _ := getSortedColumnIDsInIndexByKind(b, tableID, indexID)
	for i := 0; i < part.NumColumns(); i++ {
		keyColID := keyCols[colOffset+i]
		colName := b.QueryByID(tableID).FilterColumnName().
			Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnName) bool {
				return e.ColumnID == keyColID
			}).MustGetOneElement()
		partitionBy.Fields[i] = tree.Name(colName.Name)
	}

	// Copy the LIST of the PARTITION BY clause.
	a := &tree.DatumAlloc{}
	err := part.ForEachList(func(name string, values [][]byte, subPartitioning catalog.Partitioning) (err error) {
		lp := tree.ListPartition{
			Name:  tree.Name(name),
			Exprs: make(tree.Exprs, len(values)),
		}
		for j, v := range values {
			tuple, _, err := decodePartitionTuple(
				b,
				a,
				tableID,
				indexID,
				part,
				v,
				fakePrefixDatums,
			)
			if err != nil {
				return err
			}
			exprs, err := partitionTupleToExprs(tuple)
			if err != nil {
				return err
			}
			lp.Exprs[j] = &tree.Tuple{
				Exprs: exprs,
			}
		}
		lp.Subpartition, err = partitionByFromTableIDImpl(
			b,
			tableID,
			indexID,
			subPartitioning,
			colOffset+part.NumColumns(),
		)
		partitionBy.List = append(partitionBy.List, lp)
		return err
	})
	if err != nil {
		return nil, err
	}

	// Copy the RANGE of the PARTITION BY clause.
	err = part.ForEachRange(func(name string, from, to []byte) error {
		rp := tree.RangePartition{Name: tree.Name(name)}
		fromTuple, _, err := decodePartitionTuple(
			b, a, tableID, indexID, part, from, fakePrefixDatums)
		if err != nil {
			return err
		}
		rp.From, err = partitionTupleToExprs(fromTuple)
		if err != nil {
			return err
		}
		toTuple, _, err := decodePartitionTuple(
			b, a, tableID, indexID, part, to, fakePrefixDatums)
		if err != nil {
			return err
		}
		rp.To, err = partitionTupleToExprs(toTuple)
		partitionBy.Range = append(partitionBy.Range, rp)
		return err
	})
	if err != nil {
		return nil, err
	}

	return partitionBy, nil
}

func partitionTupleToExprs(t *rowenc.PartitionTuple) (tree.Exprs, error) {
	exprs := make(tree.Exprs, len(t.Datums)+t.SpecialCount)
	for i, d := range t.Datums {
		exprs[i] = d
	}
	for i := 0; i < t.SpecialCount; i++ {
		switch t.Special {
		case rowenc.PartitionDefaultVal:
			exprs[i+len(t.Datums)] = &tree.DefaultVal{}
		case rowenc.PartitionMinVal:
			exprs[i+len(t.Datums)] = &tree.PartitionMinVal{}
		case rowenc.PartitionMaxVal:
			exprs[i+len(t.Datums)] = &tree.PartitionMaxVal{}
		default:
			return nil, errors.AssertionFailedf("unknown special value found: %v", t.Special)
		}
	}
	return exprs, nil
}

// createPartitioning returns a set of implicit columns and a new partitioning
// descriptor to build an index with partitioning fields populated to align with
// the tree.PartitionBy clause.
func createPartitioning(
	b BuildCtx,
	tableID catid.DescID,
	partBy *tree.PartitionBy,
	oldNumImplicitColumns int,
	oldKeyColumnNames []string,
	allowedNewColumnNames []tree.Name,
	allowImplicitPartitioning bool,
) (newImplicitCols []*scpb.ColumnName, newPartitioning catpb.PartitioningDescriptor, err error) {
	if partBy == nil {
		if oldNumImplicitColumns > 0 {
			return nil, newPartitioning, unimplemented.Newf(
				"ALTER ... PARTITION BY NOTHING",
				"cannot alter to PARTITION BY NOTHING if the object has implicit column partitioning",
			)
		}
		return nil, newPartitioning, nil
	}

	// Truncate existing implicitly partitioned column names.
	newIdxColumnNames := oldKeyColumnNames[oldNumImplicitColumns:]

	if allowImplicitPartitioning {
		newImplicitCols, err = collectImplicitPartitionColumns(
			b,
			tableID,
			oldKeyColumnNames[0],
			partBy,
			allowedNewColumnNames,
		)
		if err != nil {
			return nil, newPartitioning, err
		}
	}
	if len(newImplicitCols) > 0 {
		// Prepend with new implicit column names.
		newIdxColumnNames = make([]string, len(newImplicitCols), len(newImplicitCols)+len(newIdxColumnNames))
		for i, col := range newImplicitCols {
			newIdxColumnNames[i] = col.Name
		}
		newIdxColumnNames = append(newIdxColumnNames, oldKeyColumnNames[oldNumImplicitColumns:]...)
	}

	// If we had implicit column partitioning beforehand, check we have the
	// same implicitly partitioned columns.
	// Having different implicitly partitioned columns requires rewrites,
	// which is outside the scope of createPartitioning.
	if oldNumImplicitColumns > 0 {
		if len(newImplicitCols) != oldNumImplicitColumns {
			return nil, newPartitioning, errors.AssertionFailedf(
				"mismatching number of implicit columns: old %d vs new %d",
				oldNumImplicitColumns,
				len(newImplicitCols),
			)
		}
		for i, col := range newImplicitCols {
			if oldKeyColumnNames[i] != col.Name {
				return nil, newPartitioning, errors.AssertionFailedf("found new implicit partitioning at column ordinal %d", i)
			}
		}
	}

	newPartitioning, err = createPartitioningImpl(
		b,
		tableID,
		newIdxColumnNames,
		partBy,
		allowedNewColumnNames,
		len(newImplicitCols),
		0, /* colOffset */
	)
	if err != nil {
		return nil, catpb.PartitioningDescriptor{}, err
	}
	return newImplicitCols, newPartitioning, err
}

// collectImplicitPartitionColumns collects implicit partitioning columns.
func collectImplicitPartitionColumns(
	b BuildCtx,
	tableID catid.DescID,
	indexFirstColumnName string,
	partBy *tree.PartitionBy,
	allowedNewColumnNames []tree.Name,
) (implicitCols []*scpb.ColumnName, _ error) {
	seenImplicitColumnNames := map[string]struct{}{}
	// Iterate over each field in the PARTITION BY until it matches the start
	// of the actual explicitly indexed columns.
	for _, field := range partBy.Fields {
		// As soon as the fields match, we have no implicit columns to add.
		if string(field) == indexFirstColumnName {
			break
		}

		col, err := findColumnByNameOnTable(
			b,
			tableID,
			field,
			allowedNewColumnNames,
		)
		if err != nil {
			return nil, err
		}
		if _, ok := seenImplicitColumnNames[col.Name]; ok {
			return nil, pgerror.Newf(
				pgcode.InvalidObjectDefinition,
				`found multiple definitions in partition using column "%s"`,
				col.Name,
			)
		}
		seenImplicitColumnNames[col.Name] = struct{}{}
		implicitCols = append(implicitCols, col)
	}

	return implicitCols, nil
}

func createPartitioningImpl(
	b BuildCtx,
	tableID catid.DescID,
	newIdxColumnNames []string,
	partBy *tree.PartitionBy,
	allowedNewColumnNames []tree.Name,
	numImplicitColumns int,
	colOffset int,
) (catpb.PartitioningDescriptor, error) {
	partDesc := catpb.PartitioningDescriptor{}
	if partBy == nil {
		return partDesc, nil
	}
	partDesc.NumColumns = uint32(len(partBy.Fields))
	partDesc.NumImplicitColumns = uint32(numImplicitColumns)

	partitioningString := func() string {
		// We don't have the fields for our parent partitions handy, but we can use
		// the names from the index we're partitioning. They must have matched or we
		// would have already returned an error.
		partCols := append([]string(nil), newIdxColumnNames[:colOffset]...)
		for _, p := range partBy.Fields {
			partCols = append(partCols, string(p))
		}
		return strings.Join(partCols, ", ")
	}

	var cols []*scpb.ColumnName
	for i := 0; i < len(partBy.Fields); i++ {
		if colOffset+i >= len(newIdxColumnNames) {
			return partDesc, pgerror.Newf(pgcode.Syntax,
				"declared partition columns (%s) exceed the number of columns in index being partitioned (%s)",
				partitioningString(), strings.Join(newIdxColumnNames, ", "))
		}
		// Search by name because some callsites of this method have not
		// allocated ids yet (so they are still all the 0 value).
		col, err := findColumnByNameOnTable(
			b,
			tableID,
			tree.Name(newIdxColumnNames[colOffset+i]),
			allowedNewColumnNames,
		)
		if err != nil {
			return partDesc, err
		}
		cols = append(cols, col)
		if string(partBy.Fields[i]) != col.Name {
			// This used to print the first `colOffset + len(partBy.Fields)` fields
			// but there might not be this many columns in the index. See #37682.
			n := colOffset + i + 1
			return partDesc, pgerror.Newf(pgcode.Syntax,
				"declared partition columns (%s) do not match first %d columns in index being partitioned (%s)",
				partitioningString(), n, strings.Join(newIdxColumnNames[:n], ", "))
		}
		colTypFamily := b.QueryByID(tableID).FilterColumnType().
			Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnType) bool {
				return e.ColumnID == col.ColumnID
			}).MustGetOneElement().Type.Family()
		if colTypFamily == types.ArrayFamily {
			return partDesc, unimplemented.NewWithIssuef(91766, "partitioning by array column (%s) not supported",
				col.Name)
		}
	}

	for _, l := range partBy.List {
		p := catpb.PartitioningDescriptor_List{
			Name: string(l.Name),
		}
		for _, expr := range l.Exprs {
			encodedTuple, err := valueEncodePartitionTuple(
				b, tableID, tree.PartitionByList, expr, cols,
			)
			if err != nil {
				return partDesc, errors.Wrapf(err, "PARTITION %s", p.Name)
			}
			p.Values = append(p.Values, encodedTuple)
		}
		if l.Subpartition != nil {
			newColOffset := colOffset + int(partDesc.NumColumns)
			if numImplicitColumns > 0 {
				return catpb.PartitioningDescriptor{}, unimplemented.New(
					"PARTITION BY SUBPARTITION",
					"implicit column partitioning on a subpartition is not yet supported",
				)
			}
			subpartitioning, err := createPartitioningImpl(
				b,
				tableID,
				newIdxColumnNames,
				l.Subpartition,
				allowedNewColumnNames,
				0, /* implicitColumnNames */
				newColOffset,
			)
			if err != nil {
				return partDesc, err
			}
			p.Subpartitioning = subpartitioning
		}
		partDesc.List = append(partDesc.List, p)
	}

	for _, r := range partBy.Range {
		p := catpb.PartitioningDescriptor_Range{
			Name: string(r.Name),
		}
		var err error
		p.FromInclusive, err = valueEncodePartitionTuple(
			b, tableID, tree.PartitionByRange, &tree.Tuple{Exprs: r.From}, cols,
		)
		if err != nil {
			return partDesc, errors.Wrapf(err, "PARTITION %s", p.Name)
		}
		p.ToExclusive, err = valueEncodePartitionTuple(
			b, tableID, tree.PartitionByRange, &tree.Tuple{Exprs: r.To}, cols,
		)
		if err != nil {
			return partDesc, errors.Wrapf(err, "PARTITION %s", p.Name)
		}
		if r.Subpartition != nil {
			return partDesc, errors.Newf("PARTITION %s: cannot subpartition a range partition", p.Name)
		}
		partDesc.Range = append(partDesc.Range, p)
	}

	return partDesc, nil
}

// findColumnByNameOnTable finds the given column from the table.
// By default we only allow public columns on PARTITION BY clauses.
// However, any columns appearing as allowedNewColumnNames is also
// permitted provided the caller will ensure this column is backfilled
// before the partitioning is active.
func findColumnByNameOnTable(
	b BuildCtx, tableID catid.DescID, colName tree.Name, allowedNewColumnNames []tree.Name,
) (*scpb.ColumnName, error) {
	isPublic := false
	col := b.QueryByID(tableID).FilterColumnName().
		Filter(func(curr scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnName) bool {
			isColMatch := e.Name == string(colName)
			if isColMatch {
				isPublic = curr == scpb.Status_PUBLIC
			}
			return isColMatch
		}).MustGetOneElement()
	if isPublic {
		return col, nil
	}
	for _, allowedNewColName := range allowedNewColumnNames {
		if allowedNewColName == colName {
			return col, nil
		}
	}
	return nil, colinfo.NewUndefinedColumnError(string(colName))
}

// valueEncodePartitionTuple typechecks the datums in maybeTuple. It returns the
// concatenation of these datums, each encoded using the table "value" encoding.
// The special values of DEFAULT (for list) and MAXVALUE (for range) are encoded
// as NOT NULL.
//
// TODO(dan): The typechecking here should be run during plan construction, so
// we can support placeholders.
func valueEncodePartitionTuple(
	b BuildCtx,
	tableID catid.DescID,
	typ tree.PartitionByType,
	maybeTuple tree.Expr,
	cols []*scpb.ColumnName,
) ([]byte, error) {
	// Replace any occurrences of the MINVALUE/MAXVALUE pseudo-names
	// into MinVal and MaxVal, to be recognized below.
	// We are operating in a context where the expressions cannot
	// refer to table columns, so these two names are unambiguously
	// referring to the desired partition boundaries.
	maybeTuple, _ = tree.WalkExpr(zonepb.ReplaceMinMaxValVisitor{}, maybeTuple)

	tuple, ok := maybeTuple.(*tree.Tuple)
	if !ok {
		// If we don't already have a tuple, promote whatever we have to a 1-tuple.
		tuple = &tree.Tuple{Exprs: []tree.Expr{maybeTuple}}
	}

	if len(tuple.Exprs) != len(cols) {
		return nil, errors.Errorf("partition has %d columns but %d values were supplied",
			len(cols), len(tuple.Exprs))
	}

	var value, scratch []byte
	for i, expr := range tuple.Exprs {
		expr = tree.StripParens(expr)
		switch expr.(type) {
		case tree.DefaultVal:
			if typ != tree.PartitionByList {
				return nil, errors.Errorf("%s cannot be used with PARTITION BY %s", expr, typ)
			}
			// NOT NULL is used to signal that a PartitionSpecialValCode follows.
			value = encoding.EncodeNotNullValue(value, encoding.NoColumnID)
			value = encoding.EncodeNonsortingUvarint(value, uint64(rowenc.PartitionDefaultVal))
			continue
		case tree.PartitionMinVal:
			if typ != tree.PartitionByRange {
				return nil, errors.Errorf("%s cannot be used with PARTITION BY %s", expr, typ)
			}
			// NOT NULL is used to signal that a PartitionSpecialValCode follows.
			value = encoding.EncodeNotNullValue(value, encoding.NoColumnID)
			value = encoding.EncodeNonsortingUvarint(value, uint64(rowenc.PartitionMinVal))
			continue
		case tree.PartitionMaxVal:
			if typ != tree.PartitionByRange {
				return nil, errors.Errorf("%s cannot be used with PARTITION BY %s", expr, typ)
			}
			// NOT NULL is used to signal that a PartitionSpecialValCode follows.
			value = encoding.EncodeNotNullValue(value, encoding.NoColumnID)
			value = encoding.EncodeNonsortingUvarint(value, uint64(rowenc.PartitionMaxVal))
			continue
		case *tree.Placeholder:
			return nil, unimplemented.NewWithIssuef(
				19464, "placeholders are not supported in PARTITION BY")
		default:
			// Fall-through.
		}

		var semaCtx tree.SemaContext
		colType := b.QueryByID(tableID).FilterColumnType().
			Filter(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnType) bool {
				return e.ColumnID == cols[i].ColumnID
			}).MustGetOneElement().Type
		typedExpr, err := schemaexpr.SanitizeVarFreeExpr(b, expr, colType, "partition",
			&semaCtx,
			volatility.Immutable,
			false, /*allowAssignmentCast*/
		)
		if err != nil {
			return nil, err
		}
		if !eval.IsConst(b.EvalCtx(), typedExpr) {
			return nil, pgerror.Newf(pgcode.Syntax,
				"%s: partition values must be constant", typedExpr)
		}
		datum, err := eval.Expr(b, b.EvalCtx(), typedExpr)
		if err != nil {
			return nil, errors.Wrapf(err, "evaluating %s", typedExpr)
		}
		err = colinfo.CheckDatumTypeFitsColumnType(cols[i].Name, colType, datum.ResolvedType())
		if err != nil {
			return nil, err
		}
		value, scratch, err = valueside.EncodeWithScratch(value, valueside.NoColumnID, datum, scratch[:0])
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}
