// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// CanMergeProjections returns true if the outer Projections operator never
// references any of the inner Projections columns. If true, then the outer does
// not depend on the inner, and the two can be merged into a single set.
func (c *CustomFuncs) CanMergeProjections(outer, inner memo.ProjectionsExpr) bool {
	innerCols := c.ProjectionCols(inner)
	for i := range outer {
		if outer[i].ScalarProps().OuterCols.Intersects(innerCols) {
			return false
		}
	}
	return true
}

// MergeProjections concatenates the synthesized columns from the outer
// Projections operator, and the synthesized columns from the inner Projections
// operator that are passed through by the outer. Note that the outer
// synthesized columns must never contain references to the inner synthesized
// columns; this can be verified by first calling CanMergeProjections.
func (c *CustomFuncs) MergeProjections(
	outer, inner memo.ProjectionsExpr, passthrough opt.ColSet,
) memo.ProjectionsExpr {
	// No need to recompute properties on the new projections, since they should
	// still be valid.
	newProjections := make(memo.ProjectionsExpr, len(outer), len(outer)+len(inner))
	copy(newProjections, outer)
	for i := range inner {
		item := &inner[i]
		if passthrough.Contains(item.Col) {
			newProjections = append(newProjections, *item)
		}
	}
	return newProjections
}

// MergeProjectWithValues merges a Project operator with its input Values
// operator. This is only possible in certain circumstances, which are described
// in the MergeProjectWithValues rule comment.
//
// Values columns that are part of the Project passthrough columns are retained
// in the final Values operator, and Project synthesized columns are added to
// it. Any unreferenced Values columns are discarded. For example:
//
//   SELECT column1, 3 FROM (VALUES (1, 2))
//   =>
//   (VALUES (1, 3))
//
func (c *CustomFuncs) MergeProjectWithValues(
	projections memo.ProjectionsExpr, passthrough opt.ColSet, input memo.RelExpr,
) memo.RelExpr {
	newExprs := make(memo.ScalarListExpr, 0, len(projections)+passthrough.Len())
	newTypes := make([]*types.T, 0, len(newExprs))
	newCols := make(opt.ColList, 0, len(newExprs))

	values := input.(*memo.ValuesExpr)
	tuple := values.Rows[0].(*memo.TupleExpr)
	for i, colID := range values.Cols {
		if passthrough.Contains(colID) {
			newExprs = append(newExprs, tuple.Elems[i])
			newTypes = append(newTypes, tuple.Elems[i].DataType())
			newCols = append(newCols, colID)
		}
	}

	for i := range projections {
		item := &projections[i]
		newExprs = append(newExprs, item.Element)
		newTypes = append(newTypes, item.Element.DataType())
		newCols = append(newCols, item.Col)
	}

	tupleTyp := types.MakeTuple(newTypes)
	rows := memo.ScalarListExpr{c.f.ConstructTuple(newExprs, tupleTyp)}
	return c.f.ConstructValues(rows, &memo.ValuesPrivate{
		Cols: newCols,
		ID:   values.ID,
	})
}

// CanUnnestTuplesFromValues returns true if the given single-column Values
// operator has tuples that can be unfolded into multiple columns.
// This is the case if:
// 	1. The single output column is of type tuple.
// 	2. All tuples in the single column are either TupleExpr's or ConstExpr's
//     that wrap DTuples, as opposed to dynamically generated tuples.
func (c *CustomFuncs) CanUnnestTuplesFromValues(values *memo.ValuesExpr) bool {
	colTypeFam := c.mem.Metadata().ColumnMeta(values.Cols[0]).Type.Family()
	if colTypeFam != types.TupleFamily {
		return false
	}
	for i := range values.Rows {
		if !c.IsStaticTuple(values.Rows[i].(*memo.TupleExpr).Elems[0]) {
			return false
		}
	}
	return true
}

// HasNoDirectTupleReferences ensures that the input ProjectionsExpr contains no
// direct references to the tuple represented by the given ColumnID.
func (c *CustomFuncs) HasNoDirectTupleReferences(
	projections memo.ProjectionsExpr, tupleCol opt.ColumnID,
) bool {
	var check func(expr opt.Expr) bool
	check = func(expr opt.Expr) bool {
		switch t := expr.(type) {
		case *memo.ColumnAccessExpr:
			switch t.Input.(type) {
			case *memo.VariableExpr:
				return true
			}

		case *memo.VariableExpr:
			return t.Col != tupleCol
		}
		for i, n := 0, expr.ChildCount(); i < n; i++ {
			if !check(expr.Child(i)) {
				return false
			}
		}
		return true
	}

	for i := range projections {
		if !check(projections[i].Element) {
			return false
		}
	}

	return true
}

// MakeColsForUnnestTuples takes in the ColumnID of a tuple column and adds new
// columns to metadata corresponding to each field in the tuple. A ColList
// containing these new columns is returned.
func (c *CustomFuncs) MakeColsForUnnestTuples(tupleColID opt.ColumnID) opt.ColList {
	mem := c.mem.Metadata()
	tupleType := c.mem.Metadata().ColumnMeta(tupleColID).Type

	// Create a new column for each position in the tuple. Add it to outColIDs.
	tupleLen := len(tupleType.TupleContents())
	tupleAlias := mem.ColumnMeta(tupleColID).Alias
	outColIDs := make(opt.ColList, tupleLen)
	for i := 0; i < tupleLen; i++ {
		newAlias := fmt.Sprintf("%s_%d", tupleAlias, i+1)
		newColID := mem.AddColumn(newAlias, tupleType.TupleContents()[i])
		outColIDs[i] = newColID
	}
	return outColIDs
}

// UnnestTuplesFromValues takes in a Values operator that has a single column
// of tuples and a ColList corresponding to each tuple field. It returns a new
// Values operator with the tuple expanded out into the Values rows.
// For example, these rows:
//
//   ((1, 2),)
//   ((3, 4),)
//
// would be unnested as:
//
//   (1, 2)
//   (3, 4)
//
func (c *CustomFuncs) UnnestTuplesFromValues(
	values *memo.ValuesExpr, valuesCols opt.ColList,
) memo.RelExpr {
	tupleColID := values.Cols[0]
	tupleType := c.mem.Metadata().ColumnMeta(tupleColID).Type
	outTuples := make(memo.ScalarListExpr, len(values.Rows))

	// Pull the inner tuples out of the single column of the Values operator and
	// put them into a ScalarListExpr to be used in the new Values operator.
	for i, row := range values.Rows {
		outerTuple := row.(*memo.TupleExpr)
		switch t := outerTuple.Elems[0].(type) {
		case *memo.TupleExpr:
			outTuples[i] = t

		case *memo.ConstExpr:
			dTuple := t.Value.(*tree.DTuple)
			tupleVals := make(memo.ScalarListExpr, len(dTuple.D))
			for i := range dTuple.D {
				val := c.f.ConstructConstVal(dTuple.D[i], tupleType.TupleContents()[i])
				tupleVals[i] = val
			}
			outTuples[i] = c.f.ConstructTuple(tupleVals, tupleType)

		default:
			panic(errors.AssertionFailedf("unhandled input op: %T", t))
		}
	}

	// Return new ValuesExpr with new tuples.
	valuesPrivate := &memo.ValuesPrivate{Cols: valuesCols, ID: c.mem.Metadata().NextUniqueID()}
	return c.f.ConstructValues(outTuples, valuesPrivate)
}

// FoldTupleColumnAccess constructs a new ProjectionsExpr from the old one with
// any ColumnAccess operators that refer to the original tuple column (oldColID)
// replaced by new columns from the output of the given ValuesExpr.
func (c *CustomFuncs) FoldTupleColumnAccess(
	projections memo.ProjectionsExpr, valuesCols opt.ColList, oldColID opt.ColumnID,
) memo.ProjectionsExpr {
	newProjections := make(memo.ProjectionsExpr, len(projections))

	// Recursively traverses a ProjectionsItem element and replaces references to
	// positions in the tuple rows with one of the newly constructed columns.
	var replace ReplaceFunc
	replace = func(nd opt.Expr) opt.Expr {
		if colAccess, ok := nd.(*memo.ColumnAccessExpr); ok {
			if variable, ok := colAccess.Input.(*memo.VariableExpr); ok {
				// Skip past references to columns other than the input tuple column.
				if variable.Col == oldColID {
					return c.f.ConstructVariable(valuesCols[int(colAccess.Idx)])
				}
			}
		}
		return c.f.Replace(nd, replace)
	}

	// Construct and return a new ProjectionsExpr using the new ColumnIDs.
	for i := range projections {
		projection := &projections[i]
		newProjections[i] = c.f.ConstructProjectionsItem(
			replace(projection.Element).(opt.ScalarExpr), projection.Col)
	}
	return newProjections
}

// CanUnnestJSONFromValues returns true if the Values operator has a single
// column containing JSON expressions that can be unfolded into multiple
// columns. This is the case if:
//  1. The projections don't directly reference the JSON itself. Only fields
//     within the JSON object can be referenced.
//  2. All JSON keys referenced by the projections are present in the first row.
//  3. All JSON keys present in the first row are present in all other rows.
// CanUnnestJSONFromValues should only be called if the Values operator has a
// single column and at least one row.
//
// Note: technically we only need to check that the JSON fields referenced by
// the projections exist in all the rows. For simplicity, we instead check that
// (1) all references exist in the first row and (2) that all keys from the
// first row also exist in all other rows.
func (c *CustomFuncs) CanUnnestJSONFromValues(
	values *memo.ValuesExpr, projections memo.ProjectionsExpr, jsonCol opt.ColumnID,
) bool {
	colTypeFam := c.mem.Metadata().ColumnMeta(values.Cols[0]).Type.Family()
	if colTypeFam != types.JsonFamily {
		return false
	}
	// Retrieve the JSON expression from the first row.
	constExpr, ok := values.Rows[0].(*memo.TupleExpr).Elems[0].(*memo.ConstExpr)
	if !ok {
		// The contents of the JSON expression can't be statically determined. (This
		// can happen when the row contains a reference to a JSON table column).
		return false
	}
	firstJSON, ok := constExpr.Value.(*tree.DJSON)
	if !ok {
		return false
	}
	if !(firstJSON.Type() == json.ObjectJSONType) {
		// The JSON expression must be a JSON object with key-value pairs referenced
		// by the projections.
		return false
	}

	// Recursively traverses an expression, looking for direct references to
	// jsonCol. Also ensures that any references to JSON fields are part of the
	// first row's schema. For example, this query is valid:
	//
	//    SELECT j->'x'
	//    FROM
	//    (VALUES
	//        ('{"x": "one"}'::JSON),
	//        ('{"x": "two", "y": 2}'::JSON)
	//    ) v(j);
	//
	// But these queries are not valid:
	//
	//    SELECT j
	//    FROM
	//    (VALUES
	//        ('{"x": "one"}'::JSON),
	//        ('{"x": "two", "y": 2}'::JSON)
	//    ) v(j);
	//
	//    SELECT j->'x', j->'y'
	//    FROM
	//    (VALUES
	//        ('{"x": "one"}'::JSON),
	//        ('{"x": "two", "y": 2}'::JSON)
	//    ) v(j);
	//
	var check func(expr opt.Expr) bool
	check = func(expr opt.Expr) bool {
		switch t := expr.(type) {
		case *memo.FetchValExpr:
			if v, ok := t.Json.(*memo.VariableExpr); ok {
				if constExpr, ok := t.Index.(*memo.ConstExpr); ok {
					if key, ok := constExpr.Value.(*tree.DString); ok {
						if v.Col == jsonCol {
							// Ensure that the key this projection is referencing exists in
							// the first row.
							exists, err := firstJSON.Exists(string(*key))
							return exists && err == nil
						}
						return true
					}
				}
			}
			return false

		case *memo.VariableExpr:
			// Ensure that the JSON column itself is not referenced; only its fields
			// can be referenced.
			return t.Col != jsonCol
		}
		for i, n := 0, expr.ChildCount(); i < n; i++ {
			if !check(expr.Child(i)) {
				return false
			}
		}
		return true
	}

	// Traverse all ProjectionsItems.
	for i := range projections {
		if !check(projections[i].Element) {
			return false
		}
	}

	// Ensure that all Values rows are ConstExpr's that wrap DJSON datums. Also
	// ensure that the DJSON datums have at least the keys from the first row.
	// This check is performed after the projections are walked because there may
	// be many Values rows.
	for i := 1; i < len(values.Rows); i++ {
		expr := values.Rows[i].(*memo.TupleExpr).Elems[0]
		if !c.IsConstJSON(expr) {
			// The contents of this JSON expression cannot be statically determined.
			return false
		}
		currJSON := expr.(*memo.ConstExpr).Value.(*tree.DJSON)
		if currJSON.Type() != json.ObjectJSONType {
			// This value is not an object. It is important to check, because a JSON
			// array can pass the checks below (see #60522).
			return false
		}
		iter, err := firstJSON.ObjectIter()
		if err != nil {
			return false
		}
		// Iterate through the keys of the first JSON object and ensure that they
		// all exist in every other row. For example, these are a valid set of rows:
		//
		//    ('{"x": 1}'::JSON), ('{"x": 2, "y": 'two'}'::JSON)
		//
		// but these are not:
		//
		//    ('{"x": 2, "y": 'two'}'::JSON), ('{"x": 1}'::JSON)
		//
		for iter.Next() {
			if exists, err := currJSON.Exists(iter.Key()); !exists || err != nil {
				return false
			}
		}
	}

	return true
}

// UnnestJSONFromValues takes in a Values operator that has a single column
// of JSON expressions and a ColList with columns corresponding to each JSON key
// in the first row of the ValuesExpr. It returns a new Values operator with the
// json fields expanded out into the Values rows.
func (c *CustomFuncs) UnnestJSONFromValues(
	values *memo.ValuesExpr, newCols opt.ColList,
) memo.RelExpr {
	outTuples := make(memo.ScalarListExpr, len(values.Rows))
	valTypes := make([]*types.T, len(newCols))
	for i := range valTypes {
		valTypes[i] = types.Jsonb
	}
	tupleType := types.MakeTuple(valTypes)

	// Construct new rows with values corresponding to the keys from first row.
	// As an example, given Values rows like the following:
	//
	//    ('{"x": 1, "y": 'one'}'::JSON), ('{"x": 2, "y": 'two', "z": 'zwei'}'::JSON)
	//
	// The output rows will be:
	//
	//    (1::JSON, 'one'::JSON), (2::JSON, 'two'::JSON)
	//
	// Any values corresponding to keys that are not present in the first row will
	// be discarded.
	firstJSON := values.Rows[0].(*memo.TupleExpr).Elems[0].(*memo.ConstExpr).Value.(*tree.DJSON)
	for i := range values.Rows {
		rowExpr := values.Rows[i].(*memo.TupleExpr).Elems[0]
		tupleVals := make(memo.ScalarListExpr, len(newCols))
		iter, err := firstJSON.ObjectIter()
		if err != nil {
			panic(errors.AssertionFailedf("failed to retrieve ObjectIter: %v", err))
		}
		// Iterate through the keys of the first row and use them to get the values
		// from the current row. Add these values to the new Values rows.
		idx := 0
		for iter.Next() {
			jsonVal, err := rowExpr.(*memo.ConstExpr).Value.(*tree.DJSON).FetchValKey(iter.Key())
			if err != nil {
				panic(errors.AssertionFailedf("FetchValKey failed: %v", err))
			}
			dJSON := tree.NewDJSON(jsonVal)
			tupleVals[idx] = c.f.ConstructConstVal(dJSON, types.Jsonb)
			idx++
		}
		outTuples[i] = c.f.ConstructTuple(tupleVals, tupleType)
	}

	// Construct ValuesExpr with the new rows.
	valuesPrivate := &memo.ValuesPrivate{Cols: newCols, ID: c.mem.Metadata().NextUniqueID()}
	return c.f.ConstructValues(outTuples, valuesPrivate)
}

// FoldJSONFieldAccess constructs a new ProjectionsExpr from the old one with
// any FetchVal operators that refer to the original JSON column (oldColID)
// replaced by new Variables wrapping columns from the output of the given
// ValuesExpr. Example:
//
//    SELECT j->'a' AS j_a FROM ...
// =>
//    SELECT j_a FROM ...
//
func (c *CustomFuncs) FoldJSONFieldAccess(
	projections memo.ProjectionsExpr,
	newCols opt.ColList,
	oldColID opt.ColumnID,
	oldValues *memo.ValuesExpr,
) memo.ProjectionsExpr {
	newProjections := make(memo.ProjectionsExpr, len(projections))

	// Create a mapping from JSON keys to the new columns that were created for
	// them.
	keysToCols := make(map[string]opt.ColumnID)
	firstJSON := oldValues.Rows[0].(*memo.TupleExpr).Elems[0].(*memo.ConstExpr).Value.(*tree.DJSON)
	iter, err := firstJSON.ObjectIter()
	if err != nil {
		panic(errors.AssertionFailedf("failed to retrieve ObjectIter: %v", err))
	}
	idx := 0
	for iter.Next() {
		keysToCols[iter.Key()] = newCols[idx]
		idx++
	}

	// Recursively traverses a ProjectionsItem element and replaces references to
	// json fields with one of the newly constructed columns.
	var replace ReplaceFunc
	replace = func(nd opt.Expr) opt.Expr {
		if fetchVal, ok := nd.(*memo.FetchValExpr); ok {
			if variable, ok := fetchVal.Json.(*memo.VariableExpr); ok {
				// Skip past references to columns other than the input tuple column.
				if variable.Col == oldColID {
					// Replace this reference to a JSON field with the column that was
					// created to replace it.
					key := string(*fetchVal.Index.(*memo.ConstExpr).Value.(*tree.DString))
					return c.f.ConstructVariable(keysToCols[key])
				}
			}
		}
		return c.f.Replace(nd, replace)
	}

	// Construct and return a new ProjectionsExpr using the new ColumnIDs.
	for i := range projections {
		projection := &projections[i]
		newProjections[i] = c.f.ConstructProjectionsItem(
			replace(projection.Element).(opt.ScalarExpr), projection.Col)
	}
	return newProjections
}

// MakeColsForUnnestJSON creates a new column for each key in the first row of
// the given ValuesExpr, and returns a ColList with those columns. The columns
// will be in the same order as the corresponding keys from the first row.
func (c *CustomFuncs) MakeColsForUnnestJSON(
	values *memo.ValuesExpr, jsonCol opt.ColumnID,
) opt.ColList {
	dJSON := values.Rows[0].(*memo.TupleExpr).Elems[0].(*memo.ConstExpr).Value.(*tree.DJSON)
	mem := c.mem.Metadata()
	jsonAlias := mem.ColumnMeta(jsonCol).Alias

	// Create a new column for each JSON key found in dJSON.
	iter, err := dJSON.ObjectIter()
	if err != nil {
		panic(errors.AssertionFailedf("failed to retrieve ObjectIter: %v", err))
	}
	newColIDs := make(opt.ColList, 0, dJSON.Len())
	for iter.Next() {
		newAlias := fmt.Sprintf("%s_%s", jsonAlias, iter.Key())
		newColID := mem.AddColumn(newAlias, types.Jsonb)
		newColIDs = append(newColIDs, newColID)
	}
	return newColIDs
}

// CanPushColumnRemappingIntoValues returns true if there is at least one
// ProjectionsItem for which the following conditions hold:
//
// 1. The ProjectionsItem remaps an output column from the given ValuesExpr.
//
// 2. The Values output column being remapped is not in the passthrough set.
//
func (c *CustomFuncs) CanPushColumnRemappingIntoValues(
	projections memo.ProjectionsExpr, passthrough opt.ColSet, values *memo.ValuesExpr,
) bool {
	outputCols := values.Relational().OutputCols
	for i := range projections {
		if variable, ok := projections[i].Element.(*memo.VariableExpr); ok {
			if !passthrough.Contains(variable.Col) && outputCols.Contains(variable.Col) {
				return true
			}
		}
	}
	return false
}

// PushColumnRemappingIntoValues folds ProjectionsItems into the passthrough set
// if all they do is remap output columns from the ValuesExpr input. The Values
// output columns are replaced by the corresponding columns from the folded
// ProjectionsItems.
//
// Example:
// project
//  ├── columns: x:2!null
//  ├── values
//  │    ├── columns: column1:1!null
//  │    ├── cardinality: [2 - 2]
//  │    ├── (1,)
//  │    └── (2,)
//  └── projections
//       └── column1:1 [as=x:2, outer=(1)]
// =>
// project
//  ├── columns: x:2!null
//  └── values
//       ├── columns: x:2!null
//       ├── cardinality: [2 - 2]
//       ├── (1,)
//       └── (2,)
//
// This allows other rules to fire. In the above example, EliminateProject can
// now remove the Project altogether.
func (c *CustomFuncs) PushColumnRemappingIntoValues(
	oldValues *memo.ValuesExpr, oldProjections memo.ProjectionsExpr, oldPassthrough opt.ColSet,
) memo.RelExpr {
	oldValuesCols := oldValues.Relational().OutputCols
	newPassthrough := oldPassthrough.Copy()
	replacementCols := make(map[opt.ColumnID]opt.ColumnID)
	var newProjections memo.ProjectionsExpr

	// Construct the new ProjectionsExpr and passthrough columns. Keep track of
	// which Values columns are to be replaced.
	for i := range oldProjections {
		oldItem := &oldProjections[i]

		// A column can be replaced if the following conditions hold:
		// 1. The current ProjectionsItem contains a VariableExpr.
		// 2. The VariableExpr references a column from the ValuesExpr.
		// 3. The column has not already been assigned a replacement.
		// 4. The column is not a passthrough column.
		if v, ok := oldItem.Element.(*memo.VariableExpr); ok {
			if targetCol := v.Col; oldValuesCols.Contains(targetCol) {
				if replacementCols[targetCol] == 0 {
					if !newPassthrough.Contains(targetCol) {
						// The conditions for column replacement have been met. Map the old
						// Values output column to its replacement and add the replacement
						// to newPassthrough so it will become a passthrough column.
						// Continue so that no corresponding ProjectionsItem is added to
						// newProjections.
						replacementCols[targetCol] = oldItem.Col
						newPassthrough.Add(oldItem.Col)
						continue
					}
				}
			}
		}
		// The current ProjectionsItem cannot be folded into newPassthrough because
		// the above conditions do not hold. Simply add it to newProjections. Later,
		// every ProjectionsItem will be recursively traversed and any references to
		// columns that are in replacementCols will be replaced.
		newProjections = append(newProjections, *oldItem)
	}

	// Recursively traverses a ProjectionsItem element and replaces references to
	// old ValuesExpr columns with the replacement columns. This ensures that any
	// remaining references to old columns are replaced. For example:
	//
	//   WITH t AS (SELECT x, x FROM (VALUES (1)) f(x)) SELECT * FROM t;
	//
	// The "x" column of the Values operator will be mapped to the first column of
	// t. This first column will become a passthrough column. Now, the remaining
	// reference to "x" in the second column of t needs to be replaced by the new
	// passthrough column.
	var replace ReplaceFunc
	replace = func(nd opt.Expr) opt.Expr {
		switch t := nd.(type) {
		case *memo.VariableExpr:
			if replaceCol := replacementCols[t.Col]; replaceCol != 0 {
				return c.f.ConstructVariable(replaceCol)
			}
		}
		return c.f.Replace(nd, replace)
	}

	// Traverse each element in newProjections and replace col references as
	// dictated by replacementCols.
	for i := range newProjections {
		item := &newProjections[i]
		newProjections[i] = c.f.ConstructProjectionsItem(
			replace(item.Element).(opt.ScalarExpr), item.Col)
	}

	// Replace all columns in newValuesColList that have been remapped by the old
	// ProjectionsExpr.
	oldValuesColList := oldValues.Cols
	newValuesColList := make(opt.ColList, len(oldValuesColList))
	for i := range newValuesColList {
		if replaceCol := replacementCols[oldValuesColList[i]]; replaceCol != 0 {
			newValuesColList[i] = replaceCol
		} else {
			newValuesColList[i] = oldValuesColList[i]
		}
	}

	// Construct a new ValuesExpr with the replaced cols.
	newValues := c.f.ConstructValues(
		oldValues.Rows,
		&memo.ValuesPrivate{Cols: newValuesColList, ID: c.f.Metadata().NextUniqueID()})

	// Construct and return a new ProjectExpr with the new ValuesExpr as input.
	return c.f.ConstructProject(newValues, newProjections, newPassthrough)
}

// IsStaticTuple returns true if the given ScalarExpr is either a TupleExpr or a
// ConstExpr wrapping a DTuple. Expressions within a static tuple can be
// determined during planning:
//
//   (1, 2)
//   (x, y)
//
// By contrast, expressions within a dynamic tuple can only be determined at
// run-time:
//
//   SELECT (SELECT (x, y) FROM xy)
//
// Here, if there are 0 rows in xy, the tuple value will be NULL. Or, if there
// is more than one row in xy, a dynamic error will be raised.
func (c *CustomFuncs) IsStaticTuple(expr opt.ScalarExpr) bool {
	switch t := expr.(type) {
	case *memo.TupleExpr:
		return true

	case *memo.ConstExpr:
		if _, ok := t.Value.(*tree.DTuple); ok {
			return true
		}
	}
	return false
}
