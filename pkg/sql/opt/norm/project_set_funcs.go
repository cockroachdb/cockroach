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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// unnestFuncs maps function names that are supported by
// ConvertZipArraysToValues
var unnestFuncs = map[string]struct{}{
	"unnest":               {},
	"json_array_elements":  {},
	"jsonb_array_elements": {},
}

// CanConstructValuesFromZips takes in an input ZipExpr and returns true if the
// ProjectSet to which the zip belongs can be converted to an InnerJoinApply
// with a Values operator on the right input.
func (c *CustomFuncs) CanConstructValuesFromZips(zip memo.ZipExpr) bool {
	for i := range zip {
		fn, ok := zip[i].Fn.(*memo.FunctionExpr)
		if !ok {
			// Not a FunctionExpr.
			return false
		}
		if _, ok := unnestFuncs[fn.Name]; !ok {
			// Not a supported function.
			return false
		}
		if len(fn.Args) != 1 {
			// Unnest has more than one argument.
			return false
		}
		if !c.IsStaticArray(fn.Args[0]) && !c.WrapsJSONArray(fn.Args[0]) {
			// Argument is not an ArrayExpr or ConstExpr wrapping a DArray or DJSON.
			return false
		}
	}
	return true
}

// ConstructValuesFromZips constructs a Values operator with the elements from
// the given ArrayExpr(s) or the ConstExpr(s) that wrap a DArray or DJSON in the
// given ZipExpr.
//
// The functions contained in the ZipExpr must be unnest, json_array_elements or
// jsonb_array_elements functions with a single parameter each. The parameters
// of the unnest functions must be either ArrayExprs or ConstExprs wrapping
// DArrays. The parameters of the json_array_elements functions must be
// ConstExprs wrapping DJSON datums.
func (c *CustomFuncs) ConstructValuesFromZips(zip memo.ZipExpr) memo.RelExpr {
	numCols := len(zip)
	outColTypes := make([]*types.T, numCols)
	outColIDs := make(opt.ColList, numCols)
	var outRows []memo.ScalarListExpr

	// Get type and ColumnID of each column.
	for i := range zip {
		function := zip[i].Fn.(*memo.FunctionExpr)
		expr := function.Args[0]
		switch function.Name {
		case "unnest":
			outColTypes[i] = expr.DataType().ArrayContents()

		case "json_array_elements", "jsonb_array_elements":
			outColTypes[i] = types.Jsonb

		default:
			panic(errors.AssertionFailedf("invalid function name: %v", function.Name))
		}
		outColIDs[i] = zip[i].Cols[0]
	}

	// addValToOutRows inserts a value into outRows at the given index.
	addValToOutRows := func(expr opt.ScalarExpr, rIndex, cIndex int) {
		if rIndex >= len(outRows) {
			// If this is the largest column encountered so far, make a new row and
			// fill with NullExprs.
			outRows = append(outRows, make(memo.ScalarListExpr, numCols))
			for i := 0; i < numCols; i++ {
				outRows[rIndex][i] = c.f.ConstructNull(outColTypes[cIndex])
			}
		}
		outRows[rIndex][cIndex] = expr // Insert value into outRows.
	}

	// Fill outRows with values from the arrays in the ZipExpr.
	for i := range zip {
		function := zip[i].Fn.(*memo.FunctionExpr)
		param := function.Args[0]
		switch t := param.(type) {
		case *memo.ArrayExpr:
			for j, val := range t.Elems {
				addValToOutRows(val, j, i)
			}

		case *memo.ConstExpr:
			// Use a ValueGenerator to retrieve values from the datums wrapped
			// in the ConstExpr. These generators are used at runtime to unnest
			// values from regular and JSON arrays.
			generator, err := function.Overload.Generator(c.f.evalCtx, tree.Datums{t.Value})
			if err != nil {
				panic(errors.AssertionFailedf("generator retrieval failed: %v", err))
			}
			if err = generator.Start(c.f.evalCtx.Context, c.f.evalCtx.Txn); err != nil {
				panic(errors.AssertionFailedf("generator.Start failed: %v", err))
			}

			for j := 0; ; j++ {
				hasNext, err := generator.Next(c.f.evalCtx.Context)
				if err != nil {
					panic(errors.AssertionFailedf("generator.Next failed: %v", err))
				}
				if !hasNext {
					break
				}

				vals, err := generator.Values()
				if err != nil {
					panic(errors.AssertionFailedf("failed to retrieve values: %v", err))
				}
				if len(vals) != 1 {
					panic(errors.AssertionFailedf(
						"ValueGenerator didn't return exactly one value: %v", log.Safe(vals)))
				}
				val := c.f.ConstructConstVal(vals[0], vals[0].ResolvedType())
				addValToOutRows(val, j, i)
			}
			generator.Close(c.f.evalCtx.Context)

		default:
			panic(errors.AssertionFailedf("invalid parameter type"))
		}
	}

	// Convert outRows (a slice of ScalarListExprs) into a ScalarListExpr
	// containing a tuple for each row.
	tuples := make(memo.ScalarListExpr, len(outRows))
	for i, row := range outRows {
		tuples[i] = c.f.ConstructTuple(row, types.MakeTuple(outColTypes))
	}

	// Construct and return a Values operator.
	valuesPrivate := &memo.ValuesPrivate{Cols: outColIDs, ID: c.f.Metadata().NextUniqueID()}
	return c.f.ConstructValues(tuples, valuesPrivate)
}

// IsStaticArray returns true if the expression is either a ConstExpr that
// wraps a DArray or an ArrayExpr. The complete set of expressions within a
// static array can be determined during planning:
//
//   ARRAY[1,2]
//   ARRAY[x,y]
//
// By contrast, expressions within a dynamic array can only be determined at
// run-time:
//
//   SELECT (SELECT array_agg(x) FROM xy)
//
// Here, the length of the array is only known at run-time.
func (c *CustomFuncs) IsStaticArray(scalar opt.ScalarExpr) bool {
	if _, ok := scalar.(*memo.ArrayExpr); ok {
		return true
	}
	return c.IsConstArray(scalar)
}

// WrapsJSONArray returns true if the given ScalarExpr is a ConstExpr that wraps
// a DJSON datum that contains a JSON array.
func (c *CustomFuncs) WrapsJSONArray(expr opt.ScalarExpr) bool {
	if constExpr, ok := expr.(*memo.ConstExpr); ok {
		if dJSON, ok := constExpr.Value.(*tree.DJSON); ok {
			return dJSON.JSON.Type() == json.ArrayJSONType
		}
	}
	return false
}
