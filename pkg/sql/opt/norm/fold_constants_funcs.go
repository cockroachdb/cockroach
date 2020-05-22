// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// FoldNullUnary replaces the unary operator with a typed null value having the
// same type as the unary operator would have.
func (c *CustomFuncs) FoldNullUnary(op opt.Operator, input opt.ScalarExpr) opt.ScalarExpr {
	return c.f.ConstructNull(memo.InferUnaryType(op, input.DataType()))
}

// FoldNullBinary replaces the binary operator with a typed null value having
// the same type as the binary operator would have.
func (c *CustomFuncs) FoldNullBinary(op opt.Operator, left, right opt.ScalarExpr) opt.ScalarExpr {
	return c.f.ConstructNull(memo.InferBinaryType(op, left.DataType(), right.DataType()))
}

// AllowNullArgs returns true if the binary operator with the given inputs
// allows one of those inputs to be null. If not, then the binary operator will
// simply be replaced by null.
func (c *CustomFuncs) AllowNullArgs(op opt.Operator, left, right opt.ScalarExpr) bool {
	return memo.BinaryAllowsNullArgs(op, left.DataType(), right.DataType())
}

// IsListOfConstants returns true if elems is a list of constant values or
// tuples.
func (c *CustomFuncs) IsListOfConstants(elems memo.ScalarListExpr) bool {
	for _, elem := range elems {
		if !c.IsConstValueOrTuple(elem) {
			return false
		}
	}
	return true
}

// FoldArray evaluates an Array expression with constant inputs. It returns the
// array as a Const datum with type TArray.
func (c *CustomFuncs) FoldArray(elems memo.ScalarListExpr, typ *types.T) opt.ScalarExpr {
	elemType := typ.ArrayContents()
	a := tree.NewDArray(elemType)
	a.Array = make(tree.Datums, len(elems))
	for i := range a.Array {
		a.Array[i] = memo.ExtractConstDatum(elems[i])
		if a.Array[i] == tree.DNull {
			a.HasNulls = true
		} else {
			a.HasNonNulls = true
		}
	}
	return c.f.ConstructConst(a, typ)
}

// IsConstValueOrTuple returns true if the input is a constant or a tuple of
// constants.
func (c *CustomFuncs) IsConstValueOrTuple(input opt.ScalarExpr) bool {
	return memo.CanExtractConstDatum(input)
}

// HasNullElement returns true if the input tuple has at least one constant,
// null element. Note that it only returns true if one element is known to be
// null. For example, given the tuple (1, x), it will return false because x is
// not guaranteed to be null.
func (c *CustomFuncs) HasNullElement(input opt.ScalarExpr) bool {
	tup := input.(*memo.TupleExpr)
	for _, e := range tup.Elems {
		if e.Op() == opt.NullOp {
			return true
		}
	}
	return false
}

// HasAllNullElements returns true if the input tuple has only constant, null
// elements, or if the tuple is empty (has 0 elements). Note that it only
// returns true if all elements are known to be null. For example, given the
// tuple (NULL, x), it will return false because x is not guaranteed to be
// null.
func (c *CustomFuncs) HasAllNullElements(input opt.ScalarExpr) bool {
	tup := input.(*memo.TupleExpr)
	for _, e := range tup.Elems {
		if e.Op() != opt.NullOp {
			return false
		}
	}
	return true
}

// HasNonNullElement returns true if the input tuple has at least one constant,
// non-null element. Note that it only returns true if one element is known to
// be non-null. For example, given the tuple (NULL, x), it will return false
// because x is not guaranteed to be non-null.
func (c *CustomFuncs) HasNonNullElement(input opt.ScalarExpr) bool {
	tup := input.(*memo.TupleExpr)
	for _, e := range tup.Elems {
		// It is guaranteed that the input has at least one non-null element if
		// e is not null and it is either a constant value, array, or tuple.
		// Note that it doesn't matter whether a nested tuple has non-null
		// elements or not. For example, (NULL, (NULL, NULL)) IS NULL evaluates
		// to false because one first-level element is not null - the second is
		// a tuple.
		if e.Op() != opt.NullOp && (opt.IsConstValueOp(e) || e.Op() == opt.TupleOp || e.Op() == opt.ArrayOp) {
			return true
		}
	}
	return false
}

// HasAllNonNullElements returns true if the input tuple has all constant,
// non-null elements, or if the tuple is empty (has 0 elements). Note that it
// only returns true if all elements are known to be non-null. For example,
// given the tuple (1, x), it will return false because x is not guaranteed to
// be non-null.
func (c *CustomFuncs) HasAllNonNullElements(input opt.ScalarExpr) bool {
	tup := input.(*memo.TupleExpr)
	for _, e := range tup.Elems {
		// It is not guaranteed that the input has all non-null elements if e
		// is null or it is neither a constant value, array, nor tuple. Note
		// that it doesn't matter whether a nested tuple has non-null elements
		// or not. For example, (1, (NULL, NULL)) IS NOT NULL evaluates to true
		// because all first-level elements are not null.
		if e.Op() == opt.NullOp || !(opt.IsConstValueOp(e) || e.Op() == opt.TupleOp || e.Op() == opt.ArrayOp) {
			return false
		}
	}
	return true
}

// FoldBinary evaluates a binary expression with constant inputs. It returns
// a constant expression as long as it finds an appropriate overload function
// for the given operator and input types, and the evaluation causes no error.
func (c *CustomFuncs) FoldBinary(op opt.Operator, left, right opt.ScalarExpr) opt.ScalarExpr {
	lDatum, rDatum := memo.ExtractConstDatum(left), memo.ExtractConstDatum(right)

	o, ok := memo.FindBinaryOverload(op, left.DataType(), right.DataType())
	if !ok {
		return nil
	}

	result, err := o.Fn(c.f.evalCtx, lDatum, rDatum)
	if err != nil {
		return nil
	}
	return c.f.ConstructConstVal(result, o.ReturnType)
}

// FoldUnary evaluates a unary expression with a constant input. It returns
// a constant expression as long as it finds an appropriate overload function
// for the given operator and input type, and the evaluation causes no error.
func (c *CustomFuncs) FoldUnary(op opt.Operator, input opt.ScalarExpr) opt.ScalarExpr {
	datum := memo.ExtractConstDatum(input)

	o, ok := memo.FindUnaryOverload(op, input.DataType())
	if !ok {
		return nil
	}

	result, err := o.Fn(c.f.evalCtx, datum)
	if err != nil {
		return nil
	}
	return c.f.ConstructConstVal(result, o.ReturnType)
}

// foldStringToRegclassCast resolves a string that is a table name into an OID
// by resolving the table name and returning its table ID. This permits the
// optimizer to do intelligent things like push down filters that look like:
// ... WHERE oid = 'my_table'::REGCLASS
func (c *CustomFuncs) foldStringToRegclassCast(
	input opt.ScalarExpr, typ *types.T,
) (opt.ScalarExpr, error) {
	// Special case: we're casting a string to a REGCLASS oid, which is a
	// table id lookup.
	flags := cat.Flags{AvoidDescriptorCaches: false, NoTableStats: true}
	datum := memo.ExtractConstDatum(input)
	s := tree.MustBeDString(datum)
	tn, err := parser.ParseQualifiedTableName(string(s))
	if err != nil {
		return nil, err
	}
	ds, resName, err := c.f.catalog.ResolveDataSource(c.f.evalCtx.Context, flags, tn)
	if err != nil {
		return nil, err
	}

	c.mem.Metadata().AddDependency(opt.DepByName(&resName), ds, privilege.SELECT)

	regclassOid := tree.NewDOidWithName(tree.DInt(ds.PostgresDescriptorID()), types.RegClass, string(tn.ObjectName))
	return c.f.ConstructConstVal(regclassOid, typ), nil

}

// FoldCast evaluates a cast expression with a constant input. It returns
// a constant expression as long as the evaluation causes no error.
func (c *CustomFuncs) FoldCast(input opt.ScalarExpr, typ *types.T) opt.ScalarExpr {
	if typ.Family() == types.OidFamily {
		if typ.Oid() == types.RegClass.Oid() && input.DataType().Family() == types.StringFamily {
			expr, err := c.foldStringToRegclassCast(input, typ)
			if err == nil {
				return expr
			}
		}
		// Save this cast for the execbuilder.
		return nil
	}

	datum := memo.ExtractConstDatum(input)
	texpr := tree.NewTypedCastExpr(datum, typ)

	result, err := texpr.Eval(c.f.evalCtx)
	if err != nil {
		return nil
	}

	return c.f.ConstructConstVal(result, typ)
}

// isMonotonicConversion returns true if conversion of a value from FROM to
// TO is monotonic.
// That is, if a and b are values of type FROM, then
//
//   1. a = b implies a::TO = b::TO and
//   2. a < b implies a::TO <= b::TO
//
// Property (1) can be violated by cases like:
//
//   '-0'::FLOAT = '0'::FLOAT, but '-0'::FLOAT::STRING != '0'::FLOAT::STRING
//
// Property (2) can be violated by cases like:
//
//   2 < 10, but  2::STRING > 10::STRING.
//
// Note that the stronger version of (2),
//
//   a < b implies a::TO < b::TO
//
// is not required, for instance this is not generally true of conversion from
// a TIMESTAMP to a DATE, but certain such conversions can still generate spans
// in some cases where values under FROM and TO are "the same" (such as where a
// TIMESTAMP precisely falls on a date boundary).  We don't need this property
// because we will subsequently check that the values can round-trip to ensure
// that we don't lose any information by doing the conversion.
// TODO(justin): fill this out with the complete set of such conversions.
func isMonotonicConversion(from, to *types.T) bool {
	switch from.Family() {
	case types.TimestampFamily, types.TimestampTZFamily, types.DateFamily:
		switch to.Family() {
		case types.TimestampFamily, types.TimestampTZFamily, types.DateFamily:
			return true
		}
		return false

	case types.IntFamily, types.FloatFamily, types.DecimalFamily:
		switch to.Family() {
		case types.IntFamily, types.FloatFamily, types.DecimalFamily:
			return true
		}
		return false
	}

	return false
}

// UnifyComparison attempts to convert a constant expression to the type of the
// variable expression, if that conversion can round-trip and is monotonic.
func (c *CustomFuncs) UnifyComparison(left, right opt.ScalarExpr) opt.ScalarExpr {
	v := left.(*memo.VariableExpr)
	cnst := right.(*memo.ConstExpr)

	desiredType := v.DataType()
	originalType := cnst.DataType()

	// Don't bother if they're already the same.
	if desiredType.Equivalent(originalType) {
		return nil
	}

	if !isMonotonicConversion(originalType, desiredType) {
		return nil
	}

	// Check that the datum can round-trip between the types. If this is true, it
	// means we don't lose any information needed to generate spans, and combined
	// with monotonicity means that it's safe to convert the RHS to the type of
	// the LHS.
	convertedDatum, err := tree.PerformCast(c.f.evalCtx, cnst.Value, desiredType)
	if err != nil {
		return nil
	}

	convertedBack, err := tree.PerformCast(c.f.evalCtx, convertedDatum, originalType)
	if err != nil {
		return nil
	}

	if convertedBack.Compare(c.f.evalCtx, cnst.Value) != 0 {
		return nil
	}

	return c.f.ConstructConst(convertedDatum, desiredType)
}

// FoldComparison evaluates a comparison expression with constant inputs. It
// returns a constant expression as long as it finds an appropriate overload
// function for the given operator and input types, and the evaluation causes
// no error.
func (c *CustomFuncs) FoldComparison(op opt.Operator, left, right opt.ScalarExpr) opt.ScalarExpr {
	lDatum, rDatum := memo.ExtractConstDatum(left), memo.ExtractConstDatum(right)

	var flipped, not bool
	o, flipped, not, ok := memo.FindComparisonOverload(op, left.DataType(), right.DataType())
	if !ok {
		return nil
	}

	if flipped {
		lDatum, rDatum = rDatum, lDatum
	}

	result, err := o.Fn(c.f.evalCtx, lDatum, rDatum)
	if err != nil {
		return nil
	}
	if b, ok := result.(*tree.DBool); ok && not {
		result = tree.MakeDBool(!*b)
	}
	return c.f.ConstructConstVal(result, types.Bool)
}

// FoldIndirection evaluates an array indirection operator with constant inputs.
// It returns the referenced array element as a constant value, or nil if the
// evaluation results in an error.
func (c *CustomFuncs) FoldIndirection(input, index opt.ScalarExpr) opt.ScalarExpr {
	// Index is 1-based, so convert to 0-based.
	indexD := memo.ExtractConstDatum(index)

	// Case 1: The input is a static array constructor.
	if arr, ok := input.(*memo.ArrayExpr); ok {
		if indexInt, ok := indexD.(*tree.DInt); ok {
			indexI := int(*indexInt) - 1
			if indexI >= 0 && indexI < len(arr.Elems) {
				return arr.Elems[indexI]
			}
			return c.f.ConstructNull(arr.Typ.ArrayContents())
		}
		if indexD == tree.DNull {
			return c.f.ConstructNull(arr.Typ.ArrayContents())
		}
		return nil
	}

	// Case 2: The input is a constant DArray.
	if memo.CanExtractConstDatum(input) {
		inputD := memo.ExtractConstDatum(input)
		texpr := tree.NewTypedIndirectionExpr(inputD, indexD, input.DataType().ArrayContents())
		result, err := texpr.Eval(c.f.evalCtx)
		if err == nil {
			return c.f.ConstructConstVal(result, texpr.ResolvedType())
		}
	}

	return nil
}

// FoldColumnAccess tries to evaluate a tuple column access operator with a
// constant tuple input (though tuple field values do not need to be constant).
// It returns the referenced tuple field value, or nil if folding is not
// possible or results in an error.
func (c *CustomFuncs) FoldColumnAccess(input opt.ScalarExpr, idx memo.TupleOrdinal) opt.ScalarExpr {
	// Case 1: The input is a static tuple constructor.
	if tup, ok := input.(*memo.TupleExpr); ok {
		return tup.Elems[idx]
	}

	// Case 2: The input is a constant DTuple.
	if memo.CanExtractConstDatum(input) {
		datum := memo.ExtractConstDatum(input)

		texpr := tree.NewTypedColumnAccessExpr(datum, "" /* by-index access */, int(idx))
		result, err := texpr.Eval(c.f.evalCtx)
		if err == nil {
			return c.f.ConstructConstVal(result, texpr.ResolvedType())
		}
	}

	return nil
}

// FoldFunction evaluates a function expression with constant inputs. It
// returns a constant expression as long as the function is contained in the
// FoldFunctionWhitelist, and the evaluation causes no error.
func (c *CustomFuncs) FoldFunction(
	args memo.ScalarListExpr, private *memo.FunctionPrivate,
) opt.ScalarExpr {
	// Non-normal function classes (aggregate, window, generator) cannot be
	// folded into a single constant.
	if private.Properties.Class != tree.NormalClass {
		return nil
	}
	// Functions that aren't immutable and also not in the whitelist cannot
	// be folded.
	if _, ok := FoldFunctionWhitelist[private.Name]; private.Overload.Volatility != tree.VolatilityImmutable && !ok {
		return nil
	}

	exprs := make(tree.TypedExprs, len(args))
	for i := range exprs {
		exprs[i] = memo.ExtractConstDatum(args[i])
	}
	funcRef := tree.WrapFunction(private.Name)
	fn := tree.NewTypedFuncExpr(
		funcRef,
		0, /* aggQualifier */
		exprs,
		nil, /* filter */
		nil, /* windowDef */
		private.Typ,
		private.Properties,
		private.Overload,
	)

	result, err := fn.Eval(c.f.evalCtx)
	if err != nil {
		return nil
	}
	return c.f.ConstructConstVal(result, private.Typ)
}

// FoldFunctionWhitelist contains non-immutable functions that are nevertheless
// known to be safe for folding.
var FoldFunctionWhitelist = map[string]struct{}{
	// The SQL statement is generated in the optbuilder phase, so the remaining
	// function execution is immutable.
	"addgeometrycolumn": {},

	// Query plan cache is invalidated on location changes.
	"crdb_internal.locality_value": {},
}
