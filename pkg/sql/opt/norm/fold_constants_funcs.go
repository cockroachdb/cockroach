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
	"github.com/cockroachdb/errors"
)

// FoldingControl is used to control whether normalization rules allow constant
// folding of VolatilityStable operators.
//
// FoldingControl can be initialized in either "allow stable folds" or "disallow
// stable folds" state.
//
// For a query with placeholders, we don't want to fold stable operators when
// building the reusable normalized expression; we want to fold them at
// AssignPlaceholder time.
//
// For a query without placeholders, we build and optimize the expression
// allowing stable folds; we need to know if any stable folds occurred so we can
// prevent caching the resulting plan.
//
// Examples illustrating the various cases:
//
//  1) Prepare and execute query with placeholders
//
//     SELECT * FROM t WHERE time > now() - $1
//
//     During prepare, we disable stable folds, so the now() call is not folded.
//     At execution time, we enable stable folds before running
//     AssignPlaceholders; when the expression is recreated, now() will be
//     folded, along with the subtraction. If we have an index on time, we will
//     use it.
//
//  2) Prepare and execute query without placeholders
//
//     SELECT * FROM t WHERE time > now() - '1 minute'::INTERVAL
//
//     During prepare, we disable stable folds. After building the expression,
//     we check if we actually prevented any stable folds; in this case we did.
//     Because of that, we don't fully optimize the memo at prepare time. At
//     execution time we will take the same path as in example 1, running
//     AssignPlaceholders with stable folds enabled. We don't have any
//     placeholders here, but AssignPlaceholders will nevertheless recreate the
//     expression, allowing folding to happen.
//
//  3) Execute query without placeholders
//
//     SELECT * FROM t WHERE time > now() - '1 minute'::INTERVAL
//
//     To execute a query that is not prepared in advance, we build the
//     expression with stable folds enabled. Afterwards, we check if we actually
//     had any stable folds, in which case we don't put the resulting plan in
//     the plan cache. In the future, we may want to detect queries that are
//     re-executed frequently and cache a non-folded version like in the prepare
//     case.
//
type FoldingControl struct {
	// allowStable controls whether canFoldOperator returns true or false for
	// VolatilityStable.
	allowStable bool

	// encounteredStableFold is true if canFoldOperator was called with
	// VolatilityStable.
	encounteredStableFold bool
}

// AllowStableFolds initializes the FoldingControl in "allow stable folds"
// state.
func (fc *FoldingControl) AllowStableFolds() {
	fc.allowStable = true
	fc.encounteredStableFold = false
}

// DisallowStableFolds initializes the FoldingControl in "disallow stable folds"
// state.
func (fc *FoldingControl) DisallowStableFolds() {
	fc.allowStable = false
	fc.encounteredStableFold = false
}

// TemporarilyDisallowStableFolds disallows stable folds, runs
// the given function, and restores the original FoldingControl state.
//
// This is used when building expressions like computed column expressions and
// we want to be able to check whether the expression contains stable operators.
func (fc *FoldingControl) TemporarilyDisallowStableFolds(fn func()) {
	save := *fc
	defer func() { *fc = save }()
	fc.DisallowStableFolds()
	fn()
}

func (fc *FoldingControl) canFoldOperator(v tree.Volatility) bool {
	if v < tree.VolatilityStable {
		return true
	}
	if v > tree.VolatilityStable {
		return false
	}
	fc.encounteredStableFold = true
	return fc.allowStable
}

// PreventedStableFold returns true if we disallowed a stable fold; can only be
// called if DisallowStableFolds() was called.
func (fc *FoldingControl) PreventedStableFold() bool {
	if fc.allowStable {
		panic(errors.AssertionFailedf("called in allow-stable state"))
	}
	return fc.encounteredStableFold
}

// PermittedStableFold returns true if we allowed a stable fold; can only be
// called if AllowStableFolds() was called.
//
// Note that this does not guarantee that folding actually occurred - it is
// possible for folding to fail (e.g. due to the operator hitting an error).
func (fc *FoldingControl) PermittedStableFold() bool {
	if !fc.allowStable {
		panic(errors.AssertionFailedf("called in disallow-stable state"))
	}
	return fc.encounteredStableFold
}

// CanFoldOperator returns true if we should fold an operator with the given
// volatility. This depends on the foldingVolatility setting of the factory
// (which can be either VolatilityImmutable or VolatilityStable).
func (c *CustomFuncs) CanFoldOperator(v tree.Volatility) bool {
	return c.f.foldingControl.canFoldOperator(v)
}

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
		if !c.IsConstValueOrGroupOfConstValues(elem) {
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

// IsConstValueOrGroupOfConstValues returns true if the input is a constant,
// or an array or tuple with only constant elements.
func (c *CustomFuncs) IsConstValueOrGroupOfConstValues(input opt.ScalarExpr) bool {
	return memo.CanExtractConstDatum(input)
}

// IsNeverNull returns true if the input is a non-null constant value,
// any tuple, or any array.
func (c *CustomFuncs) IsNeverNull(input opt.ScalarExpr) bool {
	switch input.Op() {
	case opt.TrueOp, opt.FalseOp, opt.ConstOp, opt.TupleOp, opt.ArrayOp:
		return true
	}

	return false
}

// HasNullElement returns true if the input tuple has at least one constant,
// null element. Note that it only returns true if one element is known to be
// null. For example, given the tuple (1, x), it will return false because x is
// not guaranteed to be null.
func (c *CustomFuncs) HasNullElement(tup *memo.TupleExpr) bool {
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
func (c *CustomFuncs) HasAllNullElements(tup *memo.TupleExpr) bool {
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
func (c *CustomFuncs) HasNonNullElement(tup *memo.TupleExpr) bool {
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
func (c *CustomFuncs) HasAllNonNullElements(tup *memo.TupleExpr) bool {
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
// Otherwise, it returns ok=false.
func (c *CustomFuncs) FoldBinary(
	op opt.Operator, left, right opt.ScalarExpr,
) (_ opt.ScalarExpr, ok bool) {
	o, ok := memo.FindBinaryOverload(op, left.DataType(), right.DataType())
	if !ok || !c.CanFoldOperator(o.Volatility) {
		return nil, false
	}

	lDatum, rDatum := memo.ExtractConstDatum(left), memo.ExtractConstDatum(right)
	result, err := o.Fn(c.f.evalCtx, lDatum, rDatum)
	if err != nil {
		return nil, false
	}
	return c.f.ConstructConstVal(result, o.ReturnType), true
}

// FoldUnary evaluates a unary expression with a constant input. It returns
// a constant expression as long as it finds an appropriate overload function
// for the given operator and input type, and the evaluation causes no error.
// Otherwise, it returns ok=false.
func (c *CustomFuncs) FoldUnary(op opt.Operator, input opt.ScalarExpr) (_ opt.ScalarExpr, ok bool) {
	datum := memo.ExtractConstDatum(input)

	o, ok := memo.FindUnaryOverload(op, input.DataType())
	if !ok {
		return nil, false
	}

	result, err := o.Fn(c.f.evalCtx, datum)
	if err != nil {
		return nil, false
	}
	return c.f.ConstructConstVal(result, o.ReturnType), true
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

// FoldCast evaluates a cast expression with a constant input. It returns a
// constant expression as long as the evaluation causes no error. Otherwise, it
// returns ok=false.
func (c *CustomFuncs) FoldCast(input opt.ScalarExpr, typ *types.T) (_ opt.ScalarExpr, ok bool) {
	if typ.Family() == types.OidFamily {
		if typ.Oid() == types.RegClass.Oid() && input.DataType().Family() == types.StringFamily {
			expr, err := c.foldStringToRegclassCast(input, typ)
			if err == nil {
				return expr, true
			}
		}
		// Save this cast for the execbuilder.
		return nil, false
	}

	volatility, ok := tree.LookupCastVolatility(input.DataType(), typ)
	if !ok || !c.CanFoldOperator(volatility) {
		return nil, false
	}

	datum := memo.ExtractConstDatum(input)
	texpr := tree.NewTypedCastExpr(datum, typ)

	result, err := texpr.Eval(c.f.evalCtx)
	if err != nil {
		return nil, false
	}

	return c.f.ConstructConstVal(result, typ), true
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

// FoldComparison evaluates a comparison expression with constant inputs. It
// returns a constant expression as long as it finds an appropriate overload
// function for the given operator and input types, and the evaluation causes
// no error. Otherwise, it returns ok=false.
func (c *CustomFuncs) FoldComparison(
	op opt.Operator, left, right opt.ScalarExpr,
) (_ opt.ScalarExpr, ok bool) {
	var flipped, not bool
	o, flipped, not, ok := memo.FindComparisonOverload(op, left.DataType(), right.DataType())
	if !ok || !c.CanFoldOperator(o.Volatility) {
		return nil, false
	}

	lDatum, rDatum := memo.ExtractConstDatum(left), memo.ExtractConstDatum(right)
	if flipped {
		lDatum, rDatum = rDatum, lDatum
	}

	result, err := o.Fn(c.f.evalCtx, lDatum, rDatum)
	if err != nil {
		return nil, false
	}
	if b, ok := result.(*tree.DBool); ok && not {
		result = tree.MakeDBool(!*b)
	}
	return c.f.ConstructConstVal(result, types.Bool), true
}

// FoldIndirection evaluates an array indirection operator with constant inputs.
// It returns the referenced array element as a constant value, or ok=false if
// the evaluation results in an error.
func (c *CustomFuncs) FoldIndirection(input, index opt.ScalarExpr) (_ opt.ScalarExpr, ok bool) {
	// Index is 1-based, so convert to 0-based.
	indexD := memo.ExtractConstDatum(index)

	// Case 1: The input is a static array constructor.
	if arr, ok := input.(*memo.ArrayExpr); ok {
		if indexInt, ok := indexD.(*tree.DInt); ok {
			indexI := int(*indexInt) - 1
			if indexI >= 0 && indexI < len(arr.Elems) {
				return arr.Elems[indexI], true
			}
			return c.f.ConstructNull(arr.Typ.ArrayContents()), true
		}
		if indexD == tree.DNull {
			return c.f.ConstructNull(arr.Typ.ArrayContents()), true
		}
		return nil, false
	}

	// Case 2: The input is a constant DArray.
	if memo.CanExtractConstDatum(input) {
		inputD := memo.ExtractConstDatum(input)
		texpr := tree.NewTypedIndirectionExpr(inputD, indexD, input.DataType().ArrayContents())
		result, err := texpr.Eval(c.f.evalCtx)
		if err == nil {
			return c.f.ConstructConstVal(result, texpr.ResolvedType()), true
		}
	}

	return nil, false
}

// FoldColumnAccess tries to evaluate a tuple column access operator with a
// constant tuple input (though tuple field values do not need to be constant).
// It returns the referenced tuple field value, or ok=false if folding is not
// possible or results in an error.
func (c *CustomFuncs) FoldColumnAccess(
	input opt.ScalarExpr, idx memo.TupleOrdinal,
) (_ opt.ScalarExpr, ok bool) {
	// Case 1: The input is NULL. This is possible when FoldIndirection has
	// already folded an Indirection expression with an out-of-bounds index to
	// Null.
	if n, ok := input.(*memo.NullExpr); ok {
		return c.f.ConstructNull(n.Typ.TupleContents()[idx]), true
	}

	// Case 2: The input is a static tuple constructor.
	if tup, ok := input.(*memo.TupleExpr); ok {
		return tup.Elems[idx], true
	}

	// Case 3: The input is a constant DTuple.
	if memo.CanExtractConstDatum(input) {
		datum := memo.ExtractConstDatum(input)

		texpr := tree.NewTypedColumnAccessExpr(datum, "" /* by-index access */, int(idx))
		result, err := texpr.Eval(c.f.evalCtx)
		if err == nil {
			return c.f.ConstructConstVal(result, texpr.ResolvedType()), true
		}
	}

	return nil, false
}

// CanFoldFunctionWithNullArg returns true if the given function can be folded
// to Null when any of its arguments are Null. A function can be folded to Null
// in this case if all of the following are true:
//
//   1. It does not allow Null arguments (NullableArgs=false).
//   2. It is a normal function, not an aggregate, window, or generator.
//
// See FoldFunctionWithNullArg for more details.
func (c *CustomFuncs) CanFoldFunctionWithNullArg(private *memo.FunctionPrivate) bool {
	return !private.Properties.NullableArgs &&
		private.Properties.Class == tree.NormalClass
}

// HasNullArg returns true if one of args is Null.
func (c *CustomFuncs) HasNullArg(args memo.ScalarListExpr) bool {
	for i := range args {
		if args[i].Op() == opt.NullOp {
			return true
		}
	}
	return false
}

// FunctionReturnType returns the return type of the given function.
func (c *CustomFuncs) FunctionReturnType(private *memo.FunctionPrivate) *types.T {
	return private.Typ
}

// FoldFunction evaluates a function expression with constant inputs. It returns
// a constant expression as long as the function is contained in the
// FoldFunctionAllowlist, and the evaluation causes no error. Otherwise, it
// returns ok=false.
func (c *CustomFuncs) FoldFunction(
	args memo.ScalarListExpr, private *memo.FunctionPrivate,
) (_ opt.ScalarExpr, ok bool) {
	// Non-normal function classes (aggregate, window, generator) cannot be
	// folded into a single constant.
	if private.Properties.Class != tree.NormalClass {
		return nil, false
	}

	if !c.CanFoldOperator(private.Overload.Volatility) {
		return nil, false
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
		return nil, false
	}
	return c.f.ConstructConstVal(result, private.Typ), true
}
