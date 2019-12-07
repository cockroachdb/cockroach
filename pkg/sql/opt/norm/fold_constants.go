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
	return c.f.ConstructConst(a)
}

// IsConstValueOrTuple returns true if the input is a constant or a tuple of
// constants.
func (c *CustomFuncs) IsConstValueOrTuple(input opt.ScalarExpr) bool {
	return memo.CanExtractConstDatum(input)
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

	regclassOid := tree.NewDOidWithName(tree.DInt(ds.PostgresDescriptorID()), types.RegClass, string(tn.TableName))
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
	texpr, err := tree.NewTypedCastExpr(datum, typ)
	if err != nil {
		return nil
	}

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

	return c.f.ConstructConst(convertedDatum)
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
	indexI := int(*indexD.(*tree.DInt)) - 1

	// Case 1: The input is a static array constructor.
	if arr, ok := input.(*memo.ArrayExpr); ok {
		if indexI >= 0 && indexI < len(arr.Elems) {
			return arr.Elems[indexI]
		}
		return c.f.ConstructNull(arr.Typ.ArrayContents())
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

		colName := input.DataType().TupleLabels()[idx]
		texpr := tree.NewTypedColumnAccessExpr(datum, colName, int(idx))
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
	if _, ok := FoldFunctionWhitelist[private.Name]; !ok {
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

// FoldFunctionWhitelist contains functions that are known to always produce
// the same result given the same set of arguments. This excludes impure
// functions and functions that rely on context such as locale or current user.
// See sql/sem/builtins/builtins.go for the function definitions.
// TODO(rytaft): This is a stopgap until #26582 is completed to identify
// functions as immutable, stable or volatile.
var FoldFunctionWhitelist = map[string]struct{}{
	"length":                       {},
	"char_length":                  {},
	"character_length":             {},
	"bit_length":                   {},
	"octet_length":                 {},
	"lower":                        {},
	"upper":                        {},
	"substr":                       {},
	"substring":                    {},
	"concat":                       {},
	"concat_ws":                    {},
	"convert_from":                 {},
	"convert_to":                   {},
	"to_uuid":                      {},
	"from_uuid":                    {},
	"abbrev":                       {},
	"broadcast":                    {},
	"family":                       {},
	"host":                         {},
	"hostmask":                     {},
	"masklen":                      {},
	"netmask":                      {},
	"set_masklen":                  {},
	"text":                         {},
	"inet_same_family":             {},
	"inet_contained_by_or_equals":  {},
	"inet_contains_or_equals":      {},
	"from_ip":                      {},
	"to_ip":                        {},
	"split_part":                   {},
	"repeat":                       {},
	"encode":                       {},
	"decode":                       {},
	"ascii":                        {},
	"chr":                          {},
	"md5":                          {},
	"sha1":                         {},
	"sha256":                       {},
	"sha512":                       {},
	"fnv32":                        {},
	"fnv32a":                       {},
	"fnv64":                        {},
	"fnv64a":                       {},
	"crc32ieee":                    {},
	"crc32c":                       {},
	"to_hex":                       {},
	"to_english":                   {},
	"strpos":                       {},
	"overlay":                      {},
	"lpad":                         {},
	"rpad":                         {},
	"btrim":                        {},
	"ltrim":                        {},
	"rtrim":                        {},
	"reverse":                      {},
	"replace":                      {},
	"translate":                    {},
	"regexp_extract":               {},
	"regexp_replace":               {},
	"like_escape":                  {},
	"not_like_escape":              {},
	"ilike_escape":                 {},
	"not_ilike_escape":             {},
	"similar_to_escape":            {},
	"not_similar_to_escape":        {},
	"initcap":                      {},
	"quote_ident":                  {},
	"left":                         {},
	"right":                        {},
	"greatest":                     {},
	"least":                        {},
	"abs":                          {},
	"acos":                         {},
	"asin":                         {},
	"atan":                         {},
	"atan2":                        {},
	"cbrt":                         {},
	"ceil":                         {},
	"ceiling":                      {},
	"cos":                          {},
	"cot":                          {},
	"degrees":                      {},
	"div":                          {},
	"exp":                          {},
	"floor":                        {},
	"isnan":                        {},
	"ln":                           {},
	"log":                          {},
	"mod":                          {},
	"pi":                           {},
	"pow":                          {},
	"power":                        {},
	"radians":                      {},
	"round":                        {},
	"sin":                          {},
	"sign":                         {},
	"sqrt":                         {},
	"tan":                          {},
	"trunc":                        {},
	"string_to_array":              {},
	"array_to_string":              {},
	"array_length":                 {},
	"array_lower":                  {},
	"array_upper":                  {},
	"array_append":                 {},
	"array_prepend":                {},
	"array_cat":                    {},
	"json_remove_path":             {},
	"json_extract_path":            {},
	"jsonb_extract_path":           {},
	"json_set":                     {},
	"jsonb_set":                    {},
	"jsonb_insert":                 {},
	"jsonb_pretty":                 {},
	"json_typeof":                  {},
	"jsonb_typeof":                 {},
	"array_to_json":                {},
	"to_json":                      {},
	"to_jsonb":                     {},
	"json_build_array":             {},
	"jsonb_build_array":            {},
	"json_build_object":            {},
	"jsonb_build_object":           {},
	"json_object":                  {},
	"jsonb_object":                 {},
	"json_strip_nulls":             {},
	"jsonb_strip_nulls":            {},
	"json_array_length":            {},
	"jsonb_array_length":           {},
	"crdb_internal.locality_value": {},
}
