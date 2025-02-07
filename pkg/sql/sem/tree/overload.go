// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
)

// SpecializedVectorizedBuiltin is used to map overloads
// to the vectorized operator that is specific to
// that implementation of the builtin function.
type SpecializedVectorizedBuiltin int

// TODO (rohany): What is the best place to put this list?
// I want to put it in builtins or exec, but those create an import
// cycle with exec. tree is imported by both of them, so
// this package seems like a good place to do it.

// Keep this list alphabetized so that it is easy to manage.
const (
	_ SpecializedVectorizedBuiltin = iota
	SubstringStringIntInt
	CrdbInternalRangeStats
	CrdbInternalRangeStatsWithErrors
)

// AggregateOverload is an opaque type which is used to box an eval.AggregateOverload.
type AggregateOverload interface {
	// Aggregate is just a marker method so folks don't think you can just shove
	// anything here. It ought to be an eval.AggregateOverload.
	Aggregate() // marker interface
}

// WindowOverload is an opaque type which is used to box an eval.WindowOverload.
type WindowOverload interface {
	// Window is just a marker method so folks don't think you can just shove
	// anything here. It ought to be an eval.WindowOverload.
	Window()
}

// FnWithExprsOverload is an opaque type used to box an
// eval.FnWithExprsOverload.
type FnWithExprsOverload interface {
	FnWithExprs()
}

// FnOverload is an opaque type used to box an eval.FnOverload.
//
// TODO(ajwerner): Give this a marker method and convert all usages.
// This is onerous at time of writing because there are so many.
type FnOverload interface{}

// GeneratorOverload is an opaque type used to box an eval.GeneratorOverload.
type GeneratorOverload interface {
	Generator()
}

// GeneratorWithExprsOverload is an opaque type used to box an eval.GeneratorWithExprsOverload.
type GeneratorWithExprsOverload interface {
	GeneratorWithExprs()
}

// SQLFnOverload is an opaque type used to box an eval.SQLFnOverload.
type SQLFnOverload interface {
	SQLFn()
}

// FunctionClass specifies the class of the builtin function.
type FunctionClass int

const (
	// NormalClass is a standard builtin function.
	NormalClass FunctionClass = iota
	// AggregateClass is a builtin aggregate function.
	AggregateClass
	// WindowClass is a builtin window function.
	WindowClass
	// GeneratorClass is a builtin generator function.
	GeneratorClass
	// SQLClass is a builtin function that executes a SQL statement as a side
	// effect of the function call.
	//
	// For example, AddGeometryColumn is a SQLClass function that executes an
	// ALTER TABLE ... ADD COLUMN statement to add a geometry column to an
	// existing table. It returns metadata about the column added.
	//
	// All builtin functions of this class should include a definition for
	// Overload.SQLFn, which returns the SQL statement to be executed. They
	// should also include a definition for Overload.Fn, which is executed
	// like a NormalClass function and returns a Datum.
	SQLClass
)

// String returns the string representation of the function class.
func (c FunctionClass) String() string {
	switch c {
	case NormalClass:
		return "normal"
	case AggregateClass:
		return "aggregate"
	case WindowClass:
		return "window"
	case GeneratorClass:
		return "generator"
	case SQLClass:
		return "SQL"
	default:
		panic(errors.AssertionFailedf("unexpected class %d", c))
	}
}

// RoutineType specifies the type of routine represented by an overload.
type RoutineType uint8

const (
	// BuiltinRoutine is a builtin function.
	BuiltinRoutine RoutineType = 1 << iota
	// UDFRoutine is a user-defined function.
	UDFRoutine
	// ProcedureRoutine is a user-defined procedure.
	ProcedureRoutine
)

// String returns the string representation of the routine type.
func (t RoutineType) String() string {
	switch t {
	case BuiltinRoutine:
		return "builtin"
	case UDFRoutine:
		return "udf"
	case ProcedureRoutine:
		return "procedure"
	default:
		panic(errors.AssertionFailedf("unexpected routine type %d", t))
	}
}

// OverloadPreference is used to disambiguate between eligible overload
// candidates during type-checking. When multiple overloads are eligible based
// on types even after applying all the other disambiguation heuristics, the
// overload with the highest preference will be chosen, if no other overloads
// have the same preference.
//
// NOTE: This is a hack that is necessary because we do not follow all of
// Postgres's type conversion rules. It should be used sparingly.
// See #75101.
type OverloadPreference int

const (
	OverloadPreferenceUnpreferred = OverloadPreference(-1)
	OverloadPreferenceNone        = OverloadPreference(0)
	OverloadPreferencePreferred   = OverloadPreference(1)
)

// Overload is one of the overloads of a built-in function.
// Each FunctionDefinition may contain one or more overloads.
type Overload struct {
	Types      TypeList
	ReturnType ReturnTyper
	Volatility volatility.V
	OverloadPreference

	// Info is a description of the function, which is surfaced on the CockroachDB
	// docs site on the "Functions and Operators" page. Descriptions typically use
	// third-person with the function as an implicit subject (e.g. "Calculates
	// infinity"), but should focus more on ease of understanding so other structures
	// might be more appropriate.
	Info string

	AggregateFunc AggregateOverload
	WindowFunc    WindowOverload

	// Only one of the "Fn", "FnWithExprs", "Generate", "GeneratorWithExprs",
	// "SQLFn" and "Body" attributes can be set.

	// Class is the kind of built-in function (normal/aggregate/window/etc.)
	Class FunctionClass

	// Fn is the normal builtin implementation function. It's for functions that
	// take in Datums and return a Datum.
	//
	// The opaque wrapper needs to be type asserted into eval.FnOverload.
	Fn FnOverload

	// FnWithExprs is for builtins that need access to their arguments as Exprs
	// and not pre-evaluated Datums, but is otherwise identical to Fn.
	FnWithExprs FnWithExprsOverload

	// Generator is for SRFs. SRFs take Datums and return multiple rows of Datums.
	Generator GeneratorOverload

	// GeneratorWithExprs is for SRFs that need access to their arguments as Exprs
	// and not pre-evaluated Datums, but is otherwise identical to Generator.
	GeneratorWithExprs GeneratorWithExprsOverload

	// SQLFn must be set for overloads of type SQLClass. It should return a SQL
	// statement which will be executed as a common table expression in the query.
	SQLFn SQLFnOverload

	// OnTypeCheck, if set, is called every time this overload is type checked.
	OnTypeCheck func()

	// SpecializedVecBuiltin is used to let the vectorized engine
	// know when an Overload has a specialized vectorized operator.
	SpecializedVecBuiltin SpecializedVectorizedBuiltin

	// IgnoreVolatilityCheck ignores checking the functions overload's
	// Volatility against Postgres's Volatility at test time.
	// This should be used with caution.
	IgnoreVolatilityCheck bool

	// Oid is the cached oidHasher.BuiltinOid result for this Overload. It's
	// populated at init-time.
	Oid oid.Oid

	// DistsqlBlocklist is set to true when a function cannot be evaluated in
	// DistSQL. One example is when the type information for function arguments
	// cannot be recovered.
	DistsqlBlocklist bool

	// CalledOnNullInput is set to true when a function is called when any of
	// its inputs are NULL. When true, the function implementation must be able
	// to handle NULL arguments.
	//
	// When set to false, the function will directly result in NULL in the
	// presence of any NULL arguments without evaluating the function's
	// implementation. Therefore, if the function is expected to produce
	// side-effects with a NULL argument, CalledOnNullInput must be true. Note
	// that if this behavior changes so that CalledOnNullInput=false functions
	// can produce side-effects, the FoldFunctionWithNullArg optimizer rule must
	// be changed to avoid folding those functions.
	//
	// NOTE: when set, a function should be prepared for any of its arguments to
	// be NULL and should act accordingly.
	CalledOnNullInput bool

	// FunctionProperties are the properties of this overload.
	FunctionProperties

	// Type indicates if the overload represents a built-in function, a
	// user-defined function, or a user-defined procedure.
	Type RoutineType
	// Body is the SQL string body of a function. It can be set even if Type is
	// BuiltinRoutine if a builtin function is defined using a SQL string.
	Body string
	// UDFContainsOnlySignature is only set to true for Overload signatures cached
	// in a Schema descriptor, which means that the full UDF descriptor need to be
	// fetched to get more info, e.g. function Body.
	UDFContainsOnlySignature bool
	// Version is the descriptor version of the descriptor used to construct
	// this version of the function overload. Only used for UDFs.
	Version uint64
	// Language is the function language that was used to define the UDF.
	// This is currently either SQL or PL/pgSQL.
	Language RoutineLanguage
	// RoutineParams contains all parameter information of the routine.
	//
	// Note that unlike Types (which only contains input parameters and defines
	// the signature of the function), RoutineParams contains all parameters as
	// well as their class.
	RoutineParams RoutineParams
	// OutParamOrdinals contains all ordinals of OUT parameters among all
	// parameters in the overload definition. For example, if the function /
	// procedure has a definition like
	//   (IN p0, INOUT p1, OUT p2, INOUT p3, OUT p4),
	// then OutParamOrdinals will contain [2, 4] (while ArgTypes will contain
	// types of [p0, p1, p3]).
	//
	// On the main code path it is only set when UDFContainsOnlySignature is
	// true (meaning that we only had access to the FunctionSignature proto).
	OutParamOrdinals []int32
	// OutParamTypes contains types of all OUT parameters (it has 1-to-1 match
	// with OutParamOrdinals).
	OutParamTypes TypeList
	// DefaultExprs specifies all DEFAULT expressions for input parameters.
	// Since the arguments can only be omitted from the end of the actual
	// argument list, we know exactly which input parameter each DEFAULT
	// expression corresponds to.
	//
	// On the main code path it is only set when UDFContainsOnlySignature is
	// true (meaning that we only had access to the FunctionSignature proto). If
	// UDFContainsOnlySignature is false, then DEFAULT expressions are included
	// into RoutineParams.
	DefaultExprs Exprs

	// SecurityMode is true when privilege checks during function execution
	// should be performed against the function owner rather than the invoking
	// user.
	SecurityMode RoutineSecurity
}

// params implements the overloadImpl interface.
func (b Overload) params() TypeList { return b.Types }

// returnType implements the overloadImpl interface.
func (b Overload) returnType() ReturnTyper { return b.ReturnType }

// preferred implements the overloadImpl interface.
func (b Overload) preference() OverloadPreference { return b.OverloadPreference }

func (b Overload) outParamInfo() (RoutineType, []int32, TypeList) {
	return b.Type, b.OutParamOrdinals, b.OutParamTypes
}

func (b Overload) defaultExprs() Exprs {
	return b.DefaultExprs
}

// FixedReturnType returns a fixed type that the function returns, returning AnyElement
// if the return type is based on the function's arguments.
func (b Overload) FixedReturnType() *types.T {
	if b.ReturnType == nil {
		return nil
	}
	return returnTypeToFixedType(b.ReturnType, nil)
}

// InferReturnTypeFromInputArgTypes returns the type that the function returns,
// inferring the type based on the function's inputTypes if necessary.
func (b Overload) InferReturnTypeFromInputArgTypes(inputTypes []*types.T) *types.T {
	retTyp := b.FixedReturnType()
	// If the output type of the function depends on its inputs, then
	// the output of FixedReturnType will be ambiguous. In the ambiguous
	// cases, use the information about the input types to construct the
	// appropriate output type. The tree.ReturnTyper interface is
	// []tree.TypedExpr -> *types.T, so construct the []tree.TypedExpr
	// from the types that we know are the inputs. Note that we don't
	// try to create datums of each input type, and instead use this
	// "TypedDummy" construct. This is because some types don't have resident
	// members (like an ENUM with no values), and we shouldn't error out
	// trying to infer the return type in those cases.
	if retTyp.IsAmbiguous() {
		args := make([]TypedExpr, len(inputTypes))
		for i, t := range inputTypes {
			args[i] = &TypedDummy{Typ: t}
		}
		// Evaluate ReturnType with the fake input set of arguments.
		retTyp = returnTypeToFixedType(b.ReturnType, args)
	}
	return retTyp
}

// IsGenerator returns true if the function is a set returning function (SRF).
func (b Overload) IsGenerator() bool {
	return b.Generator != nil || b.GeneratorWithExprs != nil
}

// HasSQLBody returns true if the function was defined using a SQL string body.
// This is the case for user-defined functions and some builtins.
func (b Overload) HasSQLBody() bool {
	return b.Type == UDFRoutine || b.Type == ProcedureRoutine || b.Body != ""
}

// Signature returns a human-readable signature.
// If simplify is bool, tuple-returning functions with just
// 1 tuple element unwrap the return type in the signature.
func (b Overload) Signature(simplify bool) string {
	return b.SignatureWithDefaults(simplify, false /* includeDefault */)
}

// SignatureWithDefaults is the same as Signature but also allows specifying
// whether DEFAULT expressions should be included.
func (b Overload) SignatureWithDefaults(simplify bool, includeDefaults bool) string {
	retType := b.FixedReturnType()
	if simplify {
		if retType.Family() == types.TupleFamily && len(retType.TupleContents()) == 1 {
			retType = retType.TupleContents()[0]
		}
	}
	t, ok := b.Types.(ParamTypes)
	if !includeDefaults || len(b.DefaultExprs) == 0 || !ok {
		return fmt.Sprintf("(%s) -> %s", b.Types.String(), retType)
	}
	return fmt.Sprintf("(%s) -> %s", t.StringWithDefaultExprs(b.DefaultExprs), retType)

}

// overloadImpl is an implementation of an overloaded function. It provides
// access to the parameter type list and the return type of the implementation.
//
// This is a more general type than Overload defined above, because it also
// works with the built-in binary and unary operators.
type overloadImpl interface {
	params() TypeList
	returnType() ReturnTyper
	preference() OverloadPreference
	// outParamInfo is only used for routines. See comment on
	// Overload.OutParamOrdinals and Overload.OutParamTypes for more details.
	outParamInfo() (_ RoutineType, outParamOrdinals []int32, outParamTypes TypeList)
	// defaultExprs is only used for routines. See comment on
	// Overload.DefaultExprs for more details.
	defaultExprs() Exprs
}

var _ overloadImpl = &Overload{}
var _ overloadImpl = &UnaryOp{}
var _ overloadImpl = &BinOp{}
var _ overloadImpl = &CmpOp{}

// GetParamsAndReturnType gets the parameters and return type of an
// overloadImpl.
func GetParamsAndReturnType(impl overloadImpl) (TypeList, ReturnTyper) {
	return impl.params(), impl.returnType()
}

// TypeList is a list of types representing a function parameter list.
type TypeList interface {
	// Match checks if all types in the TypeList match the corresponding elements in types.
	Match(types []*types.T) bool
	// MatchIdentical is similar to match but checks that the types are identical matches,
	// instead of equivalent matches. See types.T.Equivalent and types.T.Identical.
	MatchIdentical(types []*types.T) bool
	// MatchAt checks if the parameter type at index i of the TypeList matches type typ.
	// In all implementations, types.Null will match with each parameter type, allowing
	// NULL values to be used as arguments.
	MatchAt(typ *types.T, i int) bool
	// MatchAtIdentical is similar to MatchAt but checks that the type at index i of
	// the Typelist is identical to typ.
	MatchAtIdentical(typ *types.T, i int) bool
	// MatchLen checks that the TypeList can support l parameters.
	MatchLen(l int) bool
	// GetAt returns the type at the given index in the TypeList, or nil if the TypeList
	// cannot have a parameter at index i.
	GetAt(i int) *types.T
	// Length returns the number of types in the list
	Length() int
	// Types returns a realized copy of the list. variadic lists return a list of size one.
	Types() []*types.T
	// String returns a human readable signature
	String() string
}

var _ TypeList = ParamTypes{}
var _ TypeList = HomogeneousType{}
var _ TypeList = VariadicType{}

// ParamTypes is a list of function parameter names and their types.
type ParamTypes []ParamType

// ParamType encapsulate a function parameter name and type.
type ParamType struct {
	Name string
	Typ  *types.T
}

// Match is part of the TypeList interface.
func (p ParamTypes) Match(types []*types.T) bool {
	if len(types) != len(p) {
		return false
	}
	for i := range types {
		if !p.MatchAt(types[i], i) {
			return false
		}
	}
	return true
}

// MatchIdentical is part of the TypeList interface.
func (p ParamTypes) MatchIdentical(types []*types.T) bool {
	if len(types) != len(p) {
		return false
	}
	for i := range types {
		if !p.MatchAtIdentical(types[i], i) {
			return false
		}
	}
	return true
}

// MatchAt is part of the TypeList interface.
func (p ParamTypes) MatchAt(typ *types.T, i int) bool {
	// The parameterized types for Tuples are checked in the type checking
	// routines before getting here, so we only need to check if the parameter
	// type is p types.TUPLE below. This allows us to avoid defining overloads
	// for types.Tuple{}, types.Tuple{types.AnyElement}, types.Tuple{types.AnyElement, types.AnyElement},
	// etc. for Tuple operators.
	if typ.Family() == types.TupleFamily {
		typ = types.AnyTuple
	}
	return i < len(p) && (typ.Family() == types.UnknownFamily || p[i].Typ.Equivalent(typ))
}

// MatchAtIdentical is part of the TypeList interface.
func (p ParamTypes) MatchAtIdentical(typ *types.T, i int) bool {
	return i < len(p) && (typ.Family() == types.UnknownFamily ||
		p[i].Typ.Identical(typ) ||
		// Special case for CHAR, CHAR(N), and BPCHAR which are not "identical"
		// but have the same OID. See #129007.
		(p[i].Typ.Oid() == oid.T_bpchar && typ.Oid() == oid.T_bpchar) ||
		// Special case for BIT, BIT(N), and BIT(0) which are not "identical"
		// but have the same OID. See #132944.
		(p[i].Typ.Oid() == oid.T_bit && typ.Oid() == oid.T_bit))
}

// MatchLen is part of the TypeList interface.
func (p ParamTypes) MatchLen(l int) bool {
	return len(p) == l
}

// GetAt is part of the TypeList interface.
func (p ParamTypes) GetAt(i int) *types.T {
	return p[i].Typ
}

// Length is part of the TypeList interface.
func (p ParamTypes) Length() int {
	return len(p)
}

// Types is part of the TypeList interface.
func (p ParamTypes) Types() []*types.T {
	n := len(p)
	ret := make([]*types.T, n)
	for i, s := range p {
		ret[i] = s.Typ
	}
	return ret
}

func (p ParamTypes) String() string {
	var s strings.Builder
	for i, param := range p {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString(param.Name)
		s.WriteString(": ")
		s.WriteString(param.Typ.String())
	}
	return s.String()
}

// StringWithDefaultExprs extends the stringified form of ParamTypes with the
// corresponding DEFAULT expressions. defaultExprs is expected to have length no
// longer than p and to correspond to the "suffix" of p.
func (p ParamTypes) StringWithDefaultExprs(defaultExprs []Expr) string {
	var s strings.Builder
	for i, param := range p {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString(param.Name)
		s.WriteString(": ")
		s.WriteString(param.Typ.String())
		if defaultExprIdx := i - (len(p) - len(defaultExprs)); defaultExprIdx >= 0 {
			s.WriteString(" DEFAULT ")
			s.WriteString(defaultExprs[defaultExprIdx].String())
		}
	}
	return s.String()
}

// HomogeneousType is a TypeList implementation that accepts any arguments, as
// long as all are the same type or NULL. The homogeneous constraint is enforced
// in typeCheckOverloadedExprs.
type HomogeneousType struct{}

// Match is part of the TypeList interface.
func (HomogeneousType) Match(types []*types.T) bool {
	return true
}

// MatchIdentical is part of the TypeList interface.
func (HomogeneousType) MatchIdentical(types []*types.T) bool {
	return true
}

// MatchAt is part of the TypeList interface.
func (HomogeneousType) MatchAt(typ *types.T, i int) bool {
	return true
}

// MatchAtIdentical is part of the TypeList interface.
func (HomogeneousType) MatchAtIdentical(typ *types.T, i int) bool {
	return true
}

// MatchLen is part of the TypeList interface.
func (HomogeneousType) MatchLen(l int) bool {
	return true
}

// GetAt is part of the TypeList interface.
func (HomogeneousType) GetAt(i int) *types.T {
	return types.AnyElement
}

// Length is part of the TypeList interface.
func (HomogeneousType) Length() int {
	return 1
}

// Types is part of the TypeList interface.
func (HomogeneousType) Types() []*types.T {
	return []*types.T{types.AnyElement}
}

func (HomogeneousType) String() string {
	return "anyelement..."
}

// VariadicType is a TypeList implementation which accepts a fixed number of
// arguments at the beginning and an arbitrary number of homogenous arguments
// at the end.
type VariadicType struct {
	FixedTypes []*types.T
	VarType    *types.T
}

// Match is part of the TypeList interface.
func (v VariadicType) Match(types []*types.T) bool {
	for i := range types {
		if !v.MatchAt(types[i], i) {
			return false
		}
	}
	return true
}

// MatchIdentical is part of the TypeList interface.
func (VariadicType) MatchIdentical(types []*types.T) bool {
	return true
}

// MatchAt is part of the TypeList interface.
func (v VariadicType) MatchAt(typ *types.T, i int) bool {
	if i < len(v.FixedTypes) {
		return typ.Family() == types.UnknownFamily || v.FixedTypes[i].Equivalent(typ)
	}
	return typ.Family() == types.UnknownFamily || v.VarType.Equivalent(typ)
}

// MatchAtIdentical is part of the TypeList interface.
// TODO(mgartner): This will be incorrect once we add support for variadic UDFs
func (VariadicType) MatchAtIdentical(typ *types.T, i int) bool {
	return true
}

// MatchLen is part of the TypeList interface.
func (v VariadicType) MatchLen(l int) bool {
	return l >= len(v.FixedTypes)
}

// GetAt is part of the TypeList interface.
func (v VariadicType) GetAt(i int) *types.T {
	if i < len(v.FixedTypes) {
		return v.FixedTypes[i]
	}
	return v.VarType
}

// Length is part of the TypeList interface.
func (v VariadicType) Length() int {
	return len(v.FixedTypes) + 1
}

// Types is part of the TypeList interface.
func (v VariadicType) Types() []*types.T {
	result := make([]*types.T, len(v.FixedTypes)+1)
	copy(result, v.FixedTypes)
	result[len(result)-1] = v.VarType
	return result
}

func (v VariadicType) String() string {
	var s bytes.Buffer
	for i, t := range v.FixedTypes {
		if i != 0 {
			s.WriteString(", ")
		}
		s.WriteString(t.String())
	}
	if len(v.FixedTypes) > 0 {
		s.WriteString(", ")
	}
	fmt.Fprintf(&s, "%s...", v.VarType)
	return s.String()
}

// UnknownReturnType is returned from ReturnTypers when the arguments provided are
// not sufficient to determine a return type. This is necessary for cases like overload
// resolution, where the argument types are not resolved yet so the type-level function
// will be called without argument types. If a ReturnTyper returns unknownReturnType,
// then the candidate function set cannot be refined. This means that only ReturnTypers
// that never return unknownReturnType, like those created with FixedReturnType, can
// help reduce overload ambiguity.
var UnknownReturnType *types.T

// ReturnTyper defines the type-level function in which a builtin function's return type
// is determined. ReturnTypers should make sure to return unknownReturnType when necessary.
type ReturnTyper func(args []TypedExpr) *types.T

// FixedReturnType functions simply return a fixed type, independent of argument types.
func FixedReturnType(typ *types.T) ReturnTyper {
	return func(args []TypedExpr) *types.T { return typ }
}

// IdentityReturnType creates a returnType that is a projection of the idx'th
// argument type.
func IdentityReturnType(idx int) ReturnTyper {
	return func(args []TypedExpr) *types.T {
		if len(args) == 0 {
			return UnknownReturnType
		}
		return args[idx].ResolvedType()
	}
}

// ArrayOfFirstNonNullReturnType returns an array type from the first non-null
// type in the argument list.
func ArrayOfFirstNonNullReturnType() ReturnTyper {
	return func(args []TypedExpr) *types.T {
		if len(args) == 0 {
			return UnknownReturnType
		}
		for _, arg := range args {
			if t := arg.ResolvedType(); t.Family() != types.UnknownFamily {
				return types.MakeArray(t)
			}
		}
		return types.Unknown
	}
}

// FirstNonNullReturnType returns the type of the first non-null argument, or
// types.Unknown if all arguments are null. There must be at least one argument,
// or else FirstNonNullReturnType returns UnknownReturnType. This method is used
// with HomogeneousType functions, in which all arguments have been checked to
// have the same type (or be null).
func FirstNonNullReturnType() ReturnTyper {
	return func(args []TypedExpr) *types.T {
		if len(args) == 0 {
			return UnknownReturnType
		}
		for _, arg := range args {
			if t := arg.ResolvedType(); t.Family() != types.UnknownFamily {
				return t
			}
		}
		return types.Unknown
	}
}

func returnTypeToFixedType(s ReturnTyper, inputTyps []TypedExpr) *types.T {
	if t := s(inputTyps); t != UnknownReturnType {
		return t
	}
	return types.AnyElement
}

type overloadTypeChecker struct {
	overloads       []overloadImpl
	params          []TypeList
	overloadIdxs    []uint8 // index into overloads
	exprs           []Expr
	typedExprs      []TypedExpr
	resolvableIdxs  intsets.Fast // index into exprs/typedExprs
	constIdxs       intsets.Fast // index into exprs/typedExprs
	placeholderIdxs intsets.Fast // index into exprs/typedExprs
	overloadsIdxArr [16]uint8
}

var overloadTypeCheckerPool = sync.Pool{
	New: func() interface{} {
		var s overloadTypeChecker
		s.overloadIdxs = s.overloadsIdxArr[:0]
		return &s
	},
}

// getOverloadTypeChecker initialized an overloadTypeChecker from the pool. The
// returned object should be returned to the pool via its release method.
func getOverloadTypeChecker(o overloadSet, exprs ...Expr) *overloadTypeChecker {
	s := overloadTypeCheckerPool.Get().(*overloadTypeChecker)
	n := o.len()
	if n > cap(s.overloads) {
		s.overloads = make([]overloadImpl, n)
		s.params = make([]TypeList, n)
	} else {
		s.overloads = s.overloads[:n]
		s.params = s.params[:n]
	}
	for i := 0; i < n; i++ {
		got := o.get(i)
		s.overloads[i] = got
		s.params[i] = got.params()
	}
	s.exprs = append(s.exprs, exprs...)
	return s
}

func (s *overloadTypeChecker) release() {
	for i := range s.overloads {
		s.overloads[i] = nil
	}
	s.overloads = s.overloads[:0]
	for i := range s.params {
		s.params[i] = nil
	}
	s.params = s.params[:0]
	for i := range s.exprs {
		s.exprs[i] = nil
	}
	s.exprs = s.exprs[:0]
	for i := range s.typedExprs {
		s.typedExprs[i] = nil
	}
	s.typedExprs = s.typedExprs[:0]
	s.overloadIdxs = s.overloadIdxs[:0]
	s.resolvableIdxs = intsets.Fast{}
	s.constIdxs = intsets.Fast{}
	s.placeholderIdxs = intsets.Fast{}
	overloadTypeCheckerPool.Put(s)
}

type overloadSet interface {
	len() int
	get(i int) overloadImpl
}

// toParamOrdinal adjusts the provided parameter index to be within the
// appropriate parameter list and returns whether this parameter has OUT
// parameter class.
//
// It assumes that outParamOrdinals is non-empty.
func toParamOrdinal(i int, outParamOrdinals []int32) (ordinal int, isOutParam bool) {
	var outParamsSeen int
	for _, outParamOrdinal := range outParamOrdinals {
		if outParamOrdinal == int32(i) {
			// This expression corresponds to an OUT parameter.
			return outParamsSeen, true
		}
		if outParamOrdinal > int32(i) {
			break
		}
		outParamsSeen++
	}
	return i - outParamsSeen, false
}

// getParamsAndOrdinal returns the parameter list as well as the parameter
// ordinal within that list for the given routine type and parameter index.
//
//gcassert:inline
func getParamsAndOrdinal(
	routineType RoutineType, i int, params TypeList, outParamOrdinals []int32, outParams TypeList,
) (TypeList, int) {
	if routineType != ProcedureRoutine || len(outParamOrdinals) == 0 {
		return params, i
	}
	ordinal, isOutParam := toParamOrdinal(i, outParamOrdinals)
	if isOutParam {
		return outParams, ordinal
	}
	return params, ordinal
}

// typeCheckOverloadedExprs determines the correct overload to use for the given set of
// expression parameters, along with an optional desired return type. It returns the expression
// parameters after being type checked, along with a slice of candidate overloadImpls. The
// slice may have length:
//
//	 0: overload resolution failed because no compatible overloads were found
//	 1: overload resolution succeeded
//	2+: overload resolution failed because of ambiguity
//
// The inBinOp parameter denotes whether this type check is occurring within a binary operator,
// in which case we may need to make a guess that the two parameters are of the same type if one
// of them is NULL.
func (s *overloadTypeChecker) typeCheckOverloadedExprs(
	ctx context.Context, semaCtx *SemaContext, desired *types.T, inBinOp bool,
) (_ error) {
	numOverloads := len(s.overloads)
	if numOverloads > math.MaxUint8 {
		return errors.AssertionFailedf("too many overloads (%d > 255)", numOverloads)
	}

	// Special-case the HomogeneousType overload. We determine its return type by checking that
	// all parameters have the same type.
	for i := range s.params {
		// Only one overload can be provided if it has parameters with HomogeneousType.
		if _, ok := s.params[i].(HomogeneousType); ok {
			if numOverloads > 1 {
				return errors.AssertionFailedf(
					"only one overload can have HomogeneousType parameters")
			}
			typedExprs, _, err := typeCheckSameTypedExprs(ctx, semaCtx, desired, s.exprs...)
			if err != nil {
				return err
			}
			s.typedExprs = typedExprs
			s.overloadIdxs = append(s.overloadIdxs[:0], uint8(i))
			return nil
		}
	}

	// Hold the resolved type expressions of the provided exprs, in order.
	if cap(s.typedExprs) >= len(s.exprs) {
		s.typedExprs = s.typedExprs[:len(s.exprs)]
	} else {
		s.typedExprs = make([]TypedExpr, len(s.exprs))
	}
	s.constIdxs, s.placeholderIdxs, s.resolvableIdxs = typeCheckSplitExprs(s.exprs)

	// If no overloads are provided, just type check parameters and return.
	if numOverloads == 0 {
		for i, ok := s.resolvableIdxs.Next(0); ok; i, ok = s.resolvableIdxs.Next(i + 1) {
			typ, err := s.exprs[i].TypeCheck(ctx, semaCtx, types.AnyElement)
			if err != nil {
				return pgerror.Wrapf(err, pgcode.InvalidParameterValue,
					"error type checking resolved expression:")
			}
			s.typedExprs[i] = typ
		}
		if err := defaultTypeCheck(ctx, semaCtx, s, false); err != nil {
			return err
		}
		s.overloadIdxs = s.overloadIdxs[:0]
		return nil
	}

	if cap(s.overloadIdxs) < numOverloads {
		s.overloadIdxs = make([]uint8, 0, numOverloads)
	}
	s.overloadIdxs = s.overloadIdxs[:numOverloads]
	// foundOutParams and foundDefaultExprs indicate whether at least one
	// overload has non-nil outParamOrdinals mapping and defaultExprs,
	// respectively. If none are found, we can avoid redundant calls to the
	// outParamInfo() and defaultExprs() methods.
	var foundOutParams, foundDefaultExprs bool
	for i := range s.overloadIdxs {
		s.overloadIdxs[i] = uint8(i)
		if !foundOutParams {
			_, outParamOrdinals, _ := s.overloads[i].outParamInfo()
			foundOutParams = len(outParamOrdinals) > 0
		}
		if !foundDefaultExprs {
			foundDefaultExprs = len(s.overloads[i].defaultExprs()) > 0
		}
	}

	if semaCtx != nil && semaCtx.Properties.IgnoreUnpreferredOverloads {
		// Filter out unpreferred overloads.
		s.overloadIdxs = filterOverloads(s.overloadIdxs, s.overloads, func(ov overloadImpl) bool {
			return ov.preference() != OverloadPreferenceUnpreferred
		})
	}

	// Filter out incorrect parameter length overloads.
	matchLen := func(ov overloadImpl, params TypeList) bool {
		if !foundOutParams && !foundDefaultExprs {
			return params.MatchLen(len(s.exprs))
		}
		routineType, outParamOrdinals, _ := ov.outParamInfo()
		defaultExprs := ov.defaultExprs()
		numInputExprs := len(s.exprs)
		if routineType == ProcedureRoutine {
			// Ignore all OUT parameters since they are included in exprs but
			// not in params.
			numInputExprs = len(s.exprs) - len(outParamOrdinals)
		}
		if len(defaultExprs) == 0 {
			return params.MatchLen(numInputExprs)
		}
		// Some "suffix" parameters have DEFAULT expressions, so values for them
		// can be omitted from the input expressions.
		// TODO(88947): this logic might need to change to support VARIADIC.
		paramsLen := params.Length()
		return paramsLen-len(defaultExprs) <= numInputExprs && numInputExprs <= paramsLen
	}
	s.overloadIdxs = filterParams(s.overloadIdxs, s.overloads, s.params, matchLen)

	makeFilter := func(i int, fn func(params TypeList, ordinal int) bool) func(overloadImpl, TypeList) bool {
		if !foundOutParams {
			return func(_ overloadImpl, params TypeList) bool {
				return fn(params, i)
			}
		}
		return func(ov overloadImpl, params TypeList) bool {
			routineType, outParamOrdinals, outParams := ov.outParamInfo()
			p, ordinal := getParamsAndOrdinal(routineType, i, params, outParamOrdinals, outParams)
			return fn(p, ordinal)
		}
	}

	// Filter out overloads which constants cannot become.
	for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
		constExpr := s.exprs[i].(Constant)
		filter := makeFilter(i, func(params TypeList, ordinal int) bool {
			return canConstantBecome(constExpr, params.GetAt(ordinal))
		})
		s.overloadIdxs = filterParams(s.overloadIdxs, s.overloads, s.params, filter)
	}

	// TODO(nvanbenschoten): We should add a filtering step here to filter
	// out impossible candidates based on identical parameters. For instance,
	// f(int, float) is not a possible candidate for the expression f($1, $1).

	// Filter out overloads on resolved types. This includes resolved placeholders
	// and any other resolvable exprs.
	ambiguousCollatedTypes := false
	var typeableIdxs intsets.Fast
	for i, ok := s.resolvableIdxs.Next(0); ok; i, ok = s.resolvableIdxs.Next(i + 1) {
		typeableIdxs.Add(i)
	}
	for i, ok := s.placeholderIdxs.Next(0); ok; i, ok = s.placeholderIdxs.Next(i + 1) {
		if !semaCtx.isUnresolvedPlaceholder(s.exprs[i]) {
			typeableIdxs.Add(i)
		}
	}
	for i, ok := typeableIdxs.Next(0); ok; i, ok = typeableIdxs.Next(i + 1) {
		paramDesired := types.AnyElement

		// If all remaining candidates require the same type for this parameter,
		// begin desiring that type for the corresponding argument expression.
		// Note that this is always the case when we have a single overload left.
		var sameType *types.T
		for _, ovIdx := range s.overloadIdxs {
			ov := s.overloads[ovIdx]
			params, ordinal := s.params[ovIdx], i
			if foundOutParams {
				routineType, outParamOrdinals, outParams := ov.outParamInfo()
				params, ordinal = getParamsAndOrdinal(routineType, i, params, outParamOrdinals, outParams)
			}
			typ := params.GetAt(ordinal)
			if sameType == nil {
				sameType = typ
			} else if !typ.Identical(sameType) {
				sameType = nil
				break
			}
		}
		// Don't allow ambiguous types to be desired, this prevents for instance
		// AnyCollatedString from trumping the concrete collated type.
		if sameType != nil {
			paramDesired = sameType
		}
		typ, err := s.exprs[i].TypeCheck(ctx, semaCtx, paramDesired)
		if err != nil {
			return err
		}
		s.typedExprs[i] = typ
		if typ.ResolvedType().Identical(types.AnyCollatedString) {
			ambiguousCollatedTypes = true
		}
		rt := typ.ResolvedType()
		filter := makeFilter(i, func(params TypeList, ordinal int) bool {
			return params.MatchAt(rt, ordinal)
		})
		s.overloadIdxs = filterParams(s.overloadIdxs, s.overloads, s.params, filter)
	}

	// Remove any overloads with polymorphic parameters for which the supplied
	// argument types are invalid.
	s.overloadIdxs = filterOverloads(s.overloadIdxs, s.overloads, func(o overloadImpl) bool {
		ol, ok := o.(*Overload)
		if !ok || ol.Type == BuiltinRoutine {
			// Don't filter builtin routines.
			return true
		}
		params := ol.Types.(ParamTypes)
		var outParams ParamTypes
		if ol.Type == ProcedureRoutine && foundOutParams {
			outParams = ol.OutParamTypes.(ParamTypes)
		}
		hasPolymorphicTyp := false
		for i := range params {
			if params[i].Typ.IsPolymorphicType() {
				hasPolymorphicTyp = true
				break
			}
		}
		if !hasPolymorphicTyp {
			return true
		}
		argTypes := make([]*types.T, 0, len(params))
		outArgTypes := make([]*types.T, 0, len(outParams))
		for i := range s.exprs {
			typedExpr, err := s.exprs[i].TypeCheck(ctx, semaCtx, types.AnyElement)
			if err != nil {
				panic(errors.HandleAsAssertionFailure(err))
			}
			if ol.Type == ProcedureRoutine && foundOutParams {
				if _, isOutParam := toParamOrdinal(i, ol.OutParamOrdinals); isOutParam {
					// A CALL statement must specify an argument for each OUT parameter
					// of the procedure.
					outArgTypes = append(outArgTypes, typedExpr.ResolvedType())
					continue
				}
			}
			argTypes = append(argTypes, typedExpr.ResolvedType())
		}
		// Check the concrete types of the arguments supplied for IN parameters.
		// Only pass the parameters up to len(argTypes), since polymorphic type
		// checking for default expressions happens later.
		var anyElemTyp *types.T
		if ok, _, anyElemTyp = ResolvePolymorphicArgTypes(
			params[:len(argTypes)], argTypes, nil /* anyElemTyp */, false, /* enforceConsistency */
		); !ok {
			return false
		}
		if ol.Type != ProcedureRoutine || !foundOutParams {
			return true
		}
		// Check the concrete types of the arguments supplied for OUT parameters.
		// Use the concrete type previously resolved from ANYELEMENT IN parameters.
		// Note that DEFAULT expressions cannot be used for OUT parameters, so there
		// is no need to truncate the outParams slice.
		ok, _, _ = ResolvePolymorphicArgTypes(
			outParams, outArgTypes, anyElemTyp, false, /* enforceConsistency */
		)
		return ok
	})

	// If we typed any exprs as AnyCollatedString but have a concrete collated
	// string type, redo the types using the concrete type. Note we're probably
	// still lacking full compliance with PG on collation handling:
	// https://www.postgresql.org/docs/current/collation.html#id-1.6.11.4.4
	if ambiguousCollatedTypes {
		var concreteType *types.T
		for i, ok := typeableIdxs.Next(0); ok; i, ok = typeableIdxs.Next(i + 1) {
			typ := s.typedExprs[i].ResolvedType()
			if !typ.Identical(types.AnyCollatedString) {
				concreteType = typ
				break
			}
		}
		if concreteType != nil {
			for i, ok := typeableIdxs.Next(0); ok; i, ok = typeableIdxs.Next(i + 1) {
				if s.typedExprs[i].ResolvedType().Identical(types.AnyCollatedString) {
					typ, err := s.exprs[i].TypeCheck(ctx, semaCtx, concreteType)
					if err != nil {
						return err
					}
					s.typedExprs[i] = typ
				}
			}
		}
	}

	// At this point, all remaining overload candidates accept the argument list,
	// so we begin checking for a single remaining candidate implementation to choose.
	// In case there is more than one candidate remaining, the following code uses
	// heuristics to find a most preferable candidate.
	if ok, err := checkReturn(ctx, semaCtx, s); ok {
		return err
	}

	// The first heuristic is to prefer candidates that return the desired type,
	// if a desired type was provided.
	if desired.Family() != types.AnyFamily {
		s.overloadIdxs = filterOverloads(s.overloadIdxs, s.overloads, func(
			o overloadImpl,
		) bool {
			// For now, we only filter on the return type for overloads with
			// fixed return types. This could be improved, but is not currently
			// critical because we have no cases of functions with multiple
			// overloads that do not all expose FixedReturnTypes.
			if t := o.returnType()(nil); t != UnknownReturnType {
				return t.Equivalent(desired)
			}
			return true
		})
		if ok, err := checkReturn(ctx, semaCtx, s); ok {
			return err
		}
	}

	var homogeneousTyp *types.T
	if !typeableIdxs.Empty() {
		idx, _ := typeableIdxs.Next(0)
		homogeneousTyp = s.typedExprs[idx].ResolvedType()
		for i, ok := typeableIdxs.Next(idx); ok; i, ok = typeableIdxs.Next(i + 1) {
			if !homogeneousTyp.Equivalent(s.typedExprs[i].ResolvedType()) {
				homogeneousTyp = nil
				break
			}
		}
	}

	// If this is a binary operator with one untyped literal or placeholder,
	// check for a binary operator with parameter types matching the type of the
	// opposite argument.
	if inBinOp && len(s.exprs) == 2 {
		var typ *types.T
		numConsts := s.constIdxs.Len()
		numPlaceholders := s.placeholderIdxs.Len()
		if (numConsts == 1 && numPlaceholders == 0) ||
			(numConsts == 0 && numPlaceholders == 1) {
			// If one argument is a constant then it assumes the same type
			// as the other argument. This matches Postgres's behavior. See
			// the documentation about "unknown" types (this is how Postgres
			// initially types constant values):
			// https://www.postgresql.org/docs/17/typeconv-oper.html.
			if s.typedExprs[0] != nil {
				typ = s.typedExprs[0].ResolvedType()
			} else if s.typedExprs[1] != nil {
				typ = s.typedExprs[1].ResolvedType()
			}
		}
		if typ != nil {
			exactOverloads := filterOverloads(s.overloadIdxs, s.overloads,
				func(ov overloadImpl) bool {
					typs := ov.params().Types()
					return typ.Oid() == typs[0].Oid() && typ.Oid() == typs[1].Oid()
				})
			if len(exactOverloads) == 1 {
				prevOverloadIdxs := s.overloadIdxs
				s.overloadIdxs = exactOverloads
				if ok, err := checkReturn(ctx, semaCtx, s); ok {
					return err
				}
				s.overloadIdxs = prevOverloadIdxs
			}
		}
	}

	if !s.constIdxs.Empty() {
		allConstantsAreHomogenous := false
		if ok, err := filterAttempt(ctx, semaCtx, s, func() {
			// The second heuristic is to prefer candidates where all constants can
			// become a homogeneous type, if all resolvable expressions became one.
			// This is only possible if resolvable expressions were resolved
			// homogeneously up to this point.
			if homogeneousTyp != nil {
				allConstantsAreHomogenous = true
				for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
					if !canConstantBecome(s.exprs[i].(Constant), homogeneousTyp) {
						allConstantsAreHomogenous = false
						break
					}
				}
				if allConstantsAreHomogenous {
					for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
						filter := makeFilter(i, func(params TypeList, ordinal int) bool {
							return params.GetAt(ordinal).Equivalent(homogeneousTyp)
						})
						s.overloadIdxs = filterParams(s.overloadIdxs, s.overloads, s.params, filter)
					}
				}
			}
		}); ok {
			return err
		}

		if ok, err := filterAttempt(ctx, semaCtx, s, func() {
			// The third heuristic is to prefer candidates where all constants can
			// become their "natural" types.
			for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
				natural := naturalConstantType(s.exprs[i].(Constant))
				if natural != nil {
					filter := makeFilter(i, func(params TypeList, ordinal int) bool {
						return params.GetAt(ordinal).Equivalent(natural)
					})
					s.overloadIdxs = filterParams(s.overloadIdxs, s.overloads, s.params, filter)
				}
			}
		}); ok {
			return err
		}

		// At this point, it's worth seeing if we have constants that can't actually
		// parse as the type that canConstantBecome claims they can. For example,
		// every string literal will report that it can become an interval, but most
		// string literals do not encode valid intervals. This may uncover some
		// overloads with invalid type signatures.
		//
		// This parsing is sufficiently expensive (see the comment on
		// StrVal.AvailableTypes) that we wait until now, when we've eliminated most
		// overloads from consideration, so that we only need to check each constant
		// against a limited set of types. We can't hold off on this parsing any
		// longer, though: the remaining heuristics are overly aggressive and will
		// falsely reject the only valid overload in some cases.
		//
		// This case is broken into two parts. We first attempt to use the
		// information about the homogeneity of our constants collected by previous
		// heuristic passes. If:
		// * all our constants are homogeneous
		// * we only have a single overload left
		// * the constant overload parameters are homogeneous as well
		// then match this overload with the homogeneous constants. Otherwise,
		// continue to filter overloads by whether or not the constants can parse
		// into the desired types of the overloads.
		// This first case is important when resolving overloads for operations
		// between user-defined types, where we need to propagate the concrete
		// resolved type information over to the constants, rather than attempting
		// to resolve constants as the placeholder type for the user defined type
		// family (like `AnyEnum`).
		if len(s.overloadIdxs) == 1 && allConstantsAreHomogenous {
			overloadParamsAreHomogenous := true
			ovIdx := s.overloadIdxs[0]
			routineType, outParamOrdinals, outParams := s.overloads[ovIdx].outParamInfo()
			for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
				params, ordinal := getParamsAndOrdinal(routineType, i, s.params[ovIdx], outParamOrdinals, outParams)
				if !params.GetAt(ordinal).Equivalent(homogeneousTyp) {
					overloadParamsAreHomogenous = false
					break
				}
			}
			if overloadParamsAreHomogenous {
				// Type check our constants using the homogeneous type rather than
				// the type in overload parameter. This lets us type check user defined
				// types with a concrete type instance, rather than an ambiguous type.
				for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
					typ, err := s.exprs[i].TypeCheck(ctx, semaCtx, homogeneousTyp)
					if err != nil {
						return err
					}
					s.typedExprs[i] = typ
				}
				_, err := checkReturnPlaceholdersAtIdx(ctx, semaCtx, s, s.overloadIdxs[0])
				return err
			}
		}
		for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
			constExpr := s.exprs[i].(Constant)
			filter := makeFilter(i, func(params TypeList, ordinal int) bool {
				_, err := constExpr.ResolveAsType(ctx, semaCtx, params.GetAt(ordinal))
				return err == nil
			})
			s.overloadIdxs = filterParams(s.overloadIdxs, s.overloads, s.params, filter)
		}
		if ok, err := checkReturn(ctx, semaCtx, s); ok {
			return err
		}

		// The fourth heuristic is to prefer candidates that accept the "best"
		// mutual type in the resolvable type set of all constants.
		if bestConstType, ok := commonConstantType(s.exprs, s.constIdxs); ok {
			// In case all overloads are filtered out at this step,
			// keep track of previous overload indexes to return ambiguous error (>1 overloads)
			// instead of unsupported error (0 overloads) when applicable.
			prevOverloadIdxs := s.overloadIdxs
			for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
				filter := makeFilter(i, func(params TypeList, ordinal int) bool {
					return params.GetAt(ordinal).Equivalent(bestConstType)
				})
				s.overloadIdxs = filterParams(s.overloadIdxs, s.overloads, s.params, filter)
			}
			if ok, err := checkReturn(ctx, semaCtx, s); ok {
				if len(s.overloadIdxs) == 0 {
					s.overloadIdxs = prevOverloadIdxs
				}
				return err
			}
			if homogeneousTyp != nil {
				if !homogeneousTyp.Equivalent(bestConstType) {
					homogeneousTyp = nil
				}
			} else {
				homogeneousTyp = bestConstType
			}
		}
	}

	// The fifth heuristic is to defer to candidates with the highest
	// preference.
	maxPreference := OverloadPreferenceUnpreferred
	for _, idx := range s.overloadIdxs {
		if p := s.overloads[idx].preference(); p > maxPreference {
			maxPreference = p
		}
	}
	if ok, err := filterAttempt(ctx, semaCtx, s, func() {
		s.overloadIdxs = filterOverloads(
			s.overloadIdxs, s.overloads, func(o overloadImpl) bool {
				return o.preference() == maxPreference
			},
		)
	}); ok {
		return err
	}

	// The sixth heuristic is to prefer candidates where all placeholders can be
	// given the same type as all constants and resolvable expressions. This is
	// only possible if all constants and resolvable expressions were resolved
	// homogeneously up to this point.
	if homogeneousTyp != nil && !s.placeholderIdxs.Empty() {
		// Before we continue, try to propagate the homogeneous type to the
		// placeholders. This might not have happened yet, if the overloads'
		// parameter types are ambiguous (like in the case of tuple-tuple binary
		// operators).
		for i, ok := s.placeholderIdxs.Next(0); ok; i, ok = s.placeholderIdxs.Next(i + 1) {
			if _, err := s.exprs[i].TypeCheck(ctx, semaCtx, homogeneousTyp); err != nil {
				return err
			}
			filter := makeFilter(i, func(params TypeList, ordinal int) bool {
				return params.GetAt(ordinal).Equivalent(homogeneousTyp)
			})
			s.overloadIdxs = filterParams(s.overloadIdxs, s.overloads, s.params, filter)
		}
		if ok, err := checkReturn(ctx, semaCtx, s); ok {
			return err
		}
	}

	// This is a total hack for AnyEnum whilst we don't have postgres type resolution.
	// This enables AnyEnum array ops to not need a cast, e.g. array['a']::enum[] = '{a}'.
	// If we have one remaining candidate containing AnyEnum, cast all remaining
	// arguments to a known enum and check that the rest match. This is a poor man's
	// implicit cast / postgres "same argument" resolution clone.
	if len(s.overloadIdxs) == 1 {
		ovIdx := s.overloadIdxs[0]
		params := s.params[ovIdx]
		var knownEnum *types.T

		// Do not apply this hack to routines for simplicity.
		if routineType, _, _ := s.overloads[ovIdx].outParamInfo(); routineType == BuiltinRoutine {
			// Check we have all "AnyEnum" (or "AnyEnum" array) arguments and that
			// one argument is typed with an enum.
			attemptAnyEnumCast := func() bool {
				for i := 0; i < params.Length(); i++ {
					typ := params.GetAt(i)
					// Note we are deliberately looking at whether the built-in takes in
					// AnyEnum as an argument, not the exprs given to the overload itself.
					if !(typ.Identical(types.AnyEnum) || typ.Identical(types.MakeArray(types.AnyEnum))) {
						return false
					}
					if s.typedExprs[i] != nil {
						// Assign the known enum if it was previously unassigned.
						// Otherwise, double check it matches a previously defined enum.
						posEnum := s.typedExprs[i].ResolvedType()
						if !posEnum.UserDefined() {
							return false
						}
						if posEnum.Family() == types.ArrayFamily {
							posEnum = posEnum.ArrayContents()
						}
						if knownEnum == nil {
							knownEnum = posEnum
						} else if !posEnum.Identical(knownEnum) {
							return false
						}
					}
				}
				return knownEnum != nil
			}()

			// If we have all arguments as AnyEnum, and we know at least one of the
			// enum's actual type, try type cast the rest.
			if attemptAnyEnumCast {
				// Copy exprs to prevent any overwrites of underlying s.exprs array later.
				sCopy := *s
				sCopy.exprs = make([]Expr, len(s.exprs))
				copy(sCopy.exprs, s.exprs)

				if ok, err := filterAttempt(ctx, semaCtx, &sCopy, func() {
					work := func(idx int) {
						p := params.GetAt(idx)
						typCast := knownEnum
						if p.Family() == types.ArrayFamily {
							typCast = types.MakeArray(knownEnum)
						}
						sCopy.exprs[idx] = &CastExpr{Expr: sCopy.exprs[idx], Type: typCast, SyntaxMode: CastShort}
					}
					for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
						work(i)
					}
					for i, ok := s.placeholderIdxs.Next(0); ok; i, ok = s.placeholderIdxs.Next(i + 1) {
						work(i)
					}
				}); ok {
					s.exprs = sCopy.exprs
					s.typedExprs = sCopy.typedExprs
					s.overloadIdxs = append(s.overloadIdxs[:0], sCopy.overloadIdxs...)
					return err
				}
			}
		}
	}

	// In a binary expression, in the case of one of the arguments being untyped NULL,
	// we prefer overloads where we infer the type of the NULL to be the same as the
	// other argument. This is used to differentiate the behavior of
	// STRING[] || NULL and STRING || NULL.
	// TODO(mgartner): I think we can remove this heuristic in favor of the rule
	// applied above that types untyped literals and placeholders based on the
	// input types of overloads.
	if inBinOp && len(s.exprs) == 2 {
		if ok, err := filterAttempt(ctx, semaCtx, s, func() {
			var err error
			left := s.typedExprs[0]
			if left == nil {
				left, err = s.exprs[0].TypeCheck(ctx, semaCtx, types.AnyElement)
				if err != nil {
					return
				}
			}
			right := s.typedExprs[1]
			if right == nil {
				right, err = s.exprs[1].TypeCheck(ctx, semaCtx, types.AnyElement)
				if err != nil {
					return
				}
			}
			leftType := left.ResolvedType()
			rightType := right.ResolvedType()
			leftIsNull := leftType.Family() == types.UnknownFamily
			rightIsNull := rightType.Family() == types.UnknownFamily
			oneIsNull := (leftIsNull || rightIsNull) && !(leftIsNull && rightIsNull)
			if oneIsNull {
				if leftIsNull {
					leftType = rightType
				}
				if rightIsNull {
					rightType = leftType
				}
				filter := func(_ overloadImpl, params TypeList) bool {
					return params.GetAt(0).Equivalent(leftType) &&
						params.GetAt(1).Equivalent(rightType)
				}
				s.overloadIdxs = filterParams(s.overloadIdxs, s.overloads, s.params, filter)
			}
		}); ok {
			return err
		}
	}

	// After the previous heuristic, in a binary expression, in the case of one of the arguments being untyped
	// NULL, we prefer overloads where we infer the type of the NULL to be a STRING. This is used
	// to choose INT || NULL::STRING over INT || NULL::INT[].
	if inBinOp && len(s.exprs) == 2 {
		if ok, err := filterAttempt(ctx, semaCtx, s, func() {
			var err error
			left := s.typedExprs[0]
			if left == nil {
				left, err = s.exprs[0].TypeCheck(ctx, semaCtx, types.AnyElement)
				if err != nil {
					return
				}
			}
			right := s.typedExprs[1]
			if right == nil {
				right, err = s.exprs[1].TypeCheck(ctx, semaCtx, types.AnyElement)
				if err != nil {
					return
				}
			}
			leftType := left.ResolvedType()
			rightType := right.ResolvedType()
			leftIsNull := leftType.Family() == types.UnknownFamily
			rightIsNull := rightType.Family() == types.UnknownFamily
			oneIsNull := (leftIsNull || rightIsNull) && !(leftIsNull && rightIsNull)
			if oneIsNull {
				if leftIsNull {
					leftType = types.String
				}
				if rightIsNull {
					rightType = types.String
				}
				filter := func(_ overloadImpl, params TypeList) bool {
					return params.GetAt(0).Equivalent(leftType) &&
						params.GetAt(1).Equivalent(rightType)
				}
				s.overloadIdxs = filterParams(s.overloadIdxs, s.overloads, s.params, filter)
			}
		}); ok {
			return err
		}
	}

	if err := defaultTypeCheck(ctx, semaCtx, s, numOverloads > 0); err != nil {
		return err
	}

	return nil
}

// filterAttempt attempts to filter the overloads down to a single candidate.
// If it succeeds, it will return true, along with the overload (in a slice for
// convenience) and a possible error. If it fails, it will return false and
// undo any filtering performed during the attempt.
func filterAttempt(
	ctx context.Context, semaCtx *SemaContext, s *overloadTypeChecker, attempt func(),
) (ok bool, _ error) {
	before := s.overloadIdxs
	attempt()
	if len(s.overloadIdxs) == 1 {
		ok, err := checkReturn(ctx, semaCtx, s)
		if err != nil {
			return false, err
		}
		if ok {
			return true, err
		}
	}
	s.overloadIdxs = before
	return false, nil
}

func filterParams(
	idxs []uint8, overloads []overloadImpl, params []TypeList, fn func(overloadImpl, TypeList) bool,
) []uint8 {
	i, n := 0, len(idxs)
	for i < n {
		if fn(overloads[idxs[i]], params[idxs[i]]) {
			i++
		} else if n--; i != n {
			idxs[i], idxs[n] = idxs[n], idxs[i]
		}
	}
	return idxs[:n]
}

func filterOverloads(idxs []uint8, overloads []overloadImpl, fn func(overloadImpl) bool) []uint8 {
	i, n := 0, len(idxs)
	for i < n {
		if fn(overloads[idxs[i]]) {
			i++
		} else if n--; i != n {
			idxs[i], idxs[n] = idxs[n], idxs[i]
		}
	}
	return idxs[:n]
}

// defaultTypeCheck type checks the constant and placeholder expressions without a preference
// and adds them to the type checked slice.
func defaultTypeCheck(
	ctx context.Context, semaCtx *SemaContext, s *overloadTypeChecker, errorOnPlaceholders bool,
) error {
	for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
		typ, err := s.exprs[i].TypeCheck(ctx, semaCtx, types.AnyElement)
		if err != nil {
			return pgerror.Wrapf(err, pgcode.InvalidParameterValue,
				"error type checking constant value")
		}
		s.typedExprs[i] = typ
	}
	for i, ok := s.placeholderIdxs.Next(0); ok; i, ok = s.placeholderIdxs.Next(i + 1) {
		if errorOnPlaceholders {
			if _, err := s.exprs[i].TypeCheck(ctx, semaCtx, types.AnyElement); err != nil {
				return err
			}
		}
		// If we dont want to error on args, avoid type checking them without a desired type.
		s.typedExprs[i] = StripParens(s.exprs[i]).(*Placeholder)
	}
	return nil
}

// checkReturn checks the number of remaining overloaded function
// implementations.
// Returns ok=true if we should stop overload resolution, and returning either
// 1. the chosen overload in a slice, or
// 2. nil,
// along with the typed arguments.
// This modifies values within s as scratch slices, but only in the case where
// it returns true, which signals to the calling function that it should
// immediately return, so any mutations to s are irrelevant.
func checkReturn(
	ctx context.Context, semaCtx *SemaContext, s *overloadTypeChecker,
) (ok bool, _ error) {
	switch len(s.overloadIdxs) {
	case 0:
		if err := defaultTypeCheck(ctx, semaCtx, s, false); err != nil {
			return false, err
		}
		return true, nil

	case 1:
		idx := s.overloadIdxs[0]
		routineType, outParamOrdinals, outParams := s.overloads[idx].outParamInfo()
		params := s.params[idx]
		for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
			p, ordinal := getParamsAndOrdinal(routineType, i, params, outParamOrdinals, outParams)
			des := p.GetAt(ordinal)
			typ, err := s.exprs[i].TypeCheck(ctx, semaCtx, des)
			if err != nil {
				return false, pgerror.Wrapf(
					err, pgcode.InvalidParameterValue,
					"error type checking constant value",
				)
			}
			if des != nil && !typ.ResolvedType().Equivalent(des) {
				return false, errors.AssertionFailedf(
					"desired constant value type %s but set type %s",
					redact.Safe(des), redact.Safe(typ.ResolvedType()),
				)
			}
			s.typedExprs[i] = typ
		}

		return checkReturnPlaceholdersAtIdx(ctx, semaCtx, s, idx)

	default:
		return false, nil
	}
}

// checkReturnPlaceholdersAtIdx checks that the placeholders for the
// overload at the input index are valid. It has the same return values
// as checkReturn.
func checkReturnPlaceholdersAtIdx(
	ctx context.Context, semaCtx *SemaContext, s *overloadTypeChecker, idx uint8,
) (ok bool, _ error) {
	params := s.params[idx]
	routineType, outParamOrdinals, outParams := s.overloads[idx].outParamInfo()
	for i, ok := s.placeholderIdxs.Next(0); ok; i, ok = s.placeholderIdxs.Next(i + 1) {
		p, ordinal := getParamsAndOrdinal(routineType, i, params, outParamOrdinals, outParams)
		des := p.GetAt(ordinal)
		typ, err := s.exprs[i].TypeCheck(ctx, semaCtx, des)
		if err != nil {
			if des.IsAmbiguous() {
				return false, nil
			}
			return false, err
		}
		if typ.ResolvedType().IsAmbiguous() {
			return false, nil
		}
		s.typedExprs[i] = typ
	}
	s.overloadIdxs = append(s.overloadIdxs[:0], idx)
	return true, nil
}

func formatCandidates(prefix string, candidates []overloadImpl, filter []uint8) string {
	var buf bytes.Buffer
	for _, idx := range filter {
		candidate := candidates[idx]
		buf.WriteString(prefix)
		buf.WriteByte('(')
		params := candidate.params()
		defaultExprs := candidate.defaultExprs()
		tLen := params.Length()
		inputTyps := make([]TypedExpr, tLen)
		for i := 0; i < tLen; i++ {
			t := params.GetAt(i)
			inputTyps[i] = &TypedDummy{Typ: t}
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(t.String())
			if len(defaultExprs) > 0 {
				numParamsWithoutDefaultExpr := tLen - len(defaultExprs)
				if i >= numParamsWithoutDefaultExpr {
					defaultExpr := defaultExprs[i-numParamsWithoutDefaultExpr]
					buf.WriteString(" DEFAULT ")
					buf.WriteString(defaultExpr.String())
				}
			}
		}
		buf.WriteString(") -> ")
		buf.WriteString(returnTypeToFixedType(candidate.returnType(), inputTyps).String())
		switch candidate.preference() {
		case OverloadPreferencePreferred:
			buf.WriteString(" [preferred]")
		case OverloadPreferenceUnpreferred:
			buf.WriteString(" [unpreferred]")
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

// TODO(chengxiong): unify this method with Overload.Signature method if possible.
func getFuncSig(expr *FuncExpr, typedInputExprs []TypedExpr, desiredType *types.T) string {
	typeNames := make([]string, 0, len(expr.Exprs))
	for _, expr := range typedInputExprs {
		typeNames = append(typeNames, expr.ResolvedType().String())
	}
	var desStr string
	if desiredType.Family() != types.AnyFamily {
		desStr = fmt.Sprintf(" (returning <%s>)", desiredType)
	}
	return fmt.Sprintf("%s(%s)%s", &expr.Func, strings.Join(typeNames, ", "), desStr)
}
