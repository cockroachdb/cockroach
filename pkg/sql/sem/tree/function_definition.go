// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// FunctionDefinition implements a reference to the (possibly several)
// overloads for a built-in function.
// TODO(Chengxiong): Remove this struct entirely. Instead, use overloads from
// function resolution or use "GetBuiltinProperties" if the need is to only look
// at builtin functions(there are such existing use cases). Also change "Name"
// of ResolvedFunctionDefinition to Name type.
type FunctionDefinition struct {
	// Name is the short name of the function.
	Name string

	// Definition is the set of overloads for this function name.
	Definition []*Overload

	// FunctionProperties are the properties common to all overloads.
	FunctionProperties
}

// ResolvedFunctionDefinition is similar to FunctionDefinition but with all the
// overloads qualified with schema name.
type ResolvedFunctionDefinition struct {
	// Name is the name of the function and not the name of the schema. And, it's
	// not qualified.
	Name string

	Overloads []QualifiedOverload
}

type qualifiedOverloads []QualifiedOverload

func (qo qualifiedOverloads) len() int {
	return len(qo)
}

func (qo qualifiedOverloads) get(i int) overloadImpl {
	return qo[i].Overload
}

// QualifiedOverload is a wrapper of Overload prefixed with a schema name.
// It indicates that the overload is defined with the specified schema.
type QualifiedOverload struct {
	Schema string
	*Overload
}

// MakeQualifiedOverload creates a new QualifiedOverload.
func MakeQualifiedOverload(schema string, overload *Overload) QualifiedOverload {
	return QualifiedOverload{Schema: schema, Overload: overload}
}

// FunctionProperties defines the properties of the built-in
// functions that are common across all overloads.
type FunctionProperties struct {
	// UnsupportedWithIssue, if non-zero indicates the built-in is not
	// really supported; the name is a placeholder. Value -1 just says
	// "not supported" without an issue to link; values > 0 provide an
	// issue number to link.
	UnsupportedWithIssue int

	// Undocumented, when set to true, indicates that the built-in function is
	// hidden from documentation. This is currently used to hide experimental
	// functionality as it is being developed.
	Undocumented bool

	// Private, when set to true, indicates the built-in function is not
	// available for use by user queries. This is currently used by some
	// aggregates due to issue #10495. Private functions are implicitly
	// considered undocumented.
	Private bool

	// DistsqlBlocklist is set to true when a function depends on
	// members of the EvalContext that are not marshaled by DistSQL
	// (e.g. planner). Currently used for DistSQL to determine if
	// expressions can be evaluated on a different node without sending
	// over the EvalContext.
	//
	// TODO(andrei): Get rid of the planner from the EvalContext and then we can
	// get rid of this blocklist.
	DistsqlBlocklist bool

	// Category is used to generate documentation strings.
	Category string

	// AvailableOnPublicSchema indicates whether the function can be resolved
	// if it is found on the public schema.
	AvailableOnPublicSchema bool

	// ReturnLabels can be used to override the return column name of a
	// function in a FROM clause.
	// This satisfies a Postgres quirk where some json functions have
	// different return labels when used in SELECT or FROM clause.
	ReturnLabels []string

	// AmbiguousReturnType is true if the builtin's return type can't be
	// determined without extra context. This is used for formatting builtins
	// with the FmtParsable directive.
	AmbiguousReturnType bool

	// HasSequenceArguments is true if the builtin function takes in a sequence
	// name (string) and can be used in a scalar expression.
	// TODO(richardjcai): When implicit casting is supported, these builtins
	// should take RegClass as the arg type for the sequence name instead of
	// string, we will add a dependency on all RegClass types used in a view.
	HasSequenceArguments bool

	// CompositeInsensitive indicates that this function returns equal results
	// when evaluated on equal inputs. This is a non-trivial property for
	// composite types which can be equal but not identical
	// (e.g. decimals 1.0 and 1.00). For example, converting a decimal to string
	// is not CompositeInsensitive.
	//
	// See memo.CanBeCompositeSensitive.
	CompositeInsensitive bool

	// VectorizeStreaming indicates that the function is of "streaming" nature
	// from the perspective of the vectorized execution engine.
	VectorizeStreaming bool

	// ReturnsRecordType indicates that this function is a record-returning
	// function, which implies that it's unusable without a corresponding type
	// alias.
	//
	// For example, consider the case of json_to_record('{"a":"b", "c":"d"}').
	// This function returns an error unless it as an `AS t(a,b,c)` declaration,
	// since its definition is to pick out the JSON attributes within the input
	// that match, by name, to the columns in the aliased record type.
	ReturnsRecordType bool
}

// ShouldDocument returns whether the built-in function should be included in
// external-facing documentation.
func (fp *FunctionProperties) ShouldDocument() bool {
	return !(fp.Undocumented || fp.Private)
}

// NewFunctionDefinition allocates a function definition corresponding
// to the given built-in definition.
func NewFunctionDefinition(
	name string, props *FunctionProperties, def []Overload,
) *FunctionDefinition {
	overloads := make([]*Overload, len(def))

	for i := range def {
		if def[i].OverloadPreference == OverloadPreferencePreferred {
			// Builtins with a preferred overload are always ambiguous.
			props.AmbiguousReturnType = true
			break
		}
	}

	for i := range def {
		def[i].FunctionProperties = *props
		overloads[i] = &def[i]
	}
	return &FunctionDefinition{
		Name:               name,
		Definition:         overloads,
		FunctionProperties: *props,
	}
}

// FunDefs holds pre-allocated FunctionDefinition instances
// for every builtin function. Initialized by builtins.init().
//
// Note that this is extremely similar to the set stored in builtinsregistry.
// The hope is to remove this map at some point in the future as we delegate
// function definition resolution to interfaces defined in the SemaContext.
var FunDefs map[string]*FunctionDefinition

// ResolvedBuiltinFuncDefs holds pre-allocated ResolvedFunctionDefinition
// instances. Keys of the map is schema qualified function names.
var ResolvedBuiltinFuncDefs map[string]*ResolvedFunctionDefinition

// OidToBuiltinName contains a map from the hashed OID of all builtin functions
// to their name.
var OidToBuiltinName map[oid.Oid]string

// OidToQualifiedBuiltinOverload is a map from builtin function OID to an
// qualified overload.
var OidToQualifiedBuiltinOverload map[oid.Oid]QualifiedOverload

// Format implements the NodeFormatter interface.
// FunctionDefinitions should always be builtin functions, so we do not need to
// anonymize them, even if the flag is set.
func (fd *FunctionDefinition) Format(ctx *FmtCtx) {
	ctx.WriteString(fd.Name)
}

// String implements the Stringer interface.
func (fd *FunctionDefinition) String() string { return AsString(fd) }

// Format implements the NodeFormatter interface.
// ResolvedFunctionDefinitions should always be builtin functions, so we do not
// need to anonymize them, even if the flag is set.
func (fd *ResolvedFunctionDefinition) Format(ctx *FmtCtx) {
	// This is necessary when deserializing function expressions for SHOW CREATE
	// statements. When deserializing a function expression with function OID
	// references, it's guaranteed that there'll be always one overload resolved.
	// There is no need to show prefix for builtin functions since we don't
	// serialize them.
	if len(fd.Overloads) == 1 && catid.IsOIDUserDefined(fd.Overloads[0].Oid) {
		ctx.WriteString(fd.Overloads[0].Schema)
		ctx.WriteString(".")
	}
	ctx.WriteString(fd.Name)
}

// String implements the Stringer interface.
func (fd *ResolvedFunctionDefinition) String() string { return AsString(fd) }

// MergeWith is used to merge two UDF definitions with same name.
func (fd *ResolvedFunctionDefinition) MergeWith(
	another *ResolvedFunctionDefinition,
) (*ResolvedFunctionDefinition, error) {
	if fd == nil {
		return another, nil
	}
	if another == nil {
		return fd, nil
	}

	if fd.Name != another.Name {
		return nil, errors.Newf("cannot merge function definition of %q with %q", fd.Name, another.Name)
	}

	return &ResolvedFunctionDefinition{
		Name:      fd.Name,
		Overloads: combineOverloads(fd.Overloads, another.Overloads),
	}, nil
}

// MatchOverload searches an overload which has exactly the same parameter
// types. The overload from the most significant schema is returned. If
// paramTypes==nil, an error is returned if the function name is not unique in
// the most significant schema. If paramTypes is not nil, an error with
// ErrRoutineUndefined cause is returned if not matched found. Overloads that
// don't match the types in routineType are ignored.
func (fd *ResolvedFunctionDefinition) MatchOverload(
	paramTypes []*types.T, explicitSchema string, searchPath SearchPath, routineType RoutineType,
) (QualifiedOverload, error) {
	matched := func(ol QualifiedOverload, schema string) bool {
		if ol.Type == UDFRoutine || ol.Type == ProcedureRoutine {
			return schema == ol.Schema && (paramTypes == nil || ol.params().MatchIdentical(paramTypes))
		}
		return schema == ol.Schema && (paramTypes == nil || ol.params().Match(paramTypes))
	}
	typeNames := func() string {
		ns := make([]string, len(paramTypes))
		for i, t := range paramTypes {
			ns[i] = t.Name()
		}
		return strings.Join(ns, ",")
	}

	found := false
	ret := make([]QualifiedOverload, 0, len(fd.Overloads))

	findMatches := func(schema string) {
		for i := range fd.Overloads {
			if matched(fd.Overloads[i], schema) {
				found = true
				ret = append(ret, fd.Overloads[i])
			}
		}
	}

	if explicitSchema != "" {
		findMatches(explicitSchema)
	} else {
		for i, n := 0, searchPath.NumElements(); i < n; i++ {
			if findMatches(searchPath.GetSchema(i)); found {
				break
			}
		}
	}

	if len(ret) == 1 && ret[0].Type&routineType == 0 {
		if routineType == ProcedureRoutine {
			return QualifiedOverload{}, pgerror.Newf(
				pgcode.WrongObjectType, "%s(%s) is not a procedure", fd.Name, typeNames())
		} else {
			return QualifiedOverload{}, pgerror.Newf(
				pgcode.WrongObjectType, "%s(%s) is not a function", fd.Name, typeNames())
		}
	}

	// Filter out overloads that don't match the requested type.
	i := 0
	for _, o := range ret {
		if ret[i].Type&routineType != 0 {
			ret[i] = o
			i++
		}
	}
	// Clear non-matching overloads.
	for j := i; j < len(ret); j++ {
		ret[j] = QualifiedOverload{}
	}
	// Truncate the slice.
	ret = ret[:i]

	if len(ret) == 0 {
		if routineType == ProcedureRoutine {
			return QualifiedOverload{}, errors.Mark(
				pgerror.Newf(pgcode.UndefinedFunction, "procedure %s(%s) does not exist", fd.Name, typeNames()),
				ErrRoutineUndefined,
			)
		} else {
			return QualifiedOverload{}, errors.Mark(
				pgerror.Newf(pgcode.UndefinedFunction, "function %s(%s) does not exist", fd.Name, typeNames()),
				ErrRoutineUndefined,
			)
		}
	}

	if len(ret) > 1 {
		if routineType == ProcedureRoutine {
			return QualifiedOverload{}, pgerror.Newf(pgcode.AmbiguousFunction, "procedure name %q is not unique", fd.Name)
		} else {
			return QualifiedOverload{}, pgerror.Newf(pgcode.AmbiguousFunction, "function name %q is not unique", fd.Name)
		}
	}
	return ret[0], nil
}

func combineOverloads(a, b []QualifiedOverload) []QualifiedOverload {
	return append(append(make([]QualifiedOverload, 0, len(a)+len(b)), a...), b...)
}

// GetClass returns function class by checking each overload's Class and returns
// the homogeneous Class value if all overloads are the same Class. Ambiguous
// error is returned if there is any overload with different Class.
//
// TODO(chengxiong,mgartner): make sure that, at places of the use cases of this
// method, function is resolved to one overload, so that we can get rid of this
// function and similar methods below.
func (fd *ResolvedFunctionDefinition) GetClass() (FunctionClass, error) {
	if len(fd.Overloads) < 1 {
		return 0, errors.AssertionFailedf("no overloads found for function %s", fd.Name)
	}
	ret := fd.Overloads[0].Class
	for i := range fd.Overloads {
		if fd.Overloads[i].Class != ret {
			return 0, pgerror.Newf(pgcode.AmbiguousFunction, "ambiguous function class on %s", fd.Name)
		}
	}
	return ret, nil
}

// GetReturnLabel returns function ReturnLabel by checking each overload and
// returns a ReturnLabel if all overloads have a ReturnLabel of the same length.
// Ambiguous error is returned if there is any overload has ReturnLabel of a
// different length. This is good enough since we don't create UDF with
// ReturnLabel.
func (fd *ResolvedFunctionDefinition) GetReturnLabel() ([]string, error) {
	if len(fd.Overloads) < 1 {
		return nil, errors.AssertionFailedf("no overloads found for function %s", fd.Name)
	}
	ret := fd.Overloads[0].ReturnLabels
	for i := range fd.Overloads {
		if len(ret) != len(fd.Overloads[i].ReturnLabels) {
			return nil, pgerror.Newf(pgcode.AmbiguousFunction, "ambiguous function return label on %s", fd.Name)
		}
	}
	return ret, nil
}

// GetHasSequenceArguments returns function's HasSequenceArguments flag by
// checking each overload's HasSequenceArguments flag. Ambiguous error is
// returned if there is any overload has a different flag.
func (fd *ResolvedFunctionDefinition) GetHasSequenceArguments() (bool, error) {
	if len(fd.Overloads) < 1 {
		return false, errors.AssertionFailedf("no overloads found for function %s", fd.Name)
	}
	ret := fd.Overloads[0].HasSequenceArguments
	for i := range fd.Overloads {
		if ret != fd.Overloads[i].HasSequenceArguments {
			return false, pgerror.Newf(pgcode.AmbiguousFunction, "ambiguous function sequence argument on %s", fd.Name)
		}
	}
	return ret, nil
}

// QualifyBuiltinFunctionDefinition qualified all overloads in a function
// definition with a schema name. Note that this function can only be used for
// builtin function.
func QualifyBuiltinFunctionDefinition(
	def *FunctionDefinition, schema string,
) *ResolvedFunctionDefinition {
	ret := &ResolvedFunctionDefinition{
		Name:      def.Name,
		Overloads: make([]QualifiedOverload, 0, len(def.Definition)),
	}
	for _, o := range def.Definition {
		ret.Overloads = append(
			ret.Overloads,
			MakeQualifiedOverload(schema, o),
		)
	}
	return ret
}

// GetBuiltinFuncDefinitionOrFail is similar to GetBuiltinFuncDefinition but
// returns an error if function is not found.
func GetBuiltinFuncDefinitionOrFail(
	fName RoutineName, searchPath SearchPath,
) (*ResolvedFunctionDefinition, error) {
	def, err := GetBuiltinFuncDefinition(fName, searchPath)
	if err != nil {
		return nil, err
	}
	if def == nil {
		forError := fName // prevent fName from escaping
		return nil, errors.Mark(
			pgerror.Newf(pgcode.UndefinedFunction, "unknown function: %s()", ErrString(&forError)),
			ErrRoutineUndefined,
		)
	}
	return def, nil
}

// GetBuiltinFunctionByOIDOrFail retrieves a builtin function by OID.
func GetBuiltinFunctionByOIDOrFail(oid oid.Oid) (*ResolvedFunctionDefinition, error) {
	ol, ok := OidToQualifiedBuiltinOverload[oid]
	if !ok {
		return nil, errors.Mark(
			pgerror.Newf(pgcode.UndefinedFunction, "function %d not found", oid),
			ErrRoutineUndefined,
		)
	}
	fd := &ResolvedFunctionDefinition{
		Name:      OidToBuiltinName[oid],
		Overloads: []QualifiedOverload{ol},
	}
	return fd, nil
}

// GetBuiltinFuncDefinition search for a builtin function given a function name
// and a search path. If function name is prefixed, only the builtin functions
// in the specific schema are searched. Otherwise, all schemas on the given
// searchPath are searched. A nil is returned if no function is found. It's
// caller's choice to error out if function not found.
//
// In theory, this function returns an error only when the search path iterator
// errors which won't happen since the iterating function never errors out. But
// error is still checked and return from the function signature just in case
// we change the iterating function in the future.
func GetBuiltinFuncDefinition(
	fName RoutineName, searchPath SearchPath,
) (*ResolvedFunctionDefinition, error) {
	if fName.ExplicitSchema {
		return ResolvedBuiltinFuncDefs[fName.Schema()+"."+fName.Object()], nil
	}

	// First try that if we can get function directly with the function name.
	// There is a case where the part[0] of the name is a qualified string when
	// the qualified name is double quoted as a single name like "schema.fn".
	if def, ok := ResolvedBuiltinFuncDefs[fName.Object()]; ok {
		return def, nil
	}

	// Then try if it's in pg_catalog.
	if def, ok := ResolvedBuiltinFuncDefs[catconstants.PgCatalogName+"."+fName.Object()]; ok {
		return def, nil
	}

	// If not in pg_catalog, go through search path.
	var resolvedDef *ResolvedFunctionDefinition
	for i, n := 0, searchPath.NumElements(); i < n; i++ {
		schema := searchPath.GetSchema(i)
		fullName := schema + "." + fName.Object()
		if def, ok := ResolvedBuiltinFuncDefs[fullName]; ok {
			resolvedDef = def
			break
		}
	}

	return resolvedDef, nil
}
