// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// ErrConflictingFunctionOption indicates that there are conflicting or
// redundant function options from user input to either create or alter a
// function.
var ErrConflictingFunctionOption = pgerror.New(pgcode.Syntax, "conflicting or redundant options")

// FunctionName represent a function name in a UDF relevant statement, either
// DDL or DML statement. Similar to TableName, it is constructed for incoming
// SQL queries from an UnresolvedObjectName.
type FunctionName struct {
	objName
}

// MakeFunctionNameFromPrefix returns a FunctionName with the given prefix and
// function name.
func MakeFunctionNameFromPrefix(prefix ObjectNamePrefix, object Name) FunctionName {
	return FunctionName{objName{
		ObjectName:       object,
		ObjectNamePrefix: prefix,
	}}
}

// MakeQualifiedFunctionName constructs a FunctionName with the given db and
// schema name as prefix.
func MakeQualifiedFunctionName(db string, sc string, fn string) FunctionName {
	return MakeFunctionNameFromPrefix(
		ObjectNamePrefix{
			CatalogName:     Name(db),
			ExplicitCatalog: true,
			SchemaName:      Name(sc),
			ExplicitSchema:  true,
		}, Name(fn),
	)
}

// Format implements the NodeFormatter interface.
func (f *FunctionName) Format(ctx *FmtCtx) {
	f.ObjectNamePrefix.Format(ctx)
	if f.ExplicitSchema || ctx.alwaysFormatTablePrefix() {
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&f.ObjectName)
}

func (f *FunctionName) String() string { return AsString(f) }

// FQString renders the function name in full, not omitting the prefix
// schema and catalog names. Suitable for logging, etc.
func (f *FunctionName) FQString() string {
	ctx := NewFmtCtx(FmtSimple)
	ctx.FormatNode(&f.CatalogName)
	ctx.WriteByte('.')
	ctx.FormatNode(&f.SchemaName)
	ctx.WriteByte('.')
	ctx.FormatNode(&f.ObjectName)
	return ctx.CloseAndGetString()
}

func (f *FunctionName) objectName() {}

// CreateFunction represents a CREATE FUNCTION statement.
type CreateFunction struct {
	IsProcedure bool
	Replace     bool
	FuncName    FunctionName
	Params      FuncParams
	ReturnType  FuncReturnType
	Options     FunctionOptions
	RoutineBody *RoutineBody
	// BodyStatements is not assigned during initial parsing of user input. It's
	// assigned during opt builder for logging purpose at the moment. It stores
	// all parsed AST nodes of body statements with all expression in original
	// format. That is sequence names and type name in expressions are not
	// rewritten with OIDs.
	BodyStatements Statements
}

// Format implements the NodeFormatter interface.
func (node *CreateFunction) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	if node.Replace {
		ctx.WriteString("OR REPLACE ")
	}
	ctx.WriteString("FUNCTION ")
	ctx.FormatNode(&node.FuncName)
	ctx.WriteString("(")
	ctx.FormatNode(node.Params)
	ctx.WriteString(")\n\t")
	ctx.WriteString("RETURNS ")
	if node.ReturnType.IsSet {
		ctx.WriteString("SETOF ")
	}
	ctx.FormatTypeReference(node.ReturnType.Type)
	ctx.WriteString("\n\t")
	var funcBody FunctionBodyStr
	for _, option := range node.Options {
		switch t := option.(type) {
		case FunctionBodyStr:
			funcBody = t
			continue
		}
		ctx.FormatNode(option)
		ctx.WriteString("\n\t")
	}

	if ctx.HasFlags(FmtMarkRedactionNode) {
		ctx.WriteString("AS ")
		ctx.WriteString("$$")
		for i, stmt := range node.BodyStatements {
			if i > 0 {
				ctx.WriteString(" ")
			}
			ctx.FormatNode(stmt)
			ctx.WriteString(";")
		}
		ctx.WriteString("$$")
	} else if node.RoutineBody != nil {
		ctx.WriteString("BEGIN ATOMIC ")
		for _, stmt := range node.RoutineBody.Stmts {
			ctx.FormatNode(stmt)
			ctx.WriteString("; ")
		}
		ctx.WriteString("END")
	} else {
		ctx.FormatNode(funcBody)
	}
}

// RoutineBody represent a list of statements in a UDF body.
type RoutineBody struct {
	Stmts Statements
}

// RoutineReturn represent a RETURN statement in a UDF body.
type RoutineReturn struct {
	ReturnVal Expr
}

// Format implements the NodeFormatter interface.
func (node *RoutineReturn) Format(ctx *FmtCtx) {
	ctx.WriteString("RETURN ")
	ctx.FormatNode(node.ReturnVal)
}

// FunctionOptions represent a list of function options.
type FunctionOptions []FunctionOption

// FunctionOption is an interface representing UDF properties.
type FunctionOption interface {
	functionOption()
	NodeFormatter
}

func (FunctionNullInputBehavior) functionOption() {}
func (FunctionVolatility) functionOption()        {}
func (FunctionLeakproof) functionOption()         {}
func (FunctionBodyStr) functionOption()           {}
func (FunctionLanguage) functionOption()          {}

// FunctionNullInputBehavior represent the UDF property on null parameters.
type FunctionNullInputBehavior int

const (
	// FunctionCalledOnNullInput indicates that the function will be given the
	// chance to execute when presented with NULL input. This is the default if
	// no null input behavior is specified.
	FunctionCalledOnNullInput FunctionNullInputBehavior = iota
	// FunctionReturnsNullOnNullInput indicates that the function will result in
	// NULL given any NULL parameter.
	FunctionReturnsNullOnNullInput
	// FunctionStrict is the same as FunctionReturnsNullOnNullInput
	FunctionStrict
)

// Format implements the NodeFormatter interface.
func (node FunctionNullInputBehavior) Format(ctx *FmtCtx) {
	switch node {
	case FunctionCalledOnNullInput:
		ctx.WriteString("CALLED ON NULL INPUT")
	case FunctionReturnsNullOnNullInput:
		ctx.WriteString("RETURNS NULL ON NULL INPUT")
	case FunctionStrict:
		ctx.WriteString("STRICT")
	default:
		panic(pgerror.New(pgcode.InvalidParameterValue, "Unknown function option"))
	}
}

// FunctionVolatility represent UDF volatility property.
type FunctionVolatility int

const (
	// FunctionVolatile represents volatility.Volatile. This is the default
	// volatility if none is provided.
	FunctionVolatile FunctionVolatility = iota
	// FunctionImmutable represents volatility.Immutable.
	FunctionImmutable
	// FunctionStable represents volatility.Stable.
	FunctionStable
)

// Format implements the NodeFormatter interface.
func (node FunctionVolatility) Format(ctx *FmtCtx) {
	switch node {
	case FunctionVolatile:
		ctx.WriteString("VOLATILE")
	case FunctionImmutable:
		ctx.WriteString("IMMUTABLE")
	case FunctionStable:
		ctx.WriteString("STABLE")
	default:
		panic(pgerror.New(pgcode.InvalidParameterValue, "Unknown function option"))
	}
}

// FunctionLeakproof indicates whether if a UDF is leakproof or not. The default
// is NOT LEAKPROOF if no leakproof option is provided. LEAKPROOF can only be
// used with the IMMUTABLE volatility because we currently conflated LEAKPROOF
// as a volatility equal to IMMUTABLE+LEAKPROOF. Postgres allows
// STABLE+LEAKPROOF functions.
type FunctionLeakproof bool

// Format implements the NodeFormatter interface.
func (node FunctionLeakproof) Format(ctx *FmtCtx) {
	if !node {
		ctx.WriteString("NOT ")
	}
	ctx.WriteString("LEAKPROOF")
}

// FunctionLanguage indicates the language of the statements in the UDF function
// body.
type FunctionLanguage string

const (
	// FunctionLangUnknown represents an unknown language.
	FunctionLangUnknown FunctionLanguage = "unknown"
	// FunctionLangSQL represents SQL language.
	FunctionLangSQL FunctionLanguage = "SQL"
	// FunctionLangPLpgSQL represents the PL/pgSQL procedural language.
	FunctionLangPLpgSQL FunctionLanguage = "plpgsql"
)

// Format implements the NodeFormatter interface.
func (node FunctionLanguage) Format(ctx *FmtCtx) {
	ctx.WriteString("LANGUAGE ")
	ctx.WriteString(string(node))
}

// AsFunctionLanguage converts a string to a FunctionLanguage if applicable.
// No error is returned if string does not represent a valid UDF language;
// unknown languages result in an error later.
func AsFunctionLanguage(lang string) (FunctionLanguage, error) {
	switch strings.ToLower(lang) {
	case "sql":
		return FunctionLangSQL, nil
	case "plpgsql":
		return FunctionLangPLpgSQL, nil
	}
	return FunctionLanguage(lang), nil
}

// FunctionBodyStr is a string containing all statements in a UDF body.
type FunctionBodyStr string

// Format implements the NodeFormatter interface.
func (node FunctionBodyStr) Format(ctx *FmtCtx) {
	ctx.WriteString("AS ")
	ctx.WriteString("$$")
	if ctx.flags.HasFlags(FmtAnonymize) || ctx.flags.HasFlags(FmtHideConstants) {
		ctx.WriteString("_")
	} else {
		ctx.WriteString(string(node))
	}
	ctx.WriteString("$$")
}

// FuncParams represents a list of FuncParam.
type FuncParams []FuncParam

// Format implements the NodeFormatter interface.
func (node FuncParams) Format(ctx *FmtCtx) {
	for i := range node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node[i])
	}
}

// FuncParam represents a parameter in a UDF signature.
type FuncParam struct {
	Name       Name
	Type       ResolvableTypeReference
	Class      FuncParamClass
	DefaultVal Expr
}

// Format implements the NodeFormatter interface.
func (node *FuncParam) Format(ctx *FmtCtx) {
	switch node.Class {
	case FunctionParamIn:
		ctx.WriteString("IN")
	case FunctionParamOut:
		ctx.WriteString("OUT")
	case FunctionParamInOut:
		ctx.WriteString("INOUT")
	case FunctionParamVariadic:
		ctx.WriteString("VARIADIC")
	default:
		panic(pgerror.New(pgcode.InvalidParameterValue, "Unknown function option"))
	}
	ctx.WriteString(" ")
	if node.Name != "" {
		ctx.FormatNode(&node.Name)
		ctx.WriteString(" ")
	}
	ctx.FormatTypeReference(node.Type)
	if node.DefaultVal != nil {
		ctx.WriteString(" DEFAULT ")
		ctx.FormatNode(node.DefaultVal)
	}
}

// FuncParamClass indicates what type of argument an arg is.
type FuncParamClass int

const (
	// FunctionParamIn args can only be used as input.
	FunctionParamIn FuncParamClass = iota
	// FunctionParamOut args can only be used as output.
	FunctionParamOut
	// FunctionParamInOut args can be used as both input and output.
	FunctionParamInOut
	// FunctionParamVariadic args are variadic.
	FunctionParamVariadic
)

// FuncReturnType represent the return type of UDF.
type FuncReturnType struct {
	Type  ResolvableTypeReference
	IsSet bool
}

// DropFunction represents a DROP FUNCTION statement.
type DropFunction struct {
	IfExists     bool
	Functions    FuncObjs
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropFunction) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP FUNCTION ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Functions)
	if node.DropBehavior != DropDefault {
		ctx.WriteString(" ")
		ctx.WriteString(node.DropBehavior.String())
	}
}

// FuncObjs is a slice of FuncObj.
type FuncObjs []FuncObj

// Format implements the NodeFormatter interface.
func (node FuncObjs) Format(ctx *FmtCtx) {
	for i := range node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node[i])
	}
}

// FuncObj represents a function object DROP FUNCTION tries to drop.
type FuncObj struct {
	FuncName FunctionName
	Params   FuncParams
}

// Format implements the NodeFormatter interface.
func (node *FuncObj) Format(ctx *FmtCtx) {
	ctx.FormatNode(&node.FuncName)
	if node.Params != nil {
		ctx.WriteString("(")
		ctx.FormatNode(node.Params)
		ctx.WriteString(")")
	}
}

// ParamTypes returns a slice of parameter types of the function.
func (node FuncObj) ParamTypes(ctx context.Context, res TypeReferenceResolver) ([]*types.T, error) {
	// TODO(chengxiong): handle INOUT, OUT and VARIADIC argument classes when we
	// support them. This is because only IN and INOUT arg types need to be
	// considered to match a overload.
	var argTypes []*types.T
	if node.Params != nil {
		argTypes = make([]*types.T, len(node.Params))
		for i, arg := range node.Params {
			typ, err := ResolveType(ctx, arg.Type, res)
			if err != nil {
				return nil, err
			}
			argTypes[i] = typ
		}
	}
	return argTypes, nil
}

// AlterFunctionOptions represents a ALTER FUNCTION...action statement.
type AlterFunctionOptions struct {
	Function FuncObj
	Options  FunctionOptions
}

// Format implements the NodeFormatter interface.
func (node *AlterFunctionOptions) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER FUNCTION ")
	ctx.FormatNode(&node.Function)
	for _, option := range node.Options {
		ctx.WriteString(" ")
		ctx.FormatNode(option)
	}
}

// AlterFunctionRename represents a ALTER FUNCTION...RENAME statement.
type AlterFunctionRename struct {
	Function FuncObj
	NewName  Name
}

// Format implements the NodeFormatter interface.
func (node *AlterFunctionRename) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER FUNCTION ")
	ctx.FormatNode(&node.Function)
	ctx.WriteString(" RENAME TO ")
	ctx.WriteString(string(node.NewName))
}

// AlterFunctionSetSchema represents a ALTER FUNCTION...SET SCHEMA statement.
type AlterFunctionSetSchema struct {
	Function      FuncObj
	NewSchemaName Name
}

// Format implements the NodeFormatter interface.
func (node *AlterFunctionSetSchema) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER FUNCTION ")
	ctx.FormatNode(&node.Function)
	ctx.WriteString(" SET SCHEMA ")
	ctx.WriteString(string(node.NewSchemaName))
}

// AlterFunctionSetOwner represents the ALTER FUNCTION...OWNER TO statement.
type AlterFunctionSetOwner struct {
	Function FuncObj
	NewOwner RoleSpec
}

// Format implements the NodeFormatter interface.
func (node *AlterFunctionSetOwner) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER FUNCTION ")
	ctx.FormatNode(&node.Function)
	ctx.WriteString(" OWNER TO ")
	ctx.FormatNode(&node.NewOwner)
}

// AlterFunctionDepExtension represents the ALTER FUNCTION...DEPENDS ON statement.
type AlterFunctionDepExtension struct {
	Function  FuncObj
	Remove    bool
	Extension Name
}

// Format implements the NodeFormatter interface.
func (node *AlterFunctionDepExtension) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER FUNCTION  ")
	ctx.FormatNode(&node.Function)
	if node.Remove {
		ctx.WriteString(" NO")
	}
	ctx.WriteString(" DEPENDS ON EXTENSION ")
	ctx.WriteString(string(node.Extension))
}

// UDFDisallowanceVisitor is used to determine if a type checked expression
// contains any UDF function sub-expression. It's needed only temporarily to
// disallow any usage of UDF from relation objects.
type UDFDisallowanceVisitor struct {
	FoundUDF bool
}

// VisitPre implements the Visitor interface.
func (v *UDFDisallowanceVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if funcExpr, ok := expr.(*FuncExpr); ok && funcExpr.ResolvedOverload().HasSQLBody() {
		v.FoundUDF = true
		return false, expr
	}
	return true, expr
}

// VisitPost implements the Visitor interface.
func (v *UDFDisallowanceVisitor) VisitPost(expr Expr) (newNode Expr) {
	return expr
}

// SchemaExprContext indicates in which schema change context an expression is being
// used in. For example, DEFAULT VALUE of a column, CHECK CONSTRAINT's
// expression, etc.
type SchemaExprContext string

const (
	AlterColumnTypeUsingExpr        SchemaExprContext = "ALTER COLUMN TYPE USING EXPRESSION"
	StoredComputedColumnExpr        SchemaExprContext = "STORED COMPUTED COLUMN"
	VirtualComputedColumnExpr       SchemaExprContext = "VIRTUAL COMPUTED COLUMN"
	ColumnOnUpdateExpr              SchemaExprContext = "ON UPDATE"
	ColumnDefaultExprInAddColumn    SchemaExprContext = "DEFAULT (in ADD COLUMN)"
	ColumnDefaultExprInNewTable     SchemaExprContext = "DEFAULT (in CREATE TABLE)"
	ColumnDefaultExprInNewView      SchemaExprContext = "DEFAULT (in CREATE VIEW)"
	ColumnDefaultExprInSetDefault   SchemaExprContext = "DEFAULT (in SET DEFAULT)"
	CheckConstraintExpr             SchemaExprContext = "CHECK"
	UniqueWithoutIndexPredicateExpr SchemaExprContext = "UNIQUE WITHOUT INDEX PREDICATE"
	IndexPredicateExpr              SchemaExprContext = "INDEX PREDICATE"
	ExpressionIndexElementExpr      SchemaExprContext = "EXPRESSION INDEX ELEMENT"
	TTLExpirationExpr               SchemaExprContext = "TTL EXPIRATION EXPRESSION"
	TTLDefaultExpr                  SchemaExprContext = "TTL DEFAULT"
	TTLUpdateExpr                   SchemaExprContext = "TTL UPDATE"
)

func ComputedColumnExprContext(isVirtual bool) SchemaExprContext {
	if isVirtual {
		return VirtualComputedColumnExpr
	}
	return StoredComputedColumnExpr
}

// ValidateFuncOptions checks whether there are conflicting or redundant
// function options in the given slice.
func ValidateFuncOptions(options FunctionOptions) error {
	var hasLang, hasBody, hasLeakProof, hasVolatility, hasNullInputBehavior bool
	conflictingErr := func(opt FunctionOption) error {
		return errors.Wrapf(ErrConflictingFunctionOption, "%s", AsString(opt))
	}
	for _, option := range options {
		switch option.(type) {
		case FunctionLanguage:
			if hasLang {
				return conflictingErr(option)
			}
			hasLang = true
		case FunctionBodyStr:
			if hasBody {
				return conflictingErr(option)
			}
			hasBody = true
		case FunctionLeakproof:
			if hasLeakProof {
				return conflictingErr(option)
			}
			hasLeakProof = true
		case FunctionVolatility:
			if hasVolatility {
				return conflictingErr(option)
			}
			hasVolatility = true
		case FunctionNullInputBehavior:
			if hasNullInputBehavior {
				return conflictingErr(option)
			}
			hasNullInputBehavior = true
		default:
			return pgerror.Newf(pgcode.InvalidParameterValue, "unknown function option: ", AsString(option))
		}
	}

	return nil
}

// GetFuncVolatility tries to find a function volatility from the given list of
// function options. If there is no volatility found, FunctionVolatile is
// returned as the default.
func GetFuncVolatility(options FunctionOptions) FunctionVolatility {
	for _, option := range options {
		switch t := option.(type) {
		case FunctionVolatility:
			return t
		}
	}
	return FunctionVolatile
}
