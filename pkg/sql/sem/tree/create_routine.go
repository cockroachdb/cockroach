// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// ErrConflictingRoutineOption indicates that there are conflicting or
// redundant function options from user input to either create or alter a
// function.
var ErrConflictingRoutineOption = pgerror.New(pgcode.Syntax, "conflicting or redundant options")

// RoutineName represent a function name in a UDF relevant statement, either
// DDL or DML statement. Similar to TableName, it is constructed for incoming
// SQL queries from an UnresolvedObjectName.
type RoutineName struct {
	objName
}

// MakeRoutineNameFromPrefix returns a RoutineName with the given prefix and
// function name.
func MakeRoutineNameFromPrefix(prefix ObjectNamePrefix, object Name) RoutineName {
	return RoutineName{objName{
		ObjectName:       object,
		ObjectNamePrefix: prefix,
	}}
}

// MakeQualifiedRoutineName constructs a RoutineName with the given db and
// schema name as prefix.
func MakeQualifiedRoutineName(db string, sc string, fn string) RoutineName {
	return MakeRoutineNameFromPrefix(
		ObjectNamePrefix{
			CatalogName:     Name(db),
			ExplicitCatalog: true,
			SchemaName:      Name(sc),
			ExplicitSchema:  true,
		}, Name(fn),
	)
}

// Format implements the NodeFormatter interface.
func (f *RoutineName) Format(ctx *FmtCtx) {
	f.ObjectNamePrefix.Format(ctx)
	if f.ExplicitSchema || ctx.alwaysFormatTablePrefix() {
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&f.ObjectName)
}

func (f *RoutineName) String() string { return AsString(f) }

// FQString renders the function name in full, not omitting the prefix
// schema and catalog names. Suitable for logging, etc.
func (f *RoutineName) FQString() string {
	ctx := NewFmtCtx(FmtSimple)
	ctx.FormatNode(&f.CatalogName)
	ctx.WriteByte('.')
	ctx.FormatNode(&f.SchemaName)
	ctx.WriteByte('.')
	ctx.FormatNode(&f.ObjectName)
	return ctx.CloseAndGetString()
}

func (f *RoutineName) objectName() {}

// CreateRoutine represents a CREATE FUNCTION or CREATE PROCEDURE statement.
type CreateRoutine struct {
	IsProcedure bool
	Replace     bool
	Name        RoutineName
	Params      RoutineParams
	ReturnType  *RoutineReturnType
	Options     RoutineOptions
	RoutineBody *RoutineBody
	// BodyStatements is not assigned during initial parsing of user input. It's
	// assigned during opt builder for logging purpose at the moment. It stores
	// all parsed AST nodes of body statements with all expression in original
	// format. That is sequence names and type name in expressions are not
	// rewritten with OIDs.
	BodyStatements Statements
	// BodyAnnotations is not assigned during initial parsing of user input. It's
	// assigned by the opt builder when the optimizer parses the body statements.
	BodyAnnotations []*Annotations
}

// Format implements the NodeFormatter interface.
func (node *CreateRoutine) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	if node.Replace {
		ctx.WriteString("OR REPLACE ")
	}
	if node.IsProcedure {
		ctx.WriteString("PROCEDURE ")
	} else {
		ctx.WriteString("FUNCTION ")
	}
	ctx.FormatNode(&node.Name)
	ctx.WriteByte('(')
	ctx.FormatNode(node.Params)
	ctx.WriteString(")\n\t")
	if !node.IsProcedure && node.ReturnType != nil {
		ctx.WriteString("RETURNS ")
		if node.ReturnType.SetOf {
			ctx.WriteString("SETOF ")
		}
		ctx.FormatTypeReference(node.ReturnType.Type)
		ctx.WriteString("\n\t")
	}
	var isPLpgSQL bool
	var funcBody RoutineBodyStr
	for _, option := range node.Options {
		switch t := option.(type) {
		case RoutineBodyStr:
			funcBody = t
			continue
		case RoutineLeakproof, RoutineVolatility, RoutineNullInputBehavior:
			if node.IsProcedure {
				continue
			}
		case RoutineLanguage:
			isPLpgSQL = t == RoutineLangPLpgSQL
		}
		ctx.FormatNode(option)
		ctx.WriteString("\n\t")
	}

	if ctx.HasFlags(FmtMarkRedactionNode) {
		ctx.WriteString("AS ")
		ctx.WriteString("$$")
		for i, stmt := range node.BodyStatements {
			if i > 0 {
				ctx.WriteByte(' ')
			}
			oldAnn := ctx.ann
			ctx.ann = node.BodyAnnotations[i]
			ctx.FormatNode(stmt)
			if !isPLpgSQL {
				// PL/pgSQL statements handle printing semicolons themselves.
				ctx.WriteByte(';')
			}
			ctx.ann = oldAnn
		}
		ctx.WriteString("$$")
	} else if node.RoutineBody != nil {
		ctx.WriteString("BEGIN ATOMIC ")
		for _, stmt := range node.RoutineBody.Stmts {
			ctx.FormatNode(stmt)
			if !isPLpgSQL {
				// PL/pgSQL statements handle printing semicolons themselves.
				ctx.WriteByte(';')
			}
			ctx.WriteByte(' ')
		}
		ctx.WriteString("END")
	} else {
		ctx.FormatNode(funcBody)
	}
}

// RoutineBody represent a list of statements in a UDF body.
type RoutineBody struct {
	// Stmts is populated during parsing. Unlike BodyStatements, we don't need
	// to create separate Annotations for each statement.
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

// RoutineOptions represent a list of routine options.
type RoutineOptions []RoutineOption

// RoutineOption is an interface representing UDF properties.
type RoutineOption interface {
	routineOption()
	NodeFormatter
}

func (RoutineNullInputBehavior) routineOption() {}
func (RoutineVolatility) routineOption()        {}
func (RoutineLeakproof) routineOption()         {}
func (RoutineBodyStr) routineOption()           {}
func (RoutineLanguage) routineOption()          {}
func (RoutineSecurity) routineOption()          {}

// RoutineNullInputBehavior represent the UDF property on null parameters.
type RoutineNullInputBehavior int

const (
	// RoutineCalledOnNullInput indicates that the routine will be given the
	// chance to execute when presented with NULL input. This is the default if
	// no null input behavior is specified.
	RoutineCalledOnNullInput RoutineNullInputBehavior = iota
	// RoutineReturnsNullOnNullInput indicates that the routine will result in
	// NULL given any NULL parameter.
	RoutineReturnsNullOnNullInput
	// RoutineStrict is the same as RoutineReturnsNullOnNullInput
	RoutineStrict
)

// Format implements the NodeFormatter interface.
func (node RoutineNullInputBehavior) Format(ctx *FmtCtx) {
	switch node {
	case RoutineCalledOnNullInput:
		ctx.WriteString("CALLED ON NULL INPUT")
	case RoutineReturnsNullOnNullInput:
		ctx.WriteString("RETURNS NULL ON NULL INPUT")
	case RoutineStrict:
		ctx.WriteString("STRICT")
	default:
		panic(pgerror.New(pgcode.InvalidParameterValue, "Unknown routine option"))
	}
}

// RoutineVolatility represent UDF volatility property.
type RoutineVolatility int

const (
	// RoutineVolatile represents volatility.Volatile. This is the default
	// volatility if none is provided.
	RoutineVolatile RoutineVolatility = iota
	// RoutineImmutable represents volatility.Immutable.
	RoutineImmutable
	// RoutineStable represents volatility.Stable.
	RoutineStable
)

// Format implements the NodeFormatter interface.
func (node RoutineVolatility) Format(ctx *FmtCtx) {
	switch node {
	case RoutineVolatile:
		ctx.WriteString("VOLATILE")
	case RoutineImmutable:
		ctx.WriteString("IMMUTABLE")
	case RoutineStable:
		ctx.WriteString("STABLE")
	default:
		panic(pgerror.New(pgcode.InvalidParameterValue, "unknown routine option"))
	}
}

// RoutineLeakproof indicates whether a function is leakproof or not. The
// default is NOT LEAKPROOF if no leakproof option is provided. LEAKPROOF can
// only be used with the IMMUTABLE volatility because we currently conflated
// LEAKPROOF as a volatility equal to IMMUTABLE+LEAKPROOF. Postgres allows
// STABLE+LEAKPROOF functions.
type RoutineLeakproof bool

// Format implements the NodeFormatter interface.
func (node RoutineLeakproof) Format(ctx *FmtCtx) {
	if !node {
		ctx.WriteString("NOT ")
	}
	ctx.WriteString("LEAKPROOF")
}

// RoutineLanguage indicates the language of the statements in the routine body.
type RoutineLanguage string

const (
	// RoutineLangUnknown represents an unknown language.
	RoutineLangUnknown RoutineLanguage = "unknown"
	// RoutineLangSQL represents SQL language.
	RoutineLangSQL RoutineLanguage = "SQL"
	// RoutineLangPLpgSQL represents the PL/pgSQL procedural language.
	RoutineLangPLpgSQL RoutineLanguage = "plpgsql"
	// RoutineLangC represents the C language.
	RoutineLangC RoutineLanguage = "C"
)

// Format implements the NodeFormatter interface.
func (node RoutineLanguage) Format(ctx *FmtCtx) {
	ctx.WriteString("LANGUAGE ")
	ctx.WriteString(string(node))
}

// AsRoutineLanguage converts a string to a RoutineLanguage if applicable.
// No error is returned if string does not represent a valid UDF language;
// unknown languages result in an error later.
func AsRoutineLanguage(lang string) (RoutineLanguage, error) {
	switch strings.ToLower(lang) {
	case "sql":
		return RoutineLangSQL, nil
	case "plpgsql":
		return RoutineLangPLpgSQL, nil
	case "c":
		return RoutineLangC, nil
	}
	return RoutineLanguage(lang), nil
}

// RoutineSecurity indicates the mode of security that the routine will
// be executed with.
type RoutineSecurity int

const (
	// RoutineInvoker indicates that the routine is run with the privileges of
	// the user invoking it. This is the default security mode if none is
	// provided.
	RoutineInvoker RoutineSecurity = iota
	// RoutineDefiner indicates that the routine is run with the privileges of
	// the user who defined it.
	RoutineDefiner
)

// Format implements the NodeFormatter interface.
func (node RoutineSecurity) Format(ctx *FmtCtx) {
	ctx.WriteString("SECURITY ")
	switch node {
	case RoutineInvoker:
		ctx.WriteString("INVOKER")
	case RoutineDefiner:
		ctx.WriteString("DEFINER")
	default:
		panic(pgerror.New(pgcode.InvalidParameterValue, "unknown routine option"))
	}
}

// RoutineBodyStr is a string containing all statements in a UDF body.
type RoutineBodyStr string

// Format implements the NodeFormatter interface.
func (node RoutineBodyStr) Format(ctx *FmtCtx) {
	ctx.WriteString("AS ")
	ctx.FormatStringDollarQuotes(string(node))
}

// RoutineParams represents a list of RoutineParam.
type RoutineParams []RoutineParam

// Format implements the NodeFormatter interface.
func (node RoutineParams) Format(ctx *FmtCtx) {
	for i := range node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node[i])
	}
}

// RoutineParam represents a parameter in a UDF signature.
type RoutineParam struct {
	Name       Name
	Type       ResolvableTypeReference
	Class      RoutineParamClass
	DefaultVal Expr
}

// Format implements the NodeFormatter interface.
func (node *RoutineParam) Format(ctx *FmtCtx) {
	switch node.Class {
	case RoutineParamDefault:
	case RoutineParamIn:
		ctx.WriteString("IN ")
	case RoutineParamOut:
		ctx.WriteString("OUT ")
	case RoutineParamInOut:
		ctx.WriteString("INOUT ")
	case RoutineParamVariadic:
		ctx.WriteString("VARIADIC ")
	default:
		panic(pgerror.New(pgcode.InvalidParameterValue, "unknown routine option"))
	}
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

// RoutineParamClass indicates what type of argument an arg is.
type RoutineParamClass int

const (
	// RoutineParamDefault indicates that RoutineParamClass was unspecified
	// (in almost all cases it is equivalent to RoutineParamIn).
	RoutineParamDefault RoutineParamClass = iota
	// RoutineParamIn args can only be used as input.
	RoutineParamIn
	// RoutineParamOut args can only be used as output.
	RoutineParamOut
	// RoutineParamInOut args can be used as both input and output.
	RoutineParamInOut
	// RoutineParamVariadic args are variadic.
	RoutineParamVariadic
)

// IsInParamClass returns true if the given parameter class specifies an input
// parameter (i.e. either unspecified, IN or, INOUT).
func IsInParamClass(class RoutineParamClass) bool {
	switch class {
	case RoutineParamDefault, RoutineParamIn, RoutineParamInOut:
		return true
	default:
		return false
	}
}

// IsOutParamClass returns true if the given parameter class specifies an output
// parameter (i.e. either OUT or INOUT).
func IsOutParamClass(class RoutineParamClass) bool {
	switch class {
	case RoutineParamOut, RoutineParamInOut:
		return true
	default:
		return false
	}
}

// IsInParam returns true if the parameter is an input parameter (i.e. either IN
// or INOUT).
func (node *RoutineParam) IsInParam() bool {
	return IsInParamClass(node.Class)
}

// IsOutParam returns true if the parameter is an output parameter (i.e. either
// OUT or INOUT).
func (node *RoutineParam) IsOutParam() bool {
	return IsOutParamClass(node.Class)
}

// RoutineReturnType represent the return type of UDF.
type RoutineReturnType struct {
	Type  ResolvableTypeReference
	SetOf bool
}

// DropRoutine represents a DROP FUNCTION or DROP PROCEDURE statement.
type DropRoutine struct {
	IfExists     bool
	Procedure    bool
	Routines     RoutineObjs
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropRoutine) Format(ctx *FmtCtx) {
	if node.Procedure {
		ctx.WriteString("DROP PROCEDURE ")
	} else {
		ctx.WriteString("DROP FUNCTION ")
	}
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Routines)
	if node.DropBehavior != DropDefault {
		ctx.WriteString(" ")
		ctx.WriteString(node.DropBehavior.String())
	}
}

// RoutineObjs is a slice of RoutineObj.
type RoutineObjs []RoutineObj

// Format implements the NodeFormatter interface.
func (node RoutineObjs) Format(ctx *FmtCtx) {
	for i := range node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node[i])
	}
}

// RoutineObj represents a routine (function or procedure) object in DROP,
// GRANT, and REVOKE statements.
type RoutineObj struct {
	FuncName RoutineName
	Params   RoutineParams
}

// Format implements the NodeFormatter interface.
func (node *RoutineObj) Format(ctx *FmtCtx) {
	ctx.FormatNode(&node.FuncName)
	if node.Params != nil {
		ctx.WriteString("(")
		ctx.FormatNode(node.Params)
		ctx.WriteString(")")
	}
}

// AlterFunctionOptions represents a ALTER FUNCTION...action statement.
type AlterFunctionOptions struct {
	Function RoutineObj
	Options  RoutineOptions
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

// AlterRoutineRename represents a ALTER FUNCTION...RENAME or
// ALTER PROCEDURE...RENAME statement.
type AlterRoutineRename struct {
	Function  RoutineObj
	NewName   Name
	Procedure bool
}

// Format implements the NodeFormatter interface.
func (node *AlterRoutineRename) Format(ctx *FmtCtx) {
	if node.Procedure {
		ctx.WriteString("ALTER PROCEDURE ")
	} else {
		ctx.WriteString("ALTER FUNCTION ")
	}
	ctx.FormatNode(&node.Function)
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&node.NewName)
}

// AlterRoutineSetSchema represents a ALTER FUNCTION...SET SCHEMA or
// ALTER PROCEDURE...SET SCHEMA statement.
type AlterRoutineSetSchema struct {
	Function      RoutineObj
	NewSchemaName Name
	Procedure     bool
}

// Format implements the NodeFormatter interface.
func (node *AlterRoutineSetSchema) Format(ctx *FmtCtx) {
	if node.Procedure {
		ctx.WriteString("ALTER PROCEDURE ")
	} else {
		ctx.WriteString("ALTER FUNCTION ")
	}
	ctx.FormatNode(&node.Function)
	ctx.WriteString(" SET SCHEMA ")
	ctx.FormatNode(&node.NewSchemaName)
}

// AlterRoutineSetOwner represents the ALTER FUNCTION...OWNER TO or
// ALTER PROCEDURE...OWNER TO statement.
type AlterRoutineSetOwner struct {
	Function  RoutineObj
	NewOwner  RoleSpec
	Procedure bool
}

// Format implements the NodeFormatter interface.
func (node *AlterRoutineSetOwner) Format(ctx *FmtCtx) {
	if node.Procedure {
		ctx.WriteString("ALTER PROCEDURE ")
	} else {
		ctx.WriteString("ALTER FUNCTION ")
	}
	ctx.FormatNode(&node.Function)
	ctx.WriteString(" OWNER TO ")
	ctx.FormatNode(&node.NewOwner)
}

// AlterFunctionDepExtension represents the ALTER FUNCTION...DEPENDS ON statement.
type AlterFunctionDepExtension struct {
	Function  RoutineObj
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
	PolicyUsingExpr                 SchemaExprContext = "POLICY USING"
	PolicyWithCheckExpr             SchemaExprContext = "POLICY WITH CHECK"
)

func ComputedColumnExprContext(isVirtual bool) SchemaExprContext {
	if isVirtual {
		return VirtualComputedColumnExpr
	}
	return StoredComputedColumnExpr
}

// ValidateRoutineOptions checks whether there are conflicting or redundant
// routine options in the given slice.
func ValidateRoutineOptions(options RoutineOptions, isProc bool) error {
	var hasLang, hasBody, hasLeakProof, hasVolatility, hasNullInputBehavior, hasSecurity bool
	conflictingErr := func(opt RoutineOption) error {
		return errors.Wrapf(ErrConflictingRoutineOption, "%s", AsString(opt))
	}
	for _, option := range options {
		switch option.(type) {
		case RoutineLanguage:
			if hasLang {
				return conflictingErr(option)
			}
			hasLang = true
		case RoutineBodyStr:
			if hasBody {
				return conflictingErr(option)
			}
			hasBody = true
		case RoutineLeakproof:
			if isProc {
				return pgerror.Newf(pgcode.InvalidFunctionDefinition, "leakproof attribute not allowed in procedure definition")
			}
			if hasLeakProof {
				return conflictingErr(option)
			}
			hasLeakProof = true
		case RoutineVolatility:
			if isProc {
				return pgerror.Newf(pgcode.InvalidFunctionDefinition, "volatility attribute not allowed in procedure definition")
			}
			if hasVolatility {
				return conflictingErr(option)
			}
			hasVolatility = true
		case RoutineNullInputBehavior:
			if isProc {
				return pgerror.Newf(pgcode.InvalidFunctionDefinition, "null input attribute not allowed in procedure definition")
			}
			if hasNullInputBehavior {
				return conflictingErr(option)
			}
			hasNullInputBehavior = true
		case RoutineSecurity:
			if hasSecurity {
				return conflictingErr(option)
			}
			hasSecurity = true
		default:
			return pgerror.Newf(pgcode.InvalidParameterValue, "unknown function option: ", AsString(option))
		}
	}

	return nil
}

// GetRoutineVolatility tries to find a function volatility from the given list of
// function options. If there is no volatility found, RoutineVolatile is
// returned as the default.
func GetRoutineVolatility(options RoutineOptions) RoutineVolatility {
	for _, option := range options {
		switch t := option.(type) {
		case RoutineVolatility:
			return t
		}
	}
	return RoutineVolatile
}
