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

import "github.com/cockroachdb/errors"

type FunctionName struct {
	objName
}

func (f *FunctionName) Format(ctx *FmtCtx) {
	f.ObjectNamePrefix.Format(ctx)
	if f.ExplicitSchema || ctx.alwaysFormatTablePrefix() {
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&f.ObjectName)
}

type FuncArgClass string

const (
	FunctionArgIn       FuncArgClass = "IN"
	FunctionArgOut      FuncArgClass = "OUT"
	FunctionArgInOut    FuncArgClass = "INOUT"
	FunctionArgVariadic FuncArgClass = "VARIADIC"
)

type FunctionOptions []FunctionOption

type FunctionOption interface {
	functionOption()
	NodeFormatter
}

func (_ FunctionInputStrictness) functionOption() {}
func (_ FunctionVolatility) functionOption()      {}
func (_ FunctionLeakProof) functionOption()       {}
func (_ FunctionParallel) functionOption()        {}
func (_ FunctionBodyStr) functionOption()         {}
func (_ FunctionLanguage) functionOption()        {}

type FunctionInputStrictness string

func (node FunctionInputStrictness) Format(f *FmtCtx) {
	f.WriteString(string(node))
}

const (
	FunctionCalledOnNullInput      FunctionInputStrictness = "CALLED ON NULL INPUT"
	FunctionReturnsNullOnNullInput FunctionInputStrictness = "RETURNS NULL ON NULL INPUT"
	FunctionStrict                 FunctionInputStrictness = "STRICT"
)

type FunctionVolatility string

func (node FunctionVolatility) Format(f *FmtCtx) {
	f.WriteString(string(node))
}

const (
	FunctionImmutable FunctionVolatility = "IMMUTABLE"
	FunctionStable    FunctionVolatility = "STABLE"
	FunctionVolatile  FunctionVolatility = "VOLATILE"
)

type FunctionLeakProof bool

func (node FunctionLeakProof) Format(f *FmtCtx) {
	if !node {
		f.WriteString("NOT ")
	}
	f.WriteString("LEAKPROOF")
}

type FunctionParallel string

func (node FunctionParallel) Format(f *FmtCtx) {
	f.WriteString(string(node))
}

const (
	FunctionParallelUnsafe     FunctionParallel = "PARALLEL UNSAFE"
	FunctionParallelRestricted FunctionParallel = "PARALLEL RESTRICTED"
	FunctionParallelSafe       FunctionParallel = "PARALLEL SAFE"
)

type FunctionLanguage string

func (node FunctionLanguage) Format(f *FmtCtx) {
	f.WriteString("LANGUAGE ")
	f.WriteString(string(node))
}

const (
	FunctionLangSQL FunctionLanguage = "SQL"
)

func AsFunctionLanguage(lang string) (FunctionLanguage, error) {
	switch lang {
	case "sql":
		return FunctionLangSQL, nil
	}
	return "", errors.Newf("language %q does not exist", lang)
}

type FunctionBodyStr string

func (node FunctionBodyStr) Format(f *FmtCtx) {
	f.WriteString("AS ")
	f.WriteString("'")
	f.WriteString(string(node))
	f.WriteString("'")
}

type FuncArg struct {
	Name       Name
	Type       ResolvableTypeReference
	Class      FuncArgClass
	DefaultVal Expr
}

func (node *FuncArg) Format(ctx *FmtCtx) {
	ctx.WriteString(string(node.Class))
	ctx.WriteString(" ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" ")
	ctx.WriteString(node.Type.SQLString())
	if node.DefaultVal != nil {
		ctx.WriteString(" DEFAULT")
		ctx.FormatNode(node.DefaultVal)
	}
}

type FuncArgs []FuncArg

func (node FuncArgs) Format(ctx *FmtCtx) {
	for i, arg := range node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&arg)
	}
}

type CreateFunction struct {
	IsProcedure bool
	Replace     bool
	FuncName    FunctionName
	Args        FuncArgs
	ReturnType  ResolvableTypeReference
	Options     FunctionOptions
}

func (node *CreateFunction) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	if node.Replace {
		ctx.WriteString("OR REPLACE ")
	}
	ctx.WriteString("FUNCTION ")
	ctx.FormatNode(&node.FuncName)
	ctx.WriteString("(")
	ctx.FormatNode(node.Args)
	ctx.WriteString(") ")
	ctx.WriteString("RETURNS ")
	ctx.WriteString(node.ReturnType.SQLString())
	ctx.WriteString(" ")
	var funcBody FunctionBodyStr
	for _, option := range node.Options {
		switch t := option.(type) {
		case FunctionBodyStr:
			funcBody = t
			continue
		}
		ctx.FormatNode(option)
		ctx.WriteString(" ")
	}
	ctx.FormatNode(funcBody)
}
