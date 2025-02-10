// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type Expr interface {
	fmt.Stringer
	tree.NodeFormatter
}

// Identical to Expr for now.
type Accessor interface {
	Expr
}

type Jsonpath struct {
	Query  Query
	Strict bool
}

var _ Expr = Jsonpath{}

func (j Jsonpath) String() string {
	var mode string
	if j.Strict {
		mode = "strict "
	}
	return mode + j.Query.String()
}

func (j Jsonpath) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(j.String())
}

type Query struct {
	Accessors []Accessor
}

var _ Expr = Query{}

func (q Query) String() string {
	var sb strings.Builder
	for _, accessor := range q.Accessors {
		sb.WriteString(accessor.String())
	}
	return sb.String()
}

func (q Query) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(q.String())
}

type Root struct{}

var _ Accessor = Root{}

func (r Root) String() string { return "$" }

func (r Root) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(r.String())
}

type Key struct {
	Key string
}

var _ Accessor = Key{}

func (k Key) String() string { return "." + k.Key }

func (k Key) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(k.String())
}

type Wildcard struct{}

var _ Accessor = Wildcard{}

func (w Wildcard) String() string { return "[*]" }

func (w Wildcard) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(w.String())
}
