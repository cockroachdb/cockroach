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

type Jsonpath struct {
	Strict bool
	Path   Path
}

func (j Jsonpath) String() string {
	var mode string
	if j.Strict {
		mode = "strict "
	}
	return mode + j.Path.String()
}

func (j Jsonpath) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(j.String())
}

type Path interface {
	fmt.Stringer
}

type Root struct{}

var _ Path = &Root{}

func (r Root) String() string { return "$" }

type Key string

var _ Path = Key("")

func (k Key) String() string {
	return fmt.Sprintf(".%q", string(k))
}

type Wildcard struct{}

var _ Path = &Wildcard{}

func (w Wildcard) String() string { return "[*]" }

type Paths []Path

var _ Path = &Paths{}

func (p Paths) String() string {
	var sb strings.Builder
	for _, i := range p {
		sb.WriteString(i.String())
	}
	return sb.String()
}

type Variable string

var _ Path = Variable("")

func (v Variable) String() string {
	return fmt.Sprintf("$%q", string(v))
}

type Numeric struct {
	IsNegative bool
	IsFloat    bool
	FloatValue float64
	IntValue   int64
}

var _ Path = Numeric{}

func NewNumericFloat(f float64) Numeric {
	return Numeric{
		IsFloat:    true,
		FloatValue: f,
	}
}

func NewNumericInt(i int64) Numeric {
	return Numeric{
		IsFloat:    false,
		IntValue:   i,
		IsNegative: false,
	}
}

func (n Numeric) String() string {
	if n.IsFloat {
		return fmt.Sprintf("%g", n.FloatValue)
	}
	if n.IsNegative {
		return fmt.Sprintf("-%d", n.IntValue)
	}
	return fmt.Sprintf("%d", n.IntValue)
}

type ArrayIndex Numeric

var _ Path = ArrayIndex{}

func (a ArrayIndex) String() string {
	return Numeric(a).String()
}

type ArrayIndexRange struct {
	Start ArrayIndex
	End   ArrayIndex
}

var _ Path = ArrayIndexRange{}

func (a ArrayIndexRange) String() string {
	return fmt.Sprintf("%s to %s", Numeric(a.Start), Numeric(a.End))
}

type ArrayList []Path

var _ Path = ArrayList{}

func (a ArrayList) String() string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, p := range a {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(p.String())
	}
	sb.WriteString("]")
	return sb.String()
}
