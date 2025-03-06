// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

type ScalarType int

const (
	ScalarInt ScalarType = iota
	ScalarFloat
	ScalarString
	ScalarBool
	ScalarNull
	ScalarVariable
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

type Scalar struct {
	Type     ScalarType
	Value    json.JSON
	Variable string
}

var _ Path = Scalar{}

func (s Scalar) String() string {
	if s.Type == ScalarVariable {
		return fmt.Sprintf("$%q", s.Variable)
	}
	return s.Value.String()
}

type ArrayIndex Scalar

var _ Path = ArrayIndex{}

func (a ArrayIndex) String() string {
	return Scalar(a).String()
}

type ArrayIndexRange struct {
	Start ArrayIndex
	End   ArrayIndex
}

var _ Path = ArrayIndexRange{}

func (a ArrayIndexRange) String() string {
	return fmt.Sprintf("%s to %s", a.Start.Value, a.End.Value)
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
