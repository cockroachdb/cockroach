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

type ArrayIndexRange struct {
	Start Path
	End   Path
}

var _ Path = ArrayIndexRange{}

func (a ArrayIndexRange) String() string {
	return fmt.Sprintf("%s to %s", a.Start, a.End)
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

type Last struct{}

var _ Path = Last{}

func (l Last) String() string { return "last" }

type Filter struct {
	Condition Path
}

var _ Path = Filter{}

func (f Filter) String() string {
	return fmt.Sprintf("?(%s)", f.Condition)
}

type Current struct{}

var _ Path = Current{}

func (c Current) String() string { return "@" }

type Regex struct {
	Regex string
	// Flags are currently not used.
	Flags string
}

var _ Path = Regex{}

func (r Regex) String() string {
	if r.Flags == "" {
		return fmt.Sprintf("%q", r.Regex)
	}
	return fmt.Sprintf("%q flag %q", r.Regex, r.Flags)
}

var _ tree.RegexpCacheKey = Regex{}

func (r Regex) Pattern() (string, error) {
	return r.Regex, nil
}

type AnyKey struct{}

var _ Path = AnyKey{}

func (a AnyKey) String() string { return ".*" }
