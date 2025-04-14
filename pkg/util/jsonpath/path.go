// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

var (
	errCurrentInRoot = pgerror.Newf(pgcode.Syntax,
		"@ is not allowed in root expressions")
	errLastInNonArray = pgerror.Newf(pgcode.Syntax,
		"LAST is allowed only in array subscripts")
)

type Path interface {
	fmt.Stringer

	// Validate returns an error if there is a semantic error in the path, and
	// collects all variable names in a map with a strictly increasing index,
	// indicating the order of their first appearance. Leaf nodes should generally
	// return nil.
	Validate(vars map[string]int, nestingLevel int, insideArraySubscript bool) error
}

type Root struct{}

var _ Path = &Root{}

func (r Root) String() string { return "$" }

func (r Root) Validate(vars map[string]int, nestingLevel int, insideArraySubscript bool) error {
	return nil
}

type Key string

var _ Path = Key("")

func (k Key) String() string {
	return fmt.Sprintf(".%q", string(k))
}

func (k Key) Validate(vars map[string]int, nestingLevel int, insideArraySubscript bool) error {
	return nil
}

type Wildcard struct{}

var _ Path = &Wildcard{}

func (w Wildcard) String() string { return "[*]" }

func (w Wildcard) Validate(vars map[string]int, nestingLevel int, insideArraySubscript bool) error {
	return nil
}

type Paths []Path

var _ Path = &Paths{}

func (p Paths) String() string {
	var sb strings.Builder
	for _, i := range p {
		sb.WriteString(i.String())
	}
	return sb.String()
}

func (p Paths) Validate(vars map[string]int, nestingLevel int, insideArraySubscript bool) error {
	for _, p := range p {
		if err := p.Validate(vars, nestingLevel, insideArraySubscript); err != nil {
			return err
		}
	}
	return nil
}

type ArrayIndexRange struct {
	Start Path
	End   Path
}

var _ Path = ArrayIndexRange{}

func (a ArrayIndexRange) String() string {
	return fmt.Sprintf("%s to %s", a.Start, a.End)
}

func (a ArrayIndexRange) Validate(
	vars map[string]int, nestingLevel int, insideArraySubscript bool,
) error {
	if err := a.Start.Validate(vars, nestingLevel, insideArraySubscript); err != nil {
		return err
	}
	if err := a.End.Validate(vars, nestingLevel, insideArraySubscript); err != nil {
		return err
	}
	return nil
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

func (a ArrayList) Validate(
	vars map[string]int, nestingLevel int, insideArraySubscript bool,
) error {
	for _, p := range a {
		if err := p.Validate(vars, nestingLevel, true /* insideArraySubscript */); err != nil {
			return err
		}
	}
	return nil
}

type Last struct{}

var _ Path = Last{}

func (l Last) String() string { return "last" }

func (l Last) Validate(vars map[string]int, nestingLevel int, insideArraySubscript bool) error {
	if !insideArraySubscript {
		return errLastInNonArray
	}
	return nil
}

type Filter struct {
	Condition Path
}

var _ Path = Filter{}

func (f Filter) String() string {
	return fmt.Sprintf("?(%s)", f.Condition)
}

func (f Filter) Validate(vars map[string]int, nestingLevel int, insideArraySubscript bool) error {
	return f.Condition.Validate(vars, nestingLevel+1, insideArraySubscript)
}

type Current struct{}

var _ Path = Current{}

func (c Current) String() string { return "@" }

func (c Current) Validate(vars map[string]int, nestingLevel int, insideArraySubscript bool) error {
	if nestingLevel <= 0 {
		return errCurrentInRoot
	}
	return nil
}

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

func (r Regex) Validate(vars map[string]int, nestingLevel int, insideArraySubscript bool) error {
	return nil
}

// Pattern implements the tree.RegexpCacheKey interface.
func (r Regex) Pattern() (string, error) {
	return r.Regex, nil
}

type AnyKey struct{}

var _ Path = AnyKey{}

func (a AnyKey) String() string { return ".*" }

func (a AnyKey) Validate(vars map[string]int, nestingLevel int, insideArraySubscript bool) error {
	return nil
}
