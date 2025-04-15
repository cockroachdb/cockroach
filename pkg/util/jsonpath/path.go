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
	// ToString appends the string representation of the path to the given
	// strings.Builder. This implementation matches
	// postgres/src/backend/utils/adt/jsonpath.c:printJsonPathItem().
	ToString(sb *strings.Builder, inKey, printBrackets bool)

	// Validate returns an error if there is a semantic error in the path, and
	// collects all variable names in a map with a strictly increasing index,
	// indicating the order of their first appearance. Leaf nodes should generally
	// return nil.
	Validate(nestingLevel int, insideArraySubscript bool) error
}

type Root struct{}

var _ Path = &Root{}

func (r Root) ToString(sb *strings.Builder, _, _ bool) {
	sb.WriteString("$")
}

func (r Root) Validate(nestingLevel int, insideArraySubscript bool) error {
	return nil
}

type Key string

var _ Path = Key("")

func (k Key) ToString(sb *strings.Builder, inKey, _ bool) {
	if inKey {
		sb.WriteString(".")
	}
	sb.WriteString(fmt.Sprintf("%q", string(k)))
}

func (k Key) Validate(nestingLevel int, insideArraySubscript bool) error {
	return nil
}

type Wildcard struct{}

var _ Path = &Wildcard{}

func (w Wildcard) ToString(sb *strings.Builder, _, _ bool) {
	sb.WriteString("[*]")
}

func (w Wildcard) Validate(nestingLevel int, insideArraySubscript bool) error {
	return nil
}

type Paths []Path

var _ Path = &Paths{}

func (p Paths) ToString(sb *strings.Builder, _, _ bool) {
	for _, path := range p {
		path.ToString(sb, true /* inKey */, true /* printBrackets */)
	}
}

func (p Paths) Validate(nestingLevel int, insideArraySubscript bool) error {
	for _, p := range p {
		if err := p.Validate(nestingLevel, insideArraySubscript); err != nil {
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

func (a ArrayIndexRange) ToString(sb *strings.Builder, _, _ bool) {
	a.Start.ToString(sb, false /* inKey */, false /* printBrackets */)
	sb.WriteString(" to ")
	a.End.ToString(sb, false /* inKey */, false /* printBrackets */)
}

func (a ArrayIndexRange) Validate(nestingLevel int, insideArraySubscript bool) error {
	if err := a.Start.Validate(nestingLevel, insideArraySubscript); err != nil {
		return err
	}
	if err := a.End.Validate(nestingLevel, insideArraySubscript); err != nil {
		return err
	}
	return nil
}

type ArrayList []Path

var _ Path = ArrayList{}

func (a ArrayList) ToString(sb *strings.Builder, _, _ bool) {
	sb.WriteString("[")
	for i, path := range a {
		if i > 0 {
			sb.WriteString(",")
		}
		path.ToString(sb, false /* inKey */, false /* printBrackets */)
	}
	sb.WriteString("]")
}

func (a ArrayList) Validate(nestingLevel int, insideArraySubscript bool) error {
	for _, p := range a {
		if err := p.Validate(nestingLevel, true /* insideArraySubscript */); err != nil {
			return err
		}
	}
	return nil
}

type Last struct{}

var _ Path = Last{}

func (l Last) ToString(sb *strings.Builder, _, _ bool) {
	sb.WriteString("last")
}

func (l Last) Validate(nestingLevel int, insideArraySubscript bool) error {
	if !insideArraySubscript {
		return errLastInNonArray
	}
	return nil
}

type Filter struct {
	Condition Path
}

var _ Path = Filter{}

func (f Filter) ToString(sb *strings.Builder, _, _ bool) {
	sb.WriteString("?(")
	f.Condition.ToString(sb, false /* inKey */, false /* printBrackets */)
	sb.WriteString(")")
}

func (f Filter) Validate(nestingLevel int, insideArraySubscript bool) error {
	return f.Condition.Validate(nestingLevel+1, insideArraySubscript)
}

type Current struct{}

var _ Path = Current{}

func (c Current) ToString(sb *strings.Builder, _, _ bool) {
	sb.WriteString("@")
}

func (c Current) Validate(nestingLevel int, insideArraySubscript bool) error {
	if nestingLevel <= 0 {
		return errCurrentInRoot
	}
	return nil
}

type Regex struct {
	Regex string
}

var _ Path = Regex{}

func (r Regex) ToString(sb *strings.Builder, _, _ bool) {
	sb.WriteString(fmt.Sprintf("%q", r.Regex))
}

func (r Regex) Validate(nestingLevel int, insideArraySubscript bool) error {
	return nil
}

// Pattern implements the tree.RegexpCacheKey interface.
func (r Regex) Pattern() (string, error) {
	return r.Regex, nil
}

type AnyKey struct{}

var _ Path = AnyKey{}

func (a AnyKey) ToString(sb *strings.Builder, inKey, _ bool) {
	if inKey {
		sb.WriteString(".")
	}
	sb.WriteString("*")
}

func (a AnyKey) Validate(nestingLevel int, insideArraySubscript bool) error {
	return nil
}
