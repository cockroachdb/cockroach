// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import (
	"fmt"
	"regexp/syntax"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keysbase"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

var (
	errCurrentInRoot = pgerror.Newf(pgcode.Syntax,
		"@ is not allowed in root expressions")
	errLastInNonArray = pgerror.Newf(pgcode.Syntax,
		"LAST is allowed only in array subscripts")
	errRegexXFlag = pgerror.Newf(pgcode.FeatureNotSupported,
		"XQuery \"x\" flag (expanded regular expressions) is not implemented")
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

// isKey returns true if a Path is a Key.
func isKey(p Path) bool {
	_, ok := p.(Key)
	return ok
}

// lastKeyIndex returns the index of the component which is the
// last Key component in the paths.
func lastKeyIndex(ps []Path) int {
	if len(ps) == 0 {
		return -1
	}
	for i := len(ps) - 1; i >= 0; i-- {
		if isKey(ps[i]) {
			return i
		}
	}
	return -1
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
	Flags syntax.Flags
}

var _ Path = Regex{}

func (r Regex) ToString(sb *strings.Builder, _, _ bool) {
	sb.WriteString(fmt.Sprintf("%q", r.Regex))
	if r.Flags != syntax.Perl {
		sb.WriteString(" flag ")
		sb.WriteString(fmt.Sprintf("%q", flagsToString(r.Flags)))
	}
}

func flagsToString(flags syntax.Flags) string {
	s := ""
	if flags&syntax.FoldCase != 0 {
		s += "i"
	}
	if flags&syntax.DotNL != 0 {
		s += "s"
	}
	// Check if the `OneLine` flag is not set.
	if flags&syntax.OneLine == 0 {
		s += "m"
	}
	if flags&syntax.Literal != 0 {
		s += "q"
	}
	return s
}

// RegexFlagsToGoFlags converts a string of flags from `like_regex` to syntax.Flags,
// to match the flags provided by Postgres' `like_regex` flag interface.
//
// The following flags are defined in Postgres:
// - i: case-insensitive
// - s: dot matches newline
// - m: ^ and $ match at newlines
// - x: ignore whitespace in pattern (this flag is defined, but not accepted)
// - q: no special characters
func RegexFlagsToGoFlags(flags string) (syntax.Flags, error) {
	// syntax.Perl is the default flag for regexp.Compile.
	goFlags := syntax.Perl
	for _, c := range flags {
		switch c {
		case 'i':
			goFlags |= syntax.FoldCase
		case 's':
			goFlags |= syntax.DotNL
		case 'm':
			// Bit clear the `OneLine` flag, since we want to match at newlines.
			// This is turned on by default in `syntax.Perl`.
			goFlags &^= syntax.OneLine
		case 'x':
			// `x` flag is not supported in Postgres.
			return syntax.Flags(0), errRegexXFlag
		case 'q':
			goFlags |= syntax.Literal
		default:
			return syntax.Flags(0), pgerror.Newf(pgcode.Syntax, "unrecognized flag character %q in LIKE_REGEX predicate", c)
		}
	}
	return goFlags, nil
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

// isAllKeyPath returns true if the given paths matches the pattern of
// $.[key|wildcard].[key|wildcard].[key|wildcard]...
func isAllKeyPath(ps []Path) bool {
	if len(ps) == 0 {
		return false
	}
	if _, ok := ps[0].(Root); !ok {
		return false
	}
	for i := 1; i < len(ps); i++ {
		switch ps[i].(type) {
		case Wildcard, Key:
		default:
			return false
		}
	}
	return true
}

func recur(bs [][]byte, ps []Path, idx int, lastKeyIdx int) inverted.Expression {
	if len(ps) == 0 || idx == len(ps) {
		return nil
	}
	if idx == lastKeyIdx {
		var res inverted.Expression
		for _, b := range bs {
			// It could be the last key of any value path, so we should not add
			// the JSON path separator.
			objectKey := append(b[:len(b):len(b)], []byte(ps[idx].(Key))...)

			objectSpan := inverted.Span{
				Start: objectKey,
				End:   keysbase.PrefixEnd(encoding.AddJSONPathSeparator(objectKey)),
			}

			if res == nil {
				res = inverted.ExprForSpan(objectSpan, true /* tight */)
			} else {
				res = inverted.Or(res, inverted.ExprForSpan(objectSpan, true /* tight */))
			}
		}

		return res
	}

	prefixes := make([][]byte, 0)
	if isKey(ps[idx]) {
		for _, b := range bs {
			// Borrowed from encodeContainingInvertedIndexSpans.
			// For "a": "b":
			prefixes = append(prefixes, encoding.EncodeJSONKeyStringAscending(b[:len(b):len(b)], string(ps[idx].(Key)), false /* end */))
			// For "a": ["b":
			prefixes = append(prefixes, encoding.EncodeArrayAscending(encoding.EncodeJSONKeyStringAscending(b[:len(b):len(b)], string(ps[idx].(Key)), false /* end */)))
		}
	} else {
		prefixes = bs
	}

	return recur(prefixes, ps, idx+1, lastKeyIdx)
}

func EncodeJsonPathInvertedIndexSpans(
	b []byte, ps Paths,
) (invertedExpr inverted.Expression, err error) {

	if !isAllKeyPath(ps) {
		return nil, nil
	}
	// No path
	if len(ps) == 0 {
		return nil, nil
	}
	// Only the root
	if len(ps) == 1 {
		emptyObjSpanExpr := inverted.ExprForSpan(
			inverted.MakeSingleValSpan(encoding.EncodeJSONEmptyObject(b[:len(b):len(b)])), false, /* tight */
		)
		emptyObjSpanExpr.Unique = true
		return emptyObjSpanExpr, nil
	}
	return recur([][]byte{encoding.EncodeJSONAscending(b)}, ps, 0, lastKeyIndex(ps)), nil
}
