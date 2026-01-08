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
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
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

// lastKeyIndex returns the index of the component which is the last Key
// component in the paths. We traverse the path components, looking for
// the rightmost Key component. For Filter components, it also checks
// the left side of the condition for Key components. Returns -1 if no
// Key component is found.
func lastKeyIndex(ps []Path) int {
	if len(ps) == 0 {
		return -1
	}
	for i := len(ps) - 1; i >= 0; i-- {
		switch pt := ps[i].(type) {
		case Key:
			return i
		case Filter:
			left := pt.Condition.(Operation).Left.(Paths)
			for j := len(left) - 1; j >= 0; j-- {
				if isKey(left[j]) {
					return i
				}
			}
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

// isSupportedPathPattern returns true if the given paths matches one of
// the following patterns, which can be supported by the inverted index:
// - keychain mode: $.[key|wildcard].[key|wildcard]...(*)
// - end value mode: $.[key|wildcard]? (@.[key|wildcard].[key|wildcard]... == [string|number|null|boolean])
// We might call this function recursively if a Path is a Filter, which contains
// child Paths. If isSupportedPathPattern is called within a Filter, atRoot
// is set false.
func isSupportedPathPattern(ps []Path, atRoot bool) bool {
	// Wildcard components do not contribute to filtering, so we skip them.
	notWildCardPathCount := 0
	for _, p := range ps {
		if _, ok := p.(Wildcard); !ok {
			notWildCardPathCount++
		}
	}
	// If the path is empty or essentially only contains the root, no filtering
	// can be done, and thus there is no point in leveraging the inverted index.
	if notWildCardPathCount <= 1 {
		return false
	}

	// For `$`.
	if atRoot {
		if _, ok := ps[0].(Root); !ok {
			return false
		}
	} else {
		// For `@`.
		if _, ok := ps[0].(Current); !ok {
			return false
		}
	}

	for i := 1; i < len(ps); i++ {
		switch pt := ps[i].(type) {
		case Wildcard, Key:
		case AnyKey:
			// We only allow AnyKey at the end of the root path.
			return i == len(ps)-1 && atRoot
		case Filter:
			// We only allow filter at the end of the path.
			if i != len(ps)-1 {
				return false
			}
			// For `?`.
			if pt.Condition == nil {
				return false
			}
			op, ok := pt.Condition.(Operation)
			if !ok {
				return false
			}
			if op.Type != OpCompEqual {
				return false
			}
			leftPaths, ok := op.Left.(Paths)
			if !ok || len(leftPaths) == 0 {
				return false
			}
			if !isSupportedPathPattern(leftPaths, false /* atRoot */) {
				return false
			}
			rightPaths, ok := op.Right.(Paths)
			if !ok || len(rightPaths) != 1 {
				return false
			}
			rightScalar, ok := rightPaths[0].(Scalar)
			if !ok {
				return false
			}
			if rightScalar.Type == ScalarVariable {
				return false
			}
			return true
		default:
			return false
		}
	}
	return true
}

// errHandling is a helper function to handle errors during span generation.
// nil is returned to indicate that the inverted index can not be used.
var errHandling = func(err error) inverted.Expression {
	if buildutil.CrdbTestBuild {
		panic(err)
	}
	return nil
}

// addSpanToResult combines the new span with the current inverted expression,
// returning the updated result.
func addSpanToResult(result inverted.Expression, span inverted.Span) inverted.Expression {
	if result == nil {
		return inverted.ExprForSpan(span, true /* tight */)
	}
	return inverted.Or(result, inverted.ExprForSpan(span, true /* tight */))
}

// spanThreshold is a safeguard to avoid generating too many spans. Since
// the number of spans grows exponentially with the number of Key components
// in the path, we set a threshold to limit the number of spans generated.
// If the number of spans exceeds this threshold, we return nil to indicate
// that the inverted index should not be used, and we rollback to a full
// scan plan instead.
const spanThreshold = 10_000

// buildInvertedIndexSpans recursively constructs inverted index
// expressions for JSON path queries. It processes path components
// sequentially, building up encoded key prefixes until reaching
// terminal conditions, i.e. currentIndex == finalKeyIndex.
//
// filterValue stands for the right-hand side value for filter
// expressions (nil for non-filter paths). When filterValue is non-nil,
// it indicates that we should generate a single value span.
//
// The function handles two main scenarios:
//  1. Terminal case: when we reach the last Key component, we generate
//     the final spans
//  2. Recursive case: when processing intermediate components, we build
//     prefixes and recurse
//
// For Key components, we encode both object and array representations:
// - Direct object property: {"key": "value"}
// - Array element: {"key": ["value"]}
//
// Filter expressions are handled by recursively processing the
// left-hand path with the right-hand value.
//
// Example: For path $.f1.mclaren ? (@.driver == "Piastri")
//
// Initial call:
//
//	prefixBytes: [encoded_json_prefix]
//	pathComponents: [Root, Key("f1"), Key("mclaren"), Filter(...)]
//	currentIndex: 0, finalKeyIndex: 3 (last Key is "[?(@.driver == "Piastri")]")
//
// Iteration 0 (Root component ($)):
//   - Consider both the situations of object and array at root level.
//     > encoded_json_prefix -> encoded_obj_json_prefix
//     > encoded_json_prefix + array_marker -> encoded_arr_json_prefix
//   - Skip Root, advance to currentIndex=1
//   - Recurse with same prefixBytes
//
// Iteration 1 (Key "f1"):
//   - Build object prefix:
//     > encoded_obj_json_prefix + "f1" -> obj_f1_obj_prefix
//     > encoded_arr_json_prefix + "f1" -> arr_f1_obj_prefix
//   - Build array prefix:
//     > encoded_obj_json_prefix + "f1" + array_marker -> obj_f1_arr_prefix
//     > encoded_arr_json_prefix + "f1" + array_marker -> arr_f1_arr_prefix
//   - Recurse with prefixBytes: [obj_f1_obj_prefix, arr_f1_obj_prefix, obj_f1_arr_prefix, arr_f1_arr_prefix], currentIndex=2
//
// Iteration 2 (Key "mclaren"):
//   - obj_f1_obj_prefix + "mclaren" -> obj_f1_obj_mclaren_obj_prefix
//   - arr_f1_obj_prefix + "mclaren" -> arr_f1_obj_mclaren_obj_prefix
//   - obj_f1_arr_prefix + "mclaren" -> obj_f1_arr_mclaren_obj_prefix
//   - arr_f1_arr_prefix + "mclaren" -> arr_f1_arr_mclaren_obj_prefix
//   - obj_f1_obj_prefix + "mclaren" + array_marker -> obj_f1_obj_mclaren_arr_prefix
//   - arr_f1_obj_prefix + "mclaren" + array_marker -> arr_f1_obj_mclaren_arr_prefix
//   - obj_f1_arr_prefix + "mclaren" + array_marker -> obj_f1_arr_mclaren_arr_prefix
//   - arr_f1_arr_prefix + "mclaren" + array_marker -> arr_f1_arr_mclaren_arr_prefix
//   - Recurse with 8 prefixBytes above.
//   - currentIndex=3
//
// Iteration 3 (Filter - terminal case):
//   - We need to add an optional array prefix, since the Filter enables
//     unwrapping one extra layer of array.
//   - Thus we have 16 prefixes now:
//     {obj|arr}_f1_{obj|arr}_mclaren_{obj|arr}_{arr}
//   - Extract left path (@.driver) and right value ("Piastri")
//   - We now construct the new start and new last key index with the left path
//   - currentIndex: 0, finalKeyIndex: 2 (last Key is "Piastri")
//
// > Iteration 3.0 (Current component (@)):
//   - Skip Current, advance to currentIndex=1
//   - Recurse with same prefixBytes
//
// > Iteration 3.1 (Key "driver"):
//   - Similar to Iteration 1 and 2, the prefix are built for driver, so we should have
//     prefixes like f1_obj_mclaren_obj_prefix + {"driver", "driver"+array_marker} , etc.
//
// > Iteration 3.2 (Scalar "Piastri" - terminal case):
//   - We are at terminal, thus generate single value spans with each of
//     the prefixes built so far + "Piastri".
//   - Eventually, we returns the concatenation (OR) of 32 (2^5) spans:
//     > {obj|arr}_f1_{obj|arr}_mclaren_{obj|arr}_{arr}_driver_{obj|arr}_Piastri
func buildInvertedIndexSpans(
	prefixBytes [][]byte,
	pathComponents []Path,
	currentIndex int,
	finalKeyIndex int,
	filterValue Path,
) inverted.Expression {
	if len(pathComponents) == 0 || currentIndex == len(pathComponents) || finalKeyIndex < 0 {
		return nil
	}

	// We do runtime-time check to see how many []byte we've accumulated so far.
	// If it exceeds a threshold, it might be more efficient to do full table scan,
	// so we return nil to roll back to the non-indexed plan.
	if len(prefixBytes) > spanThreshold {
		return nil
	}

	// Terminal case: we've reached the last Key component in the path.
	if currentIndex == finalKeyIndex {
		switch pathType := pathComponents[currentIndex].(type) {
		case Key, Current:
			var resultExpression inverted.Expression
			for _, prefix := range prefixBytes {
				var baseKey []byte
				key, isKey := pathType.(Key)
				if isKey {
					// Build the key by appending to the current prefix.
					baseKey = append(prefix[:len(prefix):len(prefix)], []byte(key)...)
				}
				if filterValue != nil {
					// Meaning this is of the end value mode. (See isSupportedPathPattern).
					//
					// These type casts and array access are safe given the
					// checks we did in isSupportedPathPattern().
					scalar := filterValue.(Paths)[0].(Scalar)
					// Encode for direct object property: {"key": "value"}.
					objectKeys, err := scalar.Value.EncodeInvertedIndexKeys(baseKey[:len(baseKey):len(baseKey)])
					if err != nil {
						return errHandling(err)
					}
					if len(objectKeys) != 1 {
						return errHandling(errors.AssertionFailedf("expected exactly one encoded key, got %d", len(objectKeys)))
					}
					resultExpression = addSpanToResult(resultExpression, inverted.MakeSingleValSpan(objectKeys[0]))

					// Encode for array element: {"key": ["value"]}.
					arrayKeys, err := scalar.Value.EncodeInvertedIndexKeys(encoding.EncodeArrayAscending(encoding.AddJSONPathSeparator(baseKey[:len(baseKey):len(baseKey)])))
					if err != nil {
						return errHandling(err)
					}
					if len(arrayKeys) != 1 {
						return errHandling(errors.AssertionFailedf("expected exactly one encoded key, got %d", len(arrayKeys)))
					}
					resultExpression = addSpanToResult(resultExpression, inverted.MakeSingleValSpan(arrayKeys[0]))
				} else {
					resSpans := []inverted.Span{{
						Start: baseKey,
						End:   keysbase.PrefixEnd(encoding.AddJSONPathSeparator(baseKey[:len(baseKey):len(baseKey)])),
					}}
					// If the last path component is an AnyKey, it means the
					// current key must not be an end key (since AnyKey matches
					// any key under the current object/array). For example, for
					// path $.a.b.*, the following won't match because "b" is the
					// end key:
					// - {"a": {"b": "d"}}
					// - {"a": {"b": ["c"]}}
					// But the following will match:
					// - {"a": {"b": {"c": "d"}}}
					// - {"a": {"b": [{"c": {"d": "e"}}]}}
					// In these 2 cases, after "b", there is still "c"
					// as the next key, so "b" is not an end key.
					if _, isAnyKey := pathComponents[len(pathComponents)-1].(AnyKey); isAnyKey {
						// asEndValKey means the baseKey is mapped to an end value
						// in the json object. (e.g. {"a": {"b": "d"}})
						asEndValKey := encoding.AddJSONPathTerminator(baseKey[:len(baseKey):len(baseKey)])
						asEndValKeySpan := inverted.Span{
							Start: asEndValKey,
							End:   keysbase.PrefixEnd(asEndValKey),
						}
						// asEndArrayValKey means the baseKey is mapped to an end value
						// in the array object. (e.g. {"a": {"b": ["c"]}}})
						asEndArrayValKey := encoding.AddJSONPathTerminator(encoding.EncodeArrayAscending(encoding.AddJSONPathSeparator(baseKey[:len(baseKey):len(baseKey)])))
						asEndArrayValKeySpan := inverted.Span{
							Start: asEndArrayValKey,
							End:   keysbase.PrefixEnd(asEndArrayValKey),
						}
						resSpans = inverted.SubtractSpans(resSpans, []inverted.Span{asEndValKeySpan, asEndArrayValKeySpan})
					}

					for _, sp := range resSpans {
						resultExpression = addSpanToResult(resultExpression, sp)
					}
				}
			}
			return resultExpression

		case Filter:
			operation := pathType.Condition.(Operation)
			leftPaths := operation.Left.(Paths)
			prefixBytesLen := len(prefixBytes)
			for i := 0; i < prefixBytesLen; i++ {
				prefix := prefixBytes[i]
				// Filter can unwrap one extra layer of array.
				prefixBytes = append(prefixBytes, encoding.EncodeArrayAscending(prefix[:len(prefix):len(prefix)]))
			}
			// Recursively process the filter's left path with the right-hand value as the filter.
			return buildInvertedIndexSpans(prefixBytes, leftPaths, 0, lastKeyIndex(leftPaths), operation.Right)
		}
	}

	// Recursive case: build prefixes for the current path component and continue.
	var nextPrefixes [][]byte
	switch pc := pathComponents[currentIndex].(type) {
	case Key:
		// For Key components, encode both object and array prefixes.
		keyName := string(pc)
		for _, prefix := range prefixBytes {
			// Encode as object property: {"key": ...}.
			nextPrefixes = append(nextPrefixes, encoding.EncodeJSONKeyStringAscending(prefix[:len(prefix):len(prefix)], keyName, false /* end */))
			// Encode as array element: {"key": [...]}.
			nextPrefixes = append(nextPrefixes, encoding.EncodeArrayAscending(encoding.EncodeJSONKeyStringAscending(prefix[:len(prefix):len(prefix)], keyName, false /* end */)))
		}
	case Root, Wildcard:
		prefixBytesLen := len(prefixBytes)
		for i := 0; i < prefixBytesLen; i++ {
			// Wildcard or Root can unwrap one extra layer of array.
			prefix := prefixBytes[i]
			prefixBytes = append(prefixBytes, encoding.EncodeArrayAscending(prefix[:len(prefix):len(prefix)]))
		}
		nextPrefixes = prefixBytes
	case Current:
		// For Current (@), pass through existing prefixes unchanged.
		nextPrefixes = prefixBytes
	default:
		// This is a pattern we don't support for inverted index.
		return errHandling(errors.Newf("unexpected path pattern in inverted index span generation: %s", pathComponents))
	}
	// Continue recursion with the updated prefixes and next index
	return buildInvertedIndexSpans(nextPrefixes, pathComponents, currentIndex+1, finalKeyIndex, filterValue)
}

// EncodeJsonPathInvertedIndexSpans returns an inverted expression for the given
// json path expression. If nil is returned, it indicates that the inverted index
// does not support the given path expression, and the caller should fall back to
// a full table scan plan.
func EncodeJsonPathInvertedIndexSpans(b []byte, p Path) (invertedExpr inverted.Expression) {
	var ps Paths
	switch t := p.(type) {
	case Paths:
		ps = t
	default:
		return nil
	}

	if !isSupportedPathPattern(ps, true /* atRoot */) {
		return nil
	}

	if len(ps) == 0 {
		return nil
	}
	return buildInvertedIndexSpans([][]byte{encoding.EncodeJSONAscending(b)}, ps, 0, lastKeyIndex(ps), nil /* filterValue */)
}
