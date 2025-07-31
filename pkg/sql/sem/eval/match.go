// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// MatchLikeEscape matches 'unescaped' with 'pattern' using custom escape character 'escape' which
// must be either empty (which disables the escape mechanism) or a single unicode character.
func MatchLikeEscape(
	ctx *Context, unescaped, pattern, escape string, caseInsensitive bool,
) (tree.Datum, error) {
	var escapeRune rune
	if len(escape) > 0 {
		var width int
		escapeRune, width = utf8.DecodeRuneInString(escape)
		if len(escape) > width {
			return tree.DBoolFalse, pgerror.Newf(pgcode.InvalidEscapeSequence, "invalid escape string")
		}
	}

	if len(unescaped) == 0 {
		// An empty string only matches with an empty pattern or a pattern
		// consisting only of '%' (if this wildcard is not used as a custom escape
		// character). To match PostgreSQL's behavior, we have a special handling
		// of this case.
		for _, c := range pattern {
			if c != '%' || (c == '%' && escape == `%`) {
				return tree.DBoolFalse, nil
			}
		}
		return tree.DBoolTrue, nil
	}

	like, err := optimizedLikeFunc(pattern, caseInsensitive, escapeRune)
	if err != nil {
		return tree.DBoolFalse, pgerror.Wrap(
			err, pgcode.InvalidRegularExpression, "LIKE regexp compilation failed")
	}

	if like == nil {
		re, err := ConvertLikeToRegexp(ctx, pattern, caseInsensitive, escapeRune)
		if err != nil {
			return tree.DBoolFalse, err
		}
		like = func(s string) (bool, error) {
			return re.MatchString(s), nil
		}
	}
	matches, err := like(unescaped)
	return tree.MakeDBool(tree.DBool(matches)), err
}

// ConvertLikeToRegexp compiles the specified LIKE pattern as an equivalent
// regular expression.
func ConvertLikeToRegexp(
	ctx *Context, pattern string, caseInsensitive bool, escape rune,
) (*regexp.Regexp, error) {
	key := likeKey{s: pattern, caseInsensitive: caseInsensitive, escape: escape}
	re, err := ctx.ReCache.GetRegexp(key)
	if err != nil {
		return nil, pgerror.Wrap(
			err, pgcode.InvalidRegularExpression, "LIKE regexp compilation failed")
	}
	return re, nil
}

func matchLike(ctx *Context, left, right tree.Datum, caseInsensitive bool) (tree.Datum, error) {
	if left == tree.DNull || right == tree.DNull {
		return tree.DNull, nil
	}
	s, pattern := string(tree.MustBeDString(left)), string(tree.MustBeDString(right))
	if len(s) == 0 {
		// An empty string only matches with an empty pattern or a pattern
		// consisting only of '%'. To match PostgreSQL's behavior, we have a
		// special handling of this case.
		for _, c := range pattern {
			if c != '%' {
				return tree.DBoolFalse, nil
			}
		}
		return tree.DBoolTrue, nil
	}

	like, err := optimizedLikeFunc(pattern, caseInsensitive, '\\')
	if err != nil {
		return tree.DBoolFalse, pgerror.Wrap(
			err, pgcode.InvalidRegularExpression, "LIKE regexp compilation failed")
	}

	if like == nil {
		re, err := ConvertLikeToRegexp(ctx, pattern, caseInsensitive, '\\')
		if err != nil {
			return tree.DBoolFalse, err
		}
		like = func(s string) (bool, error) {
			return re.MatchString(s), nil
		}
	}
	matches, err := like(s)
	return tree.MakeDBool(tree.DBool(matches)), err
}

func matchRegexpWithKey(ctx *Context, str tree.Datum, key tree.RegexpCacheKey) (tree.Datum, error) {
	re, err := ctx.ReCache.GetRegexp(key)
	if err != nil {
		return tree.DBoolFalse, pgerror.Wrap(err, pgcode.InvalidRegularExpression, "invalid regular expression")
	}
	return tree.MakeDBool(tree.DBool(re.MatchString(string(tree.MustBeDString(str))))), nil
}

// hasUnescapedSuffix returns true if the ending byte is suffix and s has an
// even number of escapeTokens preceding suffix. Otherwise hasUnescapedSuffix
// will return false.
func hasUnescapedSuffix(s string, suffix byte, escapeToken string) bool {
	if s[len(s)-1] == suffix {
		var count int
		idx := len(s) - len(escapeToken) - 1
		for idx >= 0 && s[idx:idx+len(escapeToken)] == escapeToken {
			count++
			idx -= len(escapeToken)
		}
		return count%2 == 0
	}
	return false
}

// Simplifies LIKE/ILIKE expressions that do not need full regular expressions to
// evaluate the condition. For example, when the expression is just checking to see
// if a string starts with a given pattern.
func optimizedLikeFunc(
	pattern string, caseInsensitive bool, escape rune,
) (func(string) (bool, error), error) {
	switch len(pattern) {
	case 0:
		return func(s string) (bool, error) {
			return s == "", nil
		}, nil
	case 1:
		switch pattern[0] {
		case '%':
			if escape == '%' {
				return nil, pgerror.Newf(pgcode.InvalidEscapeSequence, "LIKE pattern must not end with escape character")
			}
			return func(s string) (bool, error) {
				return true, nil
			}, nil
		case '_':
			if escape == '_' {
				return nil, pgerror.Newf(pgcode.InvalidEscapeSequence, "LIKE pattern must not end with escape character")
			}
			return func(s string) (bool, error) {
				if len(s) == 0 {
					return false, nil
				}
				firstChar, _ := utf8.DecodeRuneInString(s)
				if firstChar == utf8.RuneError {
					return false, errors.Errorf("invalid encoding of the first character in string %s", s)
				}
				return len(s) == len(string(firstChar)), nil
			}, nil
		default:
			// Patterns without a wildcard boil down to a direct string comparison
			// (after checking for escapes).
			if rune(pattern[0]) == escape {
				return nil, pgerror.Newf(pgcode.InvalidEscapeSequence, `LIKE pattern must not end with escape character`)
			}
			return func(s string) (bool, error) {
				if caseInsensitive {
					s, pattern = strings.ToUpper(s), strings.ToUpper(pattern)
				}
				return s == pattern, nil
			}, nil
		}
	default:
		if !strings.ContainsAny(pattern[1:len(pattern)-1], "_%") {
			// Patterns with even number of escape characters preceding the ending
			// `%` will have anyEnd set to true (if `%` itself is not an escape
			// character). Otherwise anyEnd will be set to false.
			anyEnd := hasUnescapedSuffix(pattern, '%', string(escape)) && escape != '%'
			// If '%' is the escape character, then it's not a wildcard.
			anyStart := pattern[0] == '%' && escape != '%'

			// Patterns with even number of escape characters preceding the ending
			// `_` will have singleAnyEnd set to true (if `_` itself is not an escape
			// character). Otherwise singleAnyEnd will be set to false.
			singleAnyEnd := hasUnescapedSuffix(pattern, '_', string(escape)) && escape != '_'
			// If '_' is the escape character, then it's not a wildcard.
			singleAnyStart := pattern[0] == '_' && escape != '_'

			// Since we've already checked for escaped characters
			// at the end, we can un-escape every character.
			// This is required since we do direct string
			// comparison.
			var err error
			if pattern, err = unescapePattern(pattern, string(escape), true /* emitEscapeCharacterLastError */); err != nil {
				return nil, err
			}
			switch {
			case anyEnd && anyStart:
				return func(s string) (bool, error) {
					substr := pattern[1 : len(pattern)-1]
					if caseInsensitive {
						s, substr = strings.ToUpper(s), strings.ToUpper(substr)
					}
					return strings.Contains(s, substr), nil
				}, nil

			case anyEnd:
				return func(s string) (bool, error) {
					prefix := pattern[:len(pattern)-1]
					if singleAnyStart {
						if len(s) == 0 {
							return false, nil
						}
						prefix = prefix[1:]
						firstChar, _ := utf8.DecodeRuneInString(s)
						if firstChar == utf8.RuneError {
							return false, errors.Errorf("invalid encoding of the first character in string %s", s)
						}
						s = s[len(string(firstChar)):]
					}
					if caseInsensitive {
						s, prefix = strings.ToUpper(s), strings.ToUpper(prefix)
					}
					return strings.HasPrefix(s, prefix), nil
				}, nil

			case anyStart:
				return func(s string) (bool, error) {
					suffix := pattern[1:]
					if singleAnyEnd {
						if len(s) == 0 {
							return false, nil
						}

						suffix = suffix[:len(suffix)-1]
						lastChar, _ := utf8.DecodeLastRuneInString(s)
						if lastChar == utf8.RuneError {
							return false, errors.Errorf("invalid encoding of the last character in string %s", s)
						}
						s = s[:len(s)-len(string(lastChar))]
					}
					if caseInsensitive {
						s, suffix = strings.ToUpper(s), strings.ToUpper(suffix)
					}
					return strings.HasSuffix(s, suffix), nil
				}, nil

			default:
				// This default case handles (singleAnyStart || singleAnyEnd) as well as
				// the case with no wildcards at all (!singleAnyStart && !singleAnyEnd)
				// which becomes a direct string comparison after accounting for
				// escaping and case-sensitivity.
				return func(s string) (bool, error) {
					if len(s) < 1 {
						return false, nil
					}
					firstChar, _ := utf8.DecodeRuneInString(s)
					if firstChar == utf8.RuneError {
						return false, errors.Errorf("invalid encoding of the first character in string %s", s)
					}
					lastChar, _ := utf8.DecodeLastRuneInString(s)
					if lastChar == utf8.RuneError {
						return false, errors.Errorf("invalid encoding of the last character in string %s", s)
					}
					if singleAnyStart && singleAnyEnd && len(string(firstChar))+len(string(lastChar)) > len(s) {
						return false, nil
					}

					if singleAnyStart {
						pattern = pattern[1:]
						s = s[len(string(firstChar)):]
					}

					if singleAnyEnd {
						pattern = pattern[:len(pattern)-1]
						s = s[:len(s)-len(string(lastChar))]
					}

					if caseInsensitive {
						s, pattern = strings.ToUpper(s), strings.ToUpper(pattern)
					}

					// We don't have to check for
					// prefixes/suffixes since we do not
					// have '%':
					//  - singleAnyEnd && anyStart handled
					//    in case anyStart
					//  - singleAnyStart && anyEnd handled
					//    in case anyEnd
					return s == pattern, nil
				}, nil
			}
		}
	}
	return nil, nil
}

type likeKey struct {
	s               string
	caseInsensitive bool
	escape          rune
}

// LikeEscape converts a like pattern to a regexp pattern.
func LikeEscape(pattern string) (string, error) {
	key := likeKey{s: pattern, caseInsensitive: false, escape: '\\'}
	re, err := key.patternNoAnchor()
	return re, err
}

// unescapePattern unescapes a pattern for a given escape token.
// It handles escaped escape tokens properly by maintaining them as the escape
// token in the return string.
// For example, suppose we have escape token `\` (e.g. `B` is escaped in
// `A\BC` and `\` is escaped in `A\\C`).
// We need to convert
//
//	`\` --> ``
//	`\\` --> `\`
//
// We cannot simply use strings.Replace for each conversion since the first
// conversion will incorrectly replace our escaped escape token `\\` with “.
// Another example is if our escape token is `\\` (e.g. after
// regexp.QuoteMeta).
// We need to convert
//
//	`\\` --> ``
//	`\\\\` --> `\\`
func unescapePattern(
	pattern, escapeToken string, emitEscapeCharacterLastError bool,
) (string, error) {
	escapedEscapeToken := escapeToken + escapeToken

	// We need to subtract the escaped escape tokens to avoid double
	// counting.
	nEscapes := strings.Count(pattern, escapeToken) - strings.Count(pattern, escapedEscapeToken)
	if nEscapes == 0 {
		return pattern, nil
	}

	// Allocate buffer for final un-escaped pattern.
	ret := make([]byte, len(pattern)-nEscapes*len(escapeToken))
	retWidth := 0
	for i := 0; i < nEscapes; i++ {
		nextIdx := strings.Index(pattern, escapeToken)
		if nextIdx == len(pattern)-len(escapeToken) && emitEscapeCharacterLastError {
			return "", pgerror.Newf(pgcode.InvalidEscapeSequence, `LIKE pattern must not end with escape character`)
		}

		retWidth += copy(ret[retWidth:], pattern[:nextIdx])

		if nextIdx < len(pattern)-len(escapedEscapeToken) && pattern[nextIdx:nextIdx+len(escapedEscapeToken)] == escapedEscapeToken {
			// We have an escaped escape token.
			// We want to keep it as the original escape token in
			// the return string.
			retWidth += copy(ret[retWidth:], escapeToken)
			pattern = pattern[nextIdx+len(escapedEscapeToken):]
			continue
		}

		// Skip over the escape character we removed.
		pattern = pattern[nextIdx+len(escapeToken):]
	}

	retWidth += copy(ret[retWidth:], pattern)
	return string(ret[0:retWidth]), nil
}

// replaceUnescaped replaces all instances of oldStr that are not escaped (read:
// preceded) with the specified unescape token with newStr.
// For example, with an escape token of `\\`
//
//	replaceUnescaped("TE\\__ST", "_", ".", `\\`) --> "TE\\_.ST"
//	replaceUnescaped("TE\\%%ST", "%", ".*", `\\`) --> "TE\\%.*ST"
//
// If the preceding escape token is escaped, then oldStr will be replaced.
// For example
//
//	replaceUnescaped("TE\\\\_ST", "_", ".", `\\`) --> "TE\\\\.ST"
func replaceUnescaped(s, oldStr, newStr string, escapeToken string) string {
	// We count the number of occurrences of 'oldStr'.
	// This however can be an overestimate since the oldStr token could be
	// escaped.  e.g. `\\_`.
	nOld := strings.Count(s, oldStr)
	if nOld == 0 {
		return s
	}

	// Allocate buffer for final string.
	// This can be an overestimate since some of the oldStr tokens may
	// be escaped.
	// This is fine since we keep track of the running number of bytes
	// actually copied.
	// It's rather difficult to count the exact number of unescaped
	// tokens without manually iterating through the entire string and
	// keeping track of escaped escape tokens.
	retLen := len(s)
	// If len(newStr) - len(oldStr) < 0, then this can under-allocate which
	// will not behave correctly with copy.
	if addnBytes := nOld * (len(newStr) - len(oldStr)); addnBytes > 0 {
		retLen += addnBytes
	}
	ret := make([]byte, retLen)
	retWidth := 0
	start := 0
OldLoop:
	for i := 0; i < nOld; i++ {
		nextIdx := start + strings.Index(s[start:], oldStr)

		escaped := false
		for {
			// We need to look behind to check if the escape token
			// is really an escape token.
			// E.g. if our specified escape token is `\\` and oldStr
			// is `_`, then
			//    `\\_` --> escaped
			//    `\\\\_` --> not escaped
			//    `\\\\\\_` --> escaped
			curIdx := nextIdx
			lookbehindIdx := curIdx - len(escapeToken)
			for lookbehindIdx >= 0 && s[lookbehindIdx:curIdx] == escapeToken {
				escaped = !escaped
				curIdx = lookbehindIdx
				lookbehindIdx = curIdx - len(escapeToken)
			}

			// The token was not be escaped. Proceed.
			if !escaped {
				break
			}

			// Token was escaped. Copy everything over and continue.
			retWidth += copy(ret[retWidth:], s[start:nextIdx+len(oldStr)])
			start = nextIdx + len(oldStr)

			// Continue with next oldStr token.
			continue OldLoop
		}

		// Token was not escaped so we replace it with newStr.
		// Two copies is more efficient than concatenating the slices.
		retWidth += copy(ret[retWidth:], s[start:nextIdx])
		retWidth += copy(ret[retWidth:], newStr)
		start = nextIdx + len(oldStr)
	}

	retWidth += copy(ret[retWidth:], s[start:])
	return string(ret[0:retWidth])
}

// Replaces all custom escape characters in s with `\\` only when they are unescaped.          (1)
// E.g. original pattern       after QuoteMeta       after replaceCustomEscape with '@' as escape
//
//	'@w@w'          ->      '@w@w'        ->        '\\w\\w'
//	'@\@\'          ->      '@\\@\\'      ->        '\\\\\\\\'
//
// When an escape character is escaped, we replace it with its single occurrence.              (2)
// E.g. original pattern       after QuoteMeta       after replaceCustomEscape with '@' as escape
//
//	'@@w@w'         ->      '@@w@w'       ->        '@w\\w'
//	'@@@\'          ->      '@@@\\'       ->        '@\\\\'
//
// At the same time, we do not want to confuse original backslashes (which
// after QuoteMeta are '\\') with backslashes that replace our custom escape characters,
// so we escape these original backslashes again by converting '\\' into '\\\\'.               (3)
// E.g. original pattern       after QuoteMeta       after replaceCustomEscape with '@' as escape
//
//	'@\'            ->      '@\\'         ->        '\\\\\\'
//	'@\@@@\'        ->      '@\\@@@\\'    ->        '\\\\\\@\\\\\\'
//
// Explanation of the last example:
// 1. we replace '@' with '\\' since it's unescaped;
// 2. we escape single original backslash ('\' is not our escape character, so we want
// the pattern to understand it) by putting an extra backslash in front of it. However,
// we later will call unescapePattern, so we need to double our double backslashes.
// Therefore, '\\' is converted into '\\\\'.
// 3. '@@' is replaced by '@' because it is escaped escape character.
// 4. '@' is replaced with '\\' since it's unescaped.
// 5. Similar logic to step 2: '\\' -> '\\\\'.
//
// We always need to keep in mind that later call of unescapePattern
// to actually unescape '\\' and '\\\\' is necessary and that
// escape must be a single unicode character and not `\`.
func replaceCustomEscape(s string, escape rune) (string, error) {
	changed, retLen, err := calculateLengthAfterReplacingCustomEscape(s, escape)
	if err != nil {
		return "", err
	}
	if !changed {
		return s, nil
	}

	sLen := len(s)
	ret := make([]byte, retLen)
	retIndex, sIndex := 0, 0
	for retIndex < retLen {
		sRune, w := utf8.DecodeRuneInString(s[sIndex:])
		if sRune == escape {
			// We encountered an escape character.
			if sIndex+w < sLen {
				// Escape character is not the last character in s, so we need
				// to look ahead to figure out how to process it.
				tRune, _ := utf8.DecodeRuneInString(s[(sIndex + w):])
				if tRune == escape {
					// Escape character is escaped, so we replace its two occurrences with just one. See (2).
					// We copied only one escape character to ret, so we advance retIndex only by w.
					// Since we've already processed two characters in s, we advance sIndex by 2*w.
					utf8.EncodeRune(ret[retIndex:], escape)
					retIndex += w
					sIndex += 2 * w
				} else {
					// Escape character is unescaped, so we replace it with `\\`. See (1).
					// Since we've added two bytes to ret, we advance retIndex by 2.
					// We processed only a single escape character in s, we advance sIndex by w.
					ret[retIndex] = '\\'
					ret[retIndex+1] = '\\'
					retIndex += 2
					sIndex += w
				}
			} else {
				// Escape character is the last character in s which is an error
				// that must have been caught in calculateLengthAfterReplacingCustomEscape.
				return "", errors.AssertionFailedf(
					"unexpected: escape character is the last one in replaceCustomEscape.")
			}
		} else if s[sIndex] == '\\' {
			// We encountered a backslash, so we need to look ahead to figure out how
			// to process it.
			if sIndex+1 == sLen {
				// This case should never be reached since it should
				// have been caught in calculateLengthAfterReplacingCustomEscape.
				return "", errors.AssertionFailedf(
					"unexpected: a single backslash encountered in replaceCustomEscape.")
			} else if s[sIndex+1] == '\\' {
				// We want to escape '\\' to `\\\\` for correct processing later by unescapePattern. See (3).
				// Since we've added four characters to ret, we advance retIndex by 4.
				// Since we've already processed two characters in s, we advance sIndex by 2.
				ret[retIndex] = '\\'
				ret[retIndex+1] = '\\'
				ret[retIndex+2] = '\\'
				ret[retIndex+3] = '\\'
				retIndex += 4
				sIndex += 2
			} else {
				// A metacharacter other than a backslash is escaped here.
				// Note: all metacharacters are encoded as a single byte, so it is
				// correct to just convert it to string and to compare against a char
				// in s.
				if string(s[sIndex+1]) == string(escape) {
					// The metacharacter is our custom escape character. We need to look
					// ahead to process it.
					if sIndex+2 == sLen {
						// Escape character is the last character in s which is an error
						// that must have been caught in calculateLengthAfterReplacingCustomEscape.
						return "", errors.AssertionFailedf(
							"unexpected: escape character is the last one in replaceCustomEscape.")
					}
					if sIndex+4 <= sLen {
						if s[sIndex+2] == '\\' && string(s[sIndex+3]) == string(escape) {
							// We have a sequence of `\`+escape+`\`+escape which is replaced
							// by `\`+escape.
							ret[retIndex] = '\\'
							// Note: all metacharacters are encoded as a single byte, so it
							// is safe to just convert it to string and take the first
							// character.
							ret[retIndex+1] = string(escape)[0]
							retIndex += 2
							sIndex += 4
							continue
						}
					}
					// The metacharacter is escaping something different than itself, so
					// `\`+escape will be replaced by `\`.
					ret[retIndex] = '\\'
					retIndex++
					sIndex += 2
				} else {
					// The metacharacter is not our custom escape character, so we're
					// simply copying the backslash and the metacharacter.
					ret[retIndex] = '\\'
					ret[retIndex+1] = s[sIndex+1]
					retIndex += 2
					sIndex += 2
				}
			}
		} else {
			// Regular symbol, so we simply copy it.
			copy(ret[retIndex:], s[sIndex:sIndex+w])
			retIndex += w
			sIndex += w
		}
	}
	return string(ret), nil
}

// calculateLengthAfterReplacingCustomEscape returns whether the pattern changes, the length
// of the resulting pattern after calling replaceCustomEscape, and any error if found.
func calculateLengthAfterReplacingCustomEscape(s string, escape rune) (bool, int, error) {
	changed := false
	retLen, sLen := 0, len(s)
	for i := 0; i < sLen; {
		sRune, w := utf8.DecodeRuneInString(s[i:])
		if sRune == escape {
			// We encountered an escape character.
			if i+w < sLen {
				// Escape character is not the last character in s, so we need
				// to look ahead to figure out how to process it.
				tRune, _ := utf8.DecodeRuneInString(s[(i + w):])
				if tRune == escape {
					// Escape character is escaped, so we'll replace its two occurrences with just one.
					// See (2) in the comment above replaceCustomEscape.
					changed = true
					retLen += w
					i += 2 * w
				} else {
					// Escape character is unescaped, so we'll replace it with `\\`.
					// See (1) in the comment above replaceCustomEscape.
					changed = true
					retLen += 2
					i += w
				}
			} else {
				// Escape character is the last character in s, so we need to return an error.
				return false, 0, pgerror.Newf(pgcode.InvalidEscapeSequence, "LIKE pattern must not end with escape character")
			}
		} else if s[i] == '\\' {
			// We encountered a backslash, so we need to look ahead to figure out how
			// to process it.
			if i+1 == sLen {
				// This case should never be reached because the backslash should be
				// escaping one of regexp metacharacters.
				return false, 0, pgerror.Newf(pgcode.InvalidEscapeSequence, "Unexpected behavior during processing custom escape character.")
			} else if s[i+1] == '\\' {
				// We'll want to escape '\\' to `\\\\` for correct processing later by
				// unescapePattern. See (3) in the comment above replaceCustomEscape.
				changed = true
				retLen += 4
				i += 2
			} else {
				// A metacharacter other than a backslash is escaped here.
				if string(s[i+1]) == string(escape) {
					// The metacharacter is our custom escape character. We need to look
					// ahead to process it.
					if i+2 == sLen {
						// Escape character is the last character in s, so we need to return an error.
						return false, 0, pgerror.Newf(pgcode.InvalidEscapeSequence, "LIKE pattern must not end with escape character")
					}
					if i+4 <= sLen {
						if s[i+2] == '\\' && string(s[i+3]) == string(escape) {
							// We have a sequence of `\`+escape+`\`+escape which will be
							// replaced by `\`+escape.
							changed = true
							retLen += 2
							i += 4
							continue
						}
					}
					// The metacharacter is escaping something different than itself, so
					// `\`+escape will be replaced by `\`.
					changed = true
					retLen++
					i += 2
				} else {
					// The metacharacter is not our custom escape character, so we're
					// simply copying the backslash and the metacharacter.
					retLen += 2
					i += 2
				}
			}
		} else {
			// Regular symbol, so we'll simply copy it.
			retLen += w
			i += w
		}
	}
	return changed, retLen, nil
}

func (k likeKey) patternNoAnchor() (string, error) {
	// QuoteMeta escapes all regexp metacharacters (`\.+*?()|[]{}^$`) with a `\`.
	pattern := regexp.QuoteMeta(k.s)
	var err error
	if k.escape == 0 {
		// Replace all LIKE/ILIKE specific wildcards with standard wildcards
		// (escape character is empty - escape mechanism is turned off - so
		// all '%' and '_' are actual wildcards regardless of what precedes them.
		pattern = strings.Replace(pattern, `%`, `.*`, -1)
		pattern = strings.Replace(pattern, `_`, `.`, -1)
	} else if k.escape == '\\' {
		// Replace LIKE/ILIKE specific wildcards with standard wildcards only when
		// these wildcards are not escaped by '\\' (equivalent of '\' after QuoteMeta).
		pattern = replaceUnescaped(pattern, `%`, `.*`, `\\`)
		pattern = replaceUnescaped(pattern, `_`, `.`, `\\`)
	} else {
		// k.escape is non-empty and not `\`.
		// If `%` is escape character, then it's not a wildcard.
		if k.escape != '%' {
			// Replace LIKE/ILIKE specific wildcards '%' only if it's unescaped.
			if k.escape == '.' {
				// '.' is the escape character, so for correct processing later by
				// replaceCustomEscape we need to escape it by itself.
				pattern = replaceUnescaped(pattern, `%`, `..*`, regexp.QuoteMeta(string(k.escape)))
			} else if k.escape == '*' {
				// '*' is the escape character, so for correct processing later by
				// replaceCustomEscape we need to escape it by itself.
				pattern = replaceUnescaped(pattern, `%`, `.**`, regexp.QuoteMeta(string(k.escape)))
			} else {
				pattern = replaceUnescaped(pattern, `%`, `.*`, regexp.QuoteMeta(string(k.escape)))
			}
		}
		// If `_` is escape character, then it's not a wildcard.
		if k.escape != '_' {
			// Replace LIKE/ILIKE specific wildcards '_' only if it's unescaped.
			if k.escape == '.' {
				// '.' is the escape character, so for correct processing later by
				// replaceCustomEscape we need to escape it by itself.
				pattern = replaceUnescaped(pattern, `_`, `..`, regexp.QuoteMeta(string(k.escape)))
			} else {
				pattern = replaceUnescaped(pattern, `_`, `.`, regexp.QuoteMeta(string(k.escape)))
			}
		}

		// If a sequence of symbols ` escape+`\\` ` is unescaped, then that escape
		// character escapes backslash in the original pattern (we need to use double
		// backslash because of QuoteMeta behavior), so we want to "consume" the escape character.
		pattern = replaceUnescaped(pattern, string(k.escape)+`\\`, `\\`, regexp.QuoteMeta(string(k.escape)))

		// We want to replace all escape characters with `\\` only
		// when they are unescaped. When an escape character is escaped,
		// we replace it with its single occurrence.
		if pattern, err = replaceCustomEscape(pattern, k.escape); err != nil {
			return pattern, err
		}
	}

	// After QuoteMeta, all '\' were converted to '\\'.
	// After replaceCustomEscape, our custom unescaped escape characters were converted to `\\`,
	// so now our pattern contains only '\\' as escape tokens.
	// We need to unescape escaped escape tokens `\\` (now `\\\\`) and
	// other escaped characters `\A` (now `\\A`).
	if k.escape != 0 {
		// We do not want to return an error when pattern ends with the supposed escape character `\`
		// whereas the actual escape character is not `\`. The case when the pattern ends with
		// an actual escape character is handled in replaceCustomEscape. For example, with '-' as
		// the escape character on pattern 'abc\\' we do not want to return an error 'pattern ends
		// with escape character' because '\\' is not an escape character in this case.
		if pattern, err = unescapePattern(
			pattern,
			`\\`,
			k.escape == '\\', /* emitEscapeCharacterLastError */
		); err != nil {
			return "", err
		}
	}

	return pattern, nil
}

// Pattern implements the RegexpCacheKey interface.
// The strategy for handling custom escape character
// is to convert all unescaped escape character into '\'.
// k.escape can either be empty or a single character.
func (k likeKey) Pattern() (string, error) {
	pattern, err := k.patternNoAnchor()
	if err != nil {
		return "", err
	}
	return anchorPattern(pattern, k.caseInsensitive), nil
}

type similarToKey struct {
	s      string
	escape rune
}

// Pattern implements the RegexpCacheKey interface.
func (k similarToKey) Pattern() (string, error) {
	pattern := similarEscapeCustomChar(k.s, k.escape, k.escape != 0)
	return anchorPattern(pattern, false), nil
}

// SimilarToEscape checks if 'unescaped' is SIMILAR TO 'pattern' using custom escape token 'escape'
// which must be either empty (which disables the escape mechanism) or a single unicode character.
func SimilarToEscape(ctx *Context, unescaped, pattern, escape string) (tree.Datum, error) {
	key, err := makeSimilarToKey(pattern, escape)
	if err != nil {
		return tree.DBoolFalse, err
	}
	return matchRegexpWithKey(ctx, tree.NewDString(unescaped), key)
}

// SimilarPattern converts a SQL regexp 'pattern' to a POSIX regexp 'pattern' using custom escape token 'escape'
// which must be either empty (which disables the escape mechanism) or a single unicode character.
func SimilarPattern(pattern, escape string) (tree.Datum, error) {
	key, err := makeSimilarToKey(pattern, escape)
	if err != nil {
		return nil, err
	}
	pattern, err = key.Pattern()
	if err != nil {
		return nil, err
	}
	return tree.NewDString(pattern), nil
}

// makeSimilarToKey makes a similarToKey using the given 'pattern' and 'escape'.
func makeSimilarToKey(pattern, escape string) (similarToKey, error) {
	var escapeRune rune
	var width int
	escapeRune, width = utf8.DecodeRuneInString(escape)
	if len(escape) > width {
		return similarToKey{}, pgerror.Newf(pgcode.InvalidEscapeSequence, "invalid escape string")
	}
	key := similarToKey{s: pattern, escape: escapeRune}
	return key, nil
}

type regexpKey struct {
	s               string
	caseInsensitive bool
}

// Pattern implements the RegexpCacheKey interface.
func (k regexpKey) Pattern() (string, error) {
	if k.caseInsensitive {
		return caseInsensitive(k.s), nil
	}
	return k.s, nil
}

// SimilarEscape converts a SQL:2008 regexp pattern to POSIX style, so it can
// be used by our regexp engine.
func SimilarEscape(pattern string) string {
	return similarEscapeCustomChar(pattern, '\\', true)
}

// similarEscapeCustomChar converts a SQL:2008 regexp pattern to POSIX style,
// so it can be used by our regexp engine. This version of the function allows
// for a custom escape character.
// 'isEscapeNonEmpty' signals whether 'escapeChar' should be treated as empty.
func similarEscapeCustomChar(pattern string, escapeChar rune, isEscapeNonEmpty bool) string {
	patternBuilder := make([]rune, 0, utf8.RuneCountInString(pattern))

	inCharClass := false
	afterEscape := false
	numQuotes := 0
	for _, c := range pattern {
		switch {
		case afterEscape:
			// For SUBSTRING patterns
			if c == '"' && !inCharClass {
				if numQuotes%2 == 0 {
					patternBuilder = append(patternBuilder, '(')
				} else {
					patternBuilder = append(patternBuilder, ')')
				}
				numQuotes++
			} else if c == escapeChar && len(string(escapeChar)) > 1 {
				// We encountered escaped escape unicode character represented by at least two bytes,
				// so we keep only its single occurrence and need not to prepend it by '\'.
				patternBuilder = append(patternBuilder, c)
			} else {
				patternBuilder = append(patternBuilder, '\\', c)
			}
			afterEscape = false
		case utf8.ValidRune(escapeChar) && c == escapeChar && isEscapeNonEmpty:
			// SQL99 escape character; do not immediately send to output
			afterEscape = true
		case inCharClass:
			if c == '\\' {
				patternBuilder = append(patternBuilder, '\\')
			}
			patternBuilder = append(patternBuilder, c)
			if c == ']' {
				inCharClass = false
			}
		case c == '[':
			patternBuilder = append(patternBuilder, c)
			inCharClass = true
		case c == '%':
			patternBuilder = append(patternBuilder, '.', '*')
		case c == '_':
			patternBuilder = append(patternBuilder, '.')
		case c == '(':
			// Convert to non-capturing parenthesis
			patternBuilder = append(patternBuilder, '(', '?', ':')
		case c == '\\', c == '.', c == '^', c == '$':
			// Escape these characters because they are NOT
			// metacharacters for SQL-style regexp
			patternBuilder = append(patternBuilder, '\\', c)
		default:
			patternBuilder = append(patternBuilder, c)
		}
	}

	return string(patternBuilder)
}

// caseInsensitive surrounds the transformed input string with
//
//	(?i: ... )
//
// which uses a non-capturing set of parens to turn a case sensitive
// regular expression pattern into a case insensitive regular
// expression pattern.
func caseInsensitive(pattern string) string {
	return fmt.Sprintf("(?i:%s)", pattern)
}

// anchorPattern surrounds the transformed input string with
//
//	^(?s: ... )$
//
// which requires some explanation.  We need "^" and "$" to force
// the pattern to match the entire input string as per SQL99 spec.
// The "(?:" and ")" are a non-capturing set of parens; we have to have
// parens in case the string contains "|", else the "^" and "$" will
// be bound into the first and last alternatives which is not what we
// want, and the parens must be non capturing because we don't want them
// to count when selecting output for SUBSTRING.
// "?s" turns on "dot all" mode meaning a dot will match any single character
// (without turning this mode on, the dot matches any single character except
// for line breaks).
func anchorPattern(pattern string, caseInsensitive bool) string {
	if caseInsensitive {
		return fmt.Sprintf("^(?si:%s)$", pattern)
	}
	return fmt.Sprintf("^(?s:%s)$", pattern)
}
