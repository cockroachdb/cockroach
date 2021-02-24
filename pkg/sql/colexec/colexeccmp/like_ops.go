// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexeccmp

import "strings"

// LikeOpType is an enum that describes all of the different variants of LIKE
// that we support.
type LikeOpType int

const (
	// LikeConstant is used when comparing against a constant string with no
	// wildcards.
	LikeConstant LikeOpType = iota + 1
	// LikeConstantNegate is used when comparing against a constant string with
	// no wildcards, and the result is negated.
	LikeConstantNegate
	// LikeNeverMatch doesn't match anything.
	LikeNeverMatch
	// LikeAlwaysMatch matches everything.
	LikeAlwaysMatch
	// LikeSuffix is used when comparing against a constant suffix.
	LikeSuffix
	// LikeSuffixNegate is used when comparing against a constant suffix, and
	// the result is negated.
	LikeSuffixNegate
	// LikePrefix is used when comparing against a constant prefix.
	LikePrefix
	// LikePrefixNegate is used when comparing against a constant prefix, and
	// the result is negated.
	LikePrefixNegate
	// LikeContains is used when comparing against a constant substring.
	LikeContains
	// LikeContainsNegate is used when comparing against a constant substring,
	// and the result is negated.
	LikeContainsNegate
	// LikeRegexp is the default slow case when we need to fallback to RegExp
	// matching.
	LikeRegexp
	// LikeRegexpNegate is the default slow case when we need to fallback to
	// RegExp matching, but the result is negated.
	LikeRegexpNegate
)

// GetLikeOperatorType returns LikeOpType corresponding to the inputs.
func GetLikeOperatorType(pattern string, negate bool) (LikeOpType, string, error) {
	if pattern == "" {
		if negate {
			return LikeConstantNegate, "", nil
		}
		return LikeConstant, "", nil
	}
	if pattern == "%" {
		if negate {
			return LikeNeverMatch, "", nil
		}
		return LikeAlwaysMatch, "", nil
	}
	if len(pattern) > 1 && !strings.ContainsAny(pattern[1:len(pattern)-1], "_%") {
		// There are no wildcards in the middle of the string, so we only need to
		// use a regular expression if both the first and last characters are
		// wildcards.
		firstChar := pattern[0]
		lastChar := pattern[len(pattern)-1]
		if !isWildcard(firstChar) && !isWildcard(lastChar) {
			// No wildcards, so this is just an exact string match.
			if negate {
				return LikeConstantNegate, pattern, nil
			}
			return LikeConstant, pattern, nil
		}
		if firstChar == '%' && !isWildcard(lastChar) {
			suffix := pattern[1:]
			if negate {
				return LikeSuffixNegate, suffix, nil
			}
			return LikeSuffix, suffix, nil
		}
		if lastChar == '%' && !isWildcard(firstChar) {
			prefix := pattern[:len(pattern)-1]
			if negate {
				return LikePrefixNegate, prefix, nil
			}
			return LikePrefix, prefix, nil
		}
		if firstChar == '%' && lastChar == '%' {
			contains := pattern[1 : len(pattern)-1]
			if negate {
				return LikeContainsNegate, contains, nil
			}
			return LikeContains, contains, nil
		}
	}
	// Default (slow) case: execute as a regular expression match.
	if negate {
		return LikeRegexpNegate, pattern, nil
	}
	return LikeRegexp, pattern, nil
}

func isWildcard(c byte) bool {
	return c == '%' || c == '_'
}
