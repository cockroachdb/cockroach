// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexeccmp

import (
	"bytes"
	"strings"
)

// LikeOpType is an enum that describes all of the different variants of LIKE
// that we support.
type LikeOpType int

const (
	// LikeAlwaysMatch matches everything.
	LikeAlwaysMatch LikeOpType = iota + 1
	// LikeConstant is used when comparing against a constant string with no
	// wildcards.
	LikeConstant
	// LikeContains is used when comparing against a constant substring.
	LikeContains
	// LikePrefix is used when comparing against a constant prefix.
	LikePrefix
	// LikeRegexp is the default slow case when we need to fallback to RegExp
	// matching.
	LikeRegexp
	// LikeSkeleton is used when comparing against a "skeleton" string (of the
	// form '%foo%bar%' with any number of "skeleton words").
	LikeSkeleton
	// LikeSuffix is used when comparing against a constant suffix.
	LikeSuffix
)

// GetLikeOperatorType returns LikeOpType corresponding to the inputs.
//
// The second return parameter always contains a single []byte, unless
// "skeleton" LikeOpType is returned.
func GetLikeOperatorType(pattern string, caseInsensitive bool) (LikeOpType, [][]byte, error) {
	if pattern == "" {
		return LikeConstant, [][]byte{{}}, nil
	}
	if pattern == "%" {
		return LikeAlwaysMatch, [][]byte{{}}, nil
	}
	if caseInsensitive {
		pattern = strings.ToUpper(pattern)
	}
	hasEscape := strings.Contains(pattern, `\`)
	if !hasEscape && len(pattern) > 1 && !strings.ContainsAny(pattern[1:len(pattern)-1], "_%") {
		// There are no wildcards in the middle of the string as well as no
		// escape characters in the whole string, so we only need to use a
		// regular expression if both the first and last characters are
		// wildcards.
		//
		// The presence of the escape characters breaks the assumptions of the
		// optimized versions since we no longer could just use the string for a
		// direct match - we'd need to do some preprocessing here to remove the
		// escape characters.
		// TODO(yuzefovich): add that preprocessing (for example, `\\` needs to
		// be replaced with `\`).
		firstChar := pattern[0]
		lastChar := pattern[len(pattern)-1]
		if !isWildcard(firstChar) && !isWildcard(lastChar) {
			// No wildcards, so this is just an exact string match.
			return LikeConstant, [][]byte{[]byte(pattern)}, nil
		}
		if firstChar == '%' && !isWildcard(lastChar) {
			suffix := pattern[1:]
			return LikeSuffix, [][]byte{[]byte(suffix)}, nil
		}
		if lastChar == '%' && !isWildcard(firstChar) {
			prefix := pattern[:len(pattern)-1]
			return LikePrefix, [][]byte{[]byte(prefix)}, nil
		}
		if firstChar == '%' && lastChar == '%' {
			contains := pattern[1 : len(pattern)-1]
			return LikeContains, [][]byte{[]byte(contains)}, nil
		}
	}
	// Optimized handling of "skeleton" patterns like '%foo%bar%' with any
	// number of "skeleton" words. The conditions are such that the pattern
	// starts and ends with '%' character as well as at least one '%' character
	// is present in the middle of the pattern while escape and '_' characters
	// are not present at all.
	if !hasEscape && len(pattern) >= 5 && pattern[0] == '%' && pattern[len(pattern)-1] == '%' &&
		!strings.Contains(pattern, "_") && strings.Contains(pattern[1:len(pattern)-1], "%") {
		var skeleton [][]byte
		pat := []byte(pattern)
		pat = pat[1:]
		for len(pat) > 0 {
			idx := bytes.Index(pat, []byte{'%'})
			skeleton = append(skeleton, pat[:idx])
			pat = pat[idx+1:]
		}
		return LikeSkeleton, skeleton, nil
	}
	// Default (slow) case: execute as a regular expression match.
	return LikeRegexp, [][]byte{[]byte(pattern)}, nil
}

func isWildcard(c byte) bool {
	return c == '%' || c == '_'
}
