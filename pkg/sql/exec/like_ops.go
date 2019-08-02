// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// GetLikeOperator returns a selection operator which applies the specified LIKE
// pattern, or NOT LIKE if the negate argument is true. The implementation
// varies depending on the complexity of the pattern.
func GetLikeOperator(
	ctx *tree.EvalContext, input Operator, colIdx int, pattern string, negate bool,
) (Operator, error) {
	if pattern == "" {
		if negate {
			return &selNEBytesBytesConstOp{
				OneInputNode: NewOneInputNode(input),
				colIdx:       colIdx,
				constArg:     []byte{},
			}, nil
		}
		return &selEQBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     []byte{},
		}, nil
	}
	if pattern == "%" {
		if negate {
			return NewZeroOp(input), nil
		}
		// Matches everything.
		// TODO(solon): Replace this with a NOT NULL operator.
		return NewNoop(input), nil
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
				return &selNEBytesBytesConstOp{
					OneInputNode: NewOneInputNode(input),
					colIdx:       colIdx,
					constArg:     []byte(pattern),
				}, nil
			}
			return &selEQBytesBytesConstOp{
				OneInputNode: NewOneInputNode(input),
				colIdx:       colIdx,
				constArg:     []byte(pattern),
			}, nil
		}
		if firstChar == '%' && !isWildcard(lastChar) {
			suffix := []byte(pattern[1:])
			if negate {
				return &selNotSuffixBytesBytesConstOp{
					OneInputNode: NewOneInputNode(input),
					colIdx:       colIdx,
					constArg:     suffix,
				}, nil
			}
			return &selSuffixBytesBytesConstOp{
				OneInputNode: NewOneInputNode(input),
				colIdx:       colIdx,
				constArg:     suffix,
			}, nil
		}
		if lastChar == '%' && !isWildcard(firstChar) {
			prefix := []byte(pattern[:len(pattern)-1])
			if negate {
				return &selNotPrefixBytesBytesConstOp{
					OneInputNode: NewOneInputNode(input),
					colIdx:       colIdx,
					constArg:     prefix,
				}, nil
			}
			return &selPrefixBytesBytesConstOp{
				OneInputNode: NewOneInputNode(input),
				colIdx:       colIdx,
				constArg:     prefix,
			}, nil
		}
	}
	// Default (slow) case: execute as a regular expression match.
	re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
	if err != nil {
		return nil, err
	}
	if negate {
		return &selNotRegexpBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     re,
		}, nil
	}
	return &selRegexpBytesBytesConstOp{
		OneInputNode: NewOneInputNode(input),
		colIdx:       colIdx,
		constArg:     re,
	}, nil
}

func isWildcard(c byte) bool {
	return c == '%' || c == '_'
}
