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

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// likeOpType is an enum that describes all of the different variants of LIKE
// that we support.
type likeOpType int

const (
	likeConstant likeOpType = iota + 1
	likeConstantNegate
	likeNeverMatch
	likeAlwaysMatch
	likeSuffix
	likeSuffixNegate
	likePrefix
	likePrefixNegate
	likeRegexp
	likeRegexpNegate
)

func getLikeOperatorType(pattern string, negate bool) (likeOpType, string, error) {
	if pattern == "" {
		if negate {
			return likeConstantNegate, "", nil
		}
		return likeConstant, "", nil
	}
	if pattern == "%" {
		if negate {
			return likeNeverMatch, "", nil
		}
		return likeAlwaysMatch, "", nil
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
				return likeConstantNegate, pattern, nil
			}
			return likeConstant, pattern, nil
		}
		if firstChar == '%' && !isWildcard(lastChar) {
			suffix := pattern[1:]
			if negate {
				return likeSuffixNegate, suffix, nil
			}
			return likeSuffix, suffix, nil
		}
		if lastChar == '%' && !isWildcard(firstChar) {
			prefix := pattern[:len(pattern)-1]
			if negate {
				return likePrefixNegate, prefix, nil
			}
			return likePrefix, prefix, nil
		}
	}
	// Default (slow) case: execute as a regular expression match.
	if negate {
		return likeRegexpNegate, pattern, nil
	}
	return likeRegexp, pattern, nil
}

// GetLikeOperator returns a selection operator which applies the specified LIKE
// pattern, or NOT LIKE if the negate argument is true. The implementation
// varies depending on the complexity of the pattern.
func GetLikeOperator(
	ctx *tree.EvalContext, input Operator, colIdx int, pattern string, negate bool,
) (Operator, error) {
	likeOpType, pattern, err := getLikeOperatorType(pattern, negate)
	if err != nil {
		return nil, err
	}
	pat := []byte(pattern)
	switch likeOpType {
	case likeConstant:
		return &selEQBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     pat,
		}, nil
	case likeConstantNegate:
		return &selNEBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     pat,
		}, nil
	case likeNeverMatch:
		return NewZeroOp(input), nil
	case likeAlwaysMatch:
		// Matches everything.
		// TODO(solon): Replace this with a NOT NULL operator.
		return NewNoop(input), nil
	case likeSuffix:
		return &selSuffixBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     pat,
		}, nil
	case likeSuffixNegate:
		return &selNotSuffixBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     pat,
		}, nil
	case likePrefix:
		return &selPrefixBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     pat,
		}, nil
	case likePrefixNegate:
		return &selNotPrefixBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     pat,
		}, nil
	case likeRegexp:
		re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
		if err != nil {
			return nil, err
		}
		return &selRegexpBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     re,
		}, nil
	case likeRegexpNegate:
		re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
		if err != nil {
			return nil, err
		}
		return &selNotRegexpBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     re,
		}, nil
	default:
		return nil, errors.AssertionFailedf("unsupported like op type %d", likeOpType)
	}
}

func isWildcard(c byte) bool {
	return c == '%' || c == '_'
}

// GetLikeProjectionOperator returns a projection operator which projects the
// result of the specified LIKE pattern, or NOT LIKE if the negate argument is
// true. The implementation varies depending on the complexity of the pattern.
func GetLikeProjectionOperator(
	ctx *tree.EvalContext, input Operator, colIdx int, resultIdx int, pattern string, negate bool,
) (Operator, error) {
	likeOpType, pattern, err := getLikeOperatorType(pattern, negate)
	if err != nil {
		return nil, err
	}
	pat := []byte(pattern)
	switch likeOpType {
	case likeConstant:
		return &projEQBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     pat,
			outputIdx:    resultIdx,
		}, nil
	case likeConstantNegate:
		return &projNEBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     pat,
			outputIdx:    resultIdx,
		}, nil
	case likeNeverMatch:
		return NewConstOp(input, types.Bool, false, resultIdx)
	case likeAlwaysMatch:
		// Matches everything.
		// TODO(solon): Replace this with a NOT NULL operator.
		return NewConstOp(input, types.Bool, true, resultIdx)
	case likeSuffix:
		return &projSuffixBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     pat,
			outputIdx:    resultIdx,
		}, nil
	case likeSuffixNegate:
		return &projNotSuffixBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     pat,
			outputIdx:    resultIdx,
		}, nil
	case likePrefix:
		return &projPrefixBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     pat,
			outputIdx:    resultIdx,
		}, nil
	case likePrefixNegate:
		return &projNotPrefixBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     pat,
			outputIdx:    resultIdx,
		}, nil
	case likeRegexp:
		re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
		if err != nil {
			return nil, err
		}
		return &projRegexpBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     re,
			outputIdx:    resultIdx,
		}, nil
	case likeRegexpNegate:
		re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
		if err != nil {
			return nil, err
		}
		return &projNotRegexpBytesBytesConstOp{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			constArg:     re,
			outputIdx:    resultIdx,
		}, nil
	default:
		return nil, errors.AssertionFailedf("unsupported like op type %d", likeOpType)
	}
}
