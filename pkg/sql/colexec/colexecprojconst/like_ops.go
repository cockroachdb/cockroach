// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecprojconst

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexeccmp"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

var ilikeConstantPatternErr = errors.New("ILIKE and NOT ILIKE aren't supported with a constant pattern")

// GetLikeProjectionOperator returns a projection operator which projects the
// result of the specified LIKE pattern, or NOT LIKE if the negate argument is
// true. The implementation varies depending on the complexity of the pattern.
func GetLikeProjectionOperator(
	allocator *colmem.Allocator,
	evalCtx *eval.Context,
	input colexecop.Operator,
	colIdx int,
	resultIdx int,
	pattern string,
	negate bool,
	caseInsensitive bool,
) (colexecop.Operator, error) {
	likeOpType, patterns, err := colexeccmp.GetLikeOperatorType(pattern, caseInsensitive)
	if err != nil {
		return nil, err
	}
	pat := patterns[0]
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, types.Bool, resultIdx)
	base := projConstOpBase{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		allocator:      allocator,
		colIdx:         colIdx,
		outputIdx:      resultIdx,
	}
	switch likeOpType {
	case colexeccmp.LikeAlwaysMatch:
		// Use an empty prefix operator to get correct NULL behavior. We don't
		// need to pay attention to the case sensitivity here since the pattern
		// will always match anyway.
		return &projPrefixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        []byte{},
			negate:          negate,
		}, nil
	case colexeccmp.LikeConstant:
		if caseInsensitive {
			// We don't have an equivalent projection operator that would
			// convert the argument to capital letters, so for now we fall back
			// to the default comparison operator.
			return nil, ilikeConstantPatternErr
		}
		if negate {
			return &projNEBytesBytesConstOp{
				projConstOpBase: base,
				constArg:        pat,
			}, nil
		}
		return &projEQBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case colexeccmp.LikeContains:
		return &projContainsBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
			negate:          negate,
			caseInsensitive: caseInsensitive,
		}, nil
	case colexeccmp.LikePrefix:
		return &projPrefixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
			negate:          negate,
			caseInsensitive: caseInsensitive,
		}, nil
	case colexeccmp.LikeRegexp:
		re, err := eval.ConvertLikeToRegexp(evalCtx, string(patterns[0]), caseInsensitive, '\\')
		if err != nil {
			return nil, err
		}
		return &projRegexpBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        re,
			negate:          negate,
		}, nil
	case colexeccmp.LikeSkeleton:
		return &projSkeletonBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        patterns,
			negate:          negate,
			caseInsensitive: caseInsensitive,
		}, nil
	case colexeccmp.LikeSuffix:
		return &projSuffixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
			negate:          negate,
			caseInsensitive: caseInsensitive,
		}, nil
	default:
		return nil, errors.AssertionFailedf("unsupported like op type %d", likeOpType)
	}
}
