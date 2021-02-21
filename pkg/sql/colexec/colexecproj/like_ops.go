// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecproj

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecprojsel"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// GetLikeProjectionOperator returns a projection operator which projects the
// result of the specified LIKE pattern, or NOT LIKE if the negate argument is
// true. The implementation varies depending on the complexity of the pattern.
func GetLikeProjectionOperator(
	allocator *colmem.Allocator,
	ctx *tree.EvalContext,
	input colexecbase.Operator,
	colIdx int,
	resultIdx int,
	pattern string,
	negate bool,
) (colexecbase.Operator, error) {
	likeOpType, pattern, err := colexecprojsel.GetLikeOperatorType(pattern, negate)
	if err != nil {
		return nil, err
	}
	pat := []byte(pattern)
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, types.Bool, resultIdx)
	base := projConstOpBase{
		OneInputNode: colexecbase.NewOneInputNode(input),
		allocator:    allocator,
		colIdx:       colIdx,
		outputIdx:    resultIdx,
	}
	switch likeOpType {
	case colexecprojsel.LikeConstant:
		return &projEQBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case colexecprojsel.LikeConstantNegate:
		return &projNEBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case colexecprojsel.LikeNeverMatch:
		// Use an empty not-prefix operator to get correct NULL behavior.
		return &projNotPrefixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        []byte{},
		}, nil
	case colexecprojsel.LikeAlwaysMatch:
		// Use an empty prefix operator to get correct NULL behavior.
		return &projPrefixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        []byte{},
		}, nil
	case colexecprojsel.LikeSuffix:
		return &projSuffixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case colexecprojsel.LikeSuffixNegate:
		return &projNotSuffixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case colexecprojsel.LikePrefix:
		return &projPrefixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case colexecprojsel.LikePrefixNegate:
		return &projNotPrefixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case colexecprojsel.LikeContains:
		return &projContainsBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case colexecprojsel.LikeContainsNegate:
		return &projNotContainsBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case colexecprojsel.LikeRegexp:
		re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
		if err != nil {
			return nil, err
		}
		return &projRegexpBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        re,
		}, nil
	case colexecprojsel.LikeRegexpNegate:
		re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
		if err != nil {
			return nil, err
		}
		return &projNotRegexpBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        re,
		}, nil
	default:
		return nil, errors.AssertionFailedf("unsupported like op type %d", likeOpType)
	}
}
