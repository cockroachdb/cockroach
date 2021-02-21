// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecsel

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecprojsel"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// GetLikeOperator returns a selection operator which applies the specified LIKE
// pattern, or NOT LIKE if the negate argument is true. The implementation
// varies depending on the complexity of the pattern.
func GetLikeOperator(
	ctx *tree.EvalContext, input colexecbase.Operator, colIdx int, pattern string, negate bool,
) (colexecbase.Operator, error) {
	likeOpType, pattern, err := colexecprojsel.GetLikeOperatorType(pattern, negate)
	if err != nil {
		return nil, err
	}
	pat := []byte(pattern)
	base := selConstOpBase{
		OneInputNode: colexecbase.NewOneInputNode(input),
		colIdx:       colIdx,
	}
	switch likeOpType {
	case colexecprojsel.LikeConstant:
		return &selEQBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case colexecprojsel.LikeConstantNegate:
		return &selNEBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case colexecprojsel.LikeNeverMatch:
		// Use an empty not-prefix operator to get correct NULL behavior.
		return &selNotPrefixBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       []byte{},
		}, nil
	case colexecprojsel.LikeAlwaysMatch:
		// Use an empty prefix operator to get correct NULL behavior.
		return &selPrefixBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       []byte{},
		}, nil
	case colexecprojsel.LikeSuffix:
		return &selSuffixBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case colexecprojsel.LikeSuffixNegate:
		return &selNotSuffixBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case colexecprojsel.LikePrefix:
		return &selPrefixBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case colexecprojsel.LikePrefixNegate:
		return &selNotPrefixBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case colexecprojsel.LikeContains:
		return &selContainsBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case colexecprojsel.LikeContainsNegate:
		return &selNotContainsBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case colexecprojsel.LikeRegexp:
		re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
		if err != nil {
			return nil, err
		}
		return &selRegexpBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       re,
		}, nil
	case colexecprojsel.LikeRegexpNegate:
		re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
		if err != nil {
			return nil, err
		}
		return &selNotRegexpBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       re,
		}, nil
	default:
		return nil, errors.AssertionFailedf("unsupported like op type %d", likeOpType)
	}
}
