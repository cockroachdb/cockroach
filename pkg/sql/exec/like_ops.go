// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package exec

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// GetLikeOperator returns a selection operator which applies the specified LIKE
// pattern. The implementation varies depending on the complexity of the
// pattern.
func GetLikeOperator(
	ctx *tree.EvalContext, input Operator, colIdx int, pattern string,
) (Operator, error) {
	if pattern == "" {
		return &selEQBytesBytesConstOp{
			input:    input,
			colIdx:   colIdx,
			constArg: []byte{},
		}, nil
	}
	if pattern == "%" {
		// Matches everything.
		return NewNoop(input), nil
	}
	if len(pattern) > 1 && !strings.ContainsAny(pattern[1:len(pattern)-1], "_%") {
		// Special cases for patterns which are just a prefix or suffix.
		if pattern[0] == '%' {
			return &selSuffixBytesBytesConstOp{
				input:    input,
				colIdx:   colIdx,
				constArg: []byte(pattern[1:]),
			}, nil
		}
		if pattern[len(pattern)-1] == '%' {
			return &selPrefixBytesBytesConstOp{
				input:    input,
				colIdx:   colIdx,
				constArg: []byte(pattern[:len(pattern)-1]),
			}, nil
		}
	}
	// Default (slow) case: execute as a regular expression match.
	re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
	if err != nil {
		return nil, err
	}
	return &selRegexpBytesBytesConstOp{
		input:    input,
		colIdx:   colIdx,
		constArg: re,
	}, nil
}
