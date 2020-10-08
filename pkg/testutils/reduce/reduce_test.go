// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package reduce_test

import (
	"context"
	"go/parser"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/reduce"
)

func TestReduceGo(t *testing.T) {
	reduce.Walk(t, "testdata", nil /* filter */, isInterestingGo, reduce.ModeInteresting, goPasses)
}

var (
	goPasses = []reduce.Pass{
		removeLine,
		simplifyConsts,
	}
	removeLine = reduce.MakeIntPass("remove line", func(s string, i int) (string, bool, error) {
		sp := strings.Split(s, "\n")
		if i >= len(sp) {
			return "", false, nil
		}
		out := strings.Join(append(sp[:i], sp[i+1:]...), "\n")
		return out, true, nil
	})
	simplifyConstsRE = regexp.MustCompile(`[a-z0-9][a-z0-9]+`)
	simplifyConsts   = reduce.MakeIntPass("simplify consts", func(s string, i int) (string, bool, error) {
		out := simplifyConstsRE.ReplaceAllStringFunc(s, func(found string) string {
			i--
			if i == -1 {
				return found[:1]
			}
			return found
		})
		return out, i < 0, nil
	})
)

func isInterestingGo(contains string) reduce.InterestingFn {
	return func(ctx context.Context, f reduce.File) bool {
		_, err := parser.ParseExpr(string(f))
		if err == nil {
			return false
		}
		return strings.Contains(err.Error(), contains)
	}
}
