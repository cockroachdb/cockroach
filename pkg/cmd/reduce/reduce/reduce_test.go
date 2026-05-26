// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package reduce_test

import (
	"context"
	"go/parser"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/reduce/reduce"
)

func TestReduceGo(t *testing.T) {
	reduce.Walk(t, "testdata", nil /* filter */, isInterestingGo, reduce.ModeInteresting,
		nil /* chunkReducer */, goPasses)
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
	return func(ctx context.Context, f string) (bool, func()) {
		_, err := parser.ParseExpr(f)
		if err == nil {
			return false, nil
		}
		return strings.Contains(err.Error(), contains), nil
	}
}
