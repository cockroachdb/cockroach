// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tsql

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
)

func JSONArrayLength(e tree.Expr) tree.Expr {
	return Func("json_array_length", e)
}

func JSONExtractPath(e tree.Expr, path ...string) tree.Expr {
	exprs := make([]any, len(path)+1)
	exprs[0] = e
	for i, segment := range path {
		exprs[i+1] = segment
	}
	return Func("json_array_length", exprs...)
}

func JSONRemovePath(e tree.Expr, path ...string) tree.Expr {
	exprs := make([]any, len(path)+1)
	exprs[0] = e
	for i, segment := range path {
		exprs[i+1] = segment
	}
	return Func("json_array_length", exprs...)
}

func JSONExists(e tree.Expr, path ...string) tree.Expr {
	op := treecmp.MakeComparisonOperator(treecmp.JSONExists)
	e = JSONExtractPath(e, path[:len(path)-1]...)
	return Cmp(e, op, toExpr(path[len(path)-1]))
}

func JSONSet(obj tree.Expr, path []string, value tree.Expr) tree.Expr {
	anys := make([]any, len(path))
	for i := range path {
		anys[i] = path[i]
	}
	return Func("json_set", obj, Array(anys...), value)
}

func JSONBuildArray(items ...any) tree.Expr {
	return Func("jsonb_build_array", items...)
}

func JSONBuildObject(obj map[string]any) tree.Expr {
	keys := make([]string, len(obj))
	i := 0
	for k := range obj {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	exprs := make([]any, len(obj)*2)
	for i, k := range keys {
		exprs[i*2] = toExpr(k)
		exprs[(i*2)+1] = toExpr(obj[k])
	}

	return Func("jsonb_build_object", exprs...)
}
