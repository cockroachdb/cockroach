// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestTxnTimePrecisionRange checks that the precision argument of
// current_time, current_timestamp, localtime, and localtimestamp is
// range-checked before it is narrowed to int32, so precisions of 2^32 + k
// are rejected instead of being silently accepted as k. See #176245.
func TestTxnTimePrecisionRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	for _, name := range []string{"current_time", "current_timestamp", "localtime", "localtimestamp"} {
		_, overloads := builtinsregistry.GetBuiltinProperties(name)
		require.NotEmpty(t, overloads, "no overloads for %s", name)
		for _, overload := range overloads {
			if overload.Types.Length() != 1 || overload.Types.GetAt(0).Family() != types.IntFamily {
				continue
			}
			fn, ok := overload.Fn.(eval.FnOverload)
			require.True(t, ok, "%s precision overload has unexpected Fn type %T", name, overload.Fn)
			for _, prec := range []int64{0, 1, 6} {
				_, err := fn(context.Background(), evalCtx, tree.Datums{tree.NewDInt(tree.DInt(prec))})
				require.NoError(t, err, "%s(%d) should be accepted", name, prec)
			}
			for _, prec := range []int64{-1, 7, 4294967297, 4294967303} {
				_, err := fn(context.Background(), evalCtx, tree.Datums{tree.NewDInt(tree.DInt(prec))})
				require.Error(t, err, "%s(%d) should be rejected", name, prec)
				require.Contains(t, err.Error(), fmt.Sprintf("precision %d out of range", prec))
			}
		}
	}
}
