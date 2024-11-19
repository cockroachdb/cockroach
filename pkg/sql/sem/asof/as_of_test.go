// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package asof_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func BenchmarkDatumToHLC(b *testing.B) {
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	stmtTimestamp := timeutil.Now()
	usage := asof.AsOf

	for _, tc := range []struct {
		name string
		d    tree.Datum
	}{
		{name: "stringTimestamp", d: tree.NewDString("2024-04-09 12:00:00.000000")},
		{name: "stringDecimal", d: tree.NewDString("1580361670629466905.0000000001")},
		{name: "stringInterval", d: tree.NewDString("-5s")},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var err error
			for i := 0; i < b.N; i++ {
				_, err = asof.DatumToHLC(&evalCtx, stmtTimestamp, tc.d, usage)
			}
			require.NoError(b, err)
		})
	}
}
