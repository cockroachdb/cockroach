// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package flowinfra_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// BenchmarkFlowSetup sets up a flow for a scan that is dominated by the setup
// cost.
func BenchmarkFlowSetup(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	s, conn, _ := serverutils.StartServer(b, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(b, "CREATE DATABASE b; CREATE TABLE b.test (k INT);")

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	dsp := execCfg.DistSQLPlanner
	stmt, err := parser.ParseOne("SELECT k FROM b.test WHERE k=1")
	if err != nil {
		b.Fatal(err)
	}
	for _, vectorize := range []bool{true, false} {
		for _, distribute := range []bool{true, false} {
			b.Run(fmt.Sprintf("vectorize=%t/distribute=%t", vectorize, distribute), func(b *testing.B) {
				vectorizeMode := sessiondatapb.VectorizeOff
				if vectorize {
					vectorizeMode = sessiondatapb.VectorizeOn
				}
				sd := sql.NewInternalSessionData(ctx, execCfg.Settings, "test")
				sd.VectorizeMode = vectorizeMode
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// NB: planner cannot be reset and can only be used for
					// a single statement, so we create a new one on every
					// iteration.
					planner, cleanup := sql.NewInternalPlanner(
						"test",
						kv.NewTxn(ctx, s.DB(), s.NodeID()),
						username.NodeUserName(),
						&sql.MemoryMetrics{},
						&execCfg,
						sd,
					)
					b.StartTimer()
					err := dsp.Exec(
						ctx,
						planner,
						stmt,
						distribute,
					)
					b.StopTimer()
					if err != nil {
						b.Fatal(err)
					}
					cleanup()
				}
			})
		}
	}
}

// BenchmarkFlowRenderSetup sets up a flow for a scan that is dominated by the
// setup cost of many render expressions.
func BenchmarkFlowRenderSetup(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	s, conn, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(conn)
	var sb strings.Builder
	sb.WriteString("CREATE DATABASE b;")
	sb.WriteString("CREATE TABLE b.test (k INT")
	const numCols = 500
	for i := 0; i < numCols; i++ {
		sb.WriteString(fmt.Sprintf(", col%d STRING", i))
	}
	sb.WriteString(");")
	r.Exec(b, sb.String())

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	dsp := execCfg.DistSQLPlanner

	sb.Reset()
	sb.WriteString("SELECT k")
	const numRenders = 500
	for i := 0; i < numRenders; i++ {
		sb.WriteString(fmt.Sprintf(", col%d || 'foo'", i))
	}
	sb.WriteString("FROM b.test")
	stmt, err := parser.ParseOne(sb.String())

	if err != nil {
		b.Fatal(err)
	}
	for _, vectorize := range []bool{true, false} {
		for _, distribute := range []bool{true, false} {
			b.Run(fmt.Sprintf("vectorize=%t/distribute=%t", vectorize, distribute), func(b *testing.B) {
				vectorizeMode := sessiondatapb.VectorizeOff
				if vectorize {
					vectorizeMode = sessiondatapb.VectorizeOn
				}
				sd := sql.NewInternalSessionData(ctx, execCfg.Settings, "test")
				sd.VectorizeMode = vectorizeMode
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// NB: planner cannot be reset and can only be used for
					// a single statement, so we create a new one on every
					// iteration.
					planner, cleanup := sql.NewInternalPlanner(
						"test",
						kv.NewTxn(ctx, s.DB(), s.NodeID()),
						username.NodeUserName(),
						&sql.MemoryMetrics{},
						&execCfg,
						sd,
					)
					b.StartTimer()
					err := dsp.Exec(
						ctx,
						planner,
						stmt,
						distribute,
					)
					b.StopTimer()
					if err != nil {
						b.Fatal(err)
					}
					cleanup()
				}
			})
		}
	}
}
