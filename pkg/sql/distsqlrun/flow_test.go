// Copyright 2018 The Cockroach Authors.
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

package distsqlrun_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// BenchmarkFlowSetup sets up GOMAXPROCS goroutines where each is setting up
// a flow for a scan that is dominated by the setup cost.
func BenchmarkFlowSetup(b *testing.B) {
	defer leaktest.AfterTest(b)()
	logScope := log.Scope(b)
	defer logScope.Close(b)
	ctx := context.Background()

	s, conn, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(b, "CREATE DATABASE b; CREATE TABLE b.test (k INT);")

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	dsp := execCfg.DistSQLPlanner
	for _, distribute := range []bool{true, false} {
		b.Run(fmt.Sprintf("distribute=%t", distribute), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				planner, cleanup := sql.NewInternalPlanner(
					"test",
					client.NewTxn(ctx, s.DB(), s.NodeID(), client.RootTxn),
					security.RootUser,
					&sql.MemoryMetrics{},
					&execCfg,
				)
				defer cleanup()
				for pb.Next() {
					if err := dsp.Exec(
						ctx,
						planner,
						"SELECT k FROM b.test WHERE k=1",
						distribute,
					); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// TestNewEvalContext verifies that NewEvalContext produces copies of
// EvalContext that are not susceptible to data races.
// Note: the test relies on "parent" EvalContext having non-nil
// iVarContainerStack.
func TestNewEvalContext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := distsqlrun.FlowCtx{
		Settings: st,
		EvalCtx:  &evalCtx,
	}

	const numRuns = 10
	const numGoRoutines = 2

	evalCtxs := make([]*tree.EvalContext, numGoRoutines)
	for i := range evalCtxs {
		evalCtxs[i] = flowCtx.NewEvalCtx()
		defer evalCtxs[i].Stop(context.Background())
	}

	// An arbitrary IndexedVarContainer to push onto the stack.
	ivc := sqlbase.CheckHelper{}

	var wg sync.WaitGroup
	wg.Add(numGoRoutines)
	for i := 0; i < numGoRoutines; i++ {
		go func(evalCtx *tree.EvalContext) {
			for r := 0; r < numRuns; r++ {
				evalCtx.PushIVarContainer(&ivc)
				evalCtx.PopIVarContainer()
			}
			wg.Done()
		}(evalCtxs[i])
	}
	wg.Wait()
}
