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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
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

	s, conn, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(b, "CREATE DATABASE b; CREATE TABLE b.test (k INT);")

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	dsp := execCfg.DistSQLPlanner
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		planner, cleanup := sql.NewInternalPlanner(
			"test",
			client.NewTxn(s.DB(), s.NodeID(), client.RootTxn),
			security.RootUser,
			&sql.MemoryMetrics{},
			&execCfg,
		)
		defer cleanup()
		for pb.Next() {
			if err := dsp.Exec(context.Background(), planner, "SELECT * FROM b.test"); err != nil {
				b.Fatal(err)
			}
		}
	})
}
