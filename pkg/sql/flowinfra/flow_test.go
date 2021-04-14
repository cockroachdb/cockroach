// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package flowinfra_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
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
	for _, vectorize := range []bool{true, false} {
		for _, distribute := range []bool{true, false} {
			b.Run(fmt.Sprintf("vectorize=%t/distribute=%t", vectorize, distribute), func(b *testing.B) {
				vectorizeMode := sessiondatapb.VectorizeOff
				if vectorize {
					vectorizeMode = sessiondatapb.VectorizeOn
				}
				for i := 0; i < b.N; i++ {
					// NB: planner cannot be reset and can only be used for
					// a single statement, so we create a new one on every
					// iteration.
					planner, cleanup := sql.NewInternalPlanner(
						"test",
						kv.NewTxn(ctx, s.DB(), s.NodeID()),
						security.RootUserName(),
						&sql.MemoryMetrics{},
						&execCfg,
						sessiondatapb.SessionData{VectorizeMode: vectorizeMode},
					)
					if err := dsp.Exec(
						ctx,
						planner,
						"SELECT k FROM b.test WHERE k=1",
						distribute,
					); err != nil {
						b.Fatal(err)
					}
					cleanup()
				}
			})
		}
	}
}
