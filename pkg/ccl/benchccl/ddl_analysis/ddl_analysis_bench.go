package ddl_analysis

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	bench "github.com/cockroachdb/cockroach/pkg/bench/ddl_analysis"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// RunRoundTripBenchmarkMultiRegion sets up a multi-region db run the RoundTripBenchTestCase test cases
// and counts how many round trips the Stmt specified by the test case performs.
func RunRoundTripBenchmarkMultiRegion(b *testing.B, tests []bench.RoundTripBenchTestCase) {
	skip.UnderMetamorphic(b, "changes the RTTs")

	for _, tc := range tests {
		b.Run(tc.Name, func(b *testing.B) {
			defer log.Scope(b).Close(b)
			var stmtToKvBatchRequests sync.Map

			beforePlan := func(trace tracing.Recording, stmt string) {
				if _, ok := stmtToKvBatchRequests.Load(stmt); ok {
					stmtToKvBatchRequests.Store(stmt, trace)
				}
			}

			cluster, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(b, 3, base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					WithStatementTrace: beforePlan,
				},
			})
			defer cleanup()

			sqlConn := sqlutils.MakeSQLRunner(cluster.Conns[0])

			defer cluster.Stopper().Stop(context.Background())

			bench.ExecuteRoundTripTest(b, sqlConn, &stmtToKvBatchRequests, tc)
		})
	}
}
