package ddl_analysisccl

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	bench "github.com/cockroachdb/cockroach/pkg/bench/ddl_analysis"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

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

			params := base.TestClusterArgs{
				ServerArgsPerNode: make(map[int]base.TestServerArgs),
			}

			localities := [][]string{
				{"us-east-1", "us-east-1a"},
				{"us-central-1", "us-central-1a"},
				{"us-west-1", "us-west-1a"},
			}
			for i := 0; i < len(localities); i++ {
				tierInfo := localities[i]

				params.ServerArgsPerNode[i] = base.TestServerArgs{
					UseDatabase: "bench",
					Knobs: base.TestingKnobs{
						SQLExecutor: &sql.ExecutorTestingKnobs{
							WithStatementTrace: beforePlan,
						},
					},
					Locality: roachpb.Locality{Tiers: []roachpb.Tier{
						{"region", tierInfo[0]},
						{"zone", tierInfo[1]},
					}},
				}
			}

			cluster := testcluster.StartTestCluster(b, len(localities), params)
			sql := sqlutils.MakeSQLRunner(cluster.Conns[0])

			defer cluster.Stopper().Stop(context.Background())

			bench.ExecuteRoundTripTest(b, sql, &stmtToKvBatchRequests, tc)
		})
	}
}
