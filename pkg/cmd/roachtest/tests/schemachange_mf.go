package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/modular"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

func registerIndexTpccModularTest(r registry.Registry) {
	clusterSpec := r.MakeClusterSpec(5, spec.WorkloadNode())
	length := time.Minute * 15
	warehouses := 100
	testSpec := registry.TestSpec{
		Name:             fmt.Sprintf("schemachange/indexschemachange/modular/index/tpcc/w=%d", warehouses),
		Owner:            registry.OwnerSQLFoundations,
		Benchmark:        true,
		Cluster:          clusterSpec,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.DefaultLeases,
		Timeout:          length * 3,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			RunModularSchemaChange(ctx, t, c)
		},
	}
	r.Add(testSpec)
}

func initWorkload(ctx context.Context, t test.Test, c cluster.Cluster, opts tpccOptions) {
	extraArgs := opts.ExtraSetupArgs
	cmd := roachtestutil.NewCommand("%s workload init %s", test.DefaultCockroachPath, opts.getWorkloadCmd()).
		MaybeFlag(opts.DB != "", "db", opts.DB).
		Flag("warehouses", opts.Warehouses).
		Arg("%s", extraArgs).
		Arg("%s", "{pgurl:1}")

	c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd.String())
}

func RunModularSchemaChange(ctx context.Context, t test.Test, c cluster.Cluster) {
	mft := modular.NewTest(ctx, t.L(), c, c.CRDBNodes())
	mft.SetMaxConcurrency(2)

	// setup workload, change Cluster Setting
	mft.Setup("workload setup", func(ctx context.Context, logger *logger.Logger, helper *modular.Helper) error {
		return nil
	})
	mft.Setup("cluster settings", func(ctx context.Context, logger *logger.Logger, helper *modular.Helper) error {
		return runAndLogStmts(ctx, t, c, "cluster-setting", []string{
			`SET CLUSTER SETTING bulkio.index_backfill.ingest_concurrency = 2;`,
		})
	})
	mft.Background("run tpcc workload in bg", func(ctx context.Context, l *logger.Logger, helper *modular.Helper) error {
		return nil
	})

	stage := mft.NewStage("addindex")
	stage.Execute("create unique index on tpcc.order", func(ctx context.Context, l *logger.Logger, helper *modular.Helper) error {
		return runAndLogStmts(ctx, t, c, "addindex-unique-tpcc.order", []string{
			`CREATE UNIQUE INDEX ON tpcc.order (o_entry_d, o_w_id, o_d_id, o_carrier_id, o_id);`,
		})
	})

	stage.Execute("create index on tpcc.order", func(ctx context.Context, l *logger.Logger, helper *modular.Helper) error {
		return runAndLogStmts(ctx, t, c, "addindex-tpcc.order", []string{
			`CREATE INDEX ON tpcc.order (o_carrier_id);`,
		})
	})

	stage.Execute("create index on tpcc.customer", func(ctx context.Context, l *logger.Logger, helper *modular.Helper) error {
		return runAndLogStmts(ctx, t, c, "addindex-tpcc.customer", []string{
			`CREATE INDEX ON tpcc.customer (c_last, c_first);`,
		})
	})

	mft.AfterTest("validation", func(ctx context.Context, l *logger.Logger, helper *modular.Helper) error {
		return nil
	})

	t.L().Printf(mft.Plan().String())
}
