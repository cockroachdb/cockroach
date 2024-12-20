// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workloadccl_test

import (
	"context"
	"fmt"
	"math"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/workloadccl"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

const fixtureTestGenRows = 10

type fixtureTestGen struct {
	flags workload.Flags
	val   string
	empty string
	// tableAutoStatsEnabled, if set, enables auto stats collection at the table
	// level.
	tableAutoStatsEnabled bool
}

func makeTestWorkload() workload.Flagser {
	g := &fixtureTestGen{}
	g.flags.FlagSet = pflag.NewFlagSet(`fx`, pflag.ContinueOnError)
	g.flags.StringVar(&g.val, `val`, `default`, `The value for each row`)
	g.flags.StringVar(&g.empty, `empty`, ``, `An empty flag`)
	return g
}

var fixtureTestMeta = workload.Meta{
	Name:    `fixture`,
	Version: `0.0.1`,
	New: func() workload.Generator {
		return makeTestWorkload()
	},
}

func init() {
	workload.Register(fixtureTestMeta)
}

func (fixtureTestGen) Meta() workload.Meta     { return fixtureTestMeta }
func (g fixtureTestGen) Flags() workload.Flags { return g.flags }
func (g fixtureTestGen) Tables() []workload.Table {
	schema := `(key INT PRIMARY KEY, value INT)`
	if g.tableAutoStatsEnabled {
		schema += ` WITH (sql_stats_automatic_collection_enabled = true)`
	}
	return []workload.Table{{
		Name:   `fx`,
		Schema: schema,
		InitialRows: workload.Tuples(
			fixtureTestGenRows,
			func(rowIdx int) []interface{} {
				return []interface{}{rowIdx, g.val}
			},
		),
		Stats: []workload.JSONStatistic{
			// Use stats that *don't* match reality, so we can test that these
			// stats were injected and not calculated by CREATE STATISTICS.
			workload.MakeStat([]string{"key"}, 100, 100, 0),
			workload.MakeStat([]string{"value"}, 100, 1, 5),
		},
	}}
}

func TestFixture(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// This test is brittle and requires manual intervention to run.
	// COCKROACH_FIXTURE_TEST_STORAGE_PROVIDER must be specified.
	// Depending on the cloud provider you may need to change authentication parameters.
	// In addition, the bucket name defaults to "fixture-test" and you must ensure that this
	// bucket exists in the cloud provider.
	// Also note, that after the test runs, it leaves test fixtures around, so, please clean
	// those up manually.
	storageProvider := envutil.EnvOrDefaultString("COCKROACH_FIXTURE_TEST_STORAGE_PROVIDER", "")
	authParams := envutil.EnvOrDefaultString("COCKROACH_FIXTURE_TEST_AUTH_PARAMS", "AUTH=implicit")
	bucket := envutil.EnvOrDefaultString("COCKROACH_FIXTURE_TEST_BUCKET", "fixture-test")

	if storageProvider == "" {
		skip.IgnoreLint(t, "COCKROACH_FIXTURE_TEST_STORAGE_PROVIDER env var must be set")
	}

	config := workloadccl.FixtureConfig{
		StorageProvider: storageProvider,
		AuthParams:      authParams,
		Bucket:          bucket,
		Basename:        fmt.Sprintf(`TestFixture-%d`, timeutil.Now().UnixNano()),
		CSVServerURL:    "",
		TableStats:      false,
	}
	es, err := workloadccl.GetStorage(ctx, config)
	if err != nil {
		t.Fatalf(`%+v`, err)
	}
	defer func() { _ = es.Close() }()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	gen := makeTestWorkload()
	flag := fmt.Sprintf(`val=%d`, timeutil.Now().UnixNano())
	if err := gen.Flags().Parse([]string{"--" + flag}); err != nil {
		t.Fatalf(`%+v`, err)
	}

	if _, err := workloadccl.GetFixture(ctx, es, config, gen); !testutils.IsError(err, `non zero files`) {
		t.Fatalf(`expected "non zero files" error but got: %+v`, err)
	}

	fixtures, err := workloadccl.ListFixtures(ctx, es, config)
	if err != nil {
		t.Fatalf(`%+v`, err)
	}
	if len(fixtures) != 0 {
		t.Errorf(`expected no fixtures but got: %+v`, fixtures)
	}

	const filesPerNode = 1
	fixture, err := workloadccl.MakeFixture(ctx, db, es, config, gen, filesPerNode)
	if err != nil {
		t.Fatalf(`%+v`, err)
	}

	_, err = workloadccl.MakeFixture(ctx, db, es, config, gen, filesPerNode)
	if !testutils.IsError(err, `already exists`) {
		t.Fatalf(`expected 'already exists' error got: %+v`, err)
	}

	fixtures, err = workloadccl.ListFixtures(ctx, es, config)
	if err != nil {
		t.Fatalf(`%+v`, err)
	}
	if len(fixtures) != 1 {
		t.Errorf(`expected exactly one %s fixture but got: %+v`, flag, fixtures)
	}

	sqlDB.Exec(t, `CREATE DATABASE test`)
	if err := workloadccl.RestoreFixture(ctx, db, fixture, `test`, false); err != nil {
		t.Fatalf(`%+v`, err)
	}
	sqlDB.CheckQueryResults(t,
		`SELECT count(*) FROM test.fx`, [][]string{{strconv.Itoa(fixtureTestGenRows)}})
}

func TestImportFixture(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	defer func(oldRefreshInterval, oldAsOf time.Duration) {
		stats.DefaultRefreshInterval = oldRefreshInterval
		stats.DefaultAsOfTime = oldAsOf
	}(stats.DefaultRefreshInterval, stats.DefaultAsOfTime)
	stats.DefaultRefreshInterval = time.Millisecond
	stats.DefaultAsOfTime = 10 * time.Millisecond

	// Disable auto stats collection on all tables. This is needed because we
	// run only one auto stats job at a time, and collecting auto stats on
	// system tables can significantly delay the collection of stats on the
	// fixture after the IMPORT is done.
	st := cluster.MakeTestingClusterSettings()
	stats.AutomaticStatisticsClusterMode.Override(ctx, &st.SV, false)
	stats.AutomaticStatisticsOnSystemTables.Override(ctx, &st.SV, false)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings:          st,
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSharedProcessModeButDoesntYet(base.TestTenantProbabilistic, 113916),
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	gen := makeTestWorkload()
	// Enable auto stats only on the table being imported.
	gen.(*fixtureTestGen).tableAutoStatsEnabled = true
	flag := fmt.Sprintf(`val=%d`, timeutil.Now().UnixNano())
	if err := gen.Flags().Parse([]string{"--" + flag}); err != nil {
		t.Fatalf(`%+v`, err)
	}

	const filesPerNode = 1

	sqlDB.Exec(t, `CREATE DATABASE ingest`)
	_, err := workloadccl.ImportFixture(
		ctx, db, gen, `ingest`, filesPerNode, false, /* injectStats */
		``, /* csvServer */
	)
	require.NoError(t, err)
	sqlDB.CheckQueryResults(t,
		`SELECT count(*) FROM ingest.fx`, [][]string{{strconv.Itoa(fixtureTestGenRows)}})

	// Since we did not inject stats, the IMPORT should have triggered
	// automatic stats collection.
	statsQuery := fmt.Sprintf(`SELECT statistics_name, column_names, row_count, distinct_count, null_count
           FROM [SHOW STATISTICS FOR TABLE ingest.fx] WHERE row_count = %d`, fixtureTestGenRows)
	sqlDB.CheckQueryResultsRetry(t, statsQuery,
		[][]string{
			{"__auto__", "{key}", "10", "10", "0"},
			{"__auto__", "{value}", "10", "1", "0"},
		})
}

func TestImportFixtureCSVServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ts := httptest.NewServer(workload.CSVMux(workload.Registered()))
	defer ts.Close()

	s, db, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			UseDatabase: `d`,
		},
	)
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	gen := makeTestWorkload()
	flag := fmt.Sprintf(`val=%d`, timeutil.Now().UnixNano())
	if err := gen.Flags().Parse([]string{"--" + flag}); err != nil {
		t.Fatalf(`%+v`, err)
	}

	const filesPerNode = 1
	const noInjectStats = false
	sqlDB.Exec(t, `CREATE DATABASE d`)
	_, err := workloadccl.ImportFixture(
		ctx, db, gen, `d`, filesPerNode, noInjectStats, ts.URL,
	)
	require.NoError(t, err)
	sqlDB.CheckQueryResults(t,
		`SELECT count(*) FROM d.fx`, [][]string{{strconv.Itoa(fixtureTestGenRows)}})
}

func TestImportFixtureNodeCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const (
		nodes        = 3
		filesPerNode = 1
	)

	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(db)

	gen := makeTestWorkload()
	flag := fmt.Sprintf("--val=%d", timeutil.Now().UnixNano())
	require.NoError(t, gen.Flags().Parse([]string{flag}))

	sqlDB.Exec(t, "CREATE DATABASE ingest")
	_, err := workloadccl.ImportFixture(
		ctx, db, gen, "ingest", filesPerNode, false, /* injectStats */
		``, /* csvServer */
	)
	require.NoError(t, err)

	var desc string
	sqlDB.QueryRow(t, "SELECT description FROM crdb_internal.jobs WHERE job_type = 'IMPORT'").Scan(&desc)

	expectedFiles := math.Ceil(float64(fixtureTestGenRows) / float64(nodes))
	actualFiles := strings.Count(desc, "workload://")
	require.Equal(t, int(expectedFiles), actualFiles)
}
