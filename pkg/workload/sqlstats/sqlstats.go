package sqlstats

import (
	"context"
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
)

const (
	dummyTable = `(
		id UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
		col1 DECIMAL,
		col2 STRING,
		col3 BOOL,
		col4 FLOAT,
		col5 INT,
		col6 UUID,
		col7 STRING,
		col8 DECIMAL,
		col9 INT,
		col10 BOOL,
	)`
	tableNamePrefix = "sql_stats_workload_table_"
	tableCount      = 10
	defaultDbName   = "sql_stats"
)

var RandomSeed = workload.NewUint64RandomSeed()

type sqlStats struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags
}

func init() {
	workload.Register(sqlStatsMeta)
}

var sqlStatsMeta = workload.Meta{
	Name: "sqlstats",
	Description: `
This workload stresses the SQL stats subsystem within CockroachDB. It pre-fills the relevant
system tables directly with volumes of data approaching the size limits imposed on the tables.
It then runs a workload against the system that aims to produce high cardinality SQL stats data
by generating a large number of unique fingerprints. This workload provides a way for engineering
to stress test the system.
`,
	RandomSeed:    RandomSeed,
	Version:       "1.0.0",
	TestInfraOnly: true,
	New: func() workload.Generator {
		g := &sqlStats{}
		return g
	},
}

func (s *sqlStats) Meta() workload.Meta {
	return sqlStatsMeta
}

func (s *sqlStats) Flags() workload.Flags {
	return s.flags
}

func (s *sqlStats) Tables() []workload.Table {
	tables := make([]workload.Table, tableCount)
	for i := 0; i < tableCount; i++ {
		tableName := fmt.Sprintf("%s%d", tableNamePrefix, i+1)
		tables[i] = workload.Table{
			Name:   tableName,
			Schema: dummyTable,
			Splits: workload.BatchedTuples{},
		}
	}
	return tables
}

func (s *sqlStats) Hooks() workload.Hooks {
	return workload.Hooks{
		PostLoad: s.PostLoad,
	}
}

func (s *sqlStats) PostLoad(ctx context.Context, DB *gosql.DB) error {
	insertStmt := `
INSERT INTO system.statement_statistics (
  index_recommendations,
  aggregated_ts,
  fingerprint_id,
  transaction_fingerprint_id,
  plan_hash,
  app_name,
  node_id,
  agg_interval,
  metadata,
  statistics,
  plan
)
VALUES (
  ARRAY['creation : CREATE INDEX t1_k ON t1(k)'],
  '2023-07-05 15:10:11+00:00',
  'fp_1',
  'tfp_1',
  'ph_1',
  'app_1',
  1,
  '1 hr',
  'null',
  '{"statistics": {"lastExecAt" : "2023-07-05 15:10:10+00:00"}}'::JSONB,
  'null'
);
`
	_, err := DB.Exec(insertStmt)
	if err != nil {
		return err
	}
	fmt.Println("Row inserted")
	return nil
}

func (s *sqlStats) generateStmtStats(
	ctx context.Context, key appstatspb.StatementStatisticsKey, aggTS time.Time,
) *appstatspb.CollectedStatementStatistics {
	return &appstatspb.CollectedStatementStatistics{
		ID:  0,
		Key: key,
		Stats: appstatspb.StatementStatistics{
			Count:                0,
			FirstAttemptCount:    0,
			MaxRetries:           0,
			NumRows:              appstatspb.NumericStat{},
			IdleLat:              appstatspb.NumericStat{},
			ParseLat:             appstatspb.NumericStat{},
			PlanLat:              appstatspb.NumericStat{},
			RunLat:               appstatspb.NumericStat{},
			ServiceLat:           appstatspb.NumericStat{},
			OverheadLat:          appstatspb.NumericStat{},
			SensitiveInfo:        appstatspb.SensitiveInfo{},
			BytesRead:            appstatspb.NumericStat{},
			RowsRead:             appstatspb.NumericStat{},
			RowsWritten:          appstatspb.NumericStat{},
			ExecStats:            appstatspb.ExecStats{},
			SQLType:              "",
			LastExecTimestamp:    time.Time{},
			Nodes:                nil,
			Regions:              nil,
			PlanGists:            nil,
			IndexRecommendations: nil,
			Indexes:              nil,
			LatencyInfo:          appstatspb.LatencyInfo{},
			LastErrorCode:        "",
		},
		AggregatedTs:        time.Time{},
		AggregationInterval: 0,
	}
}

func (s *sqlStats) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	return workload.QueryLoad{}, nil
	//sqlDB, err := workload.SanitizeUrls(s, s.connFlags.DBOverride, urls)
	//if err != nil {
	//	return workload.QueryLoad{}, err
	//}
	//db, err := gosql.Open("cockroach", strings.Join(urls, " "))
	//if err != nil {
	//	return workload.QueryLoad{}, err
	//}
	//// Allow a maximum of concurrency+1 connections to the database.
	//db.SetMaxOpenConns(s.connFlags.Concurrency + 1)
	//db.SetMaxIdleConns(s.connFlags.Concurrency + 1)
	//
	//ql := workload.QueryLoad{SQLDatabase: sqlDB}
	//rng := rand.New(rand.NewSource(RandomSeed.Seed()))
	//
	//for i := 0; i < s.connFlags.Concurrency; i++ {
	//	hists := reg.GetHandle()
	//	workerFn := func(ctx context.Context) error {
	//
	//	}
	//}
}
