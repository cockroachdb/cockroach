// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	crdbpgx "github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgxv5"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/pflag"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
)

var RandomSeed = workload.NewUint64RandomSeed()

const (
	// A threshold that defines which duration is considered a long workload which is not meant for a benchmark result
	longDurationWorkloadThreshold = 4 * 24 * time.Hour

	// The reset time period in case we are running a long duration workload.
	warehouseWytdResetPeriod = 24 * time.Hour

	// Max rows that can be updated in a single txn, used while resetting the w_ytd values
	maxRowsToUpdateTxn = 10_000
)

type tpcc struct {
	flags        workload.Flags
	connFlags    *workload.ConnFlags
	workloadName string

	warehouses       int
	activeWarehouses int
	nowTime          time.Time
	numConns         int
	idleConns        int
	txnRetries       bool
	repairOrderIds   bool

	// txnPreambleFile queries that will be executed before each operation.
	txnPreambleFile       string
	txnPreambleStatements statements.Statements
	// txnPreambleTime metric for tracking duration for any queries that
	// we execute.
	txnPreambleTime *histogram.NamedHistogram

	// Used in non-uniform random data generation. cLoad is the value of C at load
	// time. cCustomerID is the value of C for the customer id generator. cItemID
	// is the value of C for the item id generator. See 2.1.6.
	cLoad, cCustomerID, cItemID int

	mix          string
	waitFraction float64

	// onTxnStartFns call back function to execute queries at the start of
	// a txn.
	onTxnStartFns []func(ctx context.Context, tx pgx.Tx) error

	workers                int
	activeWorkers          int
	fks                    bool
	separateColumnFamilies bool
	// deprecatedFKIndexes adds in foreign key indexes that are no longer needed
	// due to origin index restrictions being lifted.
	deprecatedFkIndexes bool

	txInfos []txInfo
	// deck contains indexes into the txInfos slice.
	deck []int

	auditor *auditor

	reg        *histogram.Registry
	txCounters txCounters

	split   bool
	scatter bool

	partitions         int
	ignorePartitions   bool
	clientPartitions   int
	affinityPartitions []int
	wPart              *partitioner
	wMRPart            *partitioner
	zoneCfg            zoneConfig
	multiRegionCfg     multiRegionConfig

	// localWarehouses determines whether or not we should force transactions to
	// operate on a local warehouse (local to where the given transaction
	// originated). This is only used for multi-region configurations, and is in
	// violation of the TPC-C spec, so it should only be used for internal
	// testing purposes.
	localWarehouses bool

	expensiveChecks bool

	replicateStaticColumns bool

	queryTraceFile string

	randomCIDsCache struct {
		syncutil.Mutex
		values [][]int
	}
	localsPool *sync.Pool

	// Set to true if duration >= longDurationWorkloadThreshold.
	// In that case, we reset w_ytd & d_ytd periodically and skip 3.3.2.1 and 3.3.2.8 consistency checks.
	isLongDurationWorkload bool

	// context group for any background reset table operation to avoid goroutine leaks during long duration workloads
	resetTableGrp      ctxgroup.Group
	resetTableCancelFn context.CancelFunc

	asOfSystemTime string
}

type waitSetter struct {
	val *float64
}

// Set implements the pflag.Value interface.
func (w *waitSetter) Set(val string) error {
	switch strings.ToLower(val) {
	case "true", "on":
		*w.val = 1.0
	case "false", "off":
		*w.val = 0.0
	default:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return err
		}
		if f < 0 {
			return errors.New("cannot set --wait to a negative value")
		}
		*w.val = f
	}
	return nil
}

// Type implements the pflag.Value interface
func (*waitSetter) Type() string { return "0.0/false - 1.0/true" }

// String implements the pflag.Value interface.
func (w *waitSetter) String() string {
	switch *w.val {
	case 0:
		return "false"
	case 1:
		return "true"
	default:
		return fmt.Sprintf("%f", *w.val)
	}
}

var _ pgx.QueryTracer = fileLoggerQueryTracer{}

type fileLoggerQueryTracer struct {
	file *os.File
}

func newFileLoggerQueryTracer(filePath string) (*fileLoggerQueryTracer, error) {
	tracer := &fileLoggerQueryTracer{}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	tracer.file = file
	return tracer, nil
}

func (t *fileLoggerQueryTracer) Close() error {
	return errors.CombineErrors(
		errors.Wrap(t.file.Sync(), "failed to sync query trace file"),
		errors.Wrap(t.file.Close(), "failed to close query trace file"),
	)
}

func (t fileLoggerQueryTracer) Write(p []byte) (n int, err error) {
	return t.file.Write(p)
}

func (t fileLoggerQueryTracer) TraceQueryStart(
	ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData,
) context.Context {
	whitespacePattern := regexp.MustCompile(`\s+`)
	fmt.Fprintf(t, "Query: %s\n", whitespacePattern.ReplaceAllString(strings.TrimSpace(data.SQL), " "))
	return ctx
}

func (t fileLoggerQueryTracer) TraceQueryEnd(
	ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData,
) {
	if data.Err == nil {
		fmt.Fprintf(t, "CommandTag: %s\n", data.CommandTag.String())
	} else {
		fmt.Fprintf(t, "Error: %s\n", data.Err)
	}
}

func init() {
	workload.Register(tpccMeta)
}

// FromWarehouses returns a tpcc generator pre-configured with the specified
// number of warehouses.
func FromWarehouses(warehouses int) workload.Generator {
	return workload.FromFlags(tpccMeta, fmt.Sprintf(`--warehouses=%d`, warehouses))
}

var tpccMeta = workload.Meta{
	Name: `tpcc`,
	Description: `TPC-C simulates a transaction processing workload` +
		` using a rich schema of multiple tables.`,
	Version:    `2.2.0`,
	RandomSeed: RandomSeed,
	New: func() workload.Generator {
		g := &tpcc{
			workloadName: "tpcc",
		}
		g.flags.FlagSet = pflag.NewFlagSet(`tpcc`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`mix`:                      {RuntimeOnly: true},
			`partitions`:               {RuntimeOnly: true},
			`ignore-partitions`:        {RuntimeOnly: true},
			`client-partitions`:        {RuntimeOnly: true},
			`partition-affinity`:       {RuntimeOnly: true},
			`partition-strategy`:       {RuntimeOnly: true},
			`zones`:                    {RuntimeOnly: true},
			`active-warehouses`:        {RuntimeOnly: true},
			`active-workers`:           {RuntimeOnly: true},
			`scatter`:                  {RuntimeOnly: true},
			`split`:                    {RuntimeOnly: true},
			`wait`:                     {RuntimeOnly: true},
			`workers`:                  {RuntimeOnly: true},
			`conns`:                    {RuntimeOnly: true},
			`idle-conns`:               {RuntimeOnly: true},
			`txn-retries`:              {RuntimeOnly: true},
			`repair-order-ids`:         {RuntimeOnly: true},
			`expensive-checks`:         {RuntimeOnly: true, CheckConsistencyOnly: true},
			`local-warehouses`:         {RuntimeOnly: true},
			`regions`:                  {RuntimeOnly: true},
			`survival-goal`:            {RuntimeOnly: true},
			`replicate-static-columns`: {RuntimeOnly: true},
			`deprecated-fk-indexes`:    {RuntimeOnly: true},
			`query-trace-file`:         {RuntimeOnly: true},
			`fake-time`:                {RuntimeOnly: true},
			`txn-preamble-file`:        {RuntimeOnly: true},
			`aost`:                     {RuntimeOnly: true, CheckConsistencyOnly: true},
		}

		g.flags.IntVar(&g.warehouses, `warehouses`, 1, `Number of warehouses for loading`)
		g.flags.BoolVar(&g.fks, `fks`, true, `Add the foreign keys`)
		g.flags.BoolVar(&g.deprecatedFkIndexes, `deprecated-fk-indexes`, false, `Add deprecated foreign keys (needed when running against v20.1 or below clusters)`)

		g.flags.StringVar(&g.mix, `mix`,
			`newOrder=10,payment=10,orderStatus=1,delivery=1,stockLevel=1`,
			`Weights for the transaction mix. The default matches the TPCC spec.`)
		g.waitFraction = 1.0
		g.flags.Var(&waitSetter{&g.waitFraction}, `wait`, `Wait mode (include think/keying sleeps): 1/true for tpcc-standard wait, 0/false for no waits, other factors also allowed`)
		g.flags.IntVar(&g.workers, `workers`, 0, fmt.Sprintf(
			`Number of concurrent workers. Defaults to --warehouses * %d`, NumWorkersPerWarehouse,
		))
		g.flags.IntVar(&g.numConns, `conns`, 0, fmt.Sprintf(
			`Number of connections. Defaults to --warehouses * %d (except in nowait mode, where it defaults to --workers`,
			numConnsPerWarehouse,
		))
		g.flags.IntVar(&g.idleConns, `idle-conns`, 0, `Number of idle connections. Defaults to 0`)
		g.flags.BoolVar(&g.txnRetries, `txn-retries`, true, `Run transactions in a retry loop`)
		g.flags.BoolVar(&g.repairOrderIds, `repair-order-ids`, false, `Attempt to repair next order id field of district rows automatically in reaction to unique violation during new-order txns (useful when running LDR on overlapping warehouses)`)
		g.flags.IntVar(&g.partitions, `partitions`, 1, `Partition tables`)
		g.flags.BoolVar(&g.ignorePartitions, `ignore-partitions`, false, `Ignore partitions during load generation`)
		g.flags.IntVar(&g.clientPartitions, `client-partitions`, 0, `Make client behave as if the tables are partitioned, but does not actually partition underlying data. Requires --partition-affinity.`)
		g.flags.IntSliceVar(&g.affinityPartitions, `partition-affinity`, nil, `Run load generator against specific partition (requires partitions). `+
			`Note that if one value is provided, the assumption is that all urls are associated with that partition. In all other cases the assumption `+
			`is that the URLs are distributed evenly over the partitions`)
		g.flags.Var(&g.zoneCfg.strategy, `partition-strategy`, `Partition tables according to which strategy [replication, leases]`)
		g.flags.StringSliceVar(&g.zoneCfg.zones, "zones", []string{}, "Zones for legacy partitioning, the number of zones should match the number of partitions and the zones used to start cockroach. Does not work with --regions.")
		g.flags.StringSliceVar(&g.multiRegionCfg.regions, "regions", []string{}, "Regions to use for multi-region partitioning. The first region is the PRIMARY REGION. Does not work with --zones.")
		g.flags.Var(&g.multiRegionCfg.survivalGoal, "survival-goal", "Survival goal to use for multi-region setups. Allowed values: [zone, region].")
		g.flags.IntVar(&g.activeWarehouses, `active-warehouses`, 0, `Run the load generator against a specific number of warehouses. Defaults to --warehouses'`)
		g.flags.IntVar(&g.activeWorkers, `active-workers`, 0, `Number of workers that can execute at any given time. Defaults to --workers'`)
		g.flags.BoolVar(&g.scatter, `scatter`, false, `Scatter ranges`)
		g.flags.BoolVar(&g.split, `split`, false, `Split tables`)
		g.flags.BoolVar(&g.expensiveChecks, `expensive-checks`, false, `Run expensive checks`)
		g.flags.BoolVar(&g.separateColumnFamilies, `families`, false, `Use separate column families for dynamic and static columns`)
		g.flags.BoolVar(&g.replicateStaticColumns, `replicate-static-columns`, false, "Create duplicate indexes for all static columns in district, items and warehouse tables, such that each zone or rack has them locally.")
		g.flags.BoolVar(&g.localWarehouses, `local-warehouses`, false, `Force transactions to use a local warehouse in all cases (in violation of the TPC-C specification)`)
		g.flags.StringVar(&g.queryTraceFile, `query-trace-file`, ``, `File to write the query traces to. Defaults to no output`)
		// Support executing a query file before each transaction.
		g.flags.StringVar(&g.txnPreambleFile, "txn-preamble-file", "", "queries that will be injected before each txn")
		g.flags.StringVar(&g.asOfSystemTime, "aost", "",
			"This is an optional parameter to specify AOST; used exclusively in conjunction with the TPC-C consistency "+
				"check. Example values are (\"'-1m'\", \"'-1h'\")")

		RandomSeed.AddFlag(&g.flags)
		g.connFlags = workload.NewConnFlags(&g.flags)
		// Hardcode this since it doesn't seem like anyone will want to change
		// it and it's really noisy in the generated fixture paths.
		var err error
		g.nowTime, err = time.Parse(`2006-01-02 15:04:05`, `2006-01-02 15:04:05`)
		if err != nil {
			panic(err) // unreachable.
		}
		return g
	},
}

func queryDatabaseRegions(db *gosql.DB) (map[string]struct{}, error) {
	regions := make(map[string]struct{})
	rows, err := db.Query(`SELECT region FROM [SHOW REGIONS FROM DATABASE]`)
	if err != nil {
		return regions, err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		if rows.Err() != nil {
			return regions, err
		}
		var region string
		if err := rows.Scan(&region); err != nil {
			return regions, err
		}
		regions[region] = struct{}{}
	}
	if rows.Err() != nil {
		return regions, err
	}
	return regions, nil
}

// Meta implements the Generator interface.
func (*tpcc) Meta() workload.Meta { return tpccMeta }

// Flags implements the Flagser interface.
func (w *tpcc) Flags() workload.Flags { return w.flags }

// ConnFlags implements the ConnFlagser interface.
func (w *tpcc) ConnFlags() *workload.ConnFlags { return w.connFlags }

// Hooks implements the Hookser interface.
func (w *tpcc) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if w.warehouses < 1 {
				return errors.Errorf(`--warehouses must be positive`)
			}

			if w.activeWarehouses > w.warehouses {
				return errors.Errorf(`--active-warehouses needs to be less than or equal to warehouses`)
			} else if w.activeWarehouses == 0 {
				w.activeWarehouses = w.warehouses
			}

			if w.partitions < 1 {
				return errors.Errorf(`--partitions must be positive`)
			}

			if len(w.zoneCfg.zones) > 0 && len(w.multiRegionCfg.regions) > 0 {
				return errors.Errorf("cannot specify both --regions and --zones")
			}

			if w.clientPartitions > 0 {
				if w.partitions > 1 {
					return errors.Errorf(`cannot specify both --partitions and --client-partitions;
					--partitions actually partitions underlying data.
					--client-partitions only modifies client behavior to access a subset of warehouses. Must be used with --partition-affinity`)
				}

				if len(w.affinityPartitions) == 0 {
					return errors.Errorf(`--client-partitions must be used with --partition-affinity.`)
				}

				for _, p := range w.affinityPartitions {
					if p >= w.clientPartitions {
						return errors.Errorf(`--partition-affinity %d in %v out of bounds of --client-partitions`,
							p, w.affinityPartitions)
					}
				}

			} else {
				for _, p := range w.affinityPartitions {
					if p < 0 || p >= w.partitions {
						return errors.Errorf(`--partition-affinity out of bounds of --partitions`)
					}
				}

				if len(w.multiRegionCfg.regions) > 0 && (len(w.multiRegionCfg.regions) != w.partitions) {
					return errors.Errorf(`--regions should have the same length as --partitions.`)
				}

				if len(w.multiRegionCfg.regions) < 3 && w.multiRegionCfg.survivalGoal == survivalGoalRegion {
					return errors.Errorf(`REGION survivability needs at least 3 regions.`)
				}

				if len(w.zoneCfg.zones) > 0 && (len(w.zoneCfg.zones) != w.partitions) {
					return errors.Errorf(`--zones should have the same length as --partitions.`)
				}
			}

			w.initNonUniformRandomConstants()

			if w.workers == 0 {
				w.workers = w.activeWarehouses * NumWorkersPerWarehouse
			}

			if w.activeWorkers > w.workers {
				return errors.Errorf(`--active-workers needs to be less than or equal to workers (%d)`, w.workers)
			} else if w.activeWorkers == 0 {
				w.activeWorkers = w.workers
			}

			if w.numConns == 0 {
				// If we're not waiting, open up a connection for each worker. If we are
				// waiting, we only use up to a set number of connections per warehouse.
				// This isn't mandated by the spec, but opening a connection per worker
				// when they each spend most of their time waiting is wasteful.
				if w.waitFraction == 0 {
					w.numConns = w.workers
				} else {
					w.numConns = w.activeWarehouses * numConnsPerWarehouse
				}
			}

			if w.waitFraction > 0 && w.activeWorkers != w.activeWarehouses*NumWorkersPerWarehouse && len(w.affinityPartitions) == 0 {
				return errors.Errorf(`--wait > 0 and --warehouses=%d and affinity_partitions=0 requires --active-workers=%d`,
					w.activeWarehouses, w.warehouses*NumWorkersPerWarehouse)
			}

			if w.queryTraceFile != `` && w.workers != 1 {
				return errors.Errorf(`--query-trace-file must be used with exactly one worker`)
			}

			// Create a partitioner to help us partition the warehouses. The base-case is
			// where w.warehouses == w.activeWarehouses and w.partitions == 1.
			partitions := w.partitions
			if w.clientPartitions > 0 {
				partitions = w.clientPartitions
			}
			var err error
			// This partitioner will not actually be used to partition the
			// data, but instead is only used to limit the warehouses the
			// client attempts to manipulate.
			w.wPart, err = makePartitioner(w.warehouses, w.activeWarehouses, partitions)
			if err != nil {
				return errors.Wrap(err, "error creating partitioner")
			}
			if len(w.multiRegionCfg.regions) != 0 {
				// For multi-region workloads, make a multi-region partitioner.
				w.wMRPart, err = makeMRPartitioner(w.warehouses, w.activeWarehouses, partitions)
				if err != nil {
					return errors.Wrap(err, "error creating multi-region partitioner")
				}
			}

			w.auditor = newAuditor(w.activeWarehouses, w.wPart, w.affinityPartitions)
			return initializeMix(w)
		},
		PreCreate: func(db *gosql.DB) error {
			if len(w.multiRegionCfg.regions) == 0 {
				// Not a multi-region deployment.
				return nil
			}

			regions, err := queryDatabaseRegions(db)
			if err != nil {
				return err
			}

			var dbName string
			if err := db.QueryRow(`SHOW DATABASE`).Scan(&dbName); err != nil {
				return err
			}

			var stmts []string
			for i, region := range w.multiRegionCfg.regions {
				var stmt string
				// Region additions should be idempotent.
				if _, ok := regions[region]; ok {
					continue
				}
				// The first region is the PRIMARY region.
				if i == 0 {
					stmt = fmt.Sprintf(`alter database %s set primary region %q`, dbName, region)
				} else {
					stmt = fmt.Sprintf(`alter database %s add region %q`, dbName, region)
				}
				stmts = append(stmts, stmt)
			}

			var survivalGoal string
			switch w.multiRegionCfg.survivalGoal {
			case survivalGoalZone:
				survivalGoal = `zone failure`
			case survivalGoalRegion:
				survivalGoal = `region failure`
			default:
				panic("unexpected")
			}
			stmts = append(stmts, fmt.Sprintf(`alter database %s survive %s`, dbName, survivalGoal))

			for _, stmt := range stmts {
				if _, err := db.Exec(stmt); err != nil {
					return err
				}
			}

			return nil
		},
		PostLoad: func(_ context.Context, db *gosql.DB) error {
			if w.fks {
				// We avoid validating foreign keys because we just generated
				// the data set and don't want to scan over the entire thing
				// again. Unfortunately, this means that we leave the foreign
				// keys unvalidated for the duration of the test, so the SQL
				// optimizer can't use them.
				// TODO(lucy-zhang): expose an internal knob to validate fk
				// relations without performing full validation. See #38833.
				fkStmts := []string{
					`alter table district add foreign key (d_w_id) references warehouse (w_id) not valid`,
					`alter table customer add foreign key (c_w_id, c_d_id) references district (d_w_id, d_id) not valid`,
					`alter table history add foreign key (h_c_w_id, h_c_d_id, h_c_id) references customer (c_w_id, c_d_id, c_id) not valid`,
					`alter table history add foreign key (h_w_id, h_d_id) references district (d_w_id, d_id) not valid`,
					`alter table "order" add foreign key (o_w_id, o_d_id, o_c_id) references customer (c_w_id, c_d_id, c_id) not valid`,
					`alter table new_order add foreign key (no_w_id, no_d_id, no_o_id) references "order" (o_w_id, o_d_id, o_id) not valid`,
					`alter table stock add foreign key (s_w_id) references warehouse (w_id) not valid`,
					`alter table stock add foreign key (s_i_id) references item (i_id) not valid`,
					`alter table order_line add foreign key (ol_w_id, ol_d_id, ol_o_id) references "order" (o_w_id, o_d_id, o_id) not valid`,
					`alter table order_line add foreign key (ol_supply_w_id, ol_i_id) references stock (s_w_id, s_i_id) not valid`,
				}

				for _, fkStmt := range fkStmts {
					if _, err := db.Exec(fkStmt); err != nil {
						const duplFKErr = "columns cannot be used by multiple foreign key constraints"
						const idxErr = "foreign key requires an existing index on columns"
						switch {
						case strings.Contains(err.Error(), idxErr):
							fmt.Println(errors.WithHint(err, "try using the --deprecated-fk-indexes flag"))
							// If the statement failed because of a missing FK index, suggest
							// to use the deprecated-fks flag.
							return errors.WithHint(err, "try using the --deprecated-fk-indexes flag")
						case strings.Contains(err.Error(), duplFKErr):
							// If the statement failed because the fk already exists,
							// ignore it. Return the error for any other reason.
						default:
							return err
						}
					}
				}
				// Set GLOBAL only after data is loaded to speed up initialization
				// time. Otherwise, the mass INSERT at workload init time takes
				// extraordinarily longer. If data is imported with IMPORT, this
				// statement is idempotent.
				if len(w.multiRegionCfg.regions) > 0 {
					if _, err := db.Exec(fmt.Sprintf(`ALTER TABLE item SET %s`, localityGlobalSuffix)); err != nil {
						return err
					}
				}
			}

			// With multi-region enabled, we do not need to partition and scatter
			// our data anymore as it has already been partitioned by the
			// computed column on REGIONAL BY ROW tables.
			if len(w.multiRegionCfg.regions) != 0 {
				return nil
			}
			return w.partitionAndScatterWithDB(db)
		},
		PostRun: func(startElapsed time.Duration) error {
			w.auditor.runChecks(w.localWarehouses)
			const totalHeader = "\n_elapsed_______tpmC____efc__avg(ms)__p50(ms)__p90(ms)__p95(ms)__p99(ms)_pMax(ms)"
			fmt.Println(totalHeader)

			partitionFactor := 1
			countAffinity := len(w.affinityPartitions)
			if countAffinity > 0 && w.wPart.parts > 0 {
				partitionFactor = w.wPart.parts / countAffinity
			}

			const newOrderName = `newOrder`
			w.reg.Tick(func(t histogram.Tick) {
				if newOrderName == t.Name {
					tpmC := float64(t.Cumulative.TotalCount()) / startElapsed.Seconds() * 60
					fmt.Printf("%7.1fs %10.1f %5.1f%% %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
						startElapsed.Seconds(),
						tpmC,
						100*tpmC/(SpecWarehouseFactor*float64(w.activeWarehouses/partitionFactor)),
						time.Duration(t.Cumulative.Mean()).Seconds()*1000,
						time.Duration(t.Cumulative.ValueAtQuantile(50)).Seconds()*1000,
						time.Duration(t.Cumulative.ValueAtQuantile(90)).Seconds()*1000,
						time.Duration(t.Cumulative.ValueAtQuantile(95)).Seconds()*1000,
						time.Duration(t.Cumulative.ValueAtQuantile(99)).Seconds()*1000,
						time.Duration(t.Cumulative.ValueAtQuantile(100)).Seconds()*1000,
					)
				}
			})
			return nil
		},
		CheckConsistency: func(ctx context.Context, db *gosql.DB) error {
			for _, check := range AllChecks() {
				if !w.expensiveChecks && check.Expensive {
					continue
				}
				if check.LoadOnly {
					// TODO(nvanbenschoten): support load-only checks.
					continue
				}

				if w.isLongDurationWorkload && check.SkipForLongDuration {
					continue
				}

				start := timeutil.Now()
				err := check.Fn(db, w.asOfSystemTime /* asOfSystemTime */)
				log.Infof(ctx, `check %s took %s`, check.Name, timeutil.Since(start))
				if err != nil {
					return errors.Wrapf(err, `check failed: %s`, check.Name)
				}
			}
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (w *tpcc) Tables() []workload.Table {
	seed := RandomSeed.Seed()
	aCharsInit := workloadimpl.PrecomputedRandInit(rand.New(rand.NewSource(seed)), precomputedLength, aCharsAlphabet)
	lettersInit := workloadimpl.PrecomputedRandInit(rand.New(rand.NewSource(seed)), precomputedLength, lettersAlphabet)
	numbersInit := workloadimpl.PrecomputedRandInit(rand.New(rand.NewSource(seed)), precomputedLength, numbersAlphabet)
	if w.localsPool == nil {
		w.localsPool = &sync.Pool{
			New: func() interface{} {
				return &generateLocals{
					rng: tpccRand{
						Rand: rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano()))),
						// Intentionally wait until here to initialize the precomputed rands
						// so a caller of Tables that only wants schema doesn't compute
						// them.
						aChars:  aCharsInit(),
						letters: lettersInit(),
						numbers: numbersInit(),
					},
				}
			},
		}
	}

	// splits is a convenience method for constructing table splits that returns
	// a zero value if the workload does not have splits enabled.
	splits := func(t workload.BatchedTuples) workload.BatchedTuples {
		if w.split {
			return t
		}
		return workload.BatchedTuples{}
	}

	// numBatches is a helper to calculate how many split batches exist exist given
	// the total number of rows and the desired number of rows per split.
	numBatches := func(total, per int) int {
		batches := total / per
		if total%per == 0 {
			batches--
		}
		return batches
	}
	warehouse := workload.Table{
		Name: `warehouse`,
		Schema: makeSchema(
			tpccWarehouseSchema,
			maybeAddLocalityRegionalByRow(w.multiRegionCfg, `w_id`),
			maybeAddColumnFamiliesSuffix(
				w.separateColumnFamilies,
				tpccWarehouseColumnFamiliesSuffix,
			),
		),
		InitialRows: workload.BatchedTuples{
			NumBatches: w.warehouses,
			FillBatch:  w.tpccWarehouseInitialRowBatch,
		},
		Splits: splits(workload.Tuples(
			numBatches(w.warehouses, numWarehousesPerRange),
			func(i int) []interface{} {
				return []interface{}{(i + 1) * numWarehousesPerRange}
			},
		)),
		Stats: w.tpccWarehouseStats(),
	}
	district := workload.Table{
		Name: `district`,
		Schema: makeSchema(
			tpccDistrictSchemaBase,
			maybeAddColumnFamiliesSuffix(
				w.separateColumnFamilies,
				tpccDistrictColumnFamiliesSuffix,
			),
			maybeAddLocalityRegionalByRow(w.multiRegionCfg, `d_w_id`),
		),
		InitialRows: workload.BatchedTuples{
			NumBatches: numDistrictsPerWarehouse * w.warehouses,
			FillBatch:  w.tpccDistrictInitialRowBatch,
		},
		Splits: splits(workload.Tuples(
			numBatches(w.warehouses, numWarehousesPerRange),
			func(i int) []interface{} {
				return []interface{}{(i + 1) * numWarehousesPerRange, 0}
			},
		)),
		Stats: w.tpccDistrictStats(),
	}
	customer := workload.Table{
		Name: `customer`,
		Schema: makeSchema(
			tpccCustomerSchemaBase,
			maybeAddColumnFamiliesSuffix(
				w.separateColumnFamilies,
				tpccCustomerColumnFamiliesSuffix,
			),
			maybeAddLocalityRegionalByRow(w.multiRegionCfg, `c_w_id`),
		),
		InitialRows: workload.BatchedTuples{
			NumBatches: numCustomersPerWarehouse * w.warehouses,
			FillBatch:  w.tpccCustomerInitialRowBatch,
		},
		Stats: w.tpccCustomerStats(),
	}
	history := workload.Table{
		Name: `history`,
		Schema: makeSchema(
			tpccHistorySchemaBase,
			maybeAddFkSuffix(
				w.deprecatedFkIndexes,
				deprecatedTpccHistorySchemaFkSuffix,
			),
			maybeAddLocalityRegionalByRow(w.multiRegionCfg, `h_w_id`),
		),
		InitialRows: workload.BatchedTuples{
			NumBatches: numHistoryPerWarehouse * w.warehouses,
			FillBatch:  w.tpccHistoryInitialRowBatch,
		},
		Splits: splits(workload.Tuples(
			numBatches(w.warehouses, numWarehousesPerRange),
			func(i int) []interface{} {
				return []interface{}{(i + 1) * numWarehousesPerRange}
			},
		)),
		Stats: w.tpccHistoryStats(),
	}
	order := workload.Table{
		Name: `order`,
		Schema: makeSchema(
			tpccOrderSchemaBase,
			maybeAddLocalityRegionalByRow(w.multiRegionCfg, `o_w_id`),
		),
		InitialRows: workload.BatchedTuples{
			NumBatches: numOrdersPerWarehouse * w.warehouses,
			FillBatch:  w.tpccOrderInitialRowBatch,
		},
		Stats: w.tpccOrderStats(),
	}
	newOrder := workload.Table{
		Name: `new_order`,
		Schema: makeSchema(
			tpccNewOrderSchema,
			maybeAddLocalityRegionalByRow(w.multiRegionCfg, `no_w_id`),
		),
		InitialRows: workload.BatchedTuples{
			NumBatches: numNewOrdersPerWarehouse * w.warehouses,
			FillBatch:  w.tpccNewOrderInitialRowBatch,
		},
		Stats: w.tpccNewOrderStats(),
	}
	item := workload.Table{
		Name: `item`,
		Schema: makeSchema(
			tpccItemSchema,
		),
		InitialRows: workload.BatchedTuples{
			NumBatches: numItems,
			FillBatch:  w.tpccItemInitialRowBatch,
		},
		Splits: splits(workload.Tuples(
			numBatches(numItems, numItemsPerRange),
			func(i int) []interface{} {
				return []interface{}{numItemsPerRange * (i + 1)}
			},
		)),
		Stats: w.tpccItemStats(),
	}
	stock := workload.Table{
		Name: `stock`,
		Schema: makeSchema(
			tpccStockSchemaBase,
			maybeAddFkSuffix(
				w.deprecatedFkIndexes,
				deprecatedTpccStockSchemaFkSuffix,
			),
			maybeAddLocalityRegionalByRow(w.multiRegionCfg, `s_w_id`),
		),
		InitialRows: workload.BatchedTuples{
			NumBatches: numStockPerWarehouse * w.warehouses,
			FillBatch:  w.tpccStockInitialRowBatch,
		},
		Stats: w.tpccStockStats(),
	}
	orderLine := workload.Table{
		Name: `order_line`,
		Schema: makeSchema(
			tpccOrderLineSchemaBase,
			maybeAddFkSuffix(
				w.deprecatedFkIndexes,
				deprecatedTpccOrderLineSchemaFkSuffix,
			),
			maybeAddLocalityRegionalByRow(w.multiRegionCfg, `ol_w_id`),
		),
		InitialRows: workload.BatchedTuples{
			NumBatches: numOrdersPerWarehouse * w.warehouses,
			FillBatch:  w.tpccOrderLineInitialRowBatch,
		},
		Stats: w.tpccOrderLineStats(),
	}
	return []workload.Table{
		warehouse, district, customer, history, order, newOrder, item, stock, orderLine,
	}
}

// Ops implements the Opser interface.
func (w *tpcc) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	if len(w.multiRegionCfg.regions) == 0 {
		// It would be nice to remove the need for this and to require that
		// partitioning and scattering occurs only when the PostLoad hook is
		// run, but to maintain backward compatibility, it's easiest to allow
		// partitioning and scattering during `workload run`.
		if err := w.partitionAndScatter(urls); err != nil {
			return workload.QueryLoad{}, err
		}
	}

	var resetTableCtx context.Context
	resetTableCtx, w.resetTableCancelFn = context.WithCancel(ctx)
	w.resetTableGrp = ctxgroup.WithContext(resetTableCtx)

	if duration, err := w.flags.GetDuration("duration"); err == nil {
		if duration == 0 || duration >= longDurationWorkloadThreshold {
			log.Warningf(ctx,
				"Warning: this workload is being used with a long or indefinite duration."+
					" This could cause it to deviate from the TPCC spec in subtle ways and is not meant for benchmarking. "+
					"Consistency checks might be disabled",
			)
			w.isLongDurationWorkload = true
		}
	} else {
		log.Warningf(ctx, "Couldn't get duration of the tpcc workload run for disabling consistency checks %v", err)
	}

	// Need idempotency - Ops might be invoked multiple times with the same
	// Registry.
	if w.reg == nil {
		w.reg = reg
		w.txCounters = setupTPCCMetrics(w.workloadName, reg.Registerer())
	}

	// We can't use a single MultiConnPool because we want to implement partition
	// affinity. Instead we have one MultiConnPool per server.
	cfg := workload.NewMultiConnPoolCfgFromFlags(w.connFlags)
	cfg.MaxTotalConnections = (w.numConns + len(urls) - 1) / len(urls) // round up

	// Limit the number of connections per pool (otherwise preparing statements at
	// startup can be slow).
	cfg.MaxConnsPerPool = w.connFlags.Concurrency
	fmt.Printf("Initializing %d connections...\n", w.numConns)

	// If queries were specified before each operation, then lets
	// execute those.
	if w.txnPreambleFile != "" {
		file, err := os.ReadFile(w.txnPreambleFile)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		txnPreambleStatements, err := parser.Parse(string(file))
		if err != nil {
			return workload.QueryLoad{}, err
		}
		w.txnPreambleStatements = txnPreambleStatements

		w.txnPreambleTime = reg.GetHandle().Get("txnPreamble")
		w.onTxnStartFns = append(w.onTxnStartFns, func(ctx context.Context, tx pgx.Tx) error {
			// Next execute any statements at the start of the txn.
			startTime := timeutil.Now()
			for _, stmt := range w.txnPreambleStatements {
				if _, err := tx.Exec(ctx, stmt.AST.String()); err != nil {
					return err
				}
			}
			w.txnPreambleTime.Record(timeutil.Since(startTime))
			return nil
		})
	}

	// Set up the query tracer.
	var tracer *fileLoggerQueryTracer
	if w.queryTraceFile != `` {
		var err error
		tracer, err = newFileLoggerQueryTracer(w.queryTraceFile)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		cfg.QueryTracer = tracer
	}

	dbs := make([]*workload.MultiConnPool, len(urls))
	var g errgroup.Group
	for i := range urls {
		i := i
		g.Go(func() error {
			var err error
			dbs[i], err = workload.NewMultiConnPool(ctx, cfg, urls[i])
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return workload.QueryLoad{}, err
	}
	var partitionDBs [][]*workload.MultiConnPool
	if w.clientPartitions > 0 {
		// Client partitions simply emulates the behavior of data partitions
		// w/r/t database connections, though all of the connections will
		// be for the same partition.
		partitionDBs = make([][]*workload.MultiConnPool, w.clientPartitions)
	} else {
		// Assign each DB connection pool to a local partition. This assumes that
		// dbs[i] is a machine that holds partition "i % *partitions". If we have an
		// affinity partition, all connections will be for the same partition.
		partitionDBs = make([][]*workload.MultiConnPool, w.partitions)
	}

	// If there is only one affinityPartition then we assume all of the URLs are
	// associated with that partition.
	if len(w.affinityPartitions) == 1 {
		// All connections are for our local partitions.
		partitionDBs[w.affinityPartitions[0]] = dbs

	} else {
		// This is making some assumptions about how racks are handed out.
		// If we have more than one affinityPartition then we assume that the
		// URLs are mapped to partitions in a round-robin fashion.
		// Imagine there are 5 partitions and 15 urls, this code assumes that urls
		// 0, 5, and 10 correspond to the 0th partition.
		if len(w.affinityPartitions) == 0 {
			for i, db := range dbs {
				p := i % w.partitions
				partitionDBs[p] = append(partitionDBs[p], db)
			}
		} else {
			for i, p := range w.affinityPartitions {
				partitionDBs[p] = append(partitionDBs[p], dbs[i])
			}
		}
		for i := range partitionDBs {
			// Possible if we have more partitions than DB connections.
			if partitionDBs[i] == nil {
				partitionDBs[i] = dbs
			}
		}
	}

	fmt.Printf("Initializing %d idle connections...\n", w.idleConns)
	var conns []*pgx.Conn
	for i := 0; i < w.idleConns; i++ {
		for _, url := range urls {
			connConfig, err := pgx.ParseConfig(url)
			if err != nil {
				return workload.QueryLoad{}, err
			}
			conn, err := pgx.ConnectConfig(ctx, connConfig)
			if err != nil {
				return workload.QueryLoad{}, err
			}
			conns = append(conns, conn)
		}
	}
	fmt.Printf("Initializing %d workers and preparing statements...\n", w.workers)
	ql := workload.QueryLoad{}
	ql.WorkerFns = make([]func(context.Context) error, 0, w.workers)
	var group errgroup.Group

	// Determines whether a partition is in the local workload's set of affinity
	// partitions.
	isMyPart := func(p int) bool {
		for _, ap := range w.affinityPartitions {
			if p == ap {
				return true
			}
		}
		// If nothing is mine, then everything is mine.
		return len(w.affinityPartitions) == 0
	}

	workersSem := quotapool.NewIntPool("workers", uint64(w.activeWorkers))

	// Limit the amount of workers we initialize in parallel, to avoid running out
	// of memory (#36897).
	sem := make(chan struct{}, 100)
	elemIndex := -1
	totalElemsLen := len(w.wPart.totalElems)
	// Function to get the next warehouse and partition
	getNextWarehouseAndPartition := func() (int, int) {
		for {
			elemIndex++
			warehouse := w.wPart.totalElems[elemIndex%totalElemsLen]
			part := w.wPart.partElemsMap[warehouse]
			if isMyPart(part) {
				return warehouse, part
			}
		}
	}

	for workerIdx := 0; workerIdx < w.workers; workerIdx++ {
		workerIdx := workerIdx
		var warehouse, p int

		if len(w.multiRegionCfg.regions) == 0 {
			warehouse, p = getNextWarehouseAndPartition()
		} else {
			// For multi-region workloads, use the multi-region partitioning.
			warehouse = w.wMRPart.totalElems[workerIdx%len(w.wMRPart.totalElems)]
			p = w.wMRPart.partElemsMap[warehouse]
		}

		// This isn't part of our local partition.
		if !isMyPart(p) {
			continue
		}
		dbs := partitionDBs[p]
		db := dbs[warehouse%len(dbs)]

		// NB: ql.WorkerFns is sized so this never re-allocs.
		ql.WorkerFns = append(ql.WorkerFns, nil)
		idx := len(ql.WorkerFns) - 1
		sem <- struct{}{}
		group.Go(func() error {
			worker, err := newWorker(ctx, w, db, reg.GetHandle(), w.txCounters, warehouse, workersSem)
			if err == nil {
				ql.WorkerFns[idx] = worker.run
			}
			<-sem
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return workload.QueryLoad{}, err
	}
	// Preregister all of the histograms so they always print.
	for _, tx := range allTxs {
		reg.GetHandle().Get(tx.name)
	}

	// Close idle connections.
	ql.Close = func(_ context.Context) error {
		for _, conn := range conns {
			if err := conn.Close(ctx); err != nil {
				log.Warningf(ctx, "%v", err)
			}
		}
		if tracer != nil {
			if err := tracer.Close(); err != nil {
				log.Warningf(ctx, "%v", err)
			}
		}

		w.resetTableCancelFn()
		if err := w.resetTableGrp.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			log.Warningf(ctx, "%v", err)
		}
		return nil
	}
	return ql, nil
}

// executeTx runs fn inside a transaction with retries, if enabled. On
// non-retryable failures, the transaction is aborted and rolled back; on
// success, the transaction is committed.
func (w *tpcc) executeTx(
	ctx context.Context, conn crdbpgx.Conn, fn func(pgx.Tx) error,
) (onTxnStartDuration time.Duration, err error) {
	txOpts := pgx.TxOptions{}
	txnFuncWithStartFuncs := func(tx pgx.Tx) (err error) {
		defer func() {
			if err != nil && !w.txnRetries {
				_ = tx.Rollback(ctx)
			}
		}()
		if w.onTxnStartFns != nil {
			startTime := timeutil.Now()
			for _, onTxnStart := range w.onTxnStartFns {
				if err = onTxnStart(ctx, tx); err != nil {
					return err
				}
			}
			onTxnStartDuration += timeutil.Since(startTime)
		}
		return fn(tx)
	}

	if w.txnRetries {
		return onTxnStartDuration, crdbpgx.ExecuteTx(ctx, conn, txOpts, txnFuncWithStartFuncs)
	}

	tx, err := conn.BeginTx(ctx, txOpts)
	if err != nil {
		return onTxnStartDuration, err
	}
	err = txnFuncWithStartFuncs(tx)
	if err != nil {
		return onTxnStartDuration, err
	}
	return onTxnStartDuration, tx.Commit(ctx)
}

func (w *tpcc) partitionAndScatter(urls []string) error {
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return err
	}
	defer db.Close()
	return w.partitionAndScatterWithDB(db)
}

func (w *tpcc) partitionAndScatterWithDB(db *gosql.DB) error {
	if w.partitions > 1 {
		// Ignore checking if data is actually partitioned.
		// Useful incase data is not partitioned, but in load
		// generation we want to use parition affinity.
		if w.ignorePartitions {
			return nil
		}
		// Repartitioning can take upwards of 10 minutes, so determine if
		// the dataset is already partitioned before launching the operation
		// again.
		if parts, err := partitionCount(db); err != nil {
			return errors.Wrapf(err, "could not determine if tables are partitioned")
		} else if parts == 0 {
			if err := partitionTables(db, w.zoneCfg, w.wPart, w.replicateStaticColumns); err != nil {
				return errors.Wrapf(err, "could not partition tables")
			}
		} else if parts != w.partitions {
			return errors.Errorf("tables are not partitioned %d way(s). "+
				"Pass the --partitions flag to 'workload init' or 'workload fixtures'.", w.partitions)
		}
	}

	if w.scatter {
		if err := scatterRanges(db); err != nil {
			return errors.Wrapf(err, "could not scatter ranges")
		}
	}

	return nil
}
