// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
	"github.com/spf13/pflag"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
)

type tpcc struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed             uint64
	warehouses       int
	activeWarehouses int
	nowString        []byte
	numConns         int

	idleConns int

	// Used in non-uniform random data generation. cLoad is the value of C at load
	// time. cCustomerID is the value of C for the customer id generator. cItemID
	// is the value of C for the item id generator. See 2.1.6.
	cLoad, cCustomerID, cItemID int

	mix                    string
	waitFraction           float64
	workers                int
	fks                    bool
	separateColumnFamilies bool
	// deprecatedFKIndexes adds in foreign key indexes that are no longer needed
	// due to origin index restrictions being lifted.
	deprecatedFkIndexes bool
	dbOverride          string

	counters txCounters
	txInfos  []txInfo
	// deck contains indexes into the txInfos slice.
	deck []int

	auditor *auditor

	reg *histogram.Registry

	split   bool
	scatter bool

	partitions         int
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

	usePostgres  bool
	serializable bool
	txOpts       pgx.TxOptions

	expensiveChecks bool

	replicateStaticColumns bool

	randomCIDsCache struct {
		syncutil.Mutex
		values [][]int
	}
	localsPool *sync.Pool

	// findBest is a special mode indicating that we should continue rerunning
	// workload until we find maximum performance.
	adaptiveState struct {
		// findBest is true if --find-best specified.
		// All fields below ignored unless findBest is true.
		findBest bool

		signalCompletion func()        // Stops this round of benchmarking
		duration         time.Duration // Duration as specified by --duration flag
		rampDuration     time.Duration // Ramp duration as specified by --ramp flag.

		// If we have been running for at least minDuration and the efficiency too low,
		// we will abort current run and adjust active warehouse count accordingly.
		minDuration       time.Duration
		lastAdaptiveCheck time.Time

		maxEfficiencyFound         bool
		maxWarehouseFound          bool
		minWarehouse, maxWarehouse int // active warehouse range.
	}
}

type runStat struct {
	elapsed                                   float64
	tpmC, eff, mean, p50, p90, p95, p99, p100 float64
	warehouses                                int
}

func (rs runStat) String() string {
	return fmt.Sprintf("%7.1fs %10.1f %5.1f%% %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f",
		rs.elapsed,
		rs.tpmC,
		rs.eff,
		rs.mean,
		rs.p50,
		rs.p90,
		rs.p95,
		rs.p99,
		rs.p100,
	)
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
		` using a rich schema of multiple tables`,
	Version:      `2.2.0`,
	PublicFacing: true,
	New: func() workload.Generator {
		g := &tpcc{}
		g.flags.FlagSet = pflag.NewFlagSet(`tpcc`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`db`:                       {RuntimeOnly: true},
			`mix`:                      {RuntimeOnly: true},
			`partitions`:               {RuntimeOnly: true},
			`client-partitions`:        {RuntimeOnly: true},
			`partition-affinity`:       {RuntimeOnly: true},
			`partition-strategy`:       {RuntimeOnly: true},
			`zones`:                    {RuntimeOnly: true},
			`active-warehouses`:        {RuntimeOnly: true},
			`scatter`:                  {RuntimeOnly: true},
			`serializable`:             {RuntimeOnly: true},
			`split`:                    {RuntimeOnly: true},
			`wait`:                     {RuntimeOnly: true},
			`workers`:                  {RuntimeOnly: true},
			`conns`:                    {RuntimeOnly: true},
			`idle-conns`:               {RuntimeOnly: true},
			`expensive-checks`:         {RuntimeOnly: true, CheckConsistencyOnly: true},
			`local-warehouses`:         {RuntimeOnly: true},
			`regions`:                  {RuntimeOnly: true},
			`survival-goal`:            {RuntimeOnly: true},
			`replicate-static-columns`: {RuntimeOnly: true},
			`deprecated-fk-indexes`:    {RuntimeOnly: true},
			`find-best`:                {RuntimeOnly: true},
			`adaptive-min-duration`:    {RuntimeOnly: true},
		}

		g.flags.Uint64Var(&g.seed, `seed`, 1, `Random number generator seed`)
		g.flags.IntVar(&g.warehouses, `warehouses`, 1, `Number of warehouses for loading`)
		g.flags.BoolVar(&g.fks, `fks`, true, `Add the foreign keys`)
		g.flags.BoolVar(&g.deprecatedFkIndexes, `deprecated-fk-indexes`, false, `Add deprecated foreign keys (needed when running against v20.1 or below clusters)`)

		g.flags.StringVar(&g.mix, `mix`,
			`newOrder=10,payment=10,orderStatus=1,delivery=1,stockLevel=1`,
			`Weights for the transaction mix. The default matches the TPCC spec.`)
		g.waitFraction = 1.0
		g.flags.Var(&waitSetter{&g.waitFraction}, `wait`, `Wait mode (include think/keying sleeps): 1/true for tpcc-standard wait, 0/false for no waits, other factors also allowed`)
		g.flags.StringVar(&g.dbOverride, `db`, ``,
			`Override for the SQL database to use. If empty, defaults to the generator name`)
		g.flags.IntVar(&g.workers, `workers`, 0, fmt.Sprintf(
			`Number of concurrent workers. Defaults to --warehouses * %d`, NumWorkersPerWarehouse,
		))
		g.flags.IntVar(&g.numConns, `conns`, 0, fmt.Sprintf(
			`Number of connections. Defaults to --warehouses * %d (except in nowait mode, where it defaults to --workers`,
			numConnsPerWarehouse,
		))
		g.flags.IntVar(&g.idleConns, `idle-conns`, 0, `Number of idle connections. Defaults to 0`)
		g.flags.IntVar(&g.partitions, `partitions`, 1, `Partition tables`)
		g.flags.IntVar(&g.clientPartitions, `client-partitions`, 0, `Make client behave as if the tables are partitioned, but does not actually partition underlying data. Requires --partition-affinity.`)
		g.flags.IntSliceVar(&g.affinityPartitions, `partition-affinity`, nil, `Run load generator against specific partition (requires partitions). `+
			`Note that if one value is provided, the assumption is that all urls are associated with that partition. In all other cases the assumption `+
			`is that the URLs are distributed evenly over the partitions`)
		g.flags.Var(&g.zoneCfg.strategy, `partition-strategy`, `Partition tables according to which strategy [replication, leases]`)
		g.flags.StringSliceVar(&g.zoneCfg.zones, "zones", []string{}, "Zones for legacy partitioning, the number of zones should match the number of partitions and the zones used to start cockroach. Does not work with --regions.")
		g.flags.StringSliceVar(&g.multiRegionCfg.regions, "regions", []string{}, "Regions to use for multi-region partitioning. The first region is the PRIMARY REGION. Does not work with --zones.")
		g.flags.Var(&g.multiRegionCfg.survivalGoal, "survival-goal", "Survival goal to use for multi-region setups. Allowed values: [zone, region].")
		g.flags.IntVar(&g.activeWarehouses, `active-warehouses`, 0, `Run the load generator against a specific number of warehouses. Defaults to --warehouses'`)
		g.flags.BoolVar(&g.adaptiveState.findBest, "find-best", false, `Keep running workload, increasing active warehouse count, until max warehouse, and best warehouse values are found`)
		g.flags.DurationVar(&g.adaptiveState.minDuration, "adaptive-min-duration", 5*time.Minute, "Run exploration phase for this amount of time.")
		g.flags.BoolVar(&g.scatter, `scatter`, false, `Scatter ranges`)
		g.flags.BoolVar(&g.serializable, `serializable`, false, `Force serializable mode`)
		g.flags.BoolVar(&g.split, `split`, false, `Split tables`)
		g.flags.BoolVar(&g.expensiveChecks, `expensive-checks`, false, `Run expensive checks`)
		g.flags.BoolVar(&g.separateColumnFamilies, `families`, false, `Use separate column families for dynamic and static columns`)
		g.flags.BoolVar(&g.replicateStaticColumns, `replicate-static-columns`, false, "Create duplicate indexes for all static columns in district, items and warehouse tables, such that each zone or rack has them locally.")
		g.flags.BoolVar(&g.localWarehouses, `local-warehouses`, false, `Force transactions to use a local warehouse in all cases (in violation of the TPC-C specification)`)
		g.connFlags = workload.NewConnFlags(&g.flags)

		// Hardcode this since it doesn't seem like anyone will want to change
		// it and it's really noisy in the generated fixture paths.
		g.nowString = []byte(`2006-01-02 15:04:05`)
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

func (w *tpcc) histToRunStats(startElapsed time.Duration, t histogram.Tick) runStat {
	tpmC := float64(t.Cumulative.TotalCount()) / startElapsed.Seconds() * 60
	return runStat{
		elapsed:    startElapsed.Seconds(),
		tpmC:       tpmC,
		eff:        100 * tpmC / (SpecWarehouseFactor * float64(w.activeWarehouses)),
		mean:       time.Duration(t.Cumulative.Mean()).Seconds() * 1000,
		p50:        time.Duration(t.Cumulative.ValueAtQuantile(50)).Seconds() * 1000,
		p90:        time.Duration(t.Cumulative.ValueAtQuantile(90)).Seconds() * 1000,
		p95:        time.Duration(t.Cumulative.ValueAtQuantile(95)).Seconds() * 1000,
		p99:        time.Duration(t.Cumulative.ValueAtQuantile(99)).Seconds() * 1000,
		p100:       time.Duration(t.Cumulative.ValueAtQuantile(100)).Seconds() * 1000,
		warehouses: w.activeWarehouses,
	}
}

const totalHeader = "\n_elapsed_______tpmC____efc__avg(ms)__p50(ms)__p90(ms)__p95(ms)__p99(ms)_pMax(ms)"
const adaptiveHeader = totalHeader + "___aWH"
const newOrderName = `newOrder`

// adaptiveTick is invoked by workload if tpcc runs under --find-best mode.
// This function periodically analyzes results so far, and if they are not what
// we expect them to be, the current workload run is aborted, and a new one with adjusted
// active warehouse count is started.
func (w *tpcc) adaptiveTick(startElapsed time.Duration, t histogram.Tick) {
	// Only care about new order table for the purpose of efficiency and tpmC calculation.
	if t.Name != newOrderName {
		return
	}

	// We ran longer than what was requested -- time to stop this run.
	if startElapsed > w.adaptiveState.duration+w.adaptiveState.rampDuration {
		log.Infof(context.Background(), "Completing workload: time expired")
		w.adaptiveState.signalCompletion()
		return
	}

	// Not going to bother checking anything unless we ran for at least ramp time
	// and adaptive min duration.
	if startElapsed < w.adaptiveState.rampDuration+w.adaptiveState.minDuration {
		return
	}

	// Don't care to recompute tpmC unless we ran for at least a minute since last check.
	if timeutil.Since(w.adaptiveState.lastAdaptiveCheck) < time.Minute {
		return
	}

	// Must have some data collected; we'll be checking 90th percentile
	// below, so require at least 10 values.
	if t.Cumulative.TotalCount() < 10 {
		return
	}

	w.adaptiveState.lastAdaptiveCheck = timeutil.Now()

	// Terminate this run if observed efficiency too low, or latency too high.
	rs := w.histToRunStats(startElapsed, t)
	if rs.eff < 80 || rs.p90 > 10000 /* 10 sec */ {
		fmt.Printf("Restarting run for active warehouse %d: eff=%f, p90(ms)=%f\n",
			w.activeWarehouses, rs.eff, rs.p90)
		w.adaptiveState.signalCompletion()
		return
	}

	fmt.Println(adaptiveHeader)
	fmt.Printf("%s %9d\n\n", rs, rs.warehouses)
}

func (w *tpcc) preRunConfiguration(retry bool) {
	if w.numConns == 0 || retry {
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

	if w.workers == 0 || retry {
		w.workers = w.activeWarehouses * NumWorkersPerWarehouse
	}

	w.auditor = newAuditor(w.activeWarehouses)
}

// Hooks implements the Hookser interface.
func (w *tpcc) Hooks() workload.Hooks {
	var setSignalCompletion func(time.Duration, time.Duration, func()) error
	var tickHook func(startElapsed time.Duration, t histogram.Tick)
	if w.adaptiveState.findBest {
		setSignalCompletion = func(duration time.Duration, rampDuration time.Duration, f func()) error {
			if duration == 0 {
				return errors.AssertionFailedf("adaptive mode requires --duration")
			}
			w.adaptiveState.duration = duration
			w.adaptiveState.rampDuration = rampDuration
			var signalOnce sync.Once
			w.adaptiveState.signalCompletion = func() {
				signalOnce.Do(f)
			}
			return nil
		}
		tickHook = w.adaptiveTick
	}

	return workload.Hooks{
		Validate: func() error {
			if w.warehouses < 1 {
				return errors.Errorf(`--warehouses must be positive`)
			}

			if w.adaptiveState.findBest {
				if w.activeWarehouses == 0 {
					w.activeWarehouses = w.warehouses / 2
				}
				if w.activeWarehouses == w.warehouses {
					return errors.Errorf(`--active-warehouses should not be equal to --warehouses with --find-best mode`)
				}

				// We'll kick off search for max efficiency configuration.
				w.adaptiveState.minWarehouse = 1
				w.adaptiveState.maxWarehouse = w.warehouses
				fmt.Printf("Starting adaptive workload from %d active warehouses\n", w.activeWarehouses)
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
			w.preRunConfiguration(false)

			if w.waitFraction > 0 && w.workers != w.activeWarehouses*NumWorkersPerWarehouse {
				return errors.Errorf(`--wait > 0 and --warehouses=%d requires --workers=%d`,
					w.activeWarehouses, w.warehouses*NumWorkersPerWarehouse)
			}

			if w.serializable {
				w.txOpts = pgx.TxOptions{IsoLevel: pgx.Serializable}
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
				// The first region is the PRIMARY region.
				if i == 0 {
					stmt = fmt.Sprintf(`alter database %s set primary region %q`, dbName, region)
				} else {
					// Region additions should be idempotent.
					if _, ok := regions[region]; ok {
						continue
					}
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
		PostLoad: func(db *gosql.DB) error {
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
			fmt.Println(totalHeader)

			var finalRunStat runStat
			w.reg.Tick(func(t histogram.Tick) {
				if newOrderName == t.Name {
					finalRunStat = w.histToRunStats(startElapsed, t)
					fmt.Println(finalRunStat)
				}
			})

			if w.adaptiveState.findBest {
				return w.maybeRetryToFindBest(finalRunStat, startElapsed)
			}
			return nil
		},
		CheckConsistency: func(ctx context.Context, db *gosql.DB) error {
			for _, check := range AllChecks() {
				if !w.expensiveChecks && check.Expensive {
					continue
				}
				start := timeutil.Now()
				err := check.Fn(db, "" /* asOfSystemTime */)
				log.Infof(ctx, `check %s took %s`, check.Name, timeutil.Since(start))
				if err != nil {
					return errors.Wrapf(err, `check failed: %s`, check.Name)
				}
			}
			return nil
		},
		CompletionHook: setSignalCompletion,
		TickHook:       tickHook,
	}
}

func (w *tpcc) maybeRetryToFindBest(rs runStat, ranFor time.Duration) error {
	runCompleted := ranFor >= w.adaptiveState.duration

	adjustActiveRange := func(min, max int) int {
		if min < 1 {
			min = 1
		}
		if max > w.warehouses {
			max = w.warehouses
		}

		w.adaptiveState.minWarehouse, w.adaptiveState.maxWarehouse = min, max
		return (max - min + 1) / 2
	}

	// adjustActive adjust active warehouse count.
	restartWorkload := func(newActive int) error {
		if newActive == 0 {
			return errors.Errorf("cannot restart workload with 0 active warehouses")
		}

		if newActive > w.warehouses {
			newActive = w.warehouses
		}

		oldActive := w.activeWarehouses
		if runCompleted && oldActive == newActive {
			fmt.Printf("Failed to adjust warehouse counts -- old and new values are the same.\n" +
				"This likely means that the max warehouse count is not large enough.\n" +
				"Stopping dynamic workload.\n")
		}
		w.activeWarehouses = newActive
		w.preRunConfiguration(true)

		fmt.Printf("Restarting  TPCC run with active warehouse count adjusted from %d to %d\n",
			oldActive, w.activeWarehouses)
		return workload.ErrRetryWorkload
	}

	const maxTPCCEffThreshold float64 = 96      // Efficiency threshold for the purpose of calculating max tpmC.
	const maxWarehouseEffThreshold float64 = 87 // Efficiency below this value considered failure.
	const effErrMargin float64 = 3
	const smallIncrementFactor float64 = 1.1 // 10% increase in warehouses.

	if runCompleted && rs.eff > maxTPCCEffThreshold-effErrMargin {
		// We are done looking for max efficiency.  Switch to finding max warehouse.
		fmt.Println(adaptiveHeader + "__maxTPMC")
		fmt.Printf("%s %9d\n", rs, w.activeWarehouses)
		w.adaptiveState.maxEfficiencyFound = true

		// When we find max efficiency candidate, try adjusting active count by small increment
		// just in case we can push a bit more before efficiency starts falling.
		return restartWorkload(int(smallIncrementFactor * float64(w.activeWarehouses+1)))
	}

	if !w.adaptiveState.maxEfficiencyFound {
		// We haven't discovered max warehouse -- efficiency too low.
		return restartWorkload(adjustActiveRange(1, w.activeWarehouses))
	}

	// We are looking for max warehouse.
	if runCompleted && rs.eff > maxWarehouseEffThreshold-effErrMargin {
		// We are done looking for max warehouse
		// (note: we require eff > maxWarehouseEffThreshold; anything lower is a failed run).
		fmt.Println(adaptiveHeader + "__maxWh")
		fmt.Printf("%s %9d\n", rs, w.activeWarehouses)

		w.adaptiveState.maxWarehouseFound = true

		// When we find max warehouse candidate, try adjusting active count by small increment
		// just in case we can push a bit more before we fail.
		return restartWorkload(int(smallIncrementFactor * float64(w.activeWarehouses+1)))
	}

	if w.adaptiveState.maxWarehouseFound {
		return nil
	}

	// Not done yet with max warehouse search.
	return restartWorkload(adjustActiveRange(w.adaptiveState.minWarehouse, w.activeWarehouses))
}

// Tables implements the Generator interface.
func (w *tpcc) Tables() []workload.Table {
	aCharsInit := workloadimpl.PrecomputedRandInit(rand.New(rand.NewSource(w.seed)), precomputedLength, aCharsAlphabet)
	lettersInit := workloadimpl.PrecomputedRandInit(rand.New(rand.NewSource(w.seed)), precomputedLength, lettersAlphabet)
	numbersInit := workloadimpl.PrecomputedRandInit(rand.New(rand.NewSource(w.seed)), precomputedLength, numbersAlphabet)
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

	if w.counters == nil {
		w.counters = setupTPCCMetrics(reg.Registerer())
	}

	sqlDatabase, err := workload.SanitizeUrls(w, w.dbOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	parsedURL, err := url.Parse(urls[0])
	if err != nil {
		return workload.QueryLoad{}, err
	}

	w.reg = reg
	w.usePostgres = parsedURL.Port() == "5432"

	// We can't use a single MultiConnPool because we want to implement partition
	// affinity. Instead we have one MultiConnPool per server.
	cfg := workload.MultiConnPoolCfg{
		MaxTotalConnections: (w.numConns + len(urls) - 1) / len(urls), // round up
		// Limit the number of connections per pool (otherwise preparing statements
		// at startup can be slow).
		MaxConnsPerPool: 50,
	}
	fmt.Printf("Initializing %d connections...\n", w.numConns)

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
		for i, db := range dbs {
			p := i % w.partitions
			partitionDBs[p] = append(partitionDBs[p], db)
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
	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
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
	// Limit the amount of workers we initialize in parallel, to avoid running out
	// of memory (#36897).
	sem := make(chan struct{}, 100)
	for workerIdx := 0; workerIdx < w.workers; workerIdx++ {
		workerIdx := workerIdx
		var warehouse int
		var p int
		if len(w.multiRegionCfg.regions) == 0 {
			warehouse = w.wPart.totalElems[workerIdx%len(w.wPart.totalElems)]
			p = w.wPart.partElemsMap[warehouse]
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
			worker, err := newWorker(ctx, w, db, reg.GetHandle(), w.counters, warehouse)
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
	ql.Close = func(context context.Context) {
		for _, conn := range conns {
			if err := conn.Close(ctx); err != nil {
				log.Warningf(ctx, "%v", err)
			}
		}
	}
	return ql, nil
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
