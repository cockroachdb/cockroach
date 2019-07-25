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
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
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
	interleaved      bool
	nowString        []byte
	numConns         int

	// Used in non-uniform random data generation. cLoad is the value of C at load
	// time. cCustomerID is the value of C for the customer id generator. cItemID
	// is the value of C for the item id generator. See 2.1.6.
	cLoad, cCustomerID, cItemID int

	mix        string
	doWaits    bool
	workers    int
	fks        bool
	dbOverride string

	txInfos []txInfo
	// deck contains indexes into the txInfos slice.
	deck []int

	auditor *auditor

	reg *histogram.Registry

	split   bool
	scatter bool

	partitions        int
	affinityPartition int
	wPart             *partitioner
	zoneCfg           zoneConfig

	usePostgres  bool
	serializable bool
	txOpts       *pgx.TxOptions

	expensiveChecks bool

	randomCIDsCache struct {
		syncutil.Mutex
		values [][]int
	}
	localsPool *sync.Pool
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
	Version:      `2.1.0`,
	PublicFacing: true,
	New: func() workload.Generator {
		g := &tpcc{}
		g.flags.FlagSet = pflag.NewFlagSet(`tpcc`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`db`:                 {RuntimeOnly: true},
			`mix`:                {RuntimeOnly: true},
			`partitions`:         {RuntimeOnly: true},
			`partition-affinity`: {RuntimeOnly: true},
			`partition-strategy`: {RuntimeOnly: true},
			`zones`:              {RuntimeOnly: true},
			`active-warehouses`:  {RuntimeOnly: true},
			`scatter`:            {RuntimeOnly: true},
			`serializable`:       {RuntimeOnly: true},
			`split`:              {RuntimeOnly: true},
			`wait`:               {RuntimeOnly: true},
			`workers`:            {RuntimeOnly: true},
			`conns`:              {RuntimeOnly: true},
			`expensive-checks`:   {RuntimeOnly: true, CheckConsistencyOnly: true},
		}

		g.flags.Uint64Var(&g.seed, `seed`, 1, `Random number generator seed`)
		g.flags.IntVar(&g.warehouses, `warehouses`, 1, `Number of warehouses for loading`)
		g.flags.BoolVar(&g.fks, `fks`, true, `Add the foreign keys`)
		g.flags.BoolVar(&g.interleaved, `interleaved`, false, `Use interleaved tables`)

		g.flags.StringVar(&g.mix, `mix`,
			`newOrder=10,payment=10,orderStatus=1,delivery=1,stockLevel=1`,
			`Weights for the transaction mix. The default matches the TPCC spec.`)
		g.flags.BoolVar(&g.doWaits, `wait`, true, `Run in wait mode (include think/keying sleeps)`)
		g.flags.StringVar(&g.dbOverride, `db`, ``,
			`Override for the SQL database to use. If empty, defaults to the generator name`)
		g.flags.IntVar(&g.workers, `workers`, 0, fmt.Sprintf(
			`Number of concurrent workers. Defaults to --warehouses * %d`, numWorkersPerWarehouse,
		))
		g.flags.IntVar(&g.numConns, `conns`, 0, fmt.Sprintf(
			`Number of connections. Defaults to --warehouses * %d (except in nowait mode, where it defaults to --workers`,
			numConnsPerWarehouse,
		))
		g.flags.IntVar(&g.partitions, `partitions`, 1, `Partition tables`)
		g.flags.IntVar(&g.affinityPartition, `partition-affinity`, -1, `Run load generator against specific partition (requires partitions)`)
		g.flags.Var(&g.zoneCfg.strategy, `partition-strategy`, `Partition tables according to which strategy [replication, leases]`)
		g.flags.StringSliceVar(&g.zoneCfg.zones, "zones", []string{}, "Zones for partitioning, the number of zones should match the number of partitions and the zones used to start cockroach.")
		g.flags.IntVar(&g.activeWarehouses, `active-warehouses`, 0, `Run the load generator against a specific number of warehouses. Defaults to --warehouses'`)
		g.flags.BoolVar(&g.scatter, `scatter`, false, `Scatter ranges`)
		g.flags.BoolVar(&g.serializable, `serializable`, false, `Force serializable mode`)
		g.flags.BoolVar(&g.split, `split`, false, `Split tables`)
		g.flags.BoolVar(&g.expensiveChecks, `expensive-checks`, false, `Run expensive checks`)
		g.connFlags = workload.NewConnFlags(&g.flags)

		// Hardcode this since it doesn't seem like anyone will want to change
		// it and it's really noisy in the generated fixture paths.
		g.nowString = []byte(`2006-01-02 15:04:05`)
		return g
	},
}

// Meta implements the Generator interface.
func (*tpcc) Meta() workload.Meta { return tpccMeta }

// Flags implements the Flagser interface.
func (w *tpcc) Flags() workload.Flags { return w.flags }

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

			if w.affinityPartition < -1 {
				return errors.Errorf(`if specified, --partition-affinity should be greater than or equal to 0`)
			} else if w.affinityPartition >= w.partitions {
				return errors.Errorf(`--partition-affinity out of bounds of --partitions`)
			}

			if len(w.zoneCfg.zones) > 0 && (len(w.zoneCfg.zones) != w.partitions) {
				return errors.Errorf(`--zones should have the sames length as --partitions.`)
			}

			w.initNonUniformRandomConstants()

			if w.workers == 0 {
				w.workers = w.activeWarehouses * numWorkersPerWarehouse
			}

			if w.numConns == 0 {
				// If we're not waiting, open up a connection for each worker. If we are
				// waiting, we only use up to a set number of connections per warehouse.
				// This isn't mandated by the spec, but opening a connection per worker
				// when they each spend most of their time waiting is wasteful.
				if !w.doWaits {
					w.numConns = w.workers
				} else {
					w.numConns = w.activeWarehouses * numConnsPerWarehouse
				}
			}

			if w.doWaits && w.workers != w.activeWarehouses*numWorkersPerWarehouse {
				return errors.Errorf(`--wait=true and --warehouses=%d requires --workers=%d`,
					w.activeWarehouses, w.warehouses*numWorkersPerWarehouse)
			}

			if w.serializable {
				w.txOpts = &pgx.TxOptions{IsoLevel: pgx.Serializable}
			}

			w.auditor = newAuditor(w.warehouses)

			// Create a partitioner to help us partition the warehouses. The base-case is
			// where w.warehouses == w.activeWarehouses and w.partitions == 1.
			var err error
			w.wPart, err = makePartitioner(w.warehouses, w.activeWarehouses, w.partitions)
			if err != nil {
				return errors.Wrap(err, "error creating partitioner")
			}

			return initializeMix(w)
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
						// If the statement failed because the fk already exists,
						// ignore it. Return the error for any other reason.
						const duplFKErr = "columns cannot be used by multiple foreign key constraints"
						if !strings.Contains(err.Error(), duplFKErr) {
							return err
						}
					}
				}
			}

			return w.partitionAndScatterWithDB(db)
		},
		PostRun: func(startElapsed time.Duration) error {
			w.auditor.runChecks()
			const totalHeader = "\n_elapsed_______tpmC____efc__avg(ms)__p50(ms)__p90(ms)__p95(ms)__p99(ms)_pMax(ms)"
			fmt.Println(totalHeader)

			const newOrderName = `newOrder`
			w.reg.Tick(func(t histogram.Tick) {
				if newOrderName == t.Name {
					tpmC := float64(t.Cumulative.TotalCount()) / startElapsed.Seconds() * 60
					fmt.Printf("%7.1fs %10.1f %5.1f%% %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
						startElapsed.Seconds(),
						tpmC,
						100*tpmC/(SpecWarehouseFactor*float64(w.activeWarehouses)),
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
				start := timeutil.Now()
				err := check.Fn(db, "" /* asOfSystemTime */)
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
		Name:   `warehouse`,
		Schema: tpccWarehouseSchema,
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
	}
	district := workload.Table{
		Name: `district`,
		Schema: maybeAddInterleaveSuffix(
			w.interleaved,
			tpccDistrictSchemaBase,
			tpccDistrictSchemaInterleaveSuffix,
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
	}
	customer := workload.Table{
		Name: `customer`,
		Schema: maybeAddInterleaveSuffix(
			w.interleaved,
			tpccCustomerSchemaBase,
			tpccCustomerSchemaInterleaveSuffix,
		),
		InitialRows: workload.BatchedTuples{
			NumBatches: numCustomersPerWarehouse * w.warehouses,
			FillBatch:  w.tpccCustomerInitialRowBatch,
		},
		Stats: w.tpccCustomerStats(),
	}
	history := workload.Table{
		Name: `history`,
		Schema: maybeAddFkSuffix(
			w.fks,
			tpccHistorySchemaBase,
			tpccHistorySchemaFkSuffix,
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
	}
	order := workload.Table{
		Name: `order`,
		Schema: maybeAddInterleaveSuffix(
			w.interleaved,
			tpccOrderSchemaBase,
			tpccOrderSchemaInterleaveSuffix,
		),
		InitialRows: workload.BatchedTuples{
			NumBatches: numOrdersPerWarehouse * w.warehouses,
			FillBatch:  w.tpccOrderInitialRowBatch,
		},
	}
	newOrder := workload.Table{
		Name:   `new_order`,
		Schema: tpccNewOrderSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: numNewOrdersPerWarehouse * w.warehouses,
			FillBatch:  w.tpccNewOrderInitialRowBatch,
		},
		Stats: w.tpccNewOrderStats(),
	}
	item := workload.Table{
		Name:   `item`,
		Schema: tpccItemSchema,
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
		Schema: maybeAddInterleaveSuffix(
			w.interleaved,
			maybeAddFkSuffix(
				w.fks,
				tpccStockSchemaBase,
				tpccStockSchemaFkSuffix,
			),
			tpccStockSchemaInterleaveSuffix,
		),
		InitialRows: workload.BatchedTuples{
			NumBatches: numStockPerWarehouse * w.warehouses,
			FillBatch:  w.tpccStockInitialRowBatch,
		},
		Stats: w.tpccStockStats(),
	}
	orderLine := workload.Table{
		Name: `order_line`,
		Schema: maybeAddInterleaveSuffix(
			w.interleaved,
			maybeAddFkSuffix(
				w.fks,
				tpccOrderLineSchemaBase,
				tpccOrderLineSchemaFkSuffix,
			),
			tpccOrderLineSchemaInterleaveSuffix,
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
func (w *tpcc) Ops(urls []string, reg *histogram.Registry) (workload.QueryLoad, error) {
	// It would be nice to remove the need for this and to require that
	// partitioning and scattering occurs only when the PostLoad hook is
	// run, but to maintain backward compatibility, it's easiest to allow
	// partitioning and scattering during `workload run`.
	if err := w.partitionAndScatter(urls); err != nil {
		return workload.QueryLoad{}, err
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
			dbs[i], err = workload.NewMultiConnPool(cfg, urls[i])
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return workload.QueryLoad{}, err
	}

	// Assign each DB connection pool to a local partition. This assumes that
	// dbs[i] is a machine that holds partition "i % *partitions". If we have an
	// affinity partition, all connections will be for the same partition.
	partitionDBs := make([][]*workload.MultiConnPool, w.partitions)
	if w.affinityPartition >= 0 {
		// All connections are for our local partition.
		partitionDBs[w.affinityPartition] = dbs
	} else {
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

	fmt.Printf("Initializing %d workers and preparing statements...\n", w.workers)
	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	ql.WorkerFns = make([]func(context.Context) error, w.workers)
	var group errgroup.Group
	// Limit the amount of workers we initialize in parallel, to avoid running out
	// of memory (#36897).
	sem := make(chan struct{}, 100)
	for workerIdx := range ql.WorkerFns {
		workerIdx := workerIdx
		warehouse := w.wPart.totalElems[workerIdx%len(w.wPart.totalElems)]

		p := w.wPart.partElemsMap[warehouse]
		if w.affinityPartition >= 0 && w.affinityPartition != p {
			// This isn't part of our local partition.
			continue
		}
		dbs := partitionDBs[p]
		db := dbs[warehouse%len(dbs)]

		sem <- struct{}{}
		group.Go(func() error {
			worker, err := newWorker(context.TODO(), w, db, reg.GetHandle(), warehouse)
			if err == nil {
				ql.WorkerFns[workerIdx] = worker.run
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
			if err := partitionTables(db, w.zoneCfg, w.wPart); err != nil {
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
