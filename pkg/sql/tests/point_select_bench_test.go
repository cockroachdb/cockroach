// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/codahale/hdrhistogram"
	"github.com/jackc/pgx/v5"
)

// pointSelectFastPathEnvVar enables the experimental
// sql.fast_path.point_select.enabled cluster setting in the benchmark
// when set to a truthy value (1, true, on, yes — any non-empty value
// other than 0/false/off/no). Default behavior (env var unset or
// empty) leaves the fast path off, matching the unspecialized
// baseline.
const pointSelectFastPathEnvVar = "POINT_SELECT_FAST_PATH"

// pointSelectNodesEnvVar controls how many nodes the in-process test
// cluster uses (default 1). Multi-node setups expose a more realistic
// KV layer: per-range cache entries and per-peer gRPC adapters are
// shared across fewer goroutines, which reduces the lock contention
// that dominated the single-node profile we captured first. Use 3 to
// approximate a small production cluster.
const pointSelectNodesEnvVar = "POINT_SELECT_NODES"

// pointSelectSplitsEnvVar controls how many ranges the kv table is
// split into before the run. With multiple nodes, splits are what
// allow leases (and therefore KV work) to distribute across nodes —
// a single range stays on one leaseholder no matter how many nodes
// exist. Defaults to nodes*16 when nodes > 1, or 0 (no splits) when
// nodes = 1.
const pointSelectSplitsEnvVar = "POINT_SELECT_SPLITS"

// pointSelectPlaceholderFastPathEnvVar leaves the optimizer's
// existing placeholder_fast_path enabled (production default) when
// set to a truthy value. Default: disabled, matching the
// "unspecialized SQL pipeline" baseline used for the headline
// measurement.
//
// Comparing baseline vs. placeholder_fast_path-on-only tells us
// how much of the headline win is already capturable by extending
// CRDB's existing optimizer-output caching to more cases, vs. how
// much requires the deeper exec-pipeline rewrite our hardcoded
// fast path represents.
const pointSelectPlaceholderFastPathEnvVar = "POINT_SELECT_PLACEHOLDER_FAST_PATH"

// BenchmarkPointSelect measures end-to-end p50/p99 latency of a prepared
// point-SELECT against a primary key, swept across several concurrency
// levels.
//
// This is the baseline for the SQL-processing-overhead experiment in
// experiments/point-select-fast-path/. Each sub-benchmark spawns the
// requested number of pgx clients, each holding its own prepared
// statement, and records per-op latency into an hdrhistogram. The merged
// histogram's p50 and p99 are reported as Go benchmark metrics named
// `p50_us` and `p99_us`.
//
// The cluster is single-node, in-process, with the same test knobs the
// sysbench microbench uses (see newTestCluster) so the results are as
// isolated from network/Raft/lease-queue noise as practical.
const (
	pointSelectTotalRows  = 100_000
	pointSelectWarmupOps  = 1000
	pointSelectMinLatency = 1_000          // 1 µs in ns
	pointSelectMaxLatency = 10_000_000_000 // 10 s in ns
)

// pointSelectSchema describes one of the table+query shapes we
// benchmark. The default benchmark uses pointSelectSchemaPK; the
// secondary-index variant uses pointSelectSchemaSecondaryIndex.
//
// All shapes use INT8 keys (so the lookup placeholder is always an
// int64) and a BYTES value column, so the benchmark client code
// stays identical across schemas.
type pointSelectSchema struct {
	// name is used as the dataset's logical identifier in logs.
	name string
	// tableName is the unqualified name of the table created in
	// defaultdb. Used for the SELECT statement and the COPY target.
	tableName string
	// tableDDL is the CREATE TABLE statement.
	tableDDL string
	// populateCols is the column list passed to pgx CopyFrom.
	populateCols []string
	// populateRow is called once per row to produce the values for
	// populateCols. Receives the row index in [0, pointSelectTotalRows).
	populateRow func(i int) []any
	// stmt is the prepared SELECT each client runs. Must take a
	// single $1 placeholder of type INT8.
	stmt string
	// postSetupSQL, if non-empty, is executed after the primary
	// populate (and after ANALYZE). Used by multi-table schemas to
	// populate additional tables. May be a sequence of statements
	// separated by `;`.
	postSetupSQL string
}

var pointSelectSchemaPK = pointSelectSchema{
	name:         "pk",
	tableName:    "kv",
	tableDDL:     `CREATE TABLE kv (k INT8 PRIMARY KEY, v BYTES)`,
	populateCols: []string{"k", "v"},
	populateRow: func(i int) []any {
		return []any{int64(i), []byte(fmt.Sprintf("v-%013d", i))}
	},
	stmt: `SELECT v FROM kv WHERE k = $1`,
}

// pointSelectSchemaJoin tests the post-optimization-pipeline savings
// on a two-table lookup join: WHERE hits a unique secondary index on
// table 1, the result of which is joined on PK to a row in table 2
// (via a foreign key column on table 1). Three KV Gets total —
// secondary index on first, primary row on first (to read fk_id),
// primary row on second.
//
// Both tables use the same population pattern (id == k == fk_id ==
// the row index) so the FK relationship is trivially 1:1; that
// keeps the schema simple without changing the KV-cost shape.
var pointSelectSchemaJoin = pointSelectSchema{
	name:      "join",
	tableName: "first",
	tableDDL: `CREATE TABLE first (
		id    INT8 PRIMARY KEY,
		k     INT8 NOT NULL,
		fk_id INT8 NOT NULL,
		UNIQUE INDEX first_k (k)
	); CREATE TABLE second (
		id INT8 PRIMARY KEY,
		v  BYTES
	)`,
	populateCols: []string{"id", "k", "fk_id"},
	populateRow: func(i int) []any {
		return []any{int64(i), int64(i), int64(i)}
	},
	stmt: `SELECT s.v FROM first f JOIN second s ON f.fk_id = s.id WHERE f.k = $1`,
	postSetupSQL: `INSERT INTO second
		SELECT i, ('v-' || lpad(i::STRING, 13, '0'))::BYTES
		FROM generate_series(0, 99999) i;
		ANALYZE second`,
}

// pointSelectSchemaSecondaryIndex tests whether the structural
// per-execute savings we measured for the PK lookup also appear when
// the lookup goes through a *non-covering* unique secondary index.
// The selected column `v` is NOT stored in the index, so satisfying
// the query requires two KV Gets: one against the secondary index to
// translate the lookup key into a PK, then one against the primary
// row to fetch the value column. This is the common production
// shape — most secondary indexes don't STORE every column callers
// might select. The placeholder fast path doesn't apply here either
// (it's PK-only), so the slow-path baseline is the unspecialized
// pipeline by construction.
var pointSelectSchemaSecondaryIndex = pointSelectSchema{
	name:      "secondary_index",
	tableName: "kv_idx",
	tableDDL: `CREATE TABLE kv_idx (
		pk_id INT8 PRIMARY KEY,
		k     INT8 NOT NULL,
		v     BYTES,
		UNIQUE INDEX kv_idx_k (k)
	)`,
	populateCols: []string{"pk_id", "k", "v"},
	populateRow: func(i int) []any {
		// pk_id and k are independently unique; using the same value
		// keeps populate trivial and keeps the row count identical
		// to the PK schema.
		return []any{int64(i), int64(i), []byte(fmt.Sprintf("v-%013d", i))}
	},
	stmt: `SELECT v FROM kv_idx WHERE k = $1`,
}

// pointSelectConcurrencies is the concurrency sweep. Held constant across
// runs so results are comparable. Order is ascending so warm caches build
// up gradually.
var pointSelectConcurrencies = []int{1, 8, 16, 32, 64, 128}

// pointSelectDriver owns the in-process test cluster, populated
// table, and per-node pgx URLs used to mint client connections.
// Reused across all concurrency sub-benchmarks within a single -count
// iteration. When the cluster has more than one node, client
// connections are distributed round-robin across `pgURLs` so the
// pgwire-side load (gateway connExecutor goroutines, pgwire socket
// dispatch) is also spread across nodes, not just KV.
type pointSelectDriver struct {
	ctx     context.Context
	pgURLs  []url.URL
	stmt    string // prepared SELECT this driver's clients run
	cleanup func()
}

// newPointSelectDriver starts an in-process test cluster (size
// controlled by POINT_SELECT_NODES, default 1), creates the schema's
// table, pre-populates it with pointSelectTotalRows rows, and (when
// the cluster has more than one node) splits the table into
// POINT_SELECT_SPLITS ranges and scatters them so leases distribute
// across nodes.
//
// The cluster is started with Insecure: true so pgwire connections skip
// TLS. We profiled the secure variant first and found that ~75% of
// CPU samples landed in syscall/TLS code at saturation, drowning out
// the SQL-stack work this experiment is meant to measure. TLS overhead
// is a real but separate workstream; for the SQL-overhead experiment
// we want the cleaner signal.
func newPointSelectDriver(b *testing.B, schema pointSelectSchema) *pointSelectDriver {
	b.Helper()
	ctx := context.Background()

	nodes := pointSelectIntFromEnv(pointSelectNodesEnvVar, 1)
	if nodes < 1 {
		b.Fatalf("POINT_SELECT_NODES must be >= 1, got %d", nodes)
	}
	defaultSplits := 0
	if nodes > 1 {
		defaultSplits = nodes * 16
	}
	splits := pointSelectIntFromEnv(pointSelectSplitsEnvVar, defaultSplits)
	if splits < 0 {
		b.Fatalf("POINT_SELECT_SPLITS must be >= 0, got %d", splits)
	}

	st := cluster.MakeTestingClusterSettings()
	disableBackgroundWork(st)
	// Disable the optimizer's placeholder fast path so the baseline
	// reflects the cost of running the full optimizer + execbuilder on
	// each prepared-statement execute. We are estimating the upper-bound
	// benefit of a hypothetical universal fast path; comparing against
	// today's already-specialized point-select path would understate that
	// upper bound, since the existing fast path already collapses
	// re-optimization for this exact query shape into a no-op. See
	// experiments/point-select-fast-path/PLAN.md for context.
	//
	// POINT_SELECT_PLACEHOLDER_FAST_PATH=1 leaves it on (production
	// default) so we can directly measure how much of the headline
	// win the existing optimizer caching already captures.
	if !pointSelectBoolFromEnv(pointSelectPlaceholderFastPathEnvVar) {
		xform.PlaceholderFastPathEnabled.Override(ctx, &st.SV, false)
	}
	// Enable the experimental hardcoded fast path when requested via env
	// var. The variant run is what the experiment compares against the
	// (above) unspecialized baseline.
	if pointSelectFastPathEnabledFromEnv() {
		sql.PointSelectFastPathEnabled.Override(ctx, &st.SV, true)
	}
	tc := serverutils.StartCluster(b, nodes, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings:  st,
			Insecure:  true,
			CacheSize: 2 * 1024 * 1024 * 1024,
			Knobs: base.TestingKnobs{
				// Enable the local-RPC fast path so in-process KV calls
				// don't pay full TCP serialization. Same setup as the
				// sysbench microbench (newTestCluster with
				// localRPCFastPath=true).
				DialerKnobs: nodedialer.DialerTestingKnobs{
					TestingNoLocalClientOptimization: false,
				},
				Server: &server.TestingKnobs{
					ContextTestingKnobs: rpc.ContextTestingKnobs{
						NoLoopbackDialer: false,
					},
				},
				Store: &kvserver.StoreTestingKnobs{
					// Prevents lease-rebalance noise across short-lived
					// benchmark runs. SCATTER below explicitly redistributes
					// leases at setup time.
					DisableLeaseQueue: true,
				},
			},
		},
	})
	for i := 0; i < nodes; i++ {
		tc.Server(i).SQLServer().(*sql.Server).GetExecutorConfig().LicenseEnforcer.Disable(ctx)
	}
	if nodes > 1 {
		if err := tc.WaitForFullReplication(); err != nil {
			b.Fatalf("wait for full replication: %v", err)
		}
	}

	// Capture pgURLs for every node. The setup work below uses node 0;
	// the benchmark distributes client connections across all nodes.
	pgURLs := make([]url.URL, nodes)
	cleanupURLFns := make([]func(), nodes)
	for i := 0; i < nodes; i++ {
		pgURLs[i], cleanupURLFns[i] = tc.ApplicationLayer(i).PGUrl(b, serverutils.DBName("defaultdb"))
	}

	setupConn, err := pgx.Connect(ctx, pgURLs[0].String())
	if err != nil {
		b.Fatalf("connect for setup: %v", err)
	}
	defer func() { _ = setupConn.Close(ctx) }()

	if _, err := setupConn.Exec(ctx, schema.tableDDL); err != nil {
		b.Fatalf("create table: %v", err)
	}

	// Populate via COPY for speed. Values are uniform 16-byte payloads so
	// pgwire row-encoding cost is the same on every read.
	rows := pgx.CopyFromSlice(pointSelectTotalRows, func(i int) ([]any, error) {
		return schema.populateRow(i), nil
	})
	if _, err := setupConn.CopyFrom(ctx, pgx.Identifier{schema.tableName}, schema.populateCols, rows); err != nil {
		b.Fatalf("populate: %v", err)
	}
	if _, err := setupConn.Exec(ctx, fmt.Sprintf(`ANALYZE %s`, schema.tableName)); err != nil {
		b.Fatalf("analyze: %v", err)
	}
	if schema.postSetupSQL != "" {
		if _, err := setupConn.Exec(ctx, schema.postSetupSQL); err != nil {
			b.Fatalf("postSetupSQL: %v", err)
		}
	}

	if splits > 0 {
		// Split at evenly-spaced points across the populated key range.
		// We aim for `splits` split points, which produces splits+1
		// ranges. SCATTER then redistributes those ranges' leases.
		var sb strings.Builder
		fmt.Fprintf(&sb, `ALTER TABLE %s SPLIT AT VALUES `, schema.tableName)
		for i := 0; i < splits; i++ {
			if i > 0 {
				sb.WriteString(", ")
			}
			// Evenly-spaced split point. With pointSelectTotalRows=100k
			// and splits=48, this yields split points at 2040, 4081, ...
			point := int64((i + 1) * pointSelectTotalRows / (splits + 1))
			fmt.Fprintf(&sb, "(%d)", point)
		}
		if _, err := setupConn.Exec(ctx, sb.String()); err != nil {
			b.Fatalf("split: %v", err)
		}
		if _, err := setupConn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s SCATTER`, schema.tableName)); err != nil {
			b.Fatalf("scatter: %v", err)
		}
	}

	return &pointSelectDriver{
		ctx:    ctx,
		pgURLs: pgURLs,
		stmt:   schema.stmt,
		cleanup: func() {
			for _, fn := range cleanupURLFns {
				fn()
			}
			tc.Stopper().Stop(ctx)
		},
	}
}

// pointSelectIntFromEnv reads a positive integer env var, returning
// `def` if unset/empty. Aborts via panic (caught by b.Fatal in callers
// indirectly) on a non-numeric value, since silently defaulting on a
// typo would invalidate experiment results.
func pointSelectIntFromEnv(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		panic(fmt.Sprintf("env var %s: cannot parse %q as int: %v", name, v, err))
	}
	return n
}

// pointSelectFastPathEnabledFromEnv parses the POINT_SELECT_FAST_PATH
// env var. Returns true for "1", "true", "on", "yes" (case-insensitive);
// false for empty/"0"/"false"/"off"/"no". Any other value also returns
// false — strict because misconfiguration silently flipping the wrong
// way would invalidate experiment results.
func pointSelectFastPathEnabledFromEnv() bool {
	return pointSelectBoolFromEnv(pointSelectFastPathEnvVar)
}

// pointSelectBoolFromEnv parses one of our truthy/falsy env vars
// the same strict way pointSelectFastPathEnabledFromEnv does:
// only the recognized truthy values turn it on; anything else
// (empty, falsy literals, unknown values) leaves it off.
func pointSelectBoolFromEnv(name string) bool {
	v := os.Getenv(name)
	switch v {
	case "", "0", "false", "off", "no", "False", "FALSE", "Off", "OFF", "No", "NO":
		return false
	case "1", "true", "on", "yes", "True", "TRUE", "On", "ON", "Yes", "YES":
		return true
	default:
		return false
	}
}

// pointSelectClient wraps one pgx connection with the prepared
// SELECT. Owned by exactly one goroutine for the duration of a run.
type pointSelectClient struct {
	ctx      context.Context
	conn     *pgx.Conn
	stmtName string
}

// newClient mints a connection to one of the cluster's nodes, chosen
// round-robin by `idx`. Distributing across gateways spreads the
// pgwire-side load when the cluster has more than one node.
func (d *pointSelectDriver) newClient(idx int) (*pointSelectClient, error) {
	pgURL := d.pgURLs[idx%len(d.pgURLs)]
	conn, err := pgx.Connect(d.ctx, pgURL.String())
	if err != nil {
		return nil, err
	}
	sd, err := conn.Prepare(d.ctx, "ps", d.stmt)
	if err != nil {
		_ = conn.Close(d.ctx)
		return nil, err
	}
	return &pointSelectClient{ctx: d.ctx, conn: conn, stmtName: sd.Name}, nil
}

func (c *pointSelectClient) close() {
	_ = c.conn.Close(c.ctx)
}

// pointSelect issues one prepared SELECT and discards the value bytes.
// Returns an error if the row is missing (which indicates a bug in the
// driver, not a normal outcome).
func (c *pointSelectClient) pointSelect(k int64) error {
	var v []byte
	err := c.conn.QueryRow(c.ctx, c.stmtName, k).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("row not found")
	}
	return err
}

// BenchmarkPointSelect is the headline measurement: prepared
// `SELECT v FROM kv WHERE k = $1` against a single-column primary
// key. See pointSelectSchemaPK.
func BenchmarkPointSelect(b *testing.B) {
	benchmarkPointSelectImpl(b, pointSelectSchemaPK)
}

// BenchmarkPointSelectSecondaryIndex runs the same workload pattern
// against a UNIQUE STORING secondary index. The optimizer's
// placeholder fast path does not apply to this shape, so the
// slow-path baseline is genuinely the unspecialized pipeline. See
// pointSelectSchemaSecondaryIndex.
func BenchmarkPointSelectSecondaryIndex(b *testing.B) {
	benchmarkPointSelectImpl(b, pointSelectSchemaSecondaryIndex)
}

// BenchmarkPointSelectJoin runs a two-table lookup-join workload:
// WHERE hits a UNIQUE secondary index on first, then joins to second
// on first.fk_id = second.id. Three KV Gets per op. See
// pointSelectSchemaJoin.
func BenchmarkPointSelectJoin(b *testing.B) {
	benchmarkPointSelectImpl(b, pointSelectSchemaJoin)
}

func benchmarkPointSelectImpl(b *testing.B, schema pointSelectSchema) {
	defer log.Scope(b).Close(b)

	// One driver per outer benchmark invocation: cluster boot + populate
	// is expensive (~seconds), and reads do not mutate state, so we share
	// it across all concurrency sub-benchmarks.
	var driver *pointSelectDriver
	b.Cleanup(func() {
		if driver != nil {
			driver.cleanup()
		}
	})

	for _, conc := range pointSelectConcurrencies {
		b.Run(fmt.Sprintf("conc=%d", conc), func(b *testing.B) {
			if driver == nil {
				driver = newPointSelectDriver(b, schema)
			}
			runPointSelectBench(b, driver, conc)
		})
	}
}

// runPointSelectBench creates `concurrency` clients, runs a fixed warmup
// per goroutine, then runs b.N ops total split across the goroutines and
// reports merged p50/p99 latency.
func runPointSelectBench(b *testing.B, d *pointSelectDriver, concurrency int) {
	clients := make([]*pointSelectClient, concurrency)
	for i := range clients {
		c, err := d.newClient(i)
		if err != nil {
			b.Fatalf("new client %d: %v", i, err)
		}
		clients[i] = c
	}
	defer func() {
		for _, c := range clients {
			c.close()
		}
	}()

	// Warm up: each goroutine runs a fixed number of ops so the prepared
	// statement reaches its generic plan, descriptor leases are cached,
	// and the connection-side type-info round trips are done. Untimed.
	runPointSelectOps(b, clients, pointSelectWarmupOps*concurrency, false /* timed */)

	b.ResetTimer()
	runPointSelectOps(b, clients, b.N, true /* timed */)
	b.StopTimer()
}

// runPointSelectOps splits totalOps across the given client goroutines.
// When timed is true, per-op latency is recorded into per-goroutine
// histograms which are merged at the end and reported as p50_us and
// p99_us via b.ReportMetric.
func runPointSelectOps(b *testing.B, clients []*pointSelectClient, totalOps int, timed bool) {
	if totalOps <= 0 {
		return
	}

	var (
		merged   *hdrhistogram.Histogram
		mergedMu sync.Mutex
	)
	if timed {
		merged = hdrhistogram.New(pointSelectMinLatency, pointSelectMaxLatency, 3)
	}

	var firstErrOnce sync.Once
	var firstErr error
	recordErr := func(err error) {
		firstErrOnce.Do(func() { firstErr = err })
	}

	var remaining atomic.Int64
	remaining.Store(int64(totalOps))

	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(c *pointSelectClient, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			var local *hdrhistogram.Histogram
			if timed {
				local = hdrhistogram.New(pointSelectMinLatency, pointSelectMaxLatency, 3)
			}
			for remaining.Add(-1) >= 0 {
				k := int64(rng.Intn(pointSelectTotalRows))
				start := time.Now()
				if err := c.pointSelect(k); err != nil {
					recordErr(err)
					return
				}
				if timed {
					_ = local.RecordValue(time.Since(start).Nanoseconds())
				}
			}
			if timed {
				mergedMu.Lock()
				merged.Merge(local)
				mergedMu.Unlock()
			}
		}(c, int64(i)+1)
	}
	wg.Wait()

	if firstErr != nil {
		b.Fatalf("query error: %v", firstErr)
	}

	if timed {
		// Report in microseconds for readability; ns is too noisy in
		// `go test -bench` output.
		b.ReportMetric(float64(merged.ValueAtQuantile(50))/1000.0, "p50_us")
		b.ReportMetric(float64(merged.ValueAtQuantile(99))/1000.0, "p99_us")
	}
}
