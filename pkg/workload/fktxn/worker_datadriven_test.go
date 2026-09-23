// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	randv2 "math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// schemaByName maps the schema names recognized by the datadriven `schema`
// directive to the DDL constants in test_schemas_test.go. Add new entries here
// to make additional schemas available to datadriven tests.
var schemaByName = map[string]string{
	"simple_pair": simplePairDDL,
	"chain":       chainDDL,
	"diamond":     diamondCompositeDDL,
	"transitive":  transitiveOverlapDDL,
	"self_ref":    selfRefDDL,
}

// loggedStmt captures one statement emitted by the worker during a chain.
type loggedStmt struct {
	sql  string
	args []interface{}
}

// recordingTx wraps a real *sql.Tx and records every statement before
// forwarding to the underlying tx. The recorded statements are flushed by
// the caller at commit time and printed in the datadriven output.
type recordingTx struct {
	mu    sync.Mutex
	stmts []loggedStmt
	inner *gosql.Tx
}

func (r *recordingTx) ExecContext(
	ctx context.Context, query string, args ...interface{},
) (gosql.Result, error) {
	r.mu.Lock()
	r.stmts = append(r.stmts, loggedStmt{sql: query, args: args})
	r.mu.Unlock()
	return r.inner.ExecContext(ctx, query, args...)
}

func (r *recordingTx) QueryContext(
	ctx context.Context, query string, args ...interface{},
) (*gosql.Rows, error) {
	r.mu.Lock()
	r.stmts = append(r.stmts, loggedStmt{sql: query, args: args})
	r.mu.Unlock()
	return r.inner.QueryContext(ctx, query, args...)
}

func (r *recordingTx) drain() []loggedStmt {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.stmts
	r.stmts = nil
	return out
}

// runRecorder is a per-event hook invoked after each FSM event applies. It
// formats the captured SQL and outcome into the datadriven output.
type runRecorder struct {
	out *strings.Builder
}

// formatLine writes "  STMT  args=[...]" lines for each captured statement.
func (r *runRecorder) formatStmts(stmts []loggedStmt) {
	for _, s := range stmts {
		fmt.Fprintf(r.out, "  %s", strings.TrimSpace(s.sql))
		if len(s.args) > 0 {
			fmt.Fprintf(r.out, "  args=%s", formatArgs(s.args))
		}
		r.out.WriteByte('\n')
	}
}

func formatArgs(args []interface{}) string {
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = formatArg(a)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func formatArg(a interface{}) string {
	if a == nil {
		return "NULL"
	}
	switch v := a.(type) {
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case string:
		return strconv.Quote(v)
	case []byte:
		return strconv.Quote(string(v))
	default:
		return fmt.Sprintf("%v", v)
	}
}

// stateName / eventName format an fsm State/Event for human-readable output
// by stripping the package prefix from the type name.
func stateName(s fsm.State) string {
	return shortTypeName(s)
}

func eventName(e fsm.Event) string {
	return shortTypeName(e)
}

func shortTypeName(v interface{}) string {
	t := reflect.TypeOf(v)
	name := t.Name()
	// Trim conventional prefixes for readability.
	switch {
	case strings.HasPrefix(name, "chainState"):
		return strings.TrimPrefix(name, "chainState")
	case strings.HasPrefix(name, "event"):
		return strings.TrimPrefix(name, "event")
	}
	return name
}

// datadrivenWorker runs N chains for a worker and records each transaction's
// state, event, SQL, and outcome to the buffer. It bypasses Worker.Run so we
// can capture per-event detail; the FSM and event-picking logic are reused
// directly.
func datadrivenWorker(
	t *testing.T,
	ctx context.Context,
	db *gosql.DB,
	rng *rand.Rand,
	sorted []*Table,
	sub *FKGraph,
	dropped []FKEdge,
	pools *Pools,
	mix OpMix,
	chains int,
	maxEvents int,
) string {
	t.Helper()
	var out strings.Builder
	rec := &runRecorder{out: &out}

	for c := 0; c < chains; c++ {
		fmt.Fprintf(&out, "chain %d:\n", c+1)
		pkIdx := AssignPKs(rng, pools)
		fmt.Fprintf(&out, "  pkIdx=%d pks: %s\n", pkIdx, formatRowPKs(t, sorted, sub, pools, pkIdx))

		ext := &chainExtended{
			rng: rng, sorted: sorted, sub: sub, dropped: dropped, pools: pools, pkIdx: pkIdx,
		}
		machine := fsm.MakeMachine(chainTransitions, chainStateUnknown{}, ext)

		for i := 0; i < maxEvents; i++ {
			prev := machine.CurState()
			event := mix.pickEvent(rng, prev)

			// Mirror the worker's policy: roll fresh non-PK FK indexes only
			// when re-writing an Exists chain. Unknown/None upserts pin to
			// pkIdx because the parent rows don't exist anywhere yet.
			_, fromExists := prev.(chainStateExists)
			ext.repointFKs = fromExists

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			rtx := &recordingTx{inner: tx}
			ext.tx = rtx

			fmt.Fprintf(&out, "  txn %d: state=%s event=%s\n",
				i+1, stateName(prev), eventName(event))

			applyErr := machine.Apply(ctx, event)
			stmts := rtx.drain()
			rec.formatStmts(stmts)

			switch {
			case applyErr != nil:
				_ = tx.Rollback()
				class := classifyError(applyErr)
				if class == "" {
					class = "fatal"
				}
				fmt.Fprintf(&out, "    -> %s: %s\n", class, summarizeErr(applyErr))
				// FSM state didn't advance on error; chain ends here.
				goto chainDone
			default:
				if err := tx.Commit(); err != nil {
					fmt.Fprintf(&out, "    -> commit failed: %v\n", err)
					goto chainDone
				}
				fmt.Fprintf(&out, "    -> committed (state=%s)\n",
					stateName(machine.CurState()))
			}
		}
	chainDone:
	}
	return out.String()
}

func summarizeErr(err error) string {
	msg := err.Error()
	// Trim everything after the first newline; keep it on one line.
	if idx := strings.Index(msg, "\n"); idx >= 0 {
		msg = msg[:idx]
	}
	// Cap length so wide error messages don't blow up the output.
	const maxLen = 200
	if len(msg) > maxLen {
		msg = msg[:maxLen] + "..."
	}
	return msg
}

// formatRowPKs renders the chain's per-table row PK derived from pools at
// pkIdx — i.e. what each table's PK columns will be when the chain writes a
// row. Used in datadriven output to keep the previous "pks: ..." line shape.
func formatRowPKs(t *testing.T, sorted []*Table, sub *FKGraph, pools *Pools, pkIdx int) string {
	t.Helper()
	parts := make([]string, 0, len(sorted))
	for _, tbl := range sorted {
		vals, err := PKValuesFor(tbl, sub, pools, pkIdx)
		require.NoError(t, err)
		parts = append(parts, fmt.Sprintf("%s=%s", tbl.Name, formatArgs(vals)))
	}
	return strings.Join(parts, " ")
}

// TestWorkerDataDriven visualizes a worker's per-transaction output. It
// produces deterministic golden text from a fixed seed; rewrite the testdata
// with `./dev test pkg/workload/fktxn -f=TestWorkerDataDriven --rewrite`.
func TestWorkerDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	dbCounter := 0

	datadriven.Walk(t, "testdata/worker", func(t *testing.T, path string) {
		// Per-file state.
		var (
			sorted  []*Table
			sub     *FKGraph
			dropped []FKEdge
			testDB  *gosql.DB
		)

		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "schema":
				// `schema <name>` — name is the first CmdArg key, or the trimmed input.
				var schemaName string
				if len(td.CmdArgs) > 0 {
					schemaName = td.CmdArgs[0].Key
				} else {
					schemaName = strings.TrimSpace(td.Input)
				}
				ddl, ok := schemaByName[schemaName]
				if !ok {
					t.Fatalf("unknown schema %q (known: %v)", schemaName, knownSchemaNames())
				}

				dbCounter++
				dbName := fmt.Sprintf("dd_%d_%s", dbCounter, schemaName)
				discoverSchemaFromDDL(t, srv, sqlDB, dbName, ddl)
				testDB = srv.ApplicationLayer().SQLConn(t, serverutils.DBName(dbName))

				s, err := DiscoverSchema(testDB, dbName)
				require.NoError(t, err)
				graphs := BuildFKGraphs(s)
				require.NotEmpty(t, graphs)
				return fmt.Sprintf("loaded schema %q: %d table(s), %d FK graph(s)\n",
					schemaName, len(s.Tables), len(graphs))

			case "run":
				if testDB == nil {
					t.Fatalf("no schema loaded; use 'schema <name>' first")
				}
				args := parseRunArgs(t, td)

				// Build sub-DAG with the run's seed for reproducibility.
				rng := rand.New(rand.NewSource(args.seed))
				rngV2 := randv2.New(randv2.NewPCG(uint64(args.seed), 0))
				s, err := DiscoverSchema(testDB, currentDBName(testDB, t))
				require.NoError(t, err)
				graphs := BuildFKGraphs(s)
				sortedNew, subNew, droppedNew, err := RandomSubDAG(rngV2, graphs[0])
				require.NoError(t, err)
				sorted, sub, dropped = sortedNew, subNew, droppedNew

				// Build per-constraint pools with a separate RNG so the chain
				// RNG (which drives op-mix and per-write index choices) is
				// independent of pool generation.
				poolRNG := rand.New(rand.NewSource(args.seed + 1))
				pools, err := BuildPools(poolRNG, s, args.poolSize)
				require.NoError(t, err)

				// Reset all tables so the run starts from a known state. This
				// keeps the output independent of any prior runs in the file.
				resetTables(t, testDB, sorted)

				return datadrivenWorker(t, ctx, testDB, rng, sorted, sub, dropped,
					pools, args.mix, args.chains, args.maxEvents)

			default:
				t.Fatalf("unknown cmd %q", td.Cmd)
				return ""
			}
		})
	})
}

type runArgs struct {
	seed      int64
	chains    int
	maxEvents int
	poolSize  int
	mix       OpMix
}

func parseRunArgs(t *testing.T, td *datadriven.TestData) runArgs {
	t.Helper()
	args := runArgs{
		seed:      0,
		chains:    1,
		maxEvents: 5,
		poolSize:  4,
		mix:       OpMix{Upsert: 70, Delete: 30},
	}
	for _, arg := range td.CmdArgs {
		switch arg.Key {
		case "seed":
			v, err := strconv.ParseInt(arg.Vals[0], 10, 64)
			require.NoError(t, err)
			args.seed = v
		case "chains":
			v, err := strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)
			args.chains = v
		case "max-events":
			v, err := strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)
			args.maxEvents = v
		case "pool-size":
			v, err := strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)
			args.poolSize = v
		case "mix":
			// mix is UPSERT/DELETE; from None only Upsert is valid (no choice).
			parts := strings.Split(arg.Vals[0], "/")
			require.Len(t, parts, 2, "mix must be UPSERT/DELETE")
			up, err := strconv.Atoi(parts[0])
			require.NoError(t, err)
			d, err := strconv.Atoi(parts[1])
			require.NoError(t, err)
			args.mix = OpMix{Upsert: up, Delete: d}
		default:
			t.Fatalf("unknown arg %q", arg.Key)
		}
	}
	return args
}

func resetTables(t *testing.T, db *gosql.DB, sorted []*Table) {
	t.Helper()
	// Delete in reverse topological order so children go before parents.
	for i := len(sorted) - 1; i >= 0; i-- {
		_, err := db.Exec(fmt.Sprintf("DELETE FROM %s", sorted[i].Name))
		require.NoError(t, err)
	}
}

func currentDBName(db *gosql.DB, t *testing.T) string {
	t.Helper()
	var name string
	require.NoError(t, db.QueryRow("SELECT current_database()").Scan(&name))
	return name
}

func knownSchemaNames() []string {
	names := make([]string, 0, len(schemaByName))
	for k := range schemaByName {
		names = append(names, k)
	}
	return names
}
