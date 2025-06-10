// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rand

import (
	"bytes"
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/spf13/pflag"
)

// NOTE: the caller is expected to pass the same seed on `init` and
// `run` when using this workload.
var RandomSeed = workload.NewInt64RandomSeed()

type random struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	batchSize int

	tableName string

	tables     int
	method     string
	primaryKey string
	nullPct    int
}

func init() {
	workload.Register(randMeta)
}

var randMeta = workload.Meta{
	Name:        `rand`,
	Description: `random writes to table`,
	RandomSeed:  RandomSeed,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &random{}
		g.flags.FlagSet = pflag.NewFlagSet(`rand`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`batch`: {RuntimeOnly: true},
		}
		g.flags.IntVar(&g.tables, `tables`, 1, `Number of tables to create`)
		g.flags.StringVar(&g.tableName, `table`, ``, `Table to write to`)
		g.flags.IntVar(&g.batchSize, `batch`, 1, `Number of rows to insert in a single SQL statement`)
		g.flags.StringVar(&g.method, `method`, `upsert`, `Choice of DML name: insert, upsert, ioc-update (insert on conflict update), ioc-nothing (insert on conflict no nothing)`)
		g.flags.StringVar(&g.primaryKey, `primary-key`, ``, `ioc-update and ioc-nothing require primary key`)
		g.flags.IntVar(&g.nullPct, `null-percent`, 5, `Percent random nulls`)
		RandomSeed.AddFlag(&g.flags)
		g.connFlags = workload.NewConnFlags(&g.flags)

		return g
	},
}

// Meta implements the Generator interface.
func (w *random) Meta() workload.Meta {
	return randMeta
}

// Flags implements the Flagser interface.
func (w *random) Flags() workload.Flags { return w.flags }

// ConnFlags implements the ConnFlagser interface.
func (w *random) ConnFlags() *workload.ConnFlags { return w.connFlags }

// Hooks implements the Hookser interface.
func (w *random) Hooks() workload.Hooks {
	return workload.Hooks{}
}

// Tables implements the Generator interface.
func (w *random) Tables() []workload.Table {
	tables := make([]workload.Table, w.tables)
	rng := rand.New(rand.NewSource(RandomSeed.Seed()))
	for i := 0; i < w.tables; i++ {
		createTable := randgen.RandCreateTable(context.Background(), rng, "table", rng.Int(), randgen.TableOptNone)
		ctx := tree.NewFmtCtx(tree.FmtParsable)
		createTable.FormatBody(ctx)
		tables[i] = workload.Table{
			Name:   string(createTable.Table.ObjectName),
			Schema: ctx.CloseAndGetString(),
		}
	}
	return tables
}

// Ops implements the Opser interface.
func (w *random) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (ql workload.QueryLoad, retErr error) {
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(w.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(w.connFlags.Concurrency + 1)

	tableName := w.tableName
	if tableName == "" {
		tableName = w.Tables()[0].Name
	}

	table, err := LoadTable(db, tableName)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	// insert on conflict requires the primary key. check information_schema if not specified on the command line
	if strings.HasPrefix(w.method, "ioc") && w.primaryKey == "" {
		if len(table.PrimaryKey) == 0 {
			return workload.QueryLoad{}, errors.New(
				"insert on conflict requires primary key to be specified via -primary if the table does " +
					"not have primary key")
		}
		var primaryKey []string
		for _, i := range table.PrimaryKey {
			primaryKey = append(primaryKey, table.Cols[i].Name)
		}
		w.primaryKey = strings.Join(primaryKey, ",")
	}

	var dmlMethod string
	var dmlSuffix bytes.Buffer
	var buf bytes.Buffer
	switch w.method {
	case "insert":
		dmlMethod = "insert"
		dmlSuffix.WriteString("")
	case "upsert":
		dmlMethod = "upsert"
		dmlSuffix.WriteString("")
	case "ioc-nothing":
		dmlMethod = "insert"
		dmlSuffix.WriteString(fmt.Sprintf(" on conflict (%s) do nothing", w.primaryKey))
	case "ioc-update":
		dmlMethod = "insert"
		dmlSuffix.WriteString(fmt.Sprintf(" on conflict (%s) do update set ", w.primaryKey))
		for i, c := range table.Cols {
			if i > 0 {
				dmlSuffix.WriteString(",")
			}
			dmlSuffix.WriteString(fmt.Sprintf("%s=EXCLUDED.%s", tree.NameString(c.Name), tree.NameString(c.Name)))
		}
	default:
		return workload.QueryLoad{}, errors.Errorf("%s DML method not valid", w.primaryKey)
	}

	var nonComputedCols []Col
	for _, c := range table.Cols {
		if !c.IsComputed {
			nonComputedCols = append(nonComputedCols, c)
		}
	}

	fmt.Fprintf(&buf, `%s INTO %s (`, dmlMethod, tree.NameString(tableName))
	for i, c := range nonComputedCols {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(tree.NameString(c.Name))
	}
	buf.WriteString(`) VALUES `)

	nCols := len(nonComputedCols)
	for i := 0; i < w.batchSize; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("(")
		for j := range nonComputedCols {
			if j > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, `$%d`, 1+j+(nCols*i))
		}
		buf.WriteString(")")
	}

	buf.WriteString(dmlSuffix.String())

	writeStmt, err := db.Prepare(buf.String())
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql = workload.QueryLoad{}

	for i := 0; i < w.connFlags.Concurrency; i++ {
		op := randOp{
			config:    w,
			hists:     reg.GetHandle(),
			db:        db,
			table:     &table,
			rng:       rand.New(rand.NewSource(RandomSeed.Seed() + int64(i))),
			writeStmt: writeStmt,
		}
		ql.WorkerFns = append(ql.WorkerFns, op.run)
	}
	return ql, nil
}

type randOp struct {
	config    *random
	hists     *histogram.Histograms
	db        *gosql.DB
	table     *Table
	rng       *rand.Rand
	writeStmt *gosql.Stmt
}

// sqlArray implements the driver.Valuer interface and abstracts away
// the differences between arrays of geographic/geometric data and
// other arrays: the former use ':' as separator, while the latter
// uses ','
type sqlArray struct {
	array     []interface{}
	paramType *types.T
}

func (sa sqlArray) Value() (driver.Value, error) {
	family := sa.paramType.Family()
	if family == types.GeographyFamily || family == types.GeometryFamily {
		var repr strings.Builder
		repr.WriteString("{")
		var sep string
		for _, elt := range sa.array {
			repr.WriteString(fmt.Sprintf(`%s"%s"`, sep, string(elt.(geopb.EWKT))))
			sep = ": "
		}

		repr.WriteString("}")
		return repr.String(), nil
	}

	return pq.Array(sa.array).Value()
}

type nullVal struct{}

func (nullVal) Value() (driver.Value, error) {
	return nil, nil
}

func (o *randOp) run(ctx context.Context) (err error) {
	params := make([]interface{}, 0, len(o.table.Cols)*o.config.batchSize)
	for j := 0; j < o.config.batchSize; j++ {
		row, err := o.table.RandomRow(o.rng, o.config.nullPct)
		if err != nil {
			return err
		}
		params = append(params, row...)
	}

	start := timeutil.Now()
	_, err = o.writeStmt.ExecContext(ctx, params...)
	if o.hists != nil {
		o.hists.Get(`write`).Record(timeutil.Since(start))
	}
	return err
}
