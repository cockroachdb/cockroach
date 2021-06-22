// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rand

import (
	"bytes"
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"fmt"
	"math/rand"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/lib/pq/oid"
	"github.com/spf13/pflag"
)

type random struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	batchSize int

	seed int64

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
		g.flags.Int64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.StringVar(&g.primaryKey, `primary-key`, ``, `ioc-update and ioc-nothing require primary key`)
		g.flags.IntVar(&g.nullPct, `null-percent`, 5, `Percent random nulls`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*random) Meta() workload.Meta { return randMeta }

// Flags implements the Flagser interface.
func (w *random) Flags() workload.Flags { return w.flags }

// Hooks implements the Hookser interface.
func (w *random) Hooks() workload.Hooks {
	return workload.Hooks{}
}

// Tables implements the Generator interface.
func (w *random) Tables() []workload.Table {
	tables := make([]workload.Table, w.tables)
	rng := rand.New(rand.NewSource(w.seed))
	for i := 0; i < w.tables; i++ {
		createTable := randgen.RandCreateTable(rng, "table", rng.Int())
		ctx := tree.NewFmtCtx(tree.FmtParsable)
		createTable.FormatBody(ctx)
		tables[i] = workload.Table{
			Name:   createTable.Table.String(),
			Schema: ctx.CloseAndGetString(),
		}
	}
	return tables
}

type col struct {
	name          string
	dataType      *types.T
	dataPrecision int
	dataScale     int
	cdefault      gosql.NullString
	isNullable    bool
	isComputed    bool
}

// Ops implements the Opser interface.
func (w *random) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(w, w.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
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

	var relid int
	if err := db.QueryRow(fmt.Sprintf("SELECT '%s'::REGCLASS::OID", tableName)).Scan(&relid); err != nil {
		return workload.QueryLoad{}, err
	}

	rows, err := db.Query(
		`
SELECT attname, atttypid, adsrc, NOT attnotnull, attgenerated != ''
FROM pg_catalog.pg_attribute
LEFT JOIN pg_catalog.pg_attrdef
ON attrelid=adrelid AND attnum=adnum
WHERE attrelid=$1`, relid)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	var cols []col
	var numCols = 0

	defer rows.Close()
	for rows.Next() {
		var c col
		c.dataPrecision = 0
		c.dataScale = 0

		var typOid int
		if err := rows.Scan(&c.name, &typOid, &c.cdefault, &c.isNullable, &c.isComputed); err != nil {
			return workload.QueryLoad{}, err
		}
		datumType := types.OidToType[oid.Oid(typOid)]
		c.dataType = datumType
		if c.cdefault.String == "unique_rowid()" { // skip
			continue
		}
		if strings.HasPrefix(c.cdefault.String, "uuid_v4()") { // skip
			continue
		}
		cols = append(cols, c)
		numCols++
	}

	if numCols == 0 {
		return workload.QueryLoad{}, errors.New("no columns detected")
	}

	// insert on conflict requires the primary key. check information_schema if not specified on the command line
	if strings.HasPrefix(w.method, "ioc") && w.primaryKey == "" {
		rows, err := db.Query(
			`
SELECT a.attname
FROM   pg_index i
JOIN   pg_attribute a ON a.attrelid = i.indrelid
                      AND a.attnum = ANY(i.indkey)
WHERE  i.indrelid = $1
AND    i.indisprimary`, relid)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		defer rows.Close()
		for rows.Next() {
			var colname string

			if err := rows.Scan(&colname); err != nil {
				return workload.QueryLoad{}, err
			}
			if w.primaryKey != "" {
				w.primaryKey += "," + colname
			} else {
				w.primaryKey += colname
			}
		}
	}

	if strings.HasPrefix(w.method, "ioc") && w.primaryKey == "" {
		err := errors.New(
			"insert on conflict requires primary key to be specified via -primary if the table does " +
				"not have primary key")
		return workload.QueryLoad{}, err
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
		for i, c := range cols {
			if i > 0 {
				dmlSuffix.WriteString(",")
			}
			dmlSuffix.WriteString(fmt.Sprintf("%s=EXCLUDED.%s", c.name, c.name))
		}
	default:
		return workload.QueryLoad{}, errors.Errorf("%s DML method not valid", w.primaryKey)
	}

	var nonComputedCols []col
	for _, c := range cols {
		if !c.isComputed {
			nonComputedCols = append(nonComputedCols, c)
		}
	}

	fmt.Fprintf(&buf, `%s INTO %s.%s (`, dmlMethod, sqlDatabase, tableName)
	for i, c := range nonComputedCols {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(c.name)
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

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}

	for i := 0; i < w.connFlags.Concurrency; i++ {
		op := randOp{
			config:    w,
			hists:     reg.GetHandle(),
			db:        db,
			cols:      nonComputedCols,
			rng:       rand.New(rand.NewSource(w.seed + int64(i))),
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
	cols      []col
	rng       *rand.Rand
	writeStmt *gosql.Stmt
}

// DatumToGoSQL converts a datum to a Go type.
func DatumToGoSQL(d tree.Datum) (interface{}, error) {
	d = tree.UnwrapDatum(nil, d)
	if d == tree.DNull {
		return nil, nil
	}
	switch d := d.(type) {
	case *tree.DBool:
		return bool(*d), nil
	case *tree.DString:
		return string(*d), nil
	case *tree.DBytes:
		return string(*d), nil
	case *tree.DDate, *tree.DTime:
		return tree.AsStringWithFlags(d, tree.FmtBareStrings), nil
	case *tree.DTimestamp:
		return d.Time, nil
	case *tree.DTimestampTZ:
		return d.Time, nil
	case *tree.DInterval:
		return d.Duration.String(), nil
	case *tree.DBitArray:
		return tree.AsStringWithFlags(d, tree.FmtBareStrings), nil
	case *tree.DInt:
		return int64(*d), nil
	case *tree.DOid:
		return int(d.DInt), nil
	case *tree.DFloat:
		return float64(*d), nil
	case *tree.DDecimal:
		return d.Float64()
	case *tree.DArray:
		arr := make([]interface{}, len(d.Array))
		for i := range d.Array {
			elt, err := DatumToGoSQL(d.Array[i])
			if err != nil {
				return nil, err
			}
			if elt == nil {
				elt = nullVal{}
			}
			arr[i] = elt
		}
		return pq.Array(arr), nil
	case *tree.DUuid:
		return d.UUID, nil
	case *tree.DIPAddr:
		return d.IPAddr.String(), nil
	case *tree.DJSON:
		return d.JSON.String(), nil
	}
	return nil, errors.Errorf("unhandled datum type: %s", reflect.TypeOf(d))
}

type nullVal struct {
}

func (nullVal) Value() (driver.Value, error) {
	return nil, nil
}

func (o *randOp) run(ctx context.Context) (err error) {
	params := make([]interface{}, len(o.cols)*o.config.batchSize)
	k := 0 // index into params
	for j := 0; j < o.config.batchSize; j++ {
		for _, c := range o.cols {
			nullPct := 0
			if c.isNullable && o.config.nullPct > 0 {
				nullPct = 100 / o.config.nullPct
			}
			d := randgen.RandDatumWithNullChance(o.rng, c.dataType, nullPct)
			params[k], err = DatumToGoSQL(d)
			if err != nil {
				return err
			}
			k++
		}
	}
	start := timeutil.Now()
	_, err = o.writeStmt.ExecContext(ctx, params...)
	o.hists.Get(`write`).Record(timeutil.Since(start))
	return err
}
