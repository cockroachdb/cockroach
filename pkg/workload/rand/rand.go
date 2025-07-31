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
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
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

type col struct {
	name          string
	dataType      *types.T
	dataPrecision int
	dataScale     int
	cdefault      gosql.NullString
	isNullable    bool
	isComputed    bool
}

// typeForOid returns the *types.T struct associated with the given
// OID. Note that for columns of type `BIT` and `CHAR`, the width is
// not recorded on the T struct itself; instead, we query the
// `information_schema.columns` view to get that information. When the
// `character_maximum_length` column is NULL, it means the column has
// variable width and we set the width of the type to 0, which will
// cause the random data generator to generate data with random width.
func typeForOid(db *gosql.DB, typeOid oid.Oid, tableName, columnName string) (*types.T, error) {
	datumType := *types.OidToType[typeOid]
	if typeOid == oid.T_bit || typeOid == oid.T_char {
		var width int32
		if err := db.QueryRow(
			`SELECT IFNULL(character_maximum_length, 0)
			FROM information_schema.columns
			WHERE table_name = $1 AND column_name = $2`,
			tableName, columnName).Scan(&width); err != nil {
			return nil, err
		}

		datumType.InternalType.Width = width
	}

	return &datumType, nil
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

	var relid int
	sqlName := tree.Name(tableName)
	if err := db.QueryRow("SELECT $1::REGCLASS::OID", sqlName.String()).Scan(&relid); err != nil {
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
	defer func() { retErr = errors.CombineErrors(retErr, rows.Close()) }()
	var cols []col
	var numCols = 0

	for rows.Next() {
		var c col
		c.dataPrecision = 0
		c.dataScale = 0

		var typOid int
		if err := rows.Scan(&c.name, &typOid, &c.cdefault, &c.isNullable, &c.isComputed); err != nil {
			return workload.QueryLoad{}, err
		}
		c.dataType, err = typeForOid(db, oid.Oid(typOid), tableName, c.name)
		if err != nil {
			return workload.QueryLoad{}, err
		}
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

	if err = rows.Err(); err != nil {
		return workload.QueryLoad{}, err
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
		defer func() { retErr = errors.CombineErrors(retErr, rows.Close()) }()
		for rows.Next() {
			var colname string

			if err := rows.Scan(&colname); err != nil {
				return workload.QueryLoad{}, err
			}
			if w.primaryKey != "" {
				w.primaryKey += "," + tree.NameString(colname)
			} else {
				w.primaryKey += tree.NameString(colname)
			}
		}
		if err = rows.Err(); err != nil {
			return workload.QueryLoad{}, err
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
			dmlSuffix.WriteString(fmt.Sprintf("%s=EXCLUDED.%s", tree.NameString(c.name), tree.NameString(c.name)))
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

	fmt.Fprintf(&buf, `%s INTO %s (`, dmlMethod, tree.NameString(tableName))
	for i, c := range nonComputedCols {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(tree.NameString(c.name))
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
			cols:      nonComputedCols,
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
	cols      []col
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

// DatumToGoSQL converts a datum to a Go type.
func DatumToGoSQL(d tree.Datum) (interface{}, error) {
	d = tree.UnwrapDOidWrapper(d)
	if d == tree.DNull {
		return nil, nil
	}
	switch d := d.(type) {
	case *tree.DBool:
		return bool(*d), nil
	case *tree.DString:
		return string(*d), nil
	case *tree.DBytes:
		return fmt.Sprintf(`x'%s'`, hex.EncodeToString([]byte(*d))), nil
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
		return uint32(d.Oid), nil
	case *tree.DFloat:
		return float64(*d), nil
	case *tree.DDecimal:
		// use string representation here since randgen might generate
		// decimals that don't fit into a float64
		return d.String(), nil
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
		return sqlArray{arr, d.ParamTyp}, nil
	case *tree.DUuid:
		return d.UUID, nil
	case *tree.DIPAddr:
		return d.IPAddr.String(), nil
	case *tree.DJSON:
		return d.JSON.String(), nil
	case *tree.DJsonpath:
		return d.String(), nil
	case *tree.DTimeTZ:
		return d.TimeTZ.String(), nil
	case *tree.DBox2D:
		return d.CartesianBoundingBox.Repr(), nil
	case *tree.DGeography:
		return geo.SpatialObjectToEWKT(d.Geography.SpatialObject(), 2)
	case *tree.DGeometry:
		return geo.SpatialObjectToEWKT(d.Geometry.SpatialObject(), 2)
	case *tree.DPGLSN:
		return d.LSN.String(), nil
	case *tree.DTSQuery:
		return d.String(), nil
	case *tree.DTSVector:
		return d.String(), nil
	case *tree.DPGVector:
		return d.String(), nil
	}
	return nil, errors.Errorf("unhandled datum type: %s", reflect.TypeOf(d))
}

type nullVal struct{}

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
			d := randgen.RandDatumWithNullChance(o.rng, c.dataType, nullPct, /* nullChance */
				false /* favorCommonData */, false /* targetColumnIsUnique */)
			params[k], err = DatumToGoSQL(d)
			if err != nil {
				return err
			}
			k++
		}
	}
	start := timeutil.Now()
	_, err = o.writeStmt.ExecContext(ctx, params...)
	if o.hists != nil {
		o.hists.Get(`write`).Record(timeutil.Since(start))
	}
	return err
}
