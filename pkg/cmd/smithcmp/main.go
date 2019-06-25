// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// smithcmp is a tool to execute random queries on a database. A TOML
// file provides configuration for which databases to connect to. If there
// is more than one, only non-mutating statements are generated, and the
// output is compared, exiting if there is a difference. If there is only
// one database, mutating and non-mutating statements are generated. A
// flag in the TOML controls whether Postgres-compatible output is generated.
package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/jackc/pgx/pgtype"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func usage() {
	const use = `Usage of %s:
	%[1]s config.toml
`

	fmt.Printf(use, os.Args[0])
	os.Exit(1)
}

type options struct {
	Postgres bool
	InitSQL  string
	Smither  string
	Seed     int64

	Databases map[string]struct {
		Addr    string
		InitSQL string
	}
}

func main() {
	args := os.Args[1:]
	if len(args) != 1 {
		usage()
	}

	tomlData, err := ioutil.ReadFile(args[0])
	if err != nil {
		log.Fatal(err)
	}

	var opts options
	if err := toml.Unmarshal(tomlData, &opts); err != nil {
		log.Fatal(err)
	}

	conns := map[string]*Conn{}
	for name, db := range opts.Databases {
		var err error
		conns[name], err = NewConn(db.Addr, db.InitSQL, opts.InitSQL)
		if err != nil {
			log.Fatalf("%s (%s): %+v", name, db.Addr, err)
		}
	}
	compare := len(conns) > 1

	rng := rand.New(rand.NewSource(opts.Seed))
	smithOpts := []sqlsmith.SmitherOption{
		sqlsmith.AvoidConsts(),
	}
	if opts.Postgres {
		smithOpts = append(smithOpts, sqlsmith.PostgresMode())
	} else if compare {
		smithOpts = append(smithOpts, sqlsmith.CompareMode())
	}
	if _, ok := conns[opts.Smither]; !ok {
		log.Fatalf("Smither option not present in databases: %s", opts.Smither)
	}
	smither, err := sqlsmith.NewSmither(conns[opts.Smither].DB, rng, smithOpts...)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; true; i++ {
		fmt.Printf("stmt: %d\n", i)
		stmt := smither.Generate()
		fmt.Println("executing...")
		if opts.Postgres {
			// TODO(mjibson): move these into sqlsmith.
			stmt = strings.Replace(stmt, ":::", "::", -1)
			stmt = strings.Replace(stmt, "STRING", "TEXT", -1)
			stmt = strings.Replace(stmt, "BYTES", "BYTEA", -1)
			stmt = strings.Replace(stmt, "FLOAT4", "FLOAT8", -1)
			stmt = strings.Replace(stmt, "INT2", "INT8", -1)
			stmt = strings.Replace(stmt, "INT4", "INT8", -1)
		}
		if compare {
			if err := compareConns(stmt, opts.Smither, conns); err != nil {
				fmt.Printf("SQL:\n%s;\nERR: %s\n\n", stmt, err)
				os.Exit(1)
			}
		} else {
			for _, conn := range conns {
				if err := conn.Exec(stmt); err != nil {
					fmt.Println(err)
				}
			}
		}

		// Make sure the servers are alive.
		for name, conn := range conns {
			if err := conn.Ping(); err != nil {
				fmt.Printf("%s: ping failure: %v\nprevious SQL:\n%s;\n", name, err, stmt)
				os.Exit(1)
			}
		}
	}
}

func compareConns(stmt string, smitherName string, conns map[string]*Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	vvs := map[string][][]interface{}{}
	var lock syncutil.Mutex
	for name := range conns {
		name := name
		g.Go(func() error {
			conn := conns[name]
			vals, err := conn.Values(ctx, stmt)
			if err != nil {
				return errors.Wrap(err, name)
			}
			// Even if the statement has an ORDER BY, it still
			// doesn't guarantee a stable output because it may
			// have been on a column not in the output or the
			// output could have multiple equal values that sort
			// the same. So we have to sort everything ourselves.
			sortVals(vals)
			lock.Lock()
			vvs[name] = vals
			lock.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		fmt.Printf("SQL:\n%s;\nERR: %s\n\n", stmt, err)
		return nil
	}
	first := vvs[smitherName]
	for name, vals := range vvs {
		if name == smitherName {
			continue
		}
		if err := compareVals(first, vals); err != nil {
			fmt.Printf("SQL:\n%s;\nerr:\n%s\n",
				stmt,
				err,
			)
			os.Exit(1)
		}
	}
	return nil
}

// sortVals sorts from right to left.
func sortVals(vals [][]interface{}) {
	if len(vals) == 0 || len(vals[0]) == 0 {
		return
	}
	cols := len(vals[0])
	for col := cols - 1; col >= 0; col-- {
		sort.SliceStable(vals, func(i, j int) bool {
			a, b := vals[i][col], vals[j][col]
			if a == nil {
				return true
			}
			if b == nil {
				return false
			}
			switch a := a.(type) {
			case float64:
				return a < b.(float64)
			case int64:
				return a < b.(int64)
			case uint32:
				return a < b.(uint32)
			default:
				return fmt.Sprint(a) < fmt.Sprint(b)
			}
		})
	}
}

func compareVals(a, b interface{}) error {
	if diff := cmp.Diff(a, b, cmpOptions...); diff != "" {
		return errors.New(diff)
	}
	return nil
}

var (
	cmpOptions = []cmp.Option{
		cmp.Transformer("", func(x []interface{}) []interface{} {
			out := make([]interface{}, len(x))
			for i, v := range x {
				switch t := v.(type) {
				case *pgtype.TextArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = ""
					}
				case *pgtype.BPCharArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = ""
					}
				case *pgtype.VarcharArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = ""
					}
				case *pgtype.Int8Array:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.Int8Array{}
					}
				case *pgtype.Float8Array:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.Float8Array{}
					}
				case *pgtype.UUIDArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.UUIDArray{}
					}
				case *pgtype.ByteaArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.ByteaArray{}
					}
				case *pgtype.InetArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.InetArray{}
					}
				case *pgtype.TimestampArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.TimestampArray{}
					}
				case *pgtype.Bit:
					vb := pgtype.Varbit(*t)
					v = &vb
				case *pgtype.Interval:
					if t.Status == pgtype.Present {
						v = duration.DecodeDuration(int64(t.Months), int64(t.Days), t.Microseconds*1000)
					}
				case string:
					v = strings.Replace(t, "T00:00:00+00:00", "T00:00:00Z", 1)
				}
				out[i] = v
			}
			return out
		}),
		cmp.Transformer("pgtype.Numeric", func(x *pgtype.Numeric) interface{} {
			if x.Status != pgtype.Present {
				return x
			}
			return apd.NewWithBigInt(x.Int, x.Exp)
		}),

		cmpopts.EquateEmpty(),
		cmpopts.EquateNaNs(),
		cmpopts.EquateApprox(0.00001, 0),
		cmp.Comparer(func(x, y *big.Int) bool {
			return x.Cmp(y) == 0
		}),
		cmp.Comparer(func(x, y *apd.Decimal) bool {
			x.Abs(x)
			y.Abs(y)

			min := &apd.Decimal{}
			if x.Cmp(y) > 1 {
				min.Set(y)
			} else {
				min.Set(x)
			}
			ctx := tree.DecimalCtx
			_, _ = ctx.Mul(min, min, decimalCloseness)
			sub := &apd.Decimal{}
			_, _ = ctx.Sub(sub, x, y)
			sub.Abs(sub)
			return sub.Cmp(min) <= 0
		}),
		cmp.Comparer(func(x, y duration.Duration) bool {
			return x.Compare(y) == 0
		}),
	}
	decimalCloseness = apd.New(1, -6)
)
