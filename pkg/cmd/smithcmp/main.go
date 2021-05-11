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
//
// Explicit SQL statements can be specified (skipping sqlsmith generation)
// using the top-level SQL array. Placeholders (`$1`, etc.) are
// supported. Random datums of the correct type will be filled in.
package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/cockroachdb/cockroach/pkg/cmd/cmpconn"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/lib/pq/oid"
)

func usage() {
	const use = `Usage of %s:
	%[1]s config.toml
`

	fmt.Printf(use, os.Args[0])
	os.Exit(1)
}

type options struct {
	Postgres        bool
	InitSQL         string
	Smither         string
	Seed            int64
	TimeoutMins     int
	StmtTimeoutSecs int
	SQL             []string

	Databases map[string]struct {
		Addr           string
		InitSQL        string
		AllowMutations bool
	}
}

var sqlMutators = []randgen.Mutator{randgen.ColumnFamilyMutator}

func enableMutations(shouldEnable bool, mutations []randgen.Mutator) []randgen.Mutator {
	if shouldEnable {
		return mutations
	}
	return nil
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
	timeout := time.Duration(opts.TimeoutMins) * time.Minute
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	stmtTimeout := time.Duration(opts.StmtTimeoutSecs) * time.Second
	if stmtTimeout <= 0 {
		stmtTimeout = time.Minute
	}

	rng := rand.New(rand.NewSource(opts.Seed))
	conns := map[string]cmpconn.Conn{}
	for name, db := range opts.Databases {
		var err error
		mutators := enableMutations(opts.Databases[name].AllowMutations, sqlMutators)
		if opts.Postgres {
			mutators = append(mutators, randgen.PostgresMutator)
		}
		conns[name], err = cmpconn.NewConnWithMutators(
			db.Addr, rng, mutators, db.InitSQL, opts.InitSQL)
		if err != nil {
			log.Fatalf("%s (%s): %+v", name, db.Addr, err)
		}
	}
	compare := len(conns) > 1

	if opts.Seed < 0 {
		opts.Seed = timeutil.Now().UnixNano()
		fmt.Println("seed:", opts.Seed)
	}
	smithOpts := []sqlsmith.SmitherOption{
		sqlsmith.AvoidConsts(),
	}
	if opts.Postgres {
		smithOpts = append(smithOpts, sqlsmith.PostgresMode())
	} else if compare {
		smithOpts = append(smithOpts,
			sqlsmith.CompareMode(),
			sqlsmith.DisableCRDBFns(),
		)
	}
	if _, ok := conns[opts.Smither]; !ok {
		log.Fatalf("Smither option not present in databases: %s", opts.Smither)
	}
	var smither *sqlsmith.Smither
	var stmts []statement
	if len(opts.SQL) == 0 {
		smither, err = sqlsmith.NewSmither(conns[opts.Smither].DB(), rng, smithOpts...)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		stmts = make([]statement, len(opts.SQL))
		for i, stmt := range opts.SQL {
			ps, err := conns[opts.Smither].PGX().Prepare("", stmt)
			if err != nil {
				log.Fatalf("bad SQL statement on %s: %v\nSQL:\n%s", opts.Smither, stmt, err)
			}
			var placeholders []*types.T
			for _, param := range ps.ParameterOIDs {
				typ, ok := types.OidToType[oid.Oid(param)]
				if !ok {
					log.Fatalf("unknown oid: %v", param)
				}
				placeholders = append(placeholders, typ)
			}
			stmts[i] = statement{
				stmt:         stmt,
				placeholders: placeholders,
			}
		}
	}

	var prep, exec string
	ctx := context.Background()
	done := time.After(timeout)
	for i := 0; true; i++ {
		select {
		case <-done:
			return
		default:
		}
		fmt.Printf("stmt: %d\n", i)
		if smither != nil {
			exec = smither.Generate()
		} else {
			randStatement := stmts[rng.Intn(len(stmts))]
			name := fmt.Sprintf("s%d", i)
			prep = fmt.Sprintf("PREPARE %s AS\n%s;", name, randStatement.stmt)
			var sb strings.Builder
			fmt.Fprintf(&sb, "EXECUTE %s", name)
			for i, typ := range randStatement.placeholders {
				if i > 0 {
					sb.WriteString(", ")
				} else {
					sb.WriteString(" (")
				}
				d := randgen.RandDatum(rng, typ, true)
				fmt.Println(i, typ, d, tree.Serialize(d))
				sb.WriteString(tree.Serialize(d))
			}
			if len(randStatement.placeholders) > 0 {
				fmt.Fprintf(&sb, ")")
			}
			fmt.Fprintf(&sb, ";")
			exec = sb.String()
			fmt.Println(exec)
		}
		if compare {
			if err := cmpconn.CompareConns(
				ctx, stmtTimeout, conns, prep, exec, true, /* ignoreSQLErrors */
			); err != nil {
				fmt.Printf("prep:\n%s;\nexec:\n%s;\nERR: %s\n\n", prep, exec, err)
				os.Exit(1)
			}
		} else {
			for _, conn := range conns {
				if err := conn.Exec(ctx, prep+exec); err != nil {
					fmt.Println(err)
				}
			}
		}

		// Make sure the servers are alive.
		for name, conn := range conns {
			start := timeutil.Now()
			fmt.Printf("pinging %s...", name)
			if err := conn.Ping(); err != nil {
				fmt.Printf("\n%s: ping failure: %v\nprevious SQL:\n%s;\n%s;\n", name, err, prep, exec)
				// Try to reconnect.
				db := opts.Databases[name]
				newConn, err := cmpconn.NewConnWithMutators(
					db.Addr, rng, enableMutations(db.AllowMutations, sqlMutators),
					db.InitSQL, opts.InitSQL,
				)
				if err != nil {
					log.Fatalf("tried to reconnect: %v\n", err)
				}
				conns[name] = newConn
			}
			fmt.Printf(" %s\n", timeutil.Since(start))
		}
	}
}

type statement struct {
	stmt         string
	placeholders []*types.T
}
