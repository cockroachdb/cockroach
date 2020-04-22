// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// "make test" would normally test this file, but it should only be tested
// within docker compose.

// +build compose

package compare

import (
	"context"
	"flag"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/cmpconn"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/sql/mutations"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var (
	flagEach      = flag.Duration("each", 10*time.Minute, "individual test timeout")
	flagArtifacts = flag.String("artifacts", "", "artifact directory")
)

func TestCompare(t *testing.T) {
	uris := map[string]struct {
		addr string
		init []string
	}{
		"postgres": {
			addr: "postgresql://postgres@postgres:5432/postgres",
			init: []string{
				"drop schema if exists public cascade",
				"create schema public",
			},
		},
		"cockroach1": {
			addr: "postgresql://root@cockroach1:26257/postgres?sslmode=disable",
			init: []string{
				"drop database if exists postgres",
				"create database postgres",
			},
		},
		"cockroach2": {
			addr: "postgresql://root@cockroach2:26257/postgres?sslmode=disable",
			init: []string{
				"drop database if exists postgres",
				"create database postgres",
			},
		},
	}
	configs := map[string]testConfig{
		"postgres": {
			setup:         sqlsmith.Setups["rand-tables"],
			setupMutators: []sqlbase.Mutator{mutations.PostgresCreateTableMutator},
			opts:          []sqlsmith.SmitherOption{sqlsmith.PostgresMode()},
			conns: []testConn{
				{
					name:     "cockroach1",
					mutators: []sqlbase.Mutator{},
				},
				{
					name:     "postgres",
					mutators: []sqlbase.Mutator{mutations.PostgresMutator},
				},
			},
		},
		"mutators": {
			setup: sqlsmith.Setups["rand-tables"],
			opts:  []sqlsmith.SmitherOption{sqlsmith.CompareMode()},
			conns: []testConn{
				{
					name:     "cockroach1",
					mutators: []sqlbase.Mutator{},
				},
				{
					name: "cockroach2",
					mutators: []sqlbase.Mutator{
						mutations.StatisticsMutator,
						mutations.ForeignKeyMutator,
						mutations.ColumnFamilyMutator,
						mutations.StatisticsMutator,
						mutations.IndexStoringMutator,
					},
				},
			},
		},
	}

	ctx := context.Background()
	for confName, config := range configs {
		t.Run(confName, func(t *testing.T) {
			rng, _ := randutil.NewPseudoRand()
			setup := config.setup(rng)
			setup, _ = mutations.ApplyString(rng, setup, config.setupMutators...)

			conns := map[string]cmpconn.Conn{}
			for _, testCn := range config.conns {
				uri, ok := uris[testCn.name]
				if !ok {
					t.Fatalf("bad connection name: %s", testCn.name)
				}
				conn, err := cmpconn.NewConnWithMutators(uri.addr, rng, testCn.mutators)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				for _, init := range uri.init {
					if err := conn.Exec(ctx, init); err != nil {
						t.Fatalf("%s: %v", testCn.name, err)
					}
				}
				connSetup, _ := mutations.ApplyString(rng, setup, testCn.mutators...)
				if err := conn.Exec(ctx, connSetup); err != nil {
					t.Log(connSetup)
					t.Fatalf("%s: %v", testCn.name, err)
				}
				conns[testCn.name] = conn
			}
			smither, err := sqlsmith.NewSmither(conns[config.conns[0].name].DB(), rng, config.opts...)
			if err != nil {
				t.Fatal(err)
			}

			until := time.After(*flagEach)
			for {
				select {
				case <-until:
					return
				default:
				}
				query := smither.Generate()
				query, _ = mutations.ApplyString(rng, query, mutations.PostgresMutator)
				if err := cmpconn.CompareConns(
					ctx, time.Second*30, conns, "" /* prep */, query, true, /* ignoreSQLErrors */
				); err != nil {
					path := filepath.Join(*flagArtifacts, confName+".log")
					if err := ioutil.WriteFile(path, []byte(err.Error()), 0666); err != nil {
						t.Log(err)
					}
					t.Fatal(err)
				}
				// Make sure we can still ping on a connection. If we can't we may have
				// crashed something.
				for name, conn := range conns {
					if err := conn.Ping(); err != nil {
						t.Log(query)
						t.Fatalf("%s: ping: %v", name, err)
					}
				}
			}
		})
	}
}

type testConfig struct {
	opts          []sqlsmith.SmitherOption
	conns         []testConn
	setup         sqlsmith.Setup
	setupMutators []sqlbase.Mutator
}

type testConn struct {
	name     string
	mutators []sqlbase.Mutator
}
