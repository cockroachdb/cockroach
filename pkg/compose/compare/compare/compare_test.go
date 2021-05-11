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
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/jackc/pgx/v4"
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
				"CREATE EXTENSION IF NOT EXISTS postgis",
				"CREATE EXTENSION IF NOT EXISTS postgis_topology",
				"CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;",
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
			setup:           sqlsmith.Setups["rand-tables"],
			setupMutators:   []randgen.Mutator{randgen.PostgresCreateTableMutator},
			opts:            []sqlsmith.SmitherOption{sqlsmith.PostgresMode()},
			ignoreSQLErrors: true,
			conns: []testConn{
				{
					name:     "cockroach1",
					mutators: []randgen.Mutator{},
				},
				{
					name:     "postgres",
					mutators: []randgen.Mutator{randgen.PostgresMutator},
				},
			},
		},
		"mutators": {
			setup:           sqlsmith.Setups["rand-tables"],
			opts:            []sqlsmith.SmitherOption{sqlsmith.CompareMode()},
			ignoreSQLErrors: true,
			conns: []testConn{
				{
					name:     "cockroach1",
					mutators: []randgen.Mutator{},
				},
				{
					name: "cockroach2",
					mutators: []randgen.Mutator{
						randgen.StatisticsMutator,
						randgen.ForeignKeyMutator,
						randgen.ColumnFamilyMutator,
						randgen.StatisticsMutator,
						randgen.IndexStoringMutator,
						randgen.PartialIndexMutator,
					},
				},
			},
		},
	}

	ctx := context.Background()

	// docker-compose requires us to manually check for when a container
	// is ready to receive connections.
	// See https://docs.docker.com/compose/startup-order/
	for name, uri := range uris {
		t.Logf("Checking connection to: %s", name)
		testutils.SucceedsSoon(t, func() error {
			_, err := pgx.Connect(ctx, uri.addr)
			return err
		})
	}

	for confName, config := range configs {
		t.Run(confName, func(t *testing.T) {
			t.Logf("starting test: %s", confName)
			rng, _ := randutil.NewPseudoRand()
			setup := config.setup(rng)
			setup, _ = randgen.ApplyString(rng, setup, config.setupMutators...)

			conns := map[string]cmpconn.Conn{}
			for _, testCn := range config.conns {
				t.Logf("initializing connection: %s", testCn.name)
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
				connSetup, _ := randgen.ApplyString(rng, setup, testCn.mutators...)
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
					t.Logf("done with test: %s", confName)
					return
				default:
				}
				query := smither.Generate()
				if err := cmpconn.CompareConns(
					ctx, time.Second*30, conns, "" /* prep */, query, config.ignoreSQLErrors,
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
	opts            []sqlsmith.SmitherOption
	conns           []testConn
	setup           sqlsmith.Setup
	setupMutators   []randgen.Mutator
	ignoreSQLErrors bool
}

type testConn struct {
	name     string
	mutators []randgen.Mutator
}
