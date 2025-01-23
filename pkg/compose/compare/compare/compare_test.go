// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package compare

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/cmpconn"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

var (
	flagEach      = flag.Duration("each", 10*time.Minute, "individual test timeout")
	flagArtifacts = flag.String("artifacts", "", "artifact directory")
)

func TestCompare(t *testing.T) {
	if os.Getenv("COCKROACH_RUN_COMPOSE_COMPARE") == "" {
		skip.IgnoreLint(t, "COCKROACH_RUN_COMPOSE_COMPARE not set")
	}
	// N.B. randomized SQL workload performed by this test may require CCL
	var license = envutil.EnvOrDefaultString("COCKROACH_DEV_LICENSE", "")
	require.NotEmptyf(t, license, "COCKROACH_DEV_LICENSE must be set")

	// Initialize GEOS libraries so that the test can use geospatial types.
	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	_, err = geos.EnsureInit(geos.EnsureInitErrorDisplayPrivate, path.Join(workingDir, "lib"))
	if err != nil {
		t.Fatal(err)
	}

	uris := map[string]struct {
		addr string
		init []string
	}{
		"cockroach1": {
			addr: "postgresql://root@cockroach1:26257/postgres?sslmode=disable",
			init: []string{
				"SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing'",
				"SET extra_float_digits = 0",   // For Postgres Compat when casting floats to strings.
				"SET null_ordered_last = true", // For Postgres Compat, see https://www.cockroachlabs.com/docs/stable/order-by#parameters
				fmt.Sprintf("SET CLUSTER SETTING enterprise.license = '%s'", license),
				"drop database if exists postgres",
				"create database postgres",
			},
		},
		"cockroach2": {
			addr: "postgresql://root@cockroach2:26257/postgres?sslmode=disable",
			init: []string{
				"SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing'",
				"SET extra_float_digits = 0",   // For Postgres Compat when casting floats to strings.
				"SET null_ordered_last = true", // For Postgres Compat https://www.cockroachlabs.com/docs/stable/order-by#parameters
				fmt.Sprintf("SET CLUSTER SETTING enterprise.license = '%s'", license),
				"drop database if exists postgres",
				"create database postgres",
			},
		},
	}
	configs := map[string]testConfig{
		"mutators": {
			setup:           sqlsmith.Setups[sqlsmith.RandTableSetupName],
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
						randgen.ForeignKeyMutator,
						randgen.ColumnFamilyMutator,
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
			rng, _ := randutil.NewTestRand()
			setup := config.setup(rng)
			for i := range setup {
				setup[i], _ = randgen.ApplyString(rng, setup[i], config.setupMutators...)
			}

			conns := map[string]cmpconn.Conn{}
			for _, testCn := range config.conns {
				t.Logf("initializing connection: %s", testCn.name)
				uri, ok := uris[testCn.name]
				if !ok {
					t.Fatalf("bad connection name: %s", testCn.name)
				}
				conn, err := cmpconn.NewConnWithMutators(ctx, uri.addr, rng, testCn.mutators)
				if err != nil {
					t.Fatal(err)
				}

				defer func(conn cmpconn.Conn) {
					conn.Close(ctx)
				}(conn)

				for _, init := range uri.init {
					if err := conn.Exec(ctx, init); err != nil {
						t.Fatalf("%s: %v", testCn.name, err)
					}
				}
				for i := range setup {
					stmt, _ := randgen.ApplyString(rng, setup[i], testCn.mutators...)
					if err := conn.Exec(ctx, stmt); err != nil {
						t.Log(stmt)
						t.Fatalf("%s: %v", testCn.name, err)
					}
				}
				conns[testCn.name] = conn
			}
			smither, err := sqlsmith.NewSmither(conns[config.conns[0].name].DB(), rng, config.opts...)
			if err != nil {
				t.Fatal(err)
			}

			ignoredErrCount := 0
			totalQueryCount := 0
			until := time.After(*flagEach)
			for {
				select {
				case <-until:
					t.Logf("done with test. totalQueryCount=%d ignoredErrCount=%d test=%s",
						totalQueryCount, ignoredErrCount, confName,
					)
					return
				default:
				}
				query := smither.Generate()
				if ignoredErr, err := cmpconn.CompareConns(
					ctx, time.Second*30, conns, "" /* prep */, query, config.ignoreSQLErrors,
				); err != nil {
					path := filepath.Join(*flagArtifacts, confName+".log")
					if err := os.WriteFile(path, []byte(err.Error()), 0666); err != nil {
						t.Log(err)
					}
					t.Fatal(err)
				} else if ignoredErr {
					ignoredErrCount++
				}
				totalQueryCount++
				// Make sure we can still ping on a connection. If we can't, we may have
				// crashed something.
				for name, conn := range conns {
					if err := conn.Ping(ctx); err != nil {
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
