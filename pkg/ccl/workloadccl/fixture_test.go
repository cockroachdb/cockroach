// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package workloadccl

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/spf13/pflag"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const fixtureTestGenRows = 10

type fixtureTestGen struct {
	flags workload.Flags
	val   string
}

func makeTestWorkload() workload.Flagser {
	g := &fixtureTestGen{}
	g.flags.FlagSet = pflag.NewFlagSet(`fx`, pflag.ContinueOnError)
	g.flags.StringVar(&g.val, `val`, `default`, `The value for each row`)
	return g
}

func (fixtureTestGen) Meta() workload.Meta     { return workload.Meta{Name: `fixture`} }
func (g fixtureTestGen) Flags() workload.Flags { return g.flags }
func (g fixtureTestGen) Tables() []workload.Table {
	return []workload.Table{{
		Name:   `fx`,
		Schema: `(key INT PRIMARY KEY, value INT)`,
		InitialRows: workload.Tuples(
			fixtureTestGenRows,
			func(rowIdx int) []interface{} {
				return []interface{}{rowIdx, g.val}
			},
		),
	}}
}

func TestFixture(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	gcsBucket := os.Getenv(`GS_BUCKET`)
	gcsKey := os.Getenv(`GS_JSONKEY`)
	if gcsBucket == "" || gcsKey == "" {
		t.Skip("GS_BUCKET and GS_JSONKEY env vars must be set")
	}

	// This prevents leaking an http conn goroutine.
	http.DefaultTransport.(*http.Transport).DisableKeepAlives = true
	source, err := google.JWTConfigFromJSON([]byte(gcsKey), storage.ScopeReadWrite)
	if err != nil {
		t.Fatalf(`%+v`, err)
	}
	gcs, err := storage.NewClient(ctx,
		option.WithScopes(storage.ScopeReadWrite),
		option.WithTokenSource(source.TokenSource(ctx)))
	if err != nil {
		t.Fatalf(`%+v`, err)
	}
	defer func() { _ = gcs.Close() }()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING cloudstorage.gs.default.key = $1`, gcsKey)

	gen := makeTestWorkload()
	flag := fmt.Sprintf(`val=%d`, timeutil.Now().UnixNano())
	if err := gen.Flags().Parse([]string{"--" + flag}); err != nil {
		t.Fatalf(`%+v`, err)
	}

	config := FixtureConfig{
		GCSBucket: gcsBucket,
		GCSPrefix: fmt.Sprintf(`TestFixture-%d`, timeutil.Now().UnixNano()),
	}

	if _, err := GetFixture(ctx, gcs, config, gen); !testutils.IsError(err, `fixture not found`) {
		t.Fatalf(`expected "fixture not found" error but got: %+v`, err)
	}

	fixtures, err := ListFixtures(ctx, gcs, config)
	if err != nil {
		t.Fatalf(`%+v`, err)
	}
	if len(fixtures) != 0 {
		t.Errorf(`expected no fixtures but got: %+v`, fixtures)
	}

	fixture, err := MakeFixture(ctx, sqlDB.DB, gcs, config, gen)
	if err != nil {
		t.Fatalf(`%+v`, err)
	}

	_, err = MakeFixture(ctx, sqlDB.DB, gcs, config, gen)
	if !testutils.IsError(err, `already exists`) {
		t.Fatalf(`expected 'already exists' error got: %+v`, err)
	}

	fixtures, err = ListFixtures(ctx, gcs, config)
	if err != nil {
		t.Fatalf(`%+v`, err)
	}
	if len(fixtures) != 1 || !strings.Contains(fixtures[0], flag) {
		t.Errorf(`expected exactly one %s fixture but got: %+v`, flag, fixtures)
	}

	sqlDB.Exec(t, `CREATE DATABASE test`)
	if err := RestoreFixture(ctx, sqlDB.DB, fixture, `test`); err != nil {
		t.Fatalf(`%+v`, err)
	}
	sqlDB.CheckQueryResults(t,
		`SELECT COUNT(*) FROM test.fx`, [][]string{{strconv.Itoa(fixtureTestGenRows)}})
}
