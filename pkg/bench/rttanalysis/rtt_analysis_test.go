// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

var reg = NewRegistry(1 /* numNodes */, MakeClusterConstructor(func(
	t testing.TB, knobs base.TestingKnobs,
) (_ *gosql.DB, cleanup func()) {
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "bench",
		Knobs:       knobs,
	})
	// Eventlog is async, and introduces jitter in the benchmark.
	if _, err := db.Exec("SET CLUSTER SETTING server.eventlog.enabled = false"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE USER testuser"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("GRANT admin TO testuser"); err != nil {
		t.Fatal(err)
	}
	url, testuserCleanup := sqlutils.PGUrl(t, s.ApplicationLayer().AdvSQLAddr(), "rttanalysis", url.User("testuser"))
	conn, err := gosql.Open("postgres", url.String())
	if err != nil {
		t.Fatal(err)
	}
	return conn, func() {
		s.Stopper().Stop(context.Background())
		testuserCleanup()
	}
}))
