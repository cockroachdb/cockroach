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
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
)

var reg = NewRegistry(1 /* numNodes */, MakeClusterConstructor(func(
	t testing.TB, knobs base.TestingKnobs,
) (_, _ *gosql.DB, cleanup func()) {
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "bench",
		Knobs:       knobs,
	})
	// Eventlog is async, and introduces jitter in the benchmark.
	if _, err := db.Exec("SET CLUSTER SETTING server.eventlog.enabled = false"); err != nil {
		t.Fatal(err)
	}
	// Create a user with admin privileges.
	if _, err := db.Exec("CREATE USER testuser"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("GRANT admin TO testuser"); err != nil {
		t.Fatal(err)
	}
	adminUserURL, adminUserCleanup := pgurlutils.PGUrl(
		t, s.ApplicationLayer().AdvSQLAddr(), "rttanalysis", url.User("testuser"),
	)
	adminUserConn, err := gosql.Open("postgres", adminUserURL.String())
	if err != nil {
		t.Fatal(err)
	}
	// Create a user with no privileges.
	if _, err := db.Exec("CREATE USER testuser2"); err != nil {
		t.Fatal(err)
	}
	nonAdminUserURL, nonAdminUserCleanup := pgurlutils.PGUrl(
		t, s.ApplicationLayer().AdvSQLAddr(), "rttanalysis", url.User("testuser2"),
	)
	nonAdminUserConn, err := gosql.Open("postgres", nonAdminUserURL.String())
	if err != nil {
		t.Fatal(err)
	}
	return adminUserConn, nonAdminUserConn, func() {
		s.Stopper().Stop(context.Background())
		adminUserCleanup()
		nonAdminUserCleanup()
	}
}))
