// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSearchPathEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, s.ServingSQLAddr(), "TestSearchPathQuotingInConnectionString" /* prefix */, url.User(username.RootUser),
	)
	defer cleanupFunc()

	pgURL.RawQuery += `&search_path="Abc", Abc`
	db, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer db.Close()

	for _, stmt := range []string{
		`CREATE SCHEMA abc`,
		`CREATE TABLE abc.t(a text);`,
		`INSERT INTO abc.t values ('lower case schema')`,
	} {
		_, err = db.Exec(stmt)
		require.NoError(t, err)
	}

	var searchPath string
	err = db.QueryRow("SHOW search_path").Scan(&searchPath)
	require.NoError(t, err)
	require.Equal(t, `"Abc", abc`, searchPath)

	var a string
	err = db.QueryRow("SELECT a FROM t").Scan(&a)
	require.NoError(t, err)
	// But the `Abc` schema in the search_path gets normalized to `abc`.
	require.Equal(t, "lower case schema", a)

	for _, stmt := range []string{
		`CREATE SCHEMA "Abc"`,
		`CREATE TABLE "Abc".t(a text);`,
		`INSERT INTO "Abc".t values ('mixed case schema')`,
	} {
		_, err = db.Exec(stmt)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = db.QueryRow("SELECT a FROM t").Scan(&a)
	require.NoError(t, err)
	// Now the `"Abc"` schema should be preferred.
	require.Equal(t, "mixed case schema", a)
}
