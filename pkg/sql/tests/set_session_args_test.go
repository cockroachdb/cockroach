// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestSetSessionArguments(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Enable enterprise features so READ COMMITTED can be tested.
	defer ccl.TestingEnableEnterprise()()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	ctx := context.Background()
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	pgURL, cleanupFunc := s.PGUrl(
		t, serverutils.CertsDirPrefix("testConnClose"), serverutils.User(username.RootUser),
	)
	defer cleanupFunc()

	testOptionValues, err := url.ParseQuery(`options=` + `  --user=test -c    search_path=public,testsp,"Abc",Def %20 ` +
		"--default-transaction-isolation=read\\ uncommitted   " +
		"-capplication_name=test  " +
		"--DateStyle=ymd\\ ,\\ iso\\  " +
		"-c intervalstyle%3DISO_8601 " +
		"-ccustom_option.custom_option=test2")
	require.NoError(t, err)

	query := pgURL.Query()
	query.Set("options", query.Get("options")+" "+testOptionValues.Get("options"))
	pgURL.RawQuery = query.Encode()

	noBufferDB, err := gosql.Open("postgres", pgURL.String())

	if err != nil {
		t.Fatal(err)
	}
	defer noBufferDB.Close()

	pgxConfig, err := pgx.ParseConfig(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.ConnectConfig(ctx, pgxConfig)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := conn.Query(ctx, "show all")
	if err != nil {
		t.Fatal(err)
	}

	expectedOptions := map[string]string{
		"search_path": "public, testsp, \"Abc\", def",
		// Setting the isolation level to read uncommitted should map
		// to read committed.
		"default_transaction_isolation": "read committed",
		"application_name":              "test",
		"datestyle":                     "ISO, YMD",
		"intervalstyle":                 "iso_8601",
	}
	expectedFoundOptions := len(expectedOptions)

	var foundOptions int
	var variable, value string
	for rows.Next() {
		err = rows.Scan(&variable, &value)
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := expectedOptions[variable]; ok {
			foundOptions++
			if v != value {
				t.Fatalf("option %q expected value %q, actual %q", variable, v, value)
			}
		}
	}
	require.Equal(t, expectedFoundOptions, foundOptions)

	// Custom session options don't show up on SHOW ALL
	var customOption string
	require.NoError(t, conn.QueryRow(ctx, "SHOW custom_option.custom_option").Scan(&customOption))
	require.Equal(t, "test2", customOption)

	if err := conn.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
