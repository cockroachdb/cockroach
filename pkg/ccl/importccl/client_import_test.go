// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestDropDatabaseCascadeDuringImportsFails ensures that dropping a database
// while an IMPORT is ongoing fails with an error. This is critical because
// otherwise we may end up with orphaned table descriptors. See #48589 for
// more details.
func TestDropDatabaseCascadeDuringImportsFails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	args := base.TestClusterArgs{}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	tc.WaitForNodeLiveness(t)
	require.NoError(t, tc.WaitForFullReplication())

	db := tc.ServerConn(0)
	runner := sqlutils.MakeSQLRunner(db)

	// Use some names that need quoting to ensure that the error quoting is correct.
	const dbName, tableName = `"fooBarBaz"`, `"foo bar"`
	runner.Exec(t, `CREATE DATABASE `+dbName)

	mkServer := func(method string, handler func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == method {
				handler(w, r)
			}
		}))
	}

	// Let's start an import into this table of ours.
	allowResponse := make(chan struct{})
	var gotRequestOnce sync.Once
	gotRequest := make(chan struct{})
	srv := mkServer("GET", func(w http.ResponseWriter, r *http.Request) {
		gotRequestOnce.Do(func() { close(gotRequest) })
		select {
		case <-allowResponse:
		case <-ctx.Done(): // Deal with test failures.
		}
		_, _ = w.Write([]byte("1,asdfasdfasdfasdf"))
	})
	defer srv.Close()

	importErrCh := make(chan error, 1)
	go func() {
		_, err := db.Exec(`IMPORT TABLE `+dbName+"."+tableName+
			` (k INT, v STRING) CSV DATA ($1)`, srv.URL)
		importErrCh <- err
	}()
	select {
	case <-gotRequest:
	case err := <-importErrCh:
		t.Fatalf("err %v", err)
	}

	_, err := db.Exec(`DROP DATABASE "fooBarBaz" CASCADE`)
	require.Regexp(t, `cannot drop a database with OFFLINE tables, ensure `+
		dbName+`\.public\.`+tableName+` is dropped or made public before dropping`+
		` database `+dbName, err)
	pgErr := new(pq.Error)
	require.True(t, errors.As(err, &pgErr))
	require.Equal(t, pgcode.ObjectNotInPrerequisiteState, pgcode.MakeCode(string(pgErr.Code)))

	close(allowResponse)
	require.NoError(t, <-importErrCh)
	runner.Exec(t, `DROP DATABASE `+dbName+` CASCADE`)
}
