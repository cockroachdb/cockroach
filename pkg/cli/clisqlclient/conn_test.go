// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlclient_test

import (
	"context"
	"database/sql/driver"
	"io/ioutil"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

func makeSQLConn(url string) clisqlclient.Conn {
	var sqlConnCtx clisqlclient.Context
	return sqlConnCtx.MakeSQLConn(ioutil.Discard, ioutil.Discard, url)
}

func TestConnRecover(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := cli.TestCLIParams{T: t}
	c := cli.NewCLITest(p)
	defer c.Cleanup()
	ctx := context.Background()

	url, cleanup := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Sanity check to establish baseline.
	rows, err := conn.Query(ctx, `SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Check that Query detects a connection close.
	defer simulateServerRestart(t, &c, p, conn)()

	// When the server restarts, the next Query() attempt may encounter a
	// TCP reset error before the SQL driver realizes there is a problem
	// and starts delivering ErrBadConn. We don't know the timing of
	// this however.
	testutils.SucceedsSoon(t, func() error {
		if sqlRows, err := conn.Query(ctx, `SELECT 1`); err == nil {
			if closeErr := sqlRows.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		} else if !errors.Is(err, driver.ErrBadConn) {
			return errors.Newf("expected ErrBadConn, got %v", err) // nolint:errwrap
		}
		return nil
	})

	// Check that Query recovers from a connection close by re-connecting.
	rows, err = conn.Query(ctx, `SELECT 1`)
	if err != nil {
		t.Fatalf("conn.Query(): expected no error after reconnect, got %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Check that Exec detects a connection close.
	defer simulateServerRestart(t, &c, p, conn)()

	// Ditto from Query().
	testutils.SucceedsSoon(t, func() error {
		if err := conn.Exec(ctx, `SELECT 1`); !errors.Is(err, driver.ErrBadConn) {
			return errors.Newf("expected ErrBadConn, got %v", err) // nolint:errwrap
		}
		return nil
	})

	// Check that Exec recovers from a connection close by re-connecting.
	if err := conn.Exec(ctx, `SELECT 1`); err != nil {
		t.Fatalf("conn.Exec(): expected no error after reconnect, got %v", err)
	}
}

// simulateServerRestart restarts the test server and reconfigures the connection
// to use the new test server's port number. This is necessary because the port
// number is selected randomly.
func simulateServerRestart(
	t *testing.T, c *cli.TestCLI, p cli.TestCLIParams, conn clisqlclient.Conn,
) func() {
	c.RestartServer(p)
	url2, cleanup2 := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	conn.SetURL(url2.String())
	return cleanup2
}

func TestTransactionRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := cli.TestCLIParams{T: t}
	c := cli.NewCLITest(p)
	defer c.Cleanup()
	ctx := context.Background()

	url, cleanup := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	var tries int
	err := conn.ExecTxn(ctx, func(ctx context.Context, conn clisqlclient.TxBoundConn) error {
		tries++
		if tries > 2 {
			return nil
		}

		// Prevent automatic server-side retries.
		rows, err := conn.Query(ctx, `SELECT now()`)
		if err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}

		// Force a client-side retry.
		rows, err = conn.Query(ctx, `SELECT crdb_internal.force_retry('1h')`)
		if err != nil {
			return err
		}
		return rows.Close()
	})
	if err != nil {
		t.Fatal(err)
	}
	if tries <= 2 {
		t.Fatalf("expected transaction to require at least two tries, but it only required %d", tries)
	}
}
