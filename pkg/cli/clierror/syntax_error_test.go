// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clierror_test

import (
	"context"
	"io/ioutil"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// This test checks that IsSQLSyntaxError works. It could stop working if e.g.
// the surrounding code stops using lib/pq as SQL driver, and/or the error type
// from query execution is not pq.Error any more.
func TestIsSQLSyntaxError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := cli.TestCLIParams{T: t}
	c := cli.NewCLITest(p)
	defer c.Cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()

	var sqlConnCtx clisqlclient.Context
	conn := sqlConnCtx.MakeSQLConn(ioutil.Discard, ioutil.Discard, url.String())
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	_, err := conn.QueryRow(context.Background(), `INVALID SYNTAX`)
	if !clierror.IsSQLSyntaxError(err) {
		t.Fatalf("expected error to be recognized as syntax error: %+v", err)
	}
}
