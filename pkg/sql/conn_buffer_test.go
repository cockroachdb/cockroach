// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/jackc/pgx"
	"github.com/stretchr/testify/require"
)

func TestBigClientMessage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	s, mainDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	// Form a 64kB string.
	str := "a"
	for len(str) < 1<<16 {
		str += str
	}

	// Check we can send a 1MB string.
	_, err := mainDB.Exec(fmt.Sprintf(`SELECT '%s'`, str))
	require.NoError(t, err)

	// Set the cluster setting to be less than 1MB.
	_, err = mainDB.Exec(`SET CLUSTER SETTING sql.conn.max_read_buffer_message_size = '32kB'`)
	require.NoError(t, err)

	// On a new connection, try send the same string back.
	{
		pgURL, cleanup := sqlutils.PGUrl(
			t,
			s.ServingSQLAddr(),
			"big message size",
			url.User(security.RootUser),
		)
		defer cleanup()

		conf, err := pgx.ParseConnectionString(pgURL.String())
		require.NoError(t, err)
		c, err := pgx.Connect(conf)
		require.NoError(t, err)
		defer func() { _ = c.Close() }()

		// A smaller string is ok.
		_, err = c.Exec(fmt.Sprintf(`SELECT '%s'`, "a"))
		require.NoError(t, err)

		// A 64kB string is too big.
		_, err = c.Exec(fmt.Sprintf(`SELECT '%s'`, str))
		require.Error(t, err)
		// TODO(#50330): assert a finer tune error message.
		// At the moment, this can be EOF or connection reset by peer.
	}
}
