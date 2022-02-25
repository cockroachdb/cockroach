// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Make sure that running a wire-protocol-level PREPARE of a SQL-level PREPARE
// doesn't cause any problems.
func TestPreparePrepare(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer srv.Stopper().Stop(context.Background())
	defer db.Close()

	_, err := db.Prepare("EXECUTE x(3)")
	require.Contains(t, err.Error(), "no such prepared statement")

	_, err = db.Exec("PREPARE x AS SELECT $1::int")
	require.NoError(t, err)

	s, err := db.Prepare("EXECUTE x(3)")
	require.NoError(t, err)

	var output int
	err = s.QueryRow().Scan(&output)
	require.NoError(t, err)
	require.Equal(t, 3, output)
}
