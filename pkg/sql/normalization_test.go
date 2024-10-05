// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestNFCNormalization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	_, err := db.Exec("CREATE TABLE café (a INT)")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE Cafe\u0301 (a INT)")
	require.Errorf(t, err, "The tables should be considered duplicates when normalized")
	require.True(t, strings.Contains(err.Error(), "already exists"))

	_, err = db.Exec("CREATE TABLE cafe\u0301 (a INT)")
	require.Errorf(t, err, "The tables should be considered duplicates when normalized")
	require.True(t, strings.Contains(err.Error(), "already exists"))

	_, err = db.Exec("CREATE TABLE caf\u00E9 (a INT)")
	require.Errorf(t, err, "The tables should be considered duplicates when normalized")
	require.True(t, strings.Contains(err.Error(), "already exists"))

	_, err = db.Exec("CREATE TABLE \"caf\u00E9\" (a INT)")
	require.Errorf(t, err, "The tables should be considered duplicates when normalized")
	require.True(t, strings.Contains(err.Error(), "already exists"))

	_, err = db.Exec("CREATE TABLE \"cafe\u0301\" (a INT)")
	require.Errorf(t, err, "The tables should be considered duplicates when normalized")
	require.True(t, strings.Contains(err.Error(), "already exists"))

	_, err = db.Exec(`CREATE TABLE "Café" (a INT)`)
	require.NoError(t, err)
	//Ensure normal strings are not normalized like double quoted strings
	var b bool
	err = db.QueryRow("SELECT 'caf\u00E9' = 'cafe\u0301'").Scan(&b)
	require.NoError(t, err)
	require.False(t, b)

}
