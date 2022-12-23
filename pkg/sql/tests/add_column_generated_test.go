// Copyright 2020 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestAddGeneratedColumnValidation tests if validation
// when creating computed columns is working properly
func TestAddGeneratedColumnValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var s serverutils.TestServerInterface
	params, _ := CreateTestServerParams()

	var db *gosql.DB
	s, db, _ = serverutils.StartServer(t, params)
	//sqlDB := sqlutils.MakeSQLRunner(db)
	defer s.Stopper().Stop(ctx)

	_, err := db.Exec(`
	CREATE TABLE t (i INT PRIMARY KEY);
	INSERT INTO t VALUES (1);`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`ALTER TABLE t ADD COLUMN s VARCHAR(2) AS ('st') STORED;`)
	require.NoError(t, err)
	//expected := "pq: value too long for type VARCHAR(2)"
	_, err = db.Exec(`ALTER TABLE t ADD COLUMN test INT;`)
	require.NoError(t, err)

	_, err = db.Exec(`ALTER TABLE t ADD COLUMN ii INT AS (test*2) STORED;`)
	require.NoError(t, err)

	_, err = db.Exec(`ALTER TABLE t ADD COLUMN iii INT AS (test*3) VIRTUAL;`)
	require.NoError(t, err)

	_, err = db.Exec(`ALTER TABLE t ADD COLUMN v2 VARCHAR(2) AS ('virtual') VIRTUAL;`)
	require.Error(t, err)
	require.True(t, err.Error() == "pq: value too long for type VARCHAR(2)")

	_, err = db.Exec(`ALTER TABLE t ADD COLUMN s2 VARCHAR(2) AS ('stored') STORED;`)
	require.Error(t, err)
	require.True(t, err.Error() == "pq: value too long for type VARCHAR(2)")

	_, err = db.Exec(`ALTER TABLE t ADD COLUMN s3 INT2 AS (32768) STORED;`)
	require.Error(t, err)
	require.True(t, err.Error() == "pq: integer out of range for type int2")

	_, err = db.Exec(`ALTER TABLE t ADD COLUMN s4 BOOL AS (32768) STORED;`)
	require.Error(t, err)
	require.True(t, err.Error() == "pq: expected computed column expression to have type bool, but '32768' has type int")

	_, err = db.Exec(`ALTER TABLE t ADD COLUMN s3 INT2 AS (32768) VIRTUAL;`)
	require.Error(t, err)
	require.True(t, err.Error() == "pq: integer out of range for type int2")

	_, err = db.Exec(`ALTER TABLE t ADD COLUMN s4 BOOL AS (32768) VIRTUAL;`)
	require.Error(t, err)
	require.True(t, err.Error() == "pq: expected computed column expression to have type bool, but '32768' has type int")

	_, err = db.Exec(`INSERT INTO t VALUES (2);`)
	require.NoError(t, err)

}
