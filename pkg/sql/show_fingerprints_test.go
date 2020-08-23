// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// NB: Most of the SHOW EXPERIMENTAL_FINGERPRINTS tests are in the
// show_fingerprints logic test. This is just to test the AS OF SYSTEM TIME
// functionality.
func TestShowFingerprintsAsOfSystemTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (a INT PRIMARY KEY, b INT, INDEX b_idx (b))`)
	sqlDB.Exec(t, `INSERT INTO d.t VALUES (1, 2)`)

	const fprintQuery = `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE d.t`
	fprint1 := sqlDB.QueryStr(t, fprintQuery)

	var ts string
	sqlDB.QueryRow(t, `SELECT now()`).Scan(&ts)

	sqlDB.Exec(t, `INSERT INTO d.t VALUES (3, 4)`)
	sqlDB.Exec(t, `DROP INDEX d.t@b_idx`)

	fprint2 := sqlDB.QueryStr(t, fprintQuery)
	if reflect.DeepEqual(fprint1, fprint2) {
		t.Errorf("expected different fingerprints: %v vs %v", fprint1, fprint2)
	}

	fprint3Query := fmt.Sprintf(`SELECT * FROM [%s] AS OF SYSTEM TIME '%s'`, fprintQuery, ts)
	sqlDB.CheckQueryResults(t, fprint3Query, fprint1)
}

func TestShowFingerprintsColumnNames(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (
		lowercase INT PRIMARY KEY,
		"cApiTaLInT" INT,
		"cApiTaLByTEs" BYTES,
		INDEX capital_int_idx ("cApiTaLInT"),
		INDEX capital_bytes_idx ("cApiTaLByTEs")
	)`)

	sqlDB.Exec(t, `INSERT INTO d.t VALUES (1, 2, 'a')`)
	fprint1 := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE d.t`)

	sqlDB.Exec(t, `TRUNCATE TABLE d.t`)
	sqlDB.Exec(t, `INSERT INTO d.t VALUES (3, 4, 'b')`)
	fprint2 := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE d.t`)

	if reflect.DeepEqual(fprint1, fprint2) {
		t.Errorf("expected different fingerprints: %v vs %v", fprint1, fprint2)
	}
}
