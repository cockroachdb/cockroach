// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"fmt"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// NB: Most of the SHOW EXPERIMENTAL_FINGERPRINTS tests are in the
// show_fingerprints logic test. This is just to test the AS OF SYSTEM TIME
// functionality.
func TestShowFingerprintsAsOfSystemTime(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := serverutils.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(t, tc.ServerConn(0))
	sqlDB.Exec(`CREATE DATABASE d`)
	sqlDB.Exec(`CREATE TABLE d.t (a INT PRIMARY KEY, b INT, INDEX b_idx (b))`)
	sqlDB.Exec(`INSERT INTO d.t VALUES (1, 2)`)

	const fprintQuery = `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE d.t`
	fprint1 := sqlDB.QueryStr(fprintQuery)

	var ts string
	sqlDB.QueryRow(`SELECT now()`).Scan(&ts)

	sqlDB.Exec(`INSERT INTO d.t VALUES (3, 4)`)
	sqlDB.Exec(`DROP INDEX d.t@b_idx`)

	fprint2 := sqlDB.QueryStr(fprintQuery)
	if reflect.DeepEqual(fprint1, fprint2) {
		t.Errorf("expected different fingerprints: %v vs %v", fprint1, fprint2)
	}

	fprint3Query := fmt.Sprintf(`%s AS OF SYSTEM TIME '%s'`, fprintQuery, ts)
	sqlDB.CheckQueryResults(fprint3Query, fprint1)
}

func TestShowFingerprintsColumnNames(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := serverutils.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(t, tc.ServerConn(0))
	sqlDB.Exec(`CREATE DATABASE d`)
	sqlDB.Exec(`CREATE TABLE d.t (
		lowercase INT PRIMARY KEY,
		"cApiTaLInT" INT,
		"cApiTaLByTEs" BYTES,
		INDEX capital_int_idx ("cApiTaLInT"),
		INDEX capital_bytes_idx ("cApiTaLByTEs")
	)`)

	sqlDB.Exec(`INSERT INTO d.t VALUES (1, 2, 'a')`)
	fprint1 := sqlDB.QueryStr(`SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE d.t`)

	sqlDB.Exec(`TRUNCATE TABLE d.t`)
	sqlDB.Exec(`INSERT INTO d.t VALUES (3, 4, 'b')`)
	fprint2 := sqlDB.QueryStr(`SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE d.t`)

	if reflect.DeepEqual(fprint1, fprint2) {
		t.Errorf("expected different fingerprints: %v vs %v", fprint1, fprint2)
	}
}
