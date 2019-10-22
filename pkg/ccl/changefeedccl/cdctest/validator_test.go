// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdctest

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func ts(i int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: i}
}

func noteRow(t *testing.T, v Validator, partition, key, value string, updated hlc.Timestamp) {
	t.Helper()
	if err := v.NoteRow(partition, key, value, updated); err != nil {
		t.Fatal(err)
	}
}

func noteResolved(t *testing.T, v Validator, partition string, resolved hlc.Timestamp) {
	t.Helper()
	if err := v.NoteResolved(partition, resolved); err != nil {
		t.Fatal(err)
	}
}

func assertValidatorFailures(t *testing.T, v Validator, expected ...string) {
	t.Helper()
	if f := v.Failures(); !reflect.DeepEqual(f, expected) {
		t.Errorf(`got %v expected %v`, f, expected)
	}
}

func TestOrderValidator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const ignored = `ignored`

	t.Run(`empty`, func(t *testing.T) {
		v := NewOrderValidator(`t1`)
		if f := v.Failures(); f != nil {
			t.Fatalf("got %v expected %v", f, nil)
		}
	})
	t.Run(`dupe okay`, func(t *testing.T) {
		v := NewOrderValidator(`t1`)
		noteRow(t, v, `p1`, `k1`, ignored, ts(1))
		noteRow(t, v, `p1`, `k1`, ignored, ts(2))
		noteRow(t, v, `p1`, `k1`, ignored, ts(1))
		assertValidatorFailures(t, v)
	})
	t.Run(`key on two partitions`, func(t *testing.T) {
		v := NewOrderValidator(`t1`)
		noteRow(t, v, `p1`, `k1`, ignored, ts(2))
		noteRow(t, v, `p2`, `k1`, ignored, ts(1))
		assertValidatorFailures(t, v,
			`key [k1] received on two partitions: p1 and p2`,
		)
	})
	t.Run(`new key with lower timestamp`, func(t *testing.T) {
		v := NewOrderValidator(`t1`)
		noteRow(t, v, `p1`, `k1`, ignored, ts(2))
		noteRow(t, v, `p1`, `k1`, ignored, ts(1))
		assertValidatorFailures(t, v,
			`topic t1 partition p1: saw new row timestamp 1.0000000000 after 2.0000000000 was seen`,
		)
	})
	t.Run(`new key after resolved`, func(t *testing.T) {
		v := NewOrderValidator(`t1`)
		noteResolved(t, v, `p2`, ts(3))
		// Okay because p2 saw the resolved timestamp but p1 didn't.
		noteRow(t, v, `p1`, `k1`, ignored, ts(1))
		noteResolved(t, v, `p1`, ts(3))
		// This one is not okay.
		noteRow(t, v, `p1`, `k1`, ignored, ts(2))
		// Still okay because we've seen it before.
		noteRow(t, v, `p1`, `k1`, ignored, ts(1))
		assertValidatorFailures(t, v,
			`topic t1 partition p1`+
				`: saw new row timestamp 2.0000000000 after 3.0000000000 was resolved`,
		)
	})
}

func TestBeforeAfterValidator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (k INT PRIMARY KEY, v INT)`)

	tsRaw := make([]string, 6)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsRaw[0])
	sqlDB.QueryRow(t,
		`UPSERT INTO foo VALUES (1, 1) RETURNING cluster_logical_timestamp()`,
	).Scan(&tsRaw[1])
	sqlDB.QueryRow(t,
		`UPSERT INTO foo VALUES (1, 2), (2, 2) RETURNING cluster_logical_timestamp()`,
	).Scan(&tsRaw[2])
	sqlDB.QueryRow(t,
		`UPSERT INTO foo VALUES (1, 3) RETURNING cluster_logical_timestamp()`,
	).Scan(&tsRaw[3])
	sqlDB.QueryRow(t,
		`DELETE FROM foo WHERE k = 1 RETURNING cluster_logical_timestamp()`,
	).Scan(&tsRaw[4])
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsRaw[5])
	ts := make([]hlc.Timestamp, len(tsRaw))
	for i := range tsRaw {
		var err error
		ts[i], err = sql.ParseHLC(tsRaw[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Run(`empty`, func(t *testing.T) {
		v, err := NewBeforeAfterValidator(sqlDBRaw, `foo`)
		require.NoError(t, err)
		assertValidatorFailures(t, v)
	})
	t.Run(`during initial`, func(t *testing.T) {
		v, err := NewBeforeAfterValidator(sqlDBRaw, `foo`)
		require.NoError(t, err)
		// "before" is ignored if missing.
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":1}}`, ts[1])
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":2}}`, ts[2])
		// However, if provided, it is validated.
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":2}, "before": {"k":1,"v":1}}`, ts[2])
		assertValidatorFailures(t, v)
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":3}, "before": {"k":1,"v":3}}`, ts[3])
		assertValidatorFailures(t, v,
			`"before" field did not agree with row at `+ts[3].Prev().AsOfSystemTime()+
				`: SELECT count(*) = 1 FROM foo AS OF SYSTEM TIME '`+ts[3].Prev().AsOfSystemTime()+
				`' WHERE k = $1 AND v = $2 [1 3]`)
	})
	t.Run(`missing before`, func(t *testing.T) {
		v, err := NewBeforeAfterValidator(sqlDBRaw, `foo`)
		require.NoError(t, err)
		noteResolved(t, v, `p`, ts[0])
		// "before" should have been provided.
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":2}}`, ts[2])
		assertValidatorFailures(t, v,
			`"before" field did not agree with row at `+ts[2].Prev().AsOfSystemTime()+
				`: SELECT count(*) = 0 FROM foo AS OF SYSTEM TIME '`+ts[2].Prev().AsOfSystemTime()+
				`' WHERE k = $1 [1]`)
	})
	t.Run(`incorrect before`, func(t *testing.T) {
		v, err := NewBeforeAfterValidator(sqlDBRaw, `foo`)
		require.NoError(t, err)
		noteResolved(t, v, `p`, ts[0])
		// "before" provided with wrong value.
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":3}, "before": {"k":5,"v":10}}`, ts[3])
		assertValidatorFailures(t, v,
			`"before" field did not agree with row at `+ts[3].Prev().AsOfSystemTime()+
				`: SELECT count(*) = 1 FROM foo AS OF SYSTEM TIME '`+ts[3].Prev().AsOfSystemTime()+
				`' WHERE k = $1 AND v = $2 [5 10]`)
	})
	t.Run(`unnecessary before`, func(t *testing.T) {
		v, err := NewBeforeAfterValidator(sqlDBRaw, `foo`)
		require.NoError(t, err)
		noteResolved(t, v, `p`, ts[0])
		// "before" provided but should not have been.
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":1}, "before": {"k":1,"v":1}}`, ts[1])
		assertValidatorFailures(t, v,
			`"before" field did not agree with row at `+ts[1].Prev().AsOfSystemTime()+
				`: SELECT count(*) = 1 FROM foo AS OF SYSTEM TIME '`+ts[1].Prev().AsOfSystemTime()+
				`' WHERE k = $1 AND v = $2 [1 1]`)
	})
	t.Run(`missing after`, func(t *testing.T) {
		v, err := NewBeforeAfterValidator(sqlDBRaw, `foo`)
		require.NoError(t, err)
		noteResolved(t, v, `p`, ts[0])
		// "after" should have been provided.
		noteRow(t, v, `p`, `[1]`, `{"before": {"k":1,"v":1}}`, ts[2])
		assertValidatorFailures(t, v,
			`"after" field did not agree with row at `+ts[2].AsOfSystemTime()+
				`: SELECT count(*) = 0 FROM foo AS OF SYSTEM TIME '`+ts[2].AsOfSystemTime()+
				`' WHERE k = $1 [1]`)
	})
	t.Run(`incorrect after`, func(t *testing.T) {
		v, err := NewBeforeAfterValidator(sqlDBRaw, `foo`)
		require.NoError(t, err)
		noteResolved(t, v, `p`, ts[0])
		// "after" provided with wrong value.
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":5}, "before": {"k":1,"v":2}}`, ts[3])
		assertValidatorFailures(t, v,
			`"after" field did not agree with row at `+ts[3].AsOfSystemTime()+
				`: SELECT count(*) = 1 FROM foo AS OF SYSTEM TIME '`+ts[3].AsOfSystemTime()+
				`' WHERE k = $1 AND v = $2 [1 5]`)
	})
	t.Run(`unnecessary after`, func(t *testing.T) {
		v, err := NewBeforeAfterValidator(sqlDBRaw, `foo`)
		require.NoError(t, err)
		noteResolved(t, v, `p`, ts[0])
		// "after" provided but should not have been.
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":3}, "before": {"k":1,"v":3}}`, ts[4])
		assertValidatorFailures(t, v,
			`"after" field did not agree with row at `+ts[4].AsOfSystemTime()+
				`: SELECT count(*) = 1 FROM foo AS OF SYSTEM TIME '`+ts[4].AsOfSystemTime()+
				`' WHERE k = $1 AND v = $2 [1 3]`)
	})
	t.Run(`incorrect before and after`, func(t *testing.T) {
		v, err := NewBeforeAfterValidator(sqlDBRaw, `foo`)
		require.NoError(t, err)
		noteResolved(t, v, `p`, ts[0])
		// "before" and "after" both provided with wrong value.
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":5}, "before": {"k":1,"v":4}}`, ts[3])
		assertValidatorFailures(t, v,
			`"after" field did not agree with row at `+ts[3].AsOfSystemTime()+
				`: SELECT count(*) = 1 FROM foo AS OF SYSTEM TIME '`+ts[3].AsOfSystemTime()+
				`' WHERE k = $1 AND v = $2 [1 5]`,
			`"before" field did not agree with row at `+ts[3].Prev().AsOfSystemTime()+
				`: SELECT count(*) = 1 FROM foo AS OF SYSTEM TIME '`+ts[3].Prev().AsOfSystemTime()+
				`' WHERE k = $1 AND v = $2 [1 4]`)
	})
	t.Run(`correct`, func(t *testing.T) {
		v, err := NewBeforeAfterValidator(sqlDBRaw, `foo`)
		require.NoError(t, err)
		noteResolved(t, v, `p`, ts[0])
		noteRow(t, v, `p`, `[1]`, `{}`, ts[0])
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":1}}`, ts[1])
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":1}, "before": null}`, ts[1])
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":2}, "before": {"k":1,"v":1}}`, ts[2])
		noteRow(t, v, `p`, `[1]`, `{"after": {"k":1,"v":3}, "before": {"k":1,"v":2}}`, ts[3])
		noteRow(t, v, `p`, `[1]`, `{                        "before": {"k":1,"v":3}}`, ts[4])
		noteRow(t, v, `p`, `[1]`, `{"after": null,          "before": {"k":1,"v":3}}`, ts[4])
		noteRow(t, v, `p`, `[2]`, `{}`, ts[1])
		noteRow(t, v, `p`, `[2]`, `{"after": {"k":2,"v":2}}`, ts[2])
		noteRow(t, v, `p`, `[2]`, `{"after": {"k":2,"v":2}, "before": null}`, ts[2])
		assertValidatorFailures(t, v)
	})
}

func TestFingerprintValidator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const ignored = `ignored`

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (k INT PRIMARY KEY, v INT)`)

	tsRaw := make([]string, 6)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsRaw[0])
	sqlDB.QueryRow(t,
		`UPSERT INTO foo VALUES (1, 1) RETURNING cluster_logical_timestamp()`,
	).Scan(&tsRaw[1])
	sqlDB.QueryRow(t,
		`UPSERT INTO foo VALUES (1, 2), (2, 2) RETURNING cluster_logical_timestamp()`,
	).Scan(&tsRaw[2])
	sqlDB.QueryRow(t,
		`UPSERT INTO foo VALUES (1, 3) RETURNING cluster_logical_timestamp()`,
	).Scan(&tsRaw[3])
	sqlDB.QueryRow(t,
		`DELETE FROM foo WHERE k = 1 RETURNING cluster_logical_timestamp()`,
	).Scan(&tsRaw[4])
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsRaw[5])
	ts := make([]hlc.Timestamp, len(tsRaw))
	for i := range tsRaw {
		var err error
		ts[i], err = sql.ParseHLC(tsRaw[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	createTableStmt := func(tableName string) string {
		return fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v INT)`, tableName)
	}
	testColumns := 0

	t.Run(`empty`, func(t *testing.T) {
		sqlDB.Exec(t, createTableStmt(`empty`))
		v, err := NewFingerprintValidator(sqlDBRaw, `foo`, `empty`, []string{`p`}, testColumns)
		require.NoError(t, err)
		noteResolved(t, v, `p`, ts[0])
		assertValidatorFailures(t, v)
	})
	t.Run(`wrong data`, func(t *testing.T) {
		sqlDB.Exec(t, createTableStmt(`wrong_data`))
		v, err := NewFingerprintValidator(sqlDBRaw, `foo`, `wrong_data`, []string{`p`}, testColumns)
		require.NoError(t, err)
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":10}}`, ts[1])
		noteResolved(t, v, `p`, ts[1])
		assertValidatorFailures(t, v,
			`fingerprints did not match at `+ts[1].AsOfSystemTime()+
				`: 590700560494856539 vs -2774220564100127343`,
		)
	})
	t.Run(`all resolved`, func(t *testing.T) {
		sqlDB.Exec(t, createTableStmt(`all_resolved`))
		v, err := NewFingerprintValidator(sqlDBRaw, `foo`, `all_resolved`, []string{`p`}, testColumns)
		require.NoError(t, err)
		if err := v.NoteResolved(`p`, ts[0]); err != nil {
			t.Fatal(err)
		}
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":1}}`, ts[1])
		noteResolved(t, v, `p`, ts[1])
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":2}}`, ts[2])
		noteRow(t, v, ignored, `[2]`, `{"after": {"k":2,"v":2}}`, ts[2])
		noteResolved(t, v, `p`, ts[2])
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":3}}`, ts[3])
		noteResolved(t, v, `p`, ts[3])
		noteRow(t, v, ignored, `[1]`, `{"after": null}`, ts[4])
		noteResolved(t, v, `p`, ts[4])
		noteResolved(t, v, `p`, ts[5])
		assertValidatorFailures(t, v)
	})
	t.Run(`rows unsorted`, func(t *testing.T) {
		sqlDB.Exec(t, createTableStmt(`rows_unsorted`))
		v, err := NewFingerprintValidator(sqlDBRaw, `foo`, `rows_unsorted`, []string{`p`}, testColumns)
		require.NoError(t, err)
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":3}}`, ts[3])
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":2}}`, ts[2])
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":1}}`, ts[1])
		noteRow(t, v, ignored, `[1]`, `{"after": null}`, ts[4])
		noteRow(t, v, ignored, `[2]`, `{"after": {"k":2,"v":2}}`, ts[2])
		noteResolved(t, v, `p`, ts[5])
		assertValidatorFailures(t, v)
	})
	t.Run(`missed initial`, func(t *testing.T) {
		sqlDB.Exec(t, createTableStmt(`missed_initial`))
		v, err := NewFingerprintValidator(sqlDBRaw, `foo`, `missed_initial`, []string{`p`}, testColumns)
		require.NoError(t, err)
		noteResolved(t, v, `p`, ts[0])
		// Intentionally missing {"k":1,"v":1} at ts[1].
		// Insert a fake row since we don't fingerprint earlier than the first seen row.
		noteRow(t, v, ignored, `[2]`, `{"after": {"k":2,"v":2}}`, ts[2].Prev())
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":2}}`, ts[2])
		noteRow(t, v, ignored, `[2]`, `{"after": {"k":2,"v":2}}`, ts[2])
		noteResolved(t, v, `p`, ts[2].Prev())
		assertValidatorFailures(t, v,
			`fingerprints did not match at `+ts[2].Prev().AsOfSystemTime()+
				`: 590700560494856539 vs 590699460983228293`,
		)
	})
	t.Run(`missed middle`, func(t *testing.T) {
		sqlDB.Exec(t, createTableStmt(`missed_middle`))
		v, err := NewFingerprintValidator(sqlDBRaw, `foo`, `missed_middle`, []string{`p`}, testColumns)
		require.NoError(t, err)
		noteResolved(t, v, `p`, ts[0])
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":1}}`, ts[1])
		// Intentionally missing {"k":1,"v":2} at ts[2].
		noteRow(t, v, ignored, `[2]`, `{"after": {"k":2,"v":2}}`, ts[2])
		noteResolved(t, v, `p`, ts[2])
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":3}}`, ts[3])
		noteResolved(t, v, `p`, ts[3])
		assertValidatorFailures(t, v,
			`fingerprints did not match at `+ts[2].AsOfSystemTime()+
				`: 1099511631581 vs 1099511631582`,
			`fingerprints did not match at `+ts[3].Prev().AsOfSystemTime()+
				`: 1099511631581 vs 1099511631582`,
		)
	})
	t.Run(`missed end`, func(t *testing.T) {
		sqlDB.Exec(t, createTableStmt(`missed_end`))
		v, err := NewFingerprintValidator(sqlDBRaw, `foo`, `missed_end`, []string{`p`}, testColumns)
		require.NoError(t, err)
		noteResolved(t, v, `p`, ts[0])
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":1}}`, ts[1])
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":2}}`, ts[2])
		noteRow(t, v, ignored, `[2]`, `{"after": {"k":2,"v":2}}`, ts[2])
		// Intentionally missing {"k":1,"v":3} at ts[3].
		noteResolved(t, v, `p`, ts[3])
		assertValidatorFailures(t, v,
			`fingerprints did not match at `+ts[3].AsOfSystemTime()+
				`: 1099511631580 vs 1099511631581`,
		)
	})
	t.Run(`initial scan`, func(t *testing.T) {
		sqlDB.Exec(t, createTableStmt(`initial_scan`))
		v, err := NewFingerprintValidator(sqlDBRaw, `foo`, `initial_scan`, []string{`p`}, testColumns)
		require.NoError(t, err)
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":3}}`, ts[3])
		noteRow(t, v, ignored, `[2]`, `{"after": {"k":2,"v":2}}`, ts[3])
		noteResolved(t, v, `p`, ts[3])
		assertValidatorFailures(t, v)
	})
	t.Run(`unknown partition`, func(t *testing.T) {
		sqlDB.Exec(t, createTableStmt(`unknown_partition`))
		v, err := NewFingerprintValidator(sqlDBRaw, `foo`, `unknown_partition`, []string{`p`}, testColumns)
		require.NoError(t, err)
		if err := v.NoteResolved(`nope`, ts[1]); !testutils.IsError(err, `unknown partition`) {
			t.Fatalf(`expected "unknown partition" error got: %+v`, err)
		}
	})
	t.Run(`resolved unsorted`, func(t *testing.T) {
		sqlDB.Exec(t, createTableStmt(`resolved_unsorted`))
		v, err := NewFingerprintValidator(sqlDBRaw, `foo`, `resolved_unsorted`, []string{`p`}, testColumns)
		require.NoError(t, err)
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":1}}`, ts[1])
		noteResolved(t, v, `p`, ts[1])
		noteResolved(t, v, `p`, ts[1])
		noteResolved(t, v, `p`, ts[0])
		assertValidatorFailures(t, v)
	})
	t.Run(`two partitions`, func(t *testing.T) {
		sqlDB.Exec(t, createTableStmt(`two_partitions`))
		v, err := NewFingerprintValidator(sqlDBRaw, `foo`, `two_partitions`, []string{`p0`, `p1`}, testColumns)
		require.NoError(t, err)
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":1}}`, ts[1])
		noteRow(t, v, ignored, `[1]`, `{"after": {"k":1,"v":2}}`, ts[2])
		// Intentionally missing {"k":2,"v":2}.
		noteResolved(t, v, `p0`, ts[2])
		noteResolved(t, v, `p0`, ts[4])
		// p1 has not been closed, so no failures yet.
		assertValidatorFailures(t, v)
		noteResolved(t, v, `p1`, ts[2])
		assertValidatorFailures(t, v,
			`fingerprints did not match at `+ts[2].AsOfSystemTime()+
				`: 1099511631581 vs 590700560494856536`,
		)
	})
}

func TestValidators(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const ignored = `ignored`

	t.Run(`empty`, func(t *testing.T) {
		v := Validators{
			NewOrderValidator(`t1`),
			NewOrderValidator(`t2`),
		}
		if f := v.Failures(); f != nil {
			t.Fatalf("got %v expected %v", f, nil)
		}
	})
	t.Run(`failures`, func(t *testing.T) {
		v := Validators{
			NewOrderValidator(`t1`),
			NewOrderValidator(`t2`),
		}
		noteResolved(t, v, `p1`, ts(2))
		noteRow(t, v, `p1`, `k1`, ignored, ts(1))
		assertValidatorFailures(t, v,
			`topic t1 partition p1`+
				`: saw new row timestamp 1.0000000000 after 2.0000000000 was resolved`,
			`topic t2 partition p1`+
				`: saw new row timestamp 1.0000000000 after 2.0000000000 was resolved`,
		)
	})
}
