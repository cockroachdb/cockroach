// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func ts(i int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: i}
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
		v.NoteRow(`p1`, `k1`, ignored, ts(1))
		v.NoteRow(`p1`, `k1`, ignored, ts(2))
		v.NoteRow(`p1`, `k1`, ignored, ts(1))
		assertValidatorFailures(t, v)
	})
	t.Run(`key on two partitions`, func(t *testing.T) {
		v := NewOrderValidator(`t1`)
		v.NoteRow(`p1`, `k1`, ignored, ts(2))
		v.NoteRow(`p2`, `k1`, ignored, ts(1))
		assertValidatorFailures(t, v,
			`key [k1] received on two partitions: p1 and p2`,
		)
	})
	t.Run(`new key with lower timestamp`, func(t *testing.T) {
		v := NewOrderValidator(`t1`)
		v.NoteRow(`p1`, `k1`, ignored, ts(2))
		v.NoteRow(`p1`, `k1`, ignored, ts(1))
		assertValidatorFailures(t, v,
			`topic t1 partition p1: saw new row timestamp 1.0000000000 after 2.0000000000 was seen`,
		)
	})
	t.Run(`new key after resolved`, func(t *testing.T) {
		v := NewOrderValidator(`t1`)
		noteResolved(t, v, `p2`, ts(3))
		// Okay because p2 saw the resolved timestamp but p1 didn't.
		v.NoteRow(`p1`, `k1`, ignored, ts(1))
		noteResolved(t, v, `p1`, ts(3))
		// This one is not okay.
		v.NoteRow(`p1`, `k1`, ignored, ts(2))
		// Still okay because we've seen it before.
		v.NoteRow(`p1`, `k1`, ignored, ts(1))
		assertValidatorFailures(t, v,
			`topic t1 partition p1`+
				`: saw new row timestamp 2.0000000000 after 3.0000000000 was resolved`,
		)
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

	tsRaw := make([]string, 5)
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
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsRaw[4])
	ts := make([]hlc.Timestamp, len(tsRaw))
	for i := range tsRaw {
		var err error
		ts[i], err = sql.ParseHLC(tsRaw[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Run(`empty`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE empty (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDBRaw, `foo`, `empty`, []string{`p`})
		noteResolved(t, v, `p`, ts[0])
		assertValidatorFailures(t, v)
	})
	t.Run(`wrong_data`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE wrong_data (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDBRaw, `foo`, `wrong_data`, []string{`p`})
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":10}`, ts[1])
		noteResolved(t, v, `p`, ts[1])
		assertValidatorFailures(t, v,
			`fingerprints did not match at `+ts[1].AsOfSystemTime()+
				`: 590700560494856539 vs -2774220564100127343`,
		)
	})
	t.Run(`all_resolved`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE all_resolved (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDBRaw, `foo`, `all_resolved`, []string{`p`})
		if err := v.NoteResolved(`p`, ts[0]); err != nil {
			t.Fatal(err)
		}
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":1}`, ts[1])
		noteResolved(t, v, `p`, ts[1])
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":2}`, ts[2])
		v.NoteRow(ignored, `[1]`, `{"k":2,"v":2}`, ts[2])
		noteResolved(t, v, `p`, ts[2])
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":3}`, ts[3])
		noteResolved(t, v, `p`, ts[3])
		noteResolved(t, v, `p`, ts[4])
		assertValidatorFailures(t, v)
	})
	t.Run(`rows_unsorted`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE rows_unsorted (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDBRaw, `foo`, `rows_unsorted`, []string{`p`})
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":3}`, ts[3])
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":2}`, ts[2])
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":1}`, ts[1])
		v.NoteRow(ignored, `[1]`, `{"k":2,"v":2}`, ts[2])
		noteResolved(t, v, `p`, ts[4])
		assertValidatorFailures(t, v)
	})
	t.Run(`missed_initial`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE missed_initial (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDBRaw, `foo`, `missed_initial`, []string{`p`})
		noteResolved(t, v, `p`, ts[0])
		// Intentionally missing {"k":1,"v":1} at ts[1].
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":2}`, ts[2])
		v.NoteRow(ignored, `[1]`, `{"k":2,"v":2}`, ts[2])
		noteResolved(t, v, `p`, ts[2])
		assertValidatorFailures(t, v,
			`fingerprints did not match at `+ts[2].Prev().AsOfSystemTime()+
				`: 590700560494856539 vs EMPTY`,
		)
	})
	t.Run(`missed_middle`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE missed_middle (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDBRaw, `foo`, `missed_middle`, []string{`p`})
		noteResolved(t, v, `p`, ts[0])
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":1}`, ts[1])
		// Intentionally missing {"k":1,"v":2} at ts[2].
		v.NoteRow(ignored, `[1]`, `{"k":2,"v":2}`, ts[2])
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":3}`, ts[3])
		noteResolved(t, v, `p`, ts[3])
		assertValidatorFailures(t, v,
			`fingerprints did not match at `+ts[2].AsOfSystemTime()+
				`: 1099511631581 vs 1099511631582`,
			`fingerprints did not match at `+ts[3].Prev().AsOfSystemTime()+
				`: 1099511631581 vs 1099511631582`,
		)
	})
	t.Run(`missed_end`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE missed_end (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDBRaw, `foo`, `missed_end`, []string{`p`})
		noteResolved(t, v, `p`, ts[0])
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":1}`, ts[1])
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":2}`, ts[2])
		v.NoteRow(ignored, `[1]`, `{"k":2,"v":2}`, ts[2])
		// Intentionally missing {"k":1,"v":3} at ts[3].
		noteResolved(t, v, `p`, ts[3])
		assertValidatorFailures(t, v,
			`fingerprints did not match at `+ts[3].AsOfSystemTime()+
				`: 1099511631580 vs 1099511631581`,
		)
	})
	t.Run(`initial_scan`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE initial_scan (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDBRaw, `foo`, `initial_scan`, []string{`p`})
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":3}`, ts[4])
		v.NoteRow(ignored, `[1]`, `{"k":2,"v":2}`, ts[4])
		noteResolved(t, v, `p`, ts[4])
		assertValidatorFailures(t, v)
	})
	t.Run(`unknown_partition`, func(t *testing.T) {
		v := NewFingerprintValidator(sqlDBRaw, `foo`, `unknown_partition`, []string{`p`})
		if err := v.NoteResolved(`nope`, ts[1]); !testutils.IsError(err, `unknown partition`) {
			t.Fatalf(`expected "unknown partition" error got: %+v`, err)
		}
	})
	t.Run(`resolved_unsorted`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE resolved_unsorted (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDBRaw, `foo`, `resolved_unsorted`, []string{`p`})
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":1}`, ts[1])
		noteResolved(t, v, `p`, ts[1])
		noteResolved(t, v, `p`, ts[1])
		noteResolved(t, v, `p`, ts[0])
		assertValidatorFailures(t, v)
	})
	t.Run(`two_partitions`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE two_partitions (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDBRaw, `foo`, `two_partitions`, []string{`p0`, `p1`})
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":1}`, ts[1])
		v.NoteRow(ignored, `[1]`, `{"k":1,"v":2}`, ts[2])
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
		v.NoteRow(`p1`, `k1`, ignored, ts(1))
		assertValidatorFailures(t, v,
			`topic t1 partition p1`+
				`: saw new row timestamp 1.0000000000 after 2.0000000000 was resolved`,
			`topic t2 partition p1`+
				`: saw new row timestamp 1.0000000000 after 2.0000000000 was resolved`,
		)
	})
}
