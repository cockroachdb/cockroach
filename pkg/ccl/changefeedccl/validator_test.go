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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestOrderValidator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run(`empty`, func(t *testing.T) {
		v := NewOrderValidator(`t1`)
		if f := v.Failures(); f != nil {
			t.Fatalf("got %v expected %v", f, nil)
		}
	})
	t.Run(`dupe okay`, func(t *testing.T) {
		v := NewOrderValidator(`t1`)
		v.NoteRow(`p1`, `k1`, `1`)
		v.NoteRow(`p1`, `k1`, `2`)
		v.NoteRow(`p1`, `k1`, `1`)
		if f := v.Failures(); f != nil {
			t.Fatalf("got %v expected %v", f, nil)
		}
	})
	t.Run(`key on two partitions`, func(t *testing.T) {
		v := NewOrderValidator(`t1`)
		v.NoteRow(`p1`, `k1`, `1`)
		v.NoteRow(`p2`, `k1`, `2`)
		expected := []string{
			`key [k1] received on two partitions: p1 and p2`,
		}
		if f := v.Failures(); !reflect.DeepEqual(f, expected) {
			t.Fatalf("got %v expected %v", f, expected)
		}
	})
	t.Run(`new key with lower timestamp`, func(t *testing.T) {
		v := NewOrderValidator(`t1`)
		v.NoteRow(`p1`, `k1`, `2`)
		v.NoteRow(`p1`, `k1`, `1`)
		expected := []string{
			`topic t1 partition p1: saw new row timestamp 1 after 2 was seen`,
		}
		if f := v.Failures(); !reflect.DeepEqual(f, expected) {
			t.Fatalf("got %v expected %v", f, expected)
		}
	})
	t.Run(`new key after resolved`, func(t *testing.T) {
		v := NewOrderValidator(`t1`)
		v.NoteResolved(`p2`, `3`)
		// Okay because p2 saw the resolved timestamp but p1 didn't.
		v.NoteRow(`p1`, `k1`, `1`)
		v.NoteResolved(`p1`, `3`)
		// This one is not okay.
		v.NoteRow(`p1`, `k1`, `2`)
		// Still okay because we've seen it before.
		v.NoteRow(`p1`, `k1`, `1`)
		expected := []string{
			`topic t1 partition p1: saw new row timestamp 2 after 3 was resolved`,
		}
		if f := v.Failures(); !reflect.DeepEqual(f, expected) {
			t.Fatalf("got %v expected %v", f, expected)
		}
	})
}

func TestFingerprintValidator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	noteResolved := func(t *testing.T, v *FingerprintValidator, partition, resolved string) {
		if err := v.NoteResolved(partition, resolved); err != nil {
			t.Fatal(err)
		}
	}
	assertFailures := func(t *testing.T, v *FingerprintValidator, expected ...string) {
		if f := v.Failures(); !reflect.DeepEqual(f, expected) {
			t.Errorf(`got %v expected %v`, f, expected)
		}
	}

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (k INT PRIMARY KEY, v INT)`)

	ts := make([]string, 5)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[0])
	sqlDB.QueryRow(t,
		`UPSERT INTO foo VALUES (1, 1) RETURNING cluster_logical_timestamp()`,
	).Scan(&ts[1])
	sqlDB.QueryRow(t,
		`UPSERT INTO foo VALUES (1, 2), (2, 2) RETURNING cluster_logical_timestamp()`,
	).Scan(&ts[2])
	sqlDB.QueryRow(t,
		`UPSERT INTO foo VALUES (1, 3) RETURNING cluster_logical_timestamp()`,
	).Scan(&ts[3])
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[4])

	t.Run(`empty`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE empty (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDB.DB, `foo`, `empty`, []string{`p`})
		noteResolved(t, v, `p`, ts[0])
		assertFailures(t, v)
	})
	t.Run(`wrong_data`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE wrong_data (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDB.DB, `foo`, `wrong_data`, []string{`p`})
		v.NoteRow(`[1]`, `{"k":1,"v":10}`, ts[1])
		noteResolved(t, v, `p`, ts[1])
		assertFailures(t, v,
			`fingerprints did not match at `+ts[1]+`: 590700560494856539 vs -2774220564100127343`,
		)
	})
	t.Run(`all_resolved`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE all_resolved (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDB.DB, `foo`, `all_resolved`, []string{`p`})
		if err := v.NoteResolved(`p`, ts[0]); err != nil {
			t.Fatal(err)
		}
		v.NoteRow(`[1]`, `{"k":1,"v":1}`, ts[1])
		noteResolved(t, v, `p`, ts[1])
		v.NoteRow(`[1]`, `{"k":1,"v":2}`, ts[2])
		v.NoteRow(`[1]`, `{"k":2,"v":2}`, ts[2])
		noteResolved(t, v, `p`, ts[2])
		v.NoteRow(`[1]`, `{"k":1,"v":3}`, ts[3])
		noteResolved(t, v, `p`, ts[3])
		noteResolved(t, v, `p`, ts[4])
		assertFailures(t, v)
	})
	t.Run(`rows_unsorted`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE rows_unsorted (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDB.DB, `foo`, `rows_unsorted`, []string{`p`})
		v.NoteRow(`[1]`, `{"k":1,"v":3}`, ts[3])
		v.NoteRow(`[1]`, `{"k":1,"v":2}`, ts[2])
		v.NoteRow(`[1]`, `{"k":1,"v":1}`, ts[1])
		v.NoteRow(`[1]`, `{"k":2,"v":2}`, ts[2])
		noteResolved(t, v, `p`, ts[4])
		assertFailures(t, v)
	})
	t.Run(`missed_initial`, func(t *testing.T) {
		t.Skip("#27101")
		sqlDB.Exec(t, `CREATE TABLE missed_initial (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDB.DB, `foo`, `missed_initial`, []string{`p`})
		// Intentionally missing {"k":1,"v":1}.
		v.NoteRow(`[1]`, `{"k":1,"v":2}`, ts[2])
		v.NoteRow(`[1]`, `{"k":2,"v":2}`, ts[2])
		noteResolved(t, v, `p`, ts[2])
		assertFailures(t, v,
			`fingerprints did not match at `+ts[2],
		)
	})
	t.Run(`missed_middle`, func(t *testing.T) {
		t.Skip("#27101")
		sqlDB.Exec(t, `CREATE TABLE missed_middle (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDB.DB, `foo`, `missed_middle`, []string{`p`})
		v.NoteRow(`[1]`, `{"k":1,"v":1}`, ts[1])
		// Intentionally missing {"k":1,"v":2}.
		v.NoteRow(`[1]`, `{"k":2,"v":2}`, ts[2])
		v.NoteRow(`[1]`, `{"k":1,"v":3}`, ts[3])
		noteResolved(t, v, `p`, ts[3])
		assertFailures(t, v,
			`fingerprints did not match at `+ts[3],
		)
	})
	t.Run(`unknown_partition`, func(t *testing.T) {
		v := NewFingerprintValidator(sqlDB.DB, `foo`, `unknown_partition`, []string{`p`})
		if err := v.NoteResolved(`nope`, ts[1]); !testutils.IsError(err, `unknown partition`) {
			t.Fatalf(`expected "unknown partition" error got: %+v`, err)
		}
	})
	t.Run(`resolved_unsorted`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE resolved_unsorted (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDB.DB, `foo`, `resolved_unsorted`, []string{`p`})
		v.NoteRow(`[1]`, `{"k":1,"v":1}`, ts[1])
		noteResolved(t, v, `p`, ts[1])
		noteResolved(t, v, `p`, ts[1])
		noteResolved(t, v, `p`, ts[0])
		assertFailures(t, v)
	})
	t.Run(`two_partitions`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE two_partitions (k INT PRIMARY KEY, v INT)`)
		v := NewFingerprintValidator(sqlDB.DB, `foo`, `two_partitions`, []string{`p0`, `p1`})
		v.NoteRow(`[1]`, `{"k":1,"v":1}`, ts[1])
		v.NoteRow(`[1]`, `{"k":1,"v":2}`, ts[2])
		// Intentionally missing {"k":2,"v":2}.
		noteResolved(t, v, `p0`, ts[2])
		noteResolved(t, v, `p0`, ts[4])
		// p1 has not been closed, so no failures yet.
		assertFailures(t, v)
		noteResolved(t, v, `p1`, ts[2])
		assertFailures(t, v,
			`fingerprints did not match at `+ts[2]+`: 1099511631581 vs 590700560494856536`,
		)
	})
}
