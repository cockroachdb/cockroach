// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package client_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func strToValue(s string) *roachpb.Value {
	v := roachpb.MakeValueFromBytes([]byte(s))
	return &v
}

func setup(t *testing.T) (serverutils.TestServerInterface, *client.DB) {
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	return s, kvDB
}

func checkIntResult(t *testing.T, expected, result int64) {
	if expected != result {
		t.Errorf("expected %d, got %d", expected, result)
	}
}

func checkResult(t *testing.T, expected, result []byte) {
	if !bytes.Equal(expected, result) {
		t.Errorf("expected \"%s\", got \"%s\"", expected, result)
	}
}

func checkResults(t *testing.T, expected map[string][]byte, results []client.Result) {
	count := 0
	for _, result := range results {
		checkRows(t, expected, result.Rows)
		count++
	}
	checkLen(t, len(expected), count)
}

func checkRows(t *testing.T, expected map[string][]byte, rows []client.KeyValue) {
	for i, row := range rows {
		if !bytes.Equal(expected[string(row.Key)], row.ValueBytes()) {
			t.Errorf("expected %d: %s=\"%s\", got %s=\"%s\"",
				i,
				row.Key,
				expected[string(row.Key)],
				row.Key,
				row.ValueBytes())
		}
	}
}

func checkLen(t *testing.T, expected, count int) {
	if expected != count {
		t.Errorf("expected length to be %d, got %d", expected, count)
	}
}

func TestDB_Get(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup(t)
	defer s.Stopper().Stop(context.TODO())

	result, err := db.Get(context.TODO(), "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte(""), result.ValueBytes())
}

func TestDB_Put(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup(t)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	if err := db.Put(context.TODO(), "aa", "1"); err != nil {
		t.Fatal(err)
	}
	result, err := db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("1"), result.ValueBytes())
}

func TestDB_CPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup(t)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	if err := db.Put(ctx, "aa", "1"); err != nil {
		t.Fatal(err)
	}
	if err := db.CPut(ctx, "aa", "2", strToValue("1")); err != nil {
		t.Fatal(err)
	}
	result, err := db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("2"), result.ValueBytes())

	if err = db.CPut(ctx, "aa", "3", strToValue("1")); err == nil {
		t.Fatal("expected error from conditional put")
	}
	result, err = db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("2"), result.ValueBytes())

	if err = db.CPut(ctx, "bb", "4", strToValue("1")); err == nil {
		t.Fatal("expected error from conditional put")
	}
	result, err = db.Get(ctx, "bb")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte(""), result.ValueBytes())

	if err = db.CPut(ctx, "bb", "4", nil); err != nil {
		t.Fatal(err)
	}
	result, err = db.Get(ctx, "bb")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("4"), result.ValueBytes())
}

func TestDB_InitPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup(t)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	if err := db.InitPut(ctx, "aa", "1", false); err != nil {
		t.Fatal(err)
	}
	if err := db.InitPut(ctx, "aa", "1", false); err != nil {
		t.Fatal(err)
	}
	if err := db.InitPut(ctx, "aa", "2", false); err == nil {
		t.Fatal("expected error from init put")
	}
	if err := db.Del(ctx, "aa"); err != nil {
		t.Fatal(err)
	}
	if err := db.InitPut(ctx, "aa", "2", true); err == nil {
		t.Fatal("expected error from init put")
	}
	if err := db.InitPut(ctx, "aa", "1", false); err != nil {
		t.Fatal(err)
	}
	result, err := db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("1"), result.ValueBytes())
}

func TestDB_Inc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup(t)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	if _, err := db.Inc(ctx, "aa", 100); err != nil {
		t.Fatal(err)
	}
	result, err := db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkIntResult(t, 100, result.ValueInt())
}

func TestBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup(t)
	defer s.Stopper().Stop(context.TODO())

	b := &client.Batch{}
	b.Get("aa")
	b.Put("bb", "2")
	if err := db.Run(context.TODO(), b); err != nil {
		t.Fatal(err)
	}

	expected := map[string][]byte{
		"aa": []byte(""),
		"bb": []byte("2"),
	}
	checkResults(t, expected, b.Results)
}

func TestDB_Scan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup(t)
	defer s.Stopper().Stop(context.TODO())

	b := &client.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("bb", "3")
	if err := db.Run(context.TODO(), b); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Scan(context.TODO(), "a", "b", 100)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string][]byte{
		"aa": []byte("1"),
		"ab": []byte("2"),
	}

	checkRows(t, expected, rows)
	checkLen(t, len(expected), len(rows))
}

func TestDB_ReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup(t)
	defer s.Stopper().Stop(context.TODO())

	b := &client.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("bb", "3")
	if err := db.Run(context.TODO(), b); err != nil {
		t.Fatal(err)
	}
	rows, err := db.ReverseScan(context.TODO(), "ab", "c", 100)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string][]byte{
		"bb": []byte("3"),
		"ab": []byte("2"),
	}

	checkRows(t, expected, rows)
	checkLen(t, len(expected), len(rows))
}

func TestDB_TxnIterate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup(t)
	defer s.Stopper().Stop(context.TODO())

	b := &client.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("bb", "3")
	if err := db.Run(context.TODO(), b); err != nil {
		t.Fatal(err)
	}

	tc := []struct{ pageSize, numPages int }{
		{1, 2},
		{2, 1},
	}
	var rows []client.KeyValue = nil
	var p int
	for _, c := range tc {
		if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
			p = 0
			rows = make([]client.KeyValue, 0)
			return txn.Iterate(context.TODO(), "a", "b", c.pageSize,
				func(rs []client.KeyValue) error {
					p++
					rows = append(rows, rs...)
					return nil
				})
		}); err != nil {
			t.Fatal(err)
		}
		expected := map[string][]byte{
			"aa": []byte("1"),
			"ab": []byte("2"),
		}

		checkRows(t, expected, rows)
		checkLen(t, len(expected), len(rows))
		if p != c.numPages {
			t.Errorf("expected %d pages, got %d", c.numPages, p)
		}
	}
}

func TestDB_Del(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup(t)
	defer s.Stopper().Stop(context.TODO())

	b := &client.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("ac", "3")
	if err := db.Run(context.TODO(), b); err != nil {
		t.Fatal(err)
	}
	if err := db.Del(context.TODO(), "ab"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Scan(context.TODO(), "a", "b", 100)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string][]byte{
		"aa": []byte("1"),
		"ac": []byte("3"),
	}
	checkRows(t, expected, rows)
	checkLen(t, len(expected), len(rows))
}

func TestTxn_Commit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup(t)
	defer s.Stopper().Stop(context.TODO())

	err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		b.Put("aa", "1")
		b.Put("ab", "2")
		return txn.CommitInBatch(ctx, b)
	})
	if err != nil {
		t.Fatal(err)
	}

	b := &client.Batch{}
	b.Get("aa")
	b.Get("ab")
	if err := db.Run(context.TODO(), b); err != nil {
		t.Fatal(err)
	}
	expected := map[string][]byte{
		"aa": []byte("1"),
		"ab": []byte("2"),
	}
	checkResults(t, expected, b.Results)
}

func TestDB_Put_insecure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	if err := db.Put(context.TODO(), "aa", "1"); err != nil {
		t.Fatal(err)
	}
	result, err := db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("1"), result.ValueBytes())
}
