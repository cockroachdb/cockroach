// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package client_test

import (
	"bytes"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

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
	if err := db.CPut(ctx, "aa", "2", "1"); err != nil {
		t.Fatal(err)
	}
	result, err := db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("2"), result.ValueBytes())

	if err = db.CPut(ctx, "aa", "3", "1"); err == nil {
		t.Fatal("expected error from conditional put")
	}
	result, err = db.Get(ctx, "aa")
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, []byte("2"), result.ValueBytes())

	if err = db.CPut(ctx, "bb", "4", "1"); err == nil {
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

	if err := db.InitPut(ctx, "aa", "1"); err != nil {
		t.Fatal(err)
	}
	if err := db.InitPut(ctx, "aa", "1"); err != nil {
		t.Fatal(err)
	}
	if err := db.InitPut(ctx, "aa", "2"); err == nil {
		t.Fatal("expected error from init put")
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

func TestDebugName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup(t)
	defer s.Stopper().Stop(context.TODO())

	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		const expected = "unnamed"
		if txn.DebugName() != expected {
			t.Fatalf("expected \"%s\", but found \"%s\"", expected, txn.DebugName())
		}
		return nil
	}); err != nil {
		t.Errorf("txn failed: %s", err)
	}
}

func TestCommonMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()
	batchType := reflect.TypeOf(&client.Batch{})
	dbType := reflect.TypeOf(&client.DB{})
	txnType := reflect.TypeOf(&client.Txn{})
	types := []reflect.Type{batchType, dbType, txnType}

	type key struct {
		typ    reflect.Type
		method string
	}
	omittedChecks := map[key]struct{}{
		// TODO(tschottdorf): removed GetProto from Batch, which necessitates
		// these two exceptions. Batch.GetProto would require wrapping each
		// request with the information that this particular Get must be
		// unmarshaled, which didn't seem worth doing as we're not using
		// Batch.GetProto at the moment.
		{dbType, "GetProto"}:                   {},
		{txnType, "GetProto"}:                  {},
		{batchType, "CheckConsistency"}:        {},
		{batchType, "AddRawRequest"}:           {},
		{batchType, "PutInline"}:               {},
		{batchType, "RawResponse"}:             {},
		{batchType, "MustPErr"}:                {},
		{dbType, "AdminMerge"}:                 {},
		{dbType, "AdminSplit"}:                 {},
		{dbType, "AdminTransferLease"}:         {},
		{dbType, "AdminChangeReplicas"}:        {},
		{dbType, "CheckConsistency"}:           {},
		{dbType, "Run"}:                        {},
		{dbType, "Txn"}:                        {},
		{dbType, "GetSender"}:                  {},
		{dbType, "PutInline"}:                  {},
		{dbType, "WriteBatch"}:                 {},
		{txnType, "Commit"}:                    {},
		{txnType, "CommitInBatch"}:             {},
		{txnType, "CommitOrCleanup"}:           {},
		{txnType, "Rollback"}:                  {},
		{txnType, "CleanupOnError"}:            {},
		{txnType, "DebugName"}:                 {},
		{txnType, "InternalSetPriority"}:       {},
		{txnType, "IsFinalized"}:               {},
		{txnType, "NewBatch"}:                  {},
		{txnType, "Exec"}:                      {},
		{txnType, "GetDeadline"}:               {},
		{txnType, "ResetDeadline"}:             {},
		{txnType, "Run"}:                       {},
		{txnType, "SetDebugName"}:              {},
		{txnType, "SetIsolation"}:              {},
		{txnType, "SetUserPriority"}:           {},
		{txnType, "SetSystemConfigTrigger"}:    {},
		{txnType, "SetTxnAnchorKey"}:           {},
		{txnType, "UpdateDeadlineMaybe"}:       {},
		{txnType, "AddCommitTrigger"}:          {},
		{txnType, "CommandCount"}:              {},
		{txnType, "IsRetryableErrMeantForTxn"}: {},
		{txnType, "Isolation"}:                 {},
		{txnType, "OrigTimestamp"}:             {},
		{txnType, "Proto"}:                     {},
		{txnType, "UserPriority"}:              {},
		{txnType, "AnchorKey"}:                 {},
	}

	for b := range omittedChecks {
		if _, ok := b.typ.MethodByName(b.method); !ok {
			t.Fatalf("blacklist method (%s).%s does not exist", b.typ, b.method)
		}
	}

	for _, typ := range types {
		for j := 0; j < typ.NumMethod(); j++ {
			m := typ.Method(j)
			if len(m.PkgPath) > 0 {
				continue
			}
			if _, ok := omittedChecks[key{typ, m.Name}]; ok {
				continue
			}
			for _, otherTyp := range types {
				if typ == otherTyp {
					continue
				}
				if _, ok := otherTyp.MethodByName(m.Name); !ok {
					t.Errorf("(%s).%s does not exist, but (%s).%s does",
						otherTyp, m.Name, typ, m.Name)
				}
			}
		}
	}
}
