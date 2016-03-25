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
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

func setup() (*server.TestServer, *client.DB) {
	s := server.StartTestServer(nil)
	return s, s.DB()
}

func ExampleDB_Get() {
	s, db := setup()
	defer s.Stop()

	result, pErr := db.Get("aa")
	if pErr != nil {
		panic(pErr)
	}
	fmt.Printf("aa=%s\n", result.ValueBytes())

	// Output:
	// aa=
}

func ExampleDB_Put() {
	s, db := setup()
	defer s.Stop()

	if pErr := db.Put("aa", "1"); pErr != nil {
		panic(pErr)
	}
	result, pErr := db.Get("aa")
	if pErr != nil {
		panic(pErr)
	}
	fmt.Printf("aa=%s\n", result.ValueBytes())

	// Output:
	// aa=1
}

func ExampleDB_CPut() {
	s, db := setup()
	defer s.Stop()

	if pErr := db.Put("aa", "1"); pErr != nil {
		panic(pErr)
	}
	if pErr := db.CPut("aa", "2", "1"); pErr != nil {
		panic(pErr)
	}
	result, pErr := db.Get("aa")
	if pErr != nil {
		panic(pErr)
	}
	fmt.Printf("aa=%s\n", result.ValueBytes())

	if pErr = db.CPut("aa", "3", "1"); pErr == nil {
		panic("expected pError from conditional put")
	}
	result, pErr = db.Get("aa")
	if pErr != nil {
		panic(pErr)
	}
	fmt.Printf("aa=%s\n", result.ValueBytes())

	if pErr = db.CPut("bb", "4", "1"); pErr == nil {
		panic("expected error from conditional put")
	}
	result, pErr = db.Get("bb")
	if pErr != nil {
		panic(pErr)
	}
	fmt.Printf("bb=%s\n", result.ValueBytes())
	if pErr = db.CPut("bb", "4", nil); pErr != nil {
		panic(pErr)
	}
	result, pErr = db.Get("bb")
	if pErr != nil {
		panic(pErr)
	}
	fmt.Printf("bb=%s\n", result.ValueBytes())

	// Output:
	// aa=2
	// aa=2
	// bb=
	// bb=4
}

func ExampleDB_Inc() {
	s, db := setup()
	defer s.Stop()

	if _, pErr := db.Inc("aa", 100); pErr != nil {
		panic(pErr)
	}
	result, pErr := db.Get("aa")
	if pErr != nil {
		panic(pErr)
	}
	fmt.Printf("aa=%d\n", result.ValueInt())

	// Output:
	// aa=100
}

func ExampleBatch() {
	s, db := setup()
	defer s.Stop()

	b := &client.Batch{}
	b.Get("aa")
	b.Put("bb", "2")
	if pErr := db.Run(b); pErr != nil {
		panic(pErr)
	}
	for _, result := range b.Results {
		for _, row := range result.Rows {
			fmt.Printf("%s=%s\n", row.Key, row.ValueBytes())
		}
	}

	// Output:
	// "aa"=
	// "bb"=2
}

func ExampleDB_Scan() {
	s, db := setup()
	defer s.Stop()

	b := &client.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("bb", "3")
	if pErr := db.Run(b); pErr != nil {
		panic(pErr)
	}
	rows, pErr := db.Scan("a", "b", 100)
	if pErr != nil {
		panic(pErr)
	}
	for i, row := range rows {
		fmt.Printf("%d: %s=%s\n", i, row.Key, row.ValueBytes())
	}

	// Output:
	// 0: "aa"=1
	// 1: "ab"=2
}

func ExampleDB_ReverseScan() {
	s, db := setup()
	defer s.Stop()

	b := &client.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("bb", "3")
	if pErr := db.Run(b); pErr != nil {
		panic(pErr)
	}
	rows, pErr := db.ReverseScan("ab", "c", 100)
	if pErr != nil {
		panic(pErr)
	}
	for i, row := range rows {
		fmt.Printf("%d: %s=%s\n", i, row.Key, row.ValueBytes())
	}

	// Output:
	// 0: "bb"=3
	// 1: "ab"=2
}

func ExampleDB_Del() {
	s, db := setup()
	defer s.Stop()

	b := &client.Batch{}
	b.Put("aa", "1")
	b.Put("ab", "2")
	b.Put("ac", "3")
	if pErr := db.Run(b); pErr != nil {
		panic(pErr)
	}
	if pErr := db.Del("ab"); pErr != nil {
		panic(pErr)
	}
	rows, pErr := db.Scan("a", "b", 100)
	if pErr != nil {
		panic(pErr)
	}
	for i, row := range rows {
		fmt.Printf("%d: %s=%s\n", i, row.Key, row.ValueBytes())
	}

	// Output:
	// 0: "aa"=1
	// 1: "ac"=3
}

func ExampleTxn_Commit() {
	s, db := setup()
	defer s.Stop()

	pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		b := txn.NewBatch()
		b.Put("aa", "1")
		b.Put("ab", "2")
		return txn.CommitInBatch(b)
	})
	if pErr != nil {
		panic(pErr)
	}

	b := &client.Batch{}
	b.Get("aa")
	b.Get("ab")
	if pErr := db.Run(b); pErr != nil {
		panic(pErr)
	}
	for i, result := range b.Results {
		for j, row := range result.Rows {
			fmt.Printf("%d/%d: %s=%s\n", i, j, row.Key, row.ValueBytes())
		}
	}

	// Output:
	// 0/0: "aa"=1
	// 1/0: "ab"=2
}

func ExampleDB_Put_insecure() {
	s := &server.TestServer{}
	s.Ctx = server.NewTestContext()
	s.Ctx.Insecure = true
	if pErr := s.Start(); pErr != nil {
		log.Fatalf("Could not start server: %v", pErr)
	}
	defer s.Stop()

	db := s.DB()
	if pErr := db.Put("aa", "1"); pErr != nil {
		panic(pErr)
	}
	result, pErr := db.Get("aa")
	if pErr != nil {
		panic(pErr)
	}
	fmt.Printf("aa=%s\n", result.ValueBytes())

	// Output:
	// aa=1
}

func TestDebugName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup()
	defer s.Stop()

	file, _, _ := caller.Lookup(0)
	if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		if !strings.HasPrefix(txn.DebugName(), file+":") {
			t.Fatalf("expected \"%s\" to have the prefix \"%s:\"", txn.DebugName(), file)
		}
		return nil
	}); pErr != nil {
		t.Errorf("txn failed: %s", pErr)
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
	blacklist := map[key]struct{}{
		// TODO(tschottdorf): removed GetProto from Batch, which necessitates
		// these two exceptions. Batch.GetProto would require wrapping each
		// request with the information that this particular Get must be
		// unmarshaled, which didn't seem worth doing as we're not using
		// Batch.GetProto at the moment.
		key{dbType, "GetProto"}:                   {},
		key{txnType, "GetProto"}:                  {},
		key{batchType, "CheckConsistency"}:        {},
		key{batchType, "InternalAddRequest"}:      {},
		key{batchType, "PutInline"}:               {},
		key{dbType, "AdminMerge"}:                 {},
		key{dbType, "AdminSplit"}:                 {},
		key{dbType, "CheckConsistency"}:           {},
		key{dbType, "NewBatch"}:                   {},
		key{dbType, "Run"}:                        {},
		key{dbType, "RunWithResponse"}:            {},
		key{dbType, "Txn"}:                        {},
		key{dbType, "GetSender"}:                  {},
		key{dbType, "GetInconsistent"}:            {},
		key{dbType, "GetProtoInconsistent"}:       {},
		key{dbType, "ScanInconsistent"}:           {},
		key{dbType, "PutInline"}:                  {},
		key{txnType, "Commit"}:                    {},
		key{txnType, "CommitBy"}:                  {},
		key{txnType, "CommitInBatch"}:             {},
		key{txnType, "CommitInBatchWithResponse"}: {},
		key{txnType, "CommitOrCleanup"}:           {},
		key{txnType, "Rollback"}:                  {},
		key{txnType, "CleanupOnError"}:            {},
		key{txnType, "DebugName"}:                 {},
		key{txnType, "InternalSetPriority"}:       {},
		key{txnType, "IsFinalized"}:               {},
		key{txnType, "NewBatch"}:                  {},
		key{txnType, "Exec"}:                      {},
		key{txnType, "Run"}:                       {},
		key{txnType, "RunWithResponse"}:           {},
		key{txnType, "SetDebugName"}:              {},
		key{txnType, "SetIsolation"}:              {},
		key{txnType, "SetUserPriority"}:           {},
		key{txnType, "SetSystemConfigTrigger"}:    {},
		key{txnType, "SystemConfigTrigger"}:       {},
	}

	for b := range blacklist {
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
			if _, ok := blacklist[key{typ, m.Name}]; ok {
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

// Verifies that an inner transaction in a nested transaction strips the transaction
// information in its error when propagating it to an other transaction.
func TestNestedTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := setup()
	defer s.Stop()

	pErr := db.Txn(func(txn1 *client.Txn) *roachpb.Error {
		if pErr := txn1.Put("a", "1"); pErr != nil {
			t.Fatalf("unexpected put error: %s", pErr)
		}
		iPErr := db.Txn(func(txn2 *client.Txn) *roachpb.Error {
			txnProto := roachpb.NewTransaction("test", roachpb.Key("a"), 1, roachpb.SERIALIZABLE, roachpb.Timestamp{}, 0)
			return roachpb.NewErrorWithTxn(util.Errorf("inner txn error"), txnProto)
		})

		if iPErr.GetTxn() != nil {
			t.Errorf("error txn must be stripped: %s", iPErr)
		}
		return iPErr

	})
	if pErr == nil {
		t.Fatal("unexpected success of txn")
	}
	if !testutils.IsPError(pErr, "inner txn error") {
		t.Errorf("unexpected failure: %s", pErr)
	}
}
