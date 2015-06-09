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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package client_test

import (
	"fmt"
	"log"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/server"
)

func setup() (*server.TestServer, *client.DB) {
	s := server.StartTestServer(nil)
	db, err := client.Open("https://root@" + s.ServingAddr() + "?certs=test_certs")
	if err != nil {
		log.Fatal(err)
	}
	return s, db
}

func ExampleDB_Get() {
	s, db := setup()
	defer s.Stop()

	result, err := db.Get("aa")
	if err != nil {
		panic(err)
	}
	fmt.Printf("aa=%s\n", result.ValueBytes())

	// Output:
	// aa=
}

func ExampleDB_Put() {
	s, db := setup()
	defer s.Stop()

	if err := db.Put("aa", "1"); err != nil {
		panic(err)
	}
	result, err := db.Get("aa")
	if err != nil {
		panic(err)
	}
	fmt.Printf("aa=%s\n", result.ValueBytes())

	// Output:
	// aa=1
}

func ExampleDB_CPut() {
	s, db := setup()
	defer s.Stop()

	if err := db.Put("aa", "1"); err != nil {
		panic(err)
	}
	if err := db.CPut("aa", "2", "1"); err != nil {
		panic(err)
	}
	result, err := db.Get("aa")
	if err != nil {
		panic(err)
	}
	fmt.Printf("aa=%s\n", result.ValueBytes())

	if err = db.CPut("aa", "3", "1"); err == nil {
		panic("expected error from conditional put")
	}
	result, err = db.Get("aa")
	if err != nil {
		panic(err)
	}
	fmt.Printf("aa=%s\n", result.ValueBytes())

	if err = db.CPut("bb", "4", "1"); err == nil {
		panic("expected error from conditional put")
	}
	result, err = db.Get("bb")
	if err != nil {
		panic(err)
	}
	fmt.Printf("bb=%s\n", result.ValueBytes())
	if err = db.CPut("bb", "4", nil); err != nil {
		panic(err)
	}
	result, err = db.Get("bb")
	if err != nil {
		panic(err)
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

	if _, err := db.Inc("aa", 100); err != nil {
		panic(err)
	}
	result, err := db.Get("aa")
	if err != nil {
		panic(err)
	}
	fmt.Printf("aa=%d\n", result.ValueInt())

	// Output:
	// aa=100
}

func ExampleBatch() {
	s, db := setup()
	defer s.Stop()

	b := db.B.Get("aa").Put("bb", "2")
	if err := db.Run(b); err != nil {
		panic(err)
	}
	for _, result := range b.Results {
		for _, row := range result.Rows {
			fmt.Printf("%s=%s\n", row.Key, row.ValueBytes())
		}
	}

	// Output:
	// aa=
	// bb=2
}

func ExampleDB_Scan() {
	s, db := setup()
	defer s.Stop()

	b := db.B.Put("aa", "1").Put("ab", "2").Put("bb", "3")
	if err := db.Run(b); err != nil {
		panic(err)
	}
	rows, err := db.Scan("a", "b", 100)
	if err != nil {
		panic(err)
	}
	for i, row := range rows {
		fmt.Printf("%d: %s=%s\n", i, row.Key, row.ValueBytes())
	}

	// Output:
	// 0: aa=1
	// 1: ab=2
}

func ExampleDB_Del() {
	s, db := setup()
	defer s.Stop()

	if err := db.Run(db.B.Put("aa", "1").Put("ab", "2").Put("ac", "3")); err != nil {
		panic(err)
	}
	if err := db.Del("ab"); err != nil {
		panic(err)
	}
	rows, err := db.Scan("a", "b", 100)
	if err != nil {
		panic(err)
	}
	for i, row := range rows {
		fmt.Printf("%d: %s=%s\n", i, row.Key, row.ValueBytes())
	}

	// Output:
	// 0: aa=1
	// 1: ac=3
}

func ExampleTx_Commit() {
	s, db := setup()
	defer s.Stop()

	err := db.Tx(func(tx *client.Tx) error {
		return tx.Commit(tx.B.Put("aa", "1").Put("ab", "2"))
	})
	if err != nil {
		panic(err)
	}

	b := db.B.Get("aa").Get("ab")
	if err := db.Run(b); err != nil {
		panic(err)
	}
	for i, result := range b.Results {
		for j, row := range result.Rows {
			fmt.Printf("%d/%d: %s=%s\n", i, j, row.Key, row.ValueBytes())
		}
	}

	// Output:
	// 0/0: aa=1
	// 1/0: ab=2
}

func ExampleDB_Insecure() {
	s := &server.TestServer{}
	s.Ctx = server.NewTestContext()
	s.Ctx.Insecure = true
	if err := s.Start(); err != nil {
		log.Fatalf("Could not start server: %v", err)
	}
	log.Printf("Test server listening on %s: %s", s.Ctx.RequestScheme(), s.ServingAddr())
	defer s.Stop()

	db, err := client.Open("http://root@" + s.ServingAddr())
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Put("aa", "1"); err != nil {
		panic(err)
	}
	result, err := db.Get("aa")
	if err != nil {
		panic(err)
	}
	fmt.Printf("aa=%s\n", result.ValueBytes())

	// Output:
	// aa=1
}

func TestOpenArgs(t *testing.T) {
	s := server.StartTestServer(t)
	defer s.Stop()

	testCases := []struct {
		addr      string
		expectErr bool
	}{
		{"https://root@" + s.ServingAddr() + "?certs=test_certs", false},
		{"https://" + s.ServingAddr() + "?certs=test_certs", false},
		{"https://" + s.ServingAddr() + "?certs=foo", true},
	}

	for _, test := range testCases {
		_, err := client.Open(test.addr)
		if test.expectErr && err == nil {
			t.Errorf("Open(%q): expected an error; got %v", test.addr, err)
		} else if !test.expectErr && err != nil {
			t.Errorf("Open(%q): expected no errors; got %v", test.addr, err)
		}
	}
}

func TestDebugName(t *testing.T) {
	s, db := setup()
	defer s.Stop()

	_, file, _, _ := runtime.Caller(0)
	base := filepath.Base(file)
	_ = db.Tx(func(tx *client.Tx) error {
		if !strings.HasPrefix(tx.DebugName(), base+":") {
			t.Fatalf("expected \"%s\" to have the prefix \"%s:\"", tx.DebugName(), base)
		}
		return nil
	})
}

func TestCommonMethods(t *testing.T) {
	batchType := reflect.TypeOf(&client.Batch{})
	batcherType := reflect.TypeOf(client.DB{}.B)
	dbType := reflect.TypeOf(&client.DB{})
	txType := reflect.TypeOf(&client.Tx{})
	types := []reflect.Type{batchType, batcherType, dbType, txType}

	type key struct {
		typ    reflect.Type
		method string
	}
	blacklist := map[key]struct{}{
		key{batchType, "InternalAddCall"}:   {},
		key{dbType, "AdminMerge"}:           {},
		key{dbType, "AdminSplit"}:           {},
		key{dbType, "InternalSender"}:       {},
		key{dbType, "InternalSetSender"}:    {},
		key{dbType, "Run"}:                  {},
		key{dbType, "Tx"}:                   {},
		key{txType, "Commit"}:               {},
		key{txType, "DebugName"}:            {},
		key{txType, "InternalSetPriority"}:  {},
		key{txType, "Run"}:                  {},
		key{txType, "SetDebugName"}:         {},
		key{txType, "SetSnapshotIsolation"}: {},
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
