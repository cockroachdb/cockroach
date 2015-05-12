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
// Author: Peter Mattis (peter.mattis@gmail.com)

package client_test

import (
	"fmt"
	"log"

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

	result := db.Get("aa")
	if result.Err != nil {
		panic(result.Err)
	}
	fmt.Printf("aa=%s\n", result.Rows[0].ValueBytes())

	// Output:
	// aa=
}

func ExampleDB_Put() {
	s, db := setup()
	defer s.Stop()

	result := db.Put("aa", "1")
	if result.Err != nil {
		panic(result.Err)
	}
	result = db.Get("aa")
	if result.Err != nil {
		panic(result.Err)
	}
	fmt.Printf("aa=%s\n", result.Rows[0].ValueBytes())

	// Output:
	// aa=1
}

func ExampleDB_CPut() {
	s, db := setup()
	defer s.Stop()

	result := db.Put("aa", "1")
	if result.Err != nil {
		panic(result.Err)
	}
	result = db.CPut("aa", "2", "1")
	if result.Err != nil {
		panic(result.Err)
	}
	result = db.Get("aa")
	if result.Err != nil {
		panic(result.Err)
	}
	fmt.Printf("aa=%s\n", result.Rows[0].ValueBytes())

	result = db.CPut("aa", "3", "1")
	if result.Err == nil {
		panic("expected error from conditional put")
	}
	result = db.Get("aa")
	if result.Err != nil {
		panic(result.Err)
	}
	fmt.Printf("aa=%s\n", result.Rows[0].ValueBytes())

	result = db.CPut("bb", "4", "1")
	if result.Err == nil {
		panic("expected error from conditional put")
	}
	result = db.Get("bb")
	if result.Err != nil {
		panic(result.Err)
	}
	fmt.Printf("bb=%s\n", result.Rows[0].ValueBytes())
	result = db.CPut("bb", "4", nil)
	if result.Err != nil {
		panic(result.Err)
	}
	result = db.Get("bb")
	if result.Err != nil {
		panic(result.Err)
	}
	fmt.Printf("bb=%s\n", result.Rows[0].ValueBytes())

	// Output:
	// aa=2
	// aa=2
	// bb=
	// bb=4
}

func ExampleDB_Inc() {
	s, db := setup()
	defer s.Stop()

	result := db.Inc("aa", 100)
	if result.Err != nil {
		panic(result.Err)
	}
	result = db.Get("aa")
	if result.Err != nil {
		panic(result.Err)
	}
	fmt.Printf("aa=%d\n", result.Rows[0].ValueInt())

	// Output:
	// aa=100
}

func ExampleBatch() {
	s, db := setup()
	defer s.Stop()

	b := db.B.Get("aa").Put("bb", "2")
	err := db.Run(b)
	if err != nil {
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
	err := db.Run(b)
	if err != nil {
		panic(err)
	}
	result := db.Scan("a", "b", 100)
	if result.Err != nil {
		panic(result.Err)
	}
	for i, row := range result.Rows {
		fmt.Printf("%d: %s=%s\n", i, row.Key, row.ValueBytes())
	}

	// Output:
	// 0: aa=1
	// 1: ab=2
}

func ExampleDB_Del() {
	s, db := setup()
	defer s.Stop()

	err := db.Run(db.B.Put("aa", "1").Put("ab", "2").Put("ac", "3"))
	if err != nil {
		panic(err)
	}
	result := db.Del("ab")
	if result.Err != nil {
		panic(result.Err)
	}
	result = db.Scan("a", "b", 100)
	if result.Err != nil {
		panic(result.Err)
	}
	for i, row := range result.Rows {
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

	result := db.Get("aa", "ab")
	if result.Err != nil {
		panic(result.Err)
	}
	for i, row := range result.Rows {
		fmt.Printf("%d: %s=%s\n", i, row.Key, row.ValueBytes())
	}

	// Output:
	// 0: aa=1
	// 1: ab=2
}
