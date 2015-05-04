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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/server"
)

func setup() (*server.TestServer, *client.DB) {
	s := server.StartTestServer(nil)
	db := client.Open("https://root@" + s.ServingAddr())
	return s, db
}

func ExampleDB_Get() {
	s, db := setup()
	defer s.Stop()

	fmt.Println(db.Get("aa").String())

	// Output:
	// 0: aa=nil
}

func ExampleDB_Put() {
	s, db := setup()
	defer s.Stop()

	_ = db.Put("aa", "1")
	fmt.Println(db.Get("aa").String())

	// Output:
	// 0: aa=1
}

func ExampleDB_CPut() {
	s, db := setup()
	defer s.Stop()

	_ = db.Put("aa", "1")
	_ = db.CPut("aa", "2", "1")
	fmt.Println(db.Get("aa").String())

	_ = db.CPut("aa", "3", "1")
	fmt.Println(db.Get("aa").String())

	_ = db.CPut("bb", "4", "1")
	fmt.Println(db.Get("bb").String())
	_ = db.CPut("bb", "4", nil)
	fmt.Println(db.Get("bb").String())

	// Output:
	// 0: aa=2
	// 0: aa=2
	// 0: bb=nil
	// 0: bb=4
}

func ExampleDB_Inc() {
	s, db := setup()
	defer s.Stop()

	_ = db.Inc("aa", 100)
	fmt.Println(db.Get("aa").String())

	// Output:
	// 0: aa=100
}

func ExampleBatch() {
	s, db := setup()
	defer s.Stop()

	b := db.B.Get("aa").Put("bb", "2")
	_ = db.Run(b)
	fmt.Println(b.Results[0].String())
	fmt.Println(b.Results[1].String())

	// Output:
	// 0: aa=nil
	// 0: bb=2
}

func ExampleDB_Scan() {
	s, db := setup()
	defer s.Stop()

	b := db.B.Put("aa", "1").Put("ab", "2").Put("bb", "3")
	_ = db.Run(b)
	fmt.Println(db.Scan("a", "b", 100).String())

	// Output:
	// 0: aa=1
	// 1: ab=2
}

func ExampleDB_Del() {
	s, db := setup()
	defer s.Stop()

	_ = db.Run(db.B.Put("aa", "1").Put("ab", "2").Put("ac", "3"))
	_ = db.Del("ab")
	fmt.Println(db.Scan("a", "b", 100).String())

	// Output:
	// 0: aa=1
	// 1: ac=3
}

func ExampleTx_Commit() {
	s, db := setup()
	defer s.Stop()

	_ = db.Tx(func(tx *client.Tx) error {
		return tx.Commit(tx.B.Put("aa", "1").Put("ab", "2"))
	})

	fmt.Println(db.Get("aa", "ab").String())

	// Output:
	// 0: aa=1
	// 1: ab=2
}
