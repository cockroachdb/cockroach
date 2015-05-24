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
// Author: Ben Darnell

package client_test

import (
	"fmt"
	"log"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils"
)

func setupCall() (*server.TestServer, *client.KV) {
	s := server.StartTestServer(nil)
	sender, err := client.NewHTTPSender(s.ServingAddr(), testutils.NewTestBaseContext())
	if err != nil {
		log.Fatal(err)
	}
	kvClient := client.NewKV(nil, sender)
	kvClient.User = "root"
	return s, kvClient
}

func ExampleCall_Get() {
	s, db := setupCall()
	defer s.Stop()

	get := client.Get(proto.Key("aa"))
	if err := db.Run(get); err != nil {
		panic(err)
	}
	fmt.Printf("aa=%s\n", get.ValueBytes())

	// Output:
	// aa=
}

func ExampleCall_Put() {
	s, db := setupCall()
	defer s.Stop()

	if err := db.Run(client.Put(proto.Key("aa"), []byte("1"))); err != nil {
		panic(err)
	}
	get := client.Get(proto.Key("aa"))
	if err := db.Run(get); err != nil {
		panic(err)
	}
	fmt.Printf("aa=%s\n", get.ValueBytes())

	// Output:
	// aa=
}

func ExampleCall_CPut() {
	s, db := setupCall()
	defer s.Stop()

	if err := db.Run(client.Put(proto.Key("aa"), []byte("1"))); err != nil {
		panic(err)
	}
	if err := db.Run(client.ConditionalPut(proto.Key("aa"), []byte("2"), []byte("1"))); err != nil {
		panic(err)
	}
}

func ExampleCall_Batch() {
	s, db := setupCall()
	defer s.Stop()

	calls := []client.Callable{client.Get(proto.Key("aa")), client.Put(proto.Key("bb"), []byte("2"))}
	if err := db.Run(calls...); err != nil {
		panic(err)
	}
	for _, call := range calls {
		if get, ok := call.(client.GetCall); ok {
			fmt.Printf("%s=%s\n", get.Args.Key, get.ValueBytes())
		}
	}

	// Output:
	// aa=
}

func ExampleCall_Batch2() {
	s, db := setupCall()
	defer s.Stop()

	get := client.Get(proto.Key("aa"))
	put := client.Put(proto.Key("aa"), []byte("2"))
	if err := db.Run(get, put); err != nil {
		panic(err)
	}

	fmt.Printf("aa=%s\n", get.ValueBytes())

	// Output:
	// aa=
}

func ExampleCall_Scan() {
	s, db := setupCall()
	defer s.Stop()

	if err := db.Run(
		client.Put(proto.Key("aa"), []byte("1")),
		client.Put(proto.Key("ab"), []byte("2")),
		client.Put(proto.Key("bb"), []byte("3"))); err != nil {
		panic(err)
	}
	scan := client.Scan(proto.Key("a"), proto.Key("b"), 100)
	if err := db.Run(scan); err != nil {
		panic(err)
	}

}
