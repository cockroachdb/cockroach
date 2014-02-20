// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/spencerkimball/cockroach/server"
)

var (
	port = flag.String("port", ":8080", "port to bind to")
)

func main() {
	flag.Parse()
	log.Println("Starting node on port", *port)
	log.Fatal(http.ListenAndServe(*port, server.New()))
}
