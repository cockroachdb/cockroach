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
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import "flag"

// Addr is the tcp address used to connect to the Cockroach cluster.
var Addr = flag.String("addr", "127.0.0.1:8080", "address for connection to cockroach cluster")

// HTTPAddr returns the fully qualified URL used to connect to the Cockroach cluster.
func HTTPAddr() string {
	return "http://" + *Addr
}
