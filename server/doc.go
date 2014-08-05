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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

/*

Package server implements the Cockroach storage node. A node
corresponds to a single instance of the cockroach binary, running on a
single physical machine, which exports the "Node" Go RPC service. Each
node multiplexes RPC requests to one or more stores, associated with
physical storage devices.

Package server also provides access to administrative tools via the
command line and also through a REST API.
*/
package server
