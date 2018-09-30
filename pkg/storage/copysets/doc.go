// Copyright 2019 The Cockroach Authors.
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

/*
Package copysets proves an implementation of copysets presented in
https://web.stanford.edu/~skatti/pubs/usenix13-copysets.pdf.

Stores are divided into sets of nodes of size replication factor (for each
replication factor) and ranges reside within a single copyset in steady state.
This significantly reduces the probability of data loss in large clusters since
as long as a quorum of nodes are alive within each copyset, there is no loss
of data.
*/
package copysets
