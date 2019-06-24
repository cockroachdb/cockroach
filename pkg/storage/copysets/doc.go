// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package copysets provides an implementation of copysets presented in
https://web.stanford.edu/~skatti/pubs/usenix13-copysets.pdf.

Stores are divided into sets of nodes of size replication factor (for each
replication factor) and ranges reside within a single copyset in steady state.
This significantly reduces the probability of data loss in large clusters since
as long as a quorum of nodes are alive within each copyset, there is no loss
of data.
*/
package copysets
