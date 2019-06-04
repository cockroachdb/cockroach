// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
