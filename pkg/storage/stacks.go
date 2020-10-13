// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

// ThreadStacks returns the stacks for all threads. The stacks are raw
// addresses, and do not contain symbols. Use addr2line (or atos on Darwin) to
// symbolize.
func ThreadStacks() string {
	// TODO(bilal): This got deleted during the RocksDB/libroach deletion. Bring
	// this back.
	return ""
}
