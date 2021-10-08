// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

// A Error wraps an error returned from a RocksDB operation.
type Error struct {
	msg string
}

// Error implements the error interface.
func (err *Error) Error() string {
	return err.msg
}
