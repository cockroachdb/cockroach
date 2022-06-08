// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package storage

// DisableReaderAssertionsI is optionally implemented by arguments to DisableReaderAssertions.
type DisableReaderAssertionsI interface {
	DisableReaderAssertions() (wrapped Reader)
}

// DisableReaderAssertions unwraps any storage.Reader implementations that may
// assert access against a given SpanSet.
func DisableReaderAssertions(reader Reader) Reader {
	switch v := reader.(type) {
	case DisableReaderAssertionsI:
		return DisableReaderAssertions(v.DisableReaderAssertions())
	default:
		return reader
	}
}
