// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracedumper

// TestingGetTraceDumpDir returns the trace directory that the TraceDumper was
// configured with. This is used solely for testing purposes.
func TestingGetTraceDumpDir(td *TraceDumper) string {
	return td.store.GetFullPath("")
}
