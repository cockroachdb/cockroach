// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
)

// WriteGoroutineDump writes a goroutine stack dump to a file in the test
// output directory and returns the file path. If the file cannot be created
// or written, it returns a description of the error.
func WriteGoroutineDump() string {
	f, err := os.CreateTemp(datapathutils.DebuggableTempDir(), "goroutine_dump_*.txt")
	if err != nil {
		return fmt.Sprintf("<failed to create dump file: %v>", err)
	}
	defer f.Close()
	if _, err := f.Write(allstacks.Get()); err != nil {
		return fmt.Sprintf("<failed to write dump to %s: %v>", f.Name(), err)
	}
	return f.Name()
}
