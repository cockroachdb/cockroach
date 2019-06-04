// Copyright 2016 The Cockroach Authors.
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

package testutils

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
)

// TempDir creates a directory and a function to clean it up at the end of the
// test.
func TempDir(t testing.TB) (string, func()) {
	dir, err := ioutil.TempDir("", fileutil.EscapeFilename(t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}
	return dir, cleanup
}
