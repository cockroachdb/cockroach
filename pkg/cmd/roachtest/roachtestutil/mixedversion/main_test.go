// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package mixedversion

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	reset := setDefaultVersions()
	code := m.Run()
	reset()

	os.Exit(code)
}
