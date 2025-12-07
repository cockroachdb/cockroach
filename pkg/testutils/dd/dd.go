// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package dd contains utilities enhancing the experience of using the
// datadriven testing package.
//
// TODO(pav-kv): migrate to the datadriven package when widely used.
package dd

import (
	"testing"

	"github.com/cockroachdb/datadriven"
)

// ScanArg is like TestData.ScanArgs, but for a single argument. Requires the
// argument to be present.
func ScanArg[T any](t testing.TB, d *datadriven.TestData, key string) T {
	t.Helper()
	var val T
	d.ScanArgs(t, key, &val)
	return val
}

// ScanArgOpt is like ScanArg, but it allows the argument to be missing. The
// returned bool indicates whether it is present.
func ScanArgOpt[T any](t testing.TB, d *datadriven.TestData, key string) (T, bool) {
	t.Helper()
	var val T
	found := d.MaybeScanArgs(t, key, &val)
	return val, found
}

// ScanArgOr is like ScanArg, but it returns the provided default value when the
// argument is not present.
func ScanArgOr[T any](t testing.TB, d *datadriven.TestData, key string, def T) T {
	t.Helper()
	d.MaybeScanArgs(t, key, &def)
	return def
}
