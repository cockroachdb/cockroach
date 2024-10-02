// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package pebbleiter exposes a type that selectively wraps a Pebble Iterator
// only in crdb_test builds. This wrapper type performs assertions in test-only
// builds. In non-test-only builds, pebbleiter exposes the pebble.Iterator
// directly.
package pebbleiter
