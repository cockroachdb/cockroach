// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package pebbleiter exposes a type that selectively wraps a Pebble Iterator
// only in crdb_test builds. This wrapper type performs assertions in test-only
// builds. In non-test-only builds, pebbleiter exposes the pebble.Iterator
// directly.
package pebbleiter
