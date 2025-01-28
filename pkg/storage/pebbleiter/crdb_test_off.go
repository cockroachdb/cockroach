// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !crdb_test || crdb_test_off

package pebbleiter

import "github.com/cockroachdb/pebble"

// Iterator redefines *pebble.Iterator.
type Iterator = *pebble.Iterator

// MaybeWrap returns the provided Iterator, unmodified.
//
//gcassert:inline
func MaybeWrap(iter *pebble.Iterator) Iterator {
	return iter
}
