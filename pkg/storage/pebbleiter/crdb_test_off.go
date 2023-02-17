// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !crdb_test || crdb_test_off
// +build !crdb_test crdb_test_off

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
