// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import "github.com/cockroachdb/cockroach/pkg/testutils/storageutils"

type kvs = storageutils.KVs

var (
	rangeKVWithTS = storageutils.RangeKVWithTS
)
