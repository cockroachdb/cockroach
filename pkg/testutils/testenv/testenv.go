// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testenv

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/base"
)

// TestStorageEngine specifies the engine type (eg. rocksdb, pebble) to use to
// instantiate stores.
var TestStorageEngine base.EngineType

func init() {
	TestStorageEngine.Set(os.Getenv("COCKROACH_TEST_STORAGE_ENGINE"))
}
