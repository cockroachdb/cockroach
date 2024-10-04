// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstreamer_test

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
)

func init() {
	kvstreamer.TestResultDiskBufferConstructor = rowcontainer.NewKVStreamerResultDiskBuffer
}
