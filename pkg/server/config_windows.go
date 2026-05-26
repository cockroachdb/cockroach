// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import "github.com/cockroachdb/cockroach/pkg/storage"

func setOpenFileLimitInner(physicalStoreCount int) (uint64, error) {
	return storage.RecommendedMaxOpenFiles, nil
}
