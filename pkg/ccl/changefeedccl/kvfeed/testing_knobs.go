// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfeed

import "github.com/cockroachdb/cockroach/pkg/kv"

// TestingKnobs are the testing knobs for kvfeed.
type TestingKnobs struct {
	// BeforeScanRequest is a callback invoked before issuing Scan request.
	BeforeScanRequest func(b *kv.Batch)
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
