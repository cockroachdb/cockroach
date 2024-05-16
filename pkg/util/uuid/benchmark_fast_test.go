// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package uuid_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func BenchmarkMakeV4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuid.MakeV4()
	}
}

func BenchmarkConcurrentMakeV4(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			uuid.MakeV4()
		}
	})
}
