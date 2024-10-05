// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package uuid_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func BenchmarkFastMakeV4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuid.FastMakeV4()
	}
}

func BenchmarkMakeV4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuid.MakeV4()
	}
}
