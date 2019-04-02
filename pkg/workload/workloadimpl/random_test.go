// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package workloadimpl_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
	"golang.org/x/exp/rand"
)

func BenchmarkRandStringFast(b *testing.B) {
	const strLen = 26
	rng := rand.NewSource(uint64(timeutil.Now().UnixNano()))
	buf := make([]byte, strLen)

	for i := 0; i < b.N; i++ {
		workloadimpl.RandStringFast(rng, buf, `0123456789abcdefghijklmnopqrstuvwxyz`)
	}
	b.SetBytes(strLen)
}
