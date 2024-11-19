// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDistribute(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, total := range []int{0, 1, 2, 5, 10, 17, 25} {
		for _, num := range []int{1, 2, 3, 4, 5, 8, 13, 15} {
			d := distribute(total, num)
			// Verify the sum is correct and that the variance is no more than 1.
			min, max, sum := d[0], d[0], d[0]
			for i := 1; i < len(d); i++ {
				sum += d[i]
				if min > d[i] {
					min = d[i]
				}
				if max < d[i] {
					max = d[i]
				}
			}
			if sum != total {
				t.Errorf("%d / %d: incorrect sum %d", total, num, sum)
			}
			if max > min+1 {
				t.Errorf("%d / %d: min value %d, max value %d", total, num, min, max)
			}
		}
	}
}
