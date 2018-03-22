// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package tpcc

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

const (
	minSignificantOrders = 10000
)

// auditor maintains statistics about TPC-C input data and runs distribution
// checks, as specified in Clause 9.2 of the TPC-C spec.
type auditor struct {
	syncutil.Mutex

	newOrderTransactions uint64
	newOrderRollbacks    uint64

	// map from order-lines count to the number of orders with that count
	orderLinesFreq map[int]uint64
}

func newAuditor() *auditor {
	return &auditor{orderLinesFreq: make(map[int]uint64)}
}

// runChecks runs the audit checks and prints warnings to stdout for those that
// fail.
func (a *auditor) runChecks() {
	type check struct {
		name string
		f    func(a *auditor) error
	}
	checks := []check{
		{"9.2.2.5.1", check92251},
		{"9.2.2.5.2", check92252},
	}
	for _, check := range checks {
		result := check.f(a)
		if result != nil {
			fmt.Println(errors.Wrapf(result, "WARN: Failed audit check %s", check.name))
		}
	}
}

func check92251(a *auditor) error {
	// At least 0.9% and at most 1.1% of the New-Order transactions roll back as a
	// result of an unused item number.
	orders := atomic.LoadUint64(&a.newOrderTransactions)
	if orders < minSignificantOrders {
		// Not enough orders to be statistically significant.
		return nil
	}
	rollbacks := atomic.LoadUint64(&a.newOrderRollbacks)
	rollbackPct := 100 * float64(rollbacks) / float64(orders)
	if rollbackPct < 0.9 || rollbackPct > 1.1 {
		return errors.Errorf(
			"new order rollback percent %.1f is not between allowed bounds [0.9, 1.1]", rollbackPct)
	}
	return nil
}

func check92252(a *auditor) error {
	// The average number of order-lines per order is in the range of 9.5 to 10.5
	// and the number of order-lines is uniformly distributed from 5 to 15 for the
	// New-Order transactions that are submitted to the SUT during the measurement
	// interval.
	a.Lock()
	defer a.Unlock()

	var orders, orderLines uint64
	for i := 5; i <= 15; i++ {
		freq := a.orderLinesFreq[i]
		orders += freq
		orderLines += freq * uint64(i)
	}
	if orders < minSignificantOrders {
		return nil
	}

	avg := float64(orderLines) / float64(orders)
	if avg < 9.5 || avg > 10.5 {
		return errors.Errorf(
			"average order-lines count %.1f is not between allowed bounds [9.5, 10.5]", avg)
	}

	expectedPct := 100.0 / 11 // uniformly distributed across 11 possible values
	tolerance := 1.0          // allow 1 percent deviation from expected
	for i := 5; i <= 15; i++ {
		freq := a.orderLinesFreq[i]
		pct := 100 * float64(freq) / float64(orders)
		if math.Abs(expectedPct-pct) > tolerance {
			return errors.Errorf(
				"order-lines count should be uniformly distributed from 5 to 15, but it was %d for "+
					"%.1f percent of orders", i, pct)
		}
	}
	return nil
}
