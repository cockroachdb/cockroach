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
)

const (
	minSignificantOrders = 10000
)

// auditor maintains statistics about TPC-C input data and runs distribution
// checks, as specified in Clause 9.2 of the TPC-C spec.
type auditor struct {
	syncutil.Mutex

	warehouses int

	newOrderTransactions uint64
	newOrderRollbacks    uint64

	// map from order-lines count to the number of orders with that count
	orderLinesFreq  map[int]uint64
	totalOrderLines uint64

	remoteWarehouseFreq map[int]uint64
}

func newAuditor(warehouses int) *auditor {
	return &auditor{
		warehouses:          warehouses,
		orderLinesFreq:      make(map[int]uint64),
		remoteWarehouseFreq: make(map[int]uint64),
	}
}

type auditResult struct {
	status      string // PASS, FAIL, or SKIP
	description string
}

var passResult = auditResult{status: "PASS"}

func newFailResult(format string, args ...interface{}) auditResult {
	return auditResult{"FAIL", fmt.Sprintf(format, args...)}
}

func newSkipResult(format string, args ...interface{}) auditResult {
	return auditResult{"SKIP", fmt.Sprintf(format, args...)}
}

// runChecks runs the audit checks and prints warnings to stdout for those that
// fail.
func (a *auditor) runChecks() {
	type check struct {
		name string
		f    func(a *auditor) auditResult
	}
	checks := []check{
		{"9.2.2.5.1", check92251},
		{"9.2.2.5.2", check92252},
		{"9.2.2.5.3", check92253},
	}
	for _, check := range checks {
		result := check.f(a)
		msg := fmt.Sprintf("Audit check %s: %s", check.name, result.status)
		if result.description == "" {
			fmt.Println(msg)
		} else {
			fmt.Println(msg + ": " + result.description)
		}
	}
}

func check92251(a *auditor) auditResult {
	// At least 0.9% and at most 1.1% of the New-Order transactions roll back as a
	// result of an unused item number.
	orders := atomic.LoadUint64(&a.newOrderTransactions)
	if orders < minSignificantOrders {
		return newSkipResult("not enough orders to be statistically significant")
	}
	rollbacks := atomic.LoadUint64(&a.newOrderRollbacks)
	rollbackPct := 100 * float64(rollbacks) / float64(orders)
	if rollbackPct < 0.9 || rollbackPct > 1.1 {
		return newFailResult(
			"new order rollback percent %.1f is not between allowed bounds [0.9, 1.1]", rollbackPct)
	}
	return passResult
}

func check92252(a *auditor) auditResult {
	// The average number of order-lines per order is in the range of 9.5 to 10.5
	// and the number of order-lines is uniformly distributed from 5 to 15 for the
	// New-Order transactions that are submitted to the SUT during the measurement
	// interval.
	a.Lock()
	defer a.Unlock()

	if a.newOrderTransactions < minSignificantOrders {
		return newSkipResult("not enough orders to be statistically significant")
	}

	avg := float64(a.totalOrderLines) / float64(a.newOrderTransactions)
	if avg < 9.5 || avg > 10.5 {
		return newFailResult(
			"average order-lines count %.1f is not between allowed bounds [9.5, 10.5]", avg)
	}

	expectedPct := 100.0 / 11 // uniformly distributed across 11 possible values
	tolerance := 1.0          // allow 1 percent deviation from expected
	for i := 5; i <= 15; i++ {
		freq := a.orderLinesFreq[i]
		pct := 100 * float64(freq) / float64(a.newOrderTransactions)
		if math.Abs(expectedPct-pct) > tolerance {
			return newFailResult(
				"order-lines count should be uniformly distributed from 5 to 15, but it was %d for %.1f "+
					"percent of orders", i, pct)
		}
	}
	return passResult
}

func check92253(a *auditor) auditResult {
	// The number of remote order-lines is at least 0.95% and at most 1.05% of the
	// number of order-lines that are filled in by the New-Order transactions that
	// are submitted to the SUT during the measurement interval, and the remote
	// warehouse numbers are uniformly distributed within the range of active
	// warehouses.
	a.Lock()
	defer a.Unlock()

	if a.newOrderTransactions < minSignificantOrders {
		return newSkipResult("not enough orders to be statistically significant")
	}

	var remoteOrderLines uint64
	for _, freq := range a.remoteWarehouseFreq {
		remoteOrderLines += freq
	}
	remotePct := 100 * float64(remoteOrderLines) / float64(a.totalOrderLines)
	if remotePct < 0.95 || remotePct > 1.05 {
		return newFailResult(
			"remote order-line percent %.1f is not between allowed bounds [0.95, 1.05]", remotePct)
	}

	// In the absence of a more sophisticated distribution check like a
	// chi-squared test, check each warehouse is used as a remote warehouse at
	// least once. We need the number of remote order-lines to be at least 15
	// times the number of warehouses (experimentally determined) to have this
	// expectation.
	if remoteOrderLines < 15*uint64(a.warehouses) {
		return newSkipResult("insufficient data for remote warehouse distribution check")
	}
	for i := 0; i < a.warehouses; i++ {
		if _, ok := a.remoteWarehouseFreq[i]; !ok {
			return newFailResult("no remote order-lines for warehouses %d", i)
		}
	}

	return passResult
}
