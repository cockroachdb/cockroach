// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const (
	minSignificantTransactions = 10000
)

// auditor maintains statistics about TPC-C input data and runs distribution
// checks, as specified in Clause 9.2 of the TPC-C spec.
type auditor struct {
	syncutil.Mutex

	warehouses int

	// transaction counts
	newOrderTransactions    uint64
	newOrderRollbacks       uint64
	paymentTransactions     uint64
	orderStatusTransactions uint64
	deliveryTransactions    uint64

	// map from order-lines count to the number of orders with that count
	orderLinesFreq map[int]uint64

	// sum of order lines across all orders
	totalOrderLines uint64

	// map from warehouse to number of remote order lines for that warehouse
	orderLineRemoteWarehouseFreq map[int]uint64
	// map from warehouse to number of remote payments for that warehouse
	paymentRemoteWarehouseFreq map[int]uint64

	// counts of how many transactions select the customer by last name
	paymentsByLastName    uint64
	orderStatusByLastName uint64

	skippedDelivieries uint64
}

func newAuditor(warehouses int) *auditor {
	return &auditor{
		warehouses:                   warehouses,
		orderLinesFreq:               make(map[int]uint64),
		orderLineRemoteWarehouseFreq: make(map[int]uint64),
		paymentRemoteWarehouseFreq:   make(map[int]uint64),
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
		{"9.2.1.7", check9217},
		{"9.2.2.5.1", check92251},
		{"9.2.2.5.2", check92252},
		{"9.2.2.5.3", check92253},
		{"9.2.2.5.4", check92254},
		{"9.2.2.5.5", check92255},
		{"9.2.2.5.6", check92256},
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

func check9217(a *auditor) auditResult {
	// Verify that no more than 1%, or no more than one (1), whichever is greater,
	// of the Delivery transactions skipped because there were fewer than
	// necessary orders present in the New-Order table.
	a.Lock()
	defer a.Unlock()

	if a.deliveryTransactions < minSignificantTransactions {
		return newSkipResult("not enough delivery transactions to be statistically significant")
	}

	var threshold uint64
	if a.deliveryTransactions > 100 {
		threshold = a.deliveryTransactions / 100
	} else {
		threshold = 1
	}
	if a.skippedDelivieries > threshold {
		return newFailResult(
			"expected no more than %d skipped deliveries, got %d", threshold, a.skippedDelivieries)
	}
	return passResult
}

func check92251(a *auditor) auditResult {
	// At least 0.9% and at most 1.1% of the New-Order transactions roll back as a
	// result of an unused item number.
	orders := atomic.LoadUint64(&a.newOrderTransactions)
	if orders < minSignificantTransactions {
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

	if a.newOrderTransactions < minSignificantTransactions {
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

	if a.warehouses == 1 {
		// Not applicable when there are no remote warehouses.
		return passResult
	}
	if a.newOrderTransactions < minSignificantTransactions {
		return newSkipResult("not enough orders to be statistically significant")
	}

	var remoteOrderLines uint64
	for _, freq := range a.orderLineRemoteWarehouseFreq {
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
		if _, ok := a.orderLineRemoteWarehouseFreq[i]; !ok {
			return newFailResult("no remote order-lines for warehouses %d", i)
		}
	}

	return passResult
}

func check92254(a *auditor) auditResult {
	// The number of remote Payment transactions is at least 14% and at most 16%
	// of the number of Payment transactions that are submitted to the SUT during
	// the measurement interval, and the remote warehouse numbers are uniformly
	// distributed within the range of active warehouses.
	a.Lock()
	defer a.Unlock()

	if a.warehouses == 1 {
		// Not applicable when there are no remote warehouses.
		return passResult
	}
	if a.paymentTransactions < minSignificantTransactions {
		return newSkipResult("not enough payments to be statistically significant")
	}

	var remotePayments uint64
	for _, freq := range a.paymentRemoteWarehouseFreq {
		remotePayments += freq
	}
	remotePct := 100 * float64(remotePayments) / float64(a.paymentTransactions)
	if remotePct < 14 || remotePct > 16 {
		return newFailResult(
			"remote payment percent %.1f is not between allowed bounds [14, 16]", remotePct)
	}

	if remotePayments < 15*uint64(a.warehouses) {
		return newSkipResult("insufficient data for remote warehouse distribution check")
	}
	for i := 0; i < a.warehouses; i++ {
		if _, ok := a.paymentRemoteWarehouseFreq[i]; !ok {
			return newFailResult("no remote payments for warehouses %d", i)
		}
	}

	return passResult
}

func check92255(a *auditor) auditResult {
	// The number of customer selections by customer last name in the Payment
	// transaction is at least 57% and at most 63% of the number of Payment
	// transactions.
	a.Lock()
	defer a.Unlock()

	if a.paymentTransactions < minSignificantTransactions {
		return newSkipResult("not enough payments to be statistically significant")
	}
	lastNamePct := 100 * float64(a.paymentsByLastName) / float64(a.paymentTransactions)
	if lastNamePct < 57 || lastNamePct > 63 {
		return newFailResult(
			"percent of customer selections by last name in payment transactions %.1f is not between "+
				"allowed bounds [57, 63]", lastNamePct)
	}

	return passResult
}

func check92256(a *auditor) auditResult {
	// The number of customer selections by customer last name in the Order-Status
	// transaction is at least 57% and at most 63% of the number of Order-Status
	// transactions.
	a.Lock()
	defer a.Unlock()

	if a.orderStatusTransactions < minSignificantTransactions {
		return newSkipResult("not enough order status transactions to be statistically significant")
	}
	lastNamePct := 100 * float64(a.orderStatusByLastName) / float64(a.orderStatusTransactions)
	if lastNamePct < 57 || lastNamePct > 63 {
		return newFailResult(
			"percent of customer selections by last name in order status transactions %.1f is not "+
				"between allowed bounds [57, 63]", lastNamePct)
	}

	return passResult
}
