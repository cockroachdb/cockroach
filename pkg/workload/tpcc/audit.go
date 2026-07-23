// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	// These are needed while checking for remote payments for order-line and
	// payments table.
	// partitionAffinity - partitionIds for which to do these checks
	// partitioner - helps map which warehouseId belongs to those partitions
	partitionAffinity []int
	partitioner       *partitioner
	// CountOfTotalPartitions / CountOfAffinityPartitions
	// Used in audit checks, if load is generated only for
	// some partition, the audit checks should also be reduced
	// by that factor.
	// Used specially in case there are multiple workload nodes
	// generating load for different partitions.
	partitionFactor int

	// transaction counts
	newOrderTransactions    atomic.Uint64
	newOrderRollbacks       atomic.Uint64
	paymentTransactions     atomic.Uint64
	orderStatusTransactions atomic.Uint64
	deliveryTransactions    atomic.Uint64

	// map from order-lines count to the number of orders with that count
	orderLinesFreq map[int]uint64

	// sum of order lines across all orders
	totalOrderLines atomic.Uint64

	// map from warehouse to number of remote order lines for that warehouse
	orderLineRemoteWarehouseFreq map[int]uint64
	// map from warehouse to number of remote payments for that warehouse
	paymentRemoteWarehouseFreq map[int]uint64

	// counts of how many transactions select the customer by last name
	paymentsByLastName    atomic.Uint64
	orderStatusByLastName atomic.Uint64

	skippedDelivieries atomic.Uint64
}

func newAuditor(warehouses int, partitioner *partitioner, partitionAffinity []int) *auditor {
	return &auditor{
		warehouses:        warehouses,
		partitioner:       partitioner,
		partitionAffinity: partitionAffinity,
		partitionFactor: func() int {
			countAffinity := len(partitionAffinity)
			if countAffinity == 0 || partitioner.parts == 0 {
				return 1
			} else {
				return partitioner.parts / countAffinity
			}
		}(),
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
func (a *auditor) runChecks(localWarehouses bool) {
	type check struct {
		name string
		f    func(a *auditor) auditResult
	}
	checks := []check{
		{"9.2.1.7", check9217},
		{"9.2.2.5.1", check92251},
		{"9.2.2.5.2", check92252},
		{"9.2.2.5.5", check92255},
		{"9.2.2.5.6", check92256},
	}

	// If we're keeping the workload local, these checks are expected to fail.
	// Bypass them instead of allowing them to fail.
	if !localWarehouses {
		checks = append(checks,
			check{"9.2.2.5.3", check92253},
			check{"9.2.2.5.4", check92254},
		)
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

	if a.deliveryTransactions.Load() < minSignificantTransactions {
		return newSkipResult("not enough delivery transactions to be statistically significant")
	}

	var threshold atomic.Uint64
	if a.deliveryTransactions.Load() > 100 {
		threshold.Store(a.deliveryTransactions.Load() / 100)
	} else {
		threshold.Store(1)
	}
	if a.skippedDelivieries.Load() > threshold.Load() {
		return newFailResult(
			"expected no more than %d skipped deliveries, got %d", threshold.Load(), a.skippedDelivieries.Load())
	}
	return passResult
}

func check92251(a *auditor) auditResult {
	// At least 0.9% and at most 1.1% of the New-Order transactions roll back as a
	// result of an unused item number.
	orders := a.newOrderTransactions.Load()
	if orders < minSignificantTransactions {
		return newSkipResult("not enough orders to be statistically significant")
	}
	rollbacks := a.newOrderRollbacks.Load()
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

	if a.newOrderTransactions.Load() < minSignificantTransactions {
		return newSkipResult("not enough orders to be statistically significant")
	}

	avg := float64(a.totalOrderLines.Load()) / float64(a.newOrderTransactions.Load())
	if avg < 9.5 || avg > 10.5 {
		return newFailResult(
			"average order-lines count %.1f is not between allowed bounds [9.5, 10.5]", avg)
	}

	expectedPct := 100.0 / 11 // uniformly distributed across 11 possible values
	tolerance := 1.0          // allow 1 percent deviation from expected
	for i := 5; i <= 15; i++ {
		freq := a.orderLinesFreq[i]
		pct := 100 * float64(freq) / float64(a.newOrderTransactions.Load())
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
	if a.newOrderTransactions.Load() < minSignificantTransactions {
		return newSkipResult("not enough orders to be statistically significant")
	}

	var remoteOrderLines uint64
	for _, freq := range a.orderLineRemoteWarehouseFreq {
		remoteOrderLines += freq
	}
	remotePct := 100 * float64(remoteOrderLines) / float64(a.totalOrderLines.Load())
	if remotePct < 0.95 || remotePct > 1.05 {
		return newFailResult(
			"remote order-line percent %.1f is not between allowed bounds [0.95, 1.05]", remotePct)
	}

	// In the absence of a more sophisticated distribution check like a
	// chi-squared test, check each warehouse is used as a remote warehouse at
	// least once. We need the number of remote order-lines to be at least 15
	// times the number of warehouses (experimentally determined) to have this
	// expectation.
	//
	var warehouses []int
	if len(a.partitionAffinity) == 0 {
		warehouses = make([]int, a.warehouses)
		for i := 0; i < a.warehouses; i++ {
			warehouses[i] = i
		}
	} else {
		for _, partition := range a.partitionAffinity {
			warehouses = append(warehouses, a.partitioner.partElems[partition]...)
		}
	}

	checkRemoteOrderLines := func(warehouses []int, orderLineRemoteWarehouseFreq map[int]uint64) error {
		for _, warehouse := range warehouses {
			if _, ok := orderLineRemoteWarehouseFreq[warehouse]; !ok {
				return fmt.Errorf("no remote order-lines for warehouse %d", warehouse)
			}
		}
		return nil
	}

	if err := checkRemoteOrderLines(warehouses, a.orderLineRemoteWarehouseFreq); err != nil {
		return newFailResult("%s", err.Error())
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
	if a.paymentTransactions.Load() < minSignificantTransactions {
		return newSkipResult("not enough payments to be statistically significant")
	}

	var remotePayments uint64
	for _, freq := range a.paymentRemoteWarehouseFreq {
		remotePayments += freq
	}
	remotePct := 100 * float64(remotePayments) / float64(a.paymentTransactions.Load())
	if remotePct < 14 || remotePct > 16 {
		return newFailResult(
			"remote payment percent %.1f is not between allowed bounds [14, 16]", remotePct)
	}

	if remotePayments < 15*uint64(a.warehouses/a.partitionFactor) {
		return newSkipResult("insufficient data for remote warehouse distribution check")
	}

	var warehouses []int
	if len(a.partitionAffinity) == 0 {
		warehouses = make([]int, a.warehouses)
		for i := 0; i < a.warehouses; i++ {
			warehouses[i] = i
		}
	} else {
		for _, partition := range a.partitionAffinity {
			warehouses = append(warehouses, a.partitioner.partElems[partition]...)
		}
	}
	checkRemotePayments := func(warehouses []int, paymentRemoteWarehouseFreq map[int]uint64) error {
		for _, warehouse := range warehouses {
			if _, ok := paymentRemoteWarehouseFreq[warehouse]; !ok {
				return fmt.Errorf("no remote payments for warehouses %d", warehouse)
			}
		}
		return nil
	}

	if err := checkRemotePayments(warehouses, a.paymentRemoteWarehouseFreq); err != nil {
		return newFailResult("%s", err.Error())
	}

	return passResult
}

func check92255(a *auditor) auditResult {
	// The number of customer selections by customer last name in the Payment
	// transaction is at least 57% and at most 63% of the number of Payment
	// transactions.
	a.Lock()
	defer a.Unlock()

	if a.paymentTransactions.Load() < minSignificantTransactions {
		return newSkipResult("not enough payments to be statistically significant")
	}
	lastNamePct := 100 * float64(a.paymentsByLastName.Load()) / float64(a.paymentTransactions.Load())
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

	if a.orderStatusTransactions.Load() < minSignificantTransactions {
		return newSkipResult("not enough order status transactions to be statistically significant")
	}
	lastNamePct := 100 * float64(a.orderStatusByLastName.Load()) / float64(a.orderStatusTransactions.Load())
	if lastNamePct < 57 || lastNamePct > 63 {
		return newFailResult(
			"percent of customer selections by last name in order status transactions %.1f is not "+
				"between allowed bounds [57, 63]", lastNamePct)
	}

	return passResult
}
