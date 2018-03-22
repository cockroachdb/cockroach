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
	"sync/atomic"

	"github.com/pkg/errors"
)

// auditor maintains statistics about TPC-C input data and runs distribution
// checks, as specified in Clause 9.2 of the TPC-C spec.
type auditor struct {
	newOrderTransactions uint64
	newOrderRollbacks    uint64
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
	if orders < 10000 {
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
