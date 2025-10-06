// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import "time"

// sqlNodeCPUOverloadIndicator is the implementation of cpuOverloadIndicator
// for a single-tenant SQL node in a multi-tenant cluster. This has to rely on
// the periodic load information from the cpu scheduler and will therefore be
// tuned towards indicating overload at higher overload points (otherwise we
// could fluctuate into underloaded territory due to restricting admission,
// and not be work conserving). Such tuning towards more overload, and
// therefore more queueing inside the scheduler, is somewhat acceptable since
// a SQL node is not multi-tenant.
//
// TODO(sumeer): implement.
type sqlNodeCPUOverloadIndicator struct {
}

var _ cpuOverloadIndicator = &sqlNodeCPUOverloadIndicator{}
var _ CPULoadListener = &sqlNodeCPUOverloadIndicator{}

func (sn *sqlNodeCPUOverloadIndicator) CPULoad(
	runnable int, procs int, samplePeriod time.Duration,
) {
}

func (sn *sqlNodeCPUOverloadIndicator) isOverloaded() bool {
	return false
}
