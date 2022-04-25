// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
)

// selectTenantPod selects a tenant pod from the given list to receive incoming
// traffic. Pods are selected based on the least connections algorithm.
func selectTenantPod(pods []*tenant.Pod, entry *tenantEntry) *tenant.Pod {
	if len(pods) == 0 || entry == nil {
		return nil
	}

	if len(pods) == 1 {
		return pods[0]
	}

	// computeActiveCounts returns a mapping of pod addresses to number of active
	// connections.
	computeActiveCounts := func() map[string]int {
		entry.mu.Lock()
		defer entry.mu.Unlock()

		counts := make(map[string]int)
		for a := range entry.assignments.active {
			counts[a.Addr()]++
		}
		return counts
	}

	// With this structure, it is possible that a ton of connections that came
	// in at the same time get routed to the same pod since we don't create a
	// new ServerAssignment object here. This is fine for now. Once we refactor
	// selectTenantPod to return a ServerAssignment object, we can revisit this
	// issue.
	activeCounts := computeActiveCounts()
	var minPod *tenant.Pod
	minCount := math.MaxInt32
	for _, pod := range pods {
		if activeCounts[pod.Addr] < minCount {
			minCount = activeCounts[pod.Addr]
			minPod = pod
		}
	}
	return minPod
}
