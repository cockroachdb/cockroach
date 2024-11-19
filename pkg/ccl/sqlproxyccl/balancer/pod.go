// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	// It is possible that connections that came in at the same time may get
	// routed to the same pod since we don't create a new ServerAssignment
	// object while holding entry's lock. This is fine for now. Since we create
	// a new ServerAssignment before dialing the pod, in practice, this window
	// is very small.
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
