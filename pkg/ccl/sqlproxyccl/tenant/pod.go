// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenant

import "math/rand"

// tenantPod is a single instance of a tenant running at the specificed
// address.
type tenantPod struct {
	// Addr is the IP:port that this tenant is serving SQL from.
	Addr string
	// Load is a number [0, 1) that indicates the CPU load being experienced by
	// this tenantPod. A higher load will result in less traffic being routed
	// to this tenantPod.
	Load float32
}

// selectTenantPod selects a tenant pod from the given list to received
// incoming traffic. Pods are weighted by their reported CPU load.
func selectTenantPod(rng *rand.Rand, pods []tenantPod) tenantPod {
	totalLoad := float32(0)
	for _, pod := range pods {
		totalLoad += 1 - pod.Load
	}

	totalLoad *= rng.Float32()

	for _, pod := range pods {
		totalLoad -= 1 - pod.Load
		if totalLoad < 0 {
			return pod
		}
	}

	// This is unreachable provided that Load is [0, 1). We fallback to the
	// final pod in the list to prevent complications if we've received
	// malformed Loads.
	return pods[len(pods)-1]
}
