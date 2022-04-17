// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import "github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"

// selectTenantPod selects a tenant pod from the given list to receive incoming
// traffic. Pods are weighted by their number of connections based on the given
// counts map. rand must be a pseudo random number within the bounds [0, 1). It
// is suggested to use Float32() of a PseudoRand instance that is guarded by a
// mutex.
//
//  rngMu.Lock()
//  rand := rng.Float32()
//  rngMu.Unlock()
//  selectTenantPod(rand, pods, counts)
func selectTenantPod(rand float32, pods []*tenant.Pod, counts map[string]int) *tenant.Pod {
	if len(pods) == 0 || counts == nil {
		return nil
	}

	if len(pods) == 1 {
		return pods[0]
	}

	weights := constructPodWeights(pods, counts)

	w := float32(0)
	for _, pod := range pods {
		w += weights[pod.Addr]
		if rand <= w {
			return pod
		}
	}

	// This is unreachable since constructPodWeights guarantee that the sum of
	// weights would be 1. We will fallback to the final pod in the list to
	// prevent complications if we've received malformed inputs.
	return pods[len(pods)-1]
}

// constructPodWeights constructs a weights map for the input list of pods.
// Weights are assigned proportionally based on the number of connections to
// the given pod. It is guaranteed that the sum of weights returned will be 1
// (or <= 1 due to float32).
//
// Here are some examples:
// - c1: 0, c2: 0, c3: 0 => w1: 1/3, w2: 1/3, w3: 1/3
// - c1: 100, c2: 100 => w1: 1/2, w2: 1/2
// - c1: 1, c2: 4, c3: 8 => w1: 8/11, w2: 2/11, w3: 1/11
// - c1: 0, c2: 10 => w1: 10/11, w2: 1/11
func constructPodWeights(pods []*tenant.Pod, counts map[string]int) map[string]float32 {
	// gcd returns the greatest common denominator of two positive numbers.
	gcd := func(x, y int) int {
		for y != 0 {
			x, y = y, x%y
		}
		return x
	}

	// lcm returns the lowest common multiple of two positive numbers.
	lcm := func(x, y int) int {
		return (x * y) / gcd(x, y)
	}

	// Find the lowest common multiple of all the individual connection counts.
	weights := make(map[string]float32)
	cumulativeLCM := 1
	for _, pod := range pods {
		val := counts[pod.Addr]
		weights[pod.Addr] = float32(val)
		if val > 0 {
			cumulativeLCM = lcm(cumulativeLCM, val)
		}
	}

	// Assign actual weights to pods.
	totalWeights := float32(0)
	for addr, w := range weights {
		if w == 0 {
			weights[addr] = float32(cumulativeLCM)
		} else {
			weights[addr] = float32(cumulativeLCM) / w
		}
		totalWeights += weights[addr]
	}

	// Normalize the weights so that all of them are between [0, 1].
	for addr := range weights {
		weights[addr] /= totalWeights
	}
	return weights
}
