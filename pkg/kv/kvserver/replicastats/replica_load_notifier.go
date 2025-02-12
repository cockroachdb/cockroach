// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicastats

import "github.com/cockroachdb/cockroach/pkg/util/syncutil"

// ReplicaLoadNotifier is used to signal a subscriber that load in the system has passed some threshold.
type ReplicaLoadNotifier struct {
	mu               syncutil.Mutex
	overloaded       bool
	overloadedStores map[string]struct{}
	ch               chan bool
}

// NewReplicaLoadNotifier creates a new ReplicaLoadNotifier.
func NewReplicaLoadNotifier(subscriber chan bool) *ReplicaLoadNotifier {
	return &ReplicaLoadNotifier{
		overloadedStores: make(map[string]struct{}),
		ch:               make(chan bool, 0),
	}
}

// NotifyOverload is called by stores to signal that they are overloaded.
func (r *ReplicaLoadNotifier) NotifyOverload(storeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.overloadedStores[storeID]; !exists {
		r.overloadedStores[storeID] = struct{}{}
		if !r.overloaded {
			r.overloaded = true
			r.ch <- true
		}
	}
}

// NotifyUnderload is called by stores to signal that they are no longer overloaded.
func (r *ReplicaLoadNotifier) NotifyUnderload(storeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.overloadedStores[storeID]; exists {
		delete(r.overloadedStores, storeID)
		if len(r.overloadedStores) == 0 {
			r.overloaded = false
			r.ch <- false
		}
	}
}
