// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package storage

// WaitForInit waits for any asynchronous processes begun in Start()
// to complete their initialization. In particular, this includes
// gossiping. In some cases this may block until the range GC queue
// has completed its scan. Only for testing.
func (s *Store) WaitForInit() {
	s.initComplete.Wait()
}

// ForceReplicationScan iterates over all ranges and enqueues any that
// need to be replicated. Exposed only for testing.
func (s *Store) ForceReplicationScan() {
	s.mu.Lock()

	for _, r := range s.replicas {
		s.replicateQueue.MaybeAdd(r, s.ctx.Clock.Now())
	}

	s.mu.Unlock()
}

// DisableReplicaGCQueue disables or enables the replica GC queue.
// Exposed only for testing.
func (s *Store) DisableReplicaGCQueue(disabled bool) {
	s.replicaGCQueue.SetDisabled(disabled)
}

// ForceReplicaGCScanAndProcess iterates over all ranges and enqueues any that
// may need to be GC'd. Exposed only for testing.
func (s *Store) ForceReplicaGCScanAndProcess() {
	s.mu.Lock()

	for _, r := range s.replicas {
		s.replicaGCQueue.MaybeAdd(r, s.ctx.Clock.Now())
	}
	s.mu.Unlock()

	s.replicaGCQueue.DrainQueue(s.ctx.Clock)
}

// DisableRaftLogQueue disables or enables the raft log queue.
// Exposed only for testing.
func (s *Store) DisableRaftLogQueue(disabled bool) {
	s.raftLogQueue.SetDisabled(disabled)
}

// ForceRaftLogScanAndProcess iterates over all ranges and enqueues any that
// need their raft logs truncated and then process each of them.
// Exposed only for testing.
func (s *Store) ForceRaftLogScanAndProcess() {
	// NB: This copy is necessary to avoid deadlocks. Without the copy, a
	// concurrent call to (*Store).mu.Lock() will deadlock the system:
	// (*Store).mu.RLock()
	//
	// Start of (*Store).mu.Lock() danger zone
	//
	// raftLogQueue.MaybeAdd
	// raftLogQueue.shouldQueue
	// getTruncatableIndexes
	// (*Store).RaftStatus
	//
	// End of (*Store).mu.Lock() danger zone
	//
	// (*Store).mu.RLock()
	//
	// See http://play.golang.org/p/xTJiAiRfYf
	replicas := make([]*Replica, 0, len(s.replicas))
	s.mu.RLock()
	for _, r := range s.replicas {
		replicas = append(replicas, r)
	}
	s.mu.RUnlock()

	// Add each range to the queue.
	for _, r := range replicas {
		s.raftLogQueue.MaybeAdd(r, s.ctx.Clock.Now())
	}

	s.raftLogQueue.DrainQueue(s.ctx.Clock)
}
