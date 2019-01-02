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
// permissions and limitations under the License.

/*
Package spanlatch provides a latch management structure for serializing access
to keys and key ranges. Latch acquitions affecting keys or key ranges must wait
on already-acquired latches which overlap their key range to be released.

The evolution of complexity can best be understood as a series of incremental
changes, each in the name of increased lock granularity to reduce contention and
enable more concurrency between requests. The structure can trace its lineage
back to a simple sync.Mutex. From there, the structure evolved through the
following progression:

    * The structure began by enforcing strict mutual exclusion for access to any
      keys. Conceptually, it was a sync.Mutex.
    * Concurrent read-only access to keys and key ranges was permitted. Read and
      writes were serialized with each other, writes were serialized with each other,
      but no ordering was enforced between reads. Conceptually, the structure became
      a sync.RWMutex.
    * The structure became key range-aware and concurrent access to non-overlapping
      key ranges was permitted. Conceptually, the structure became an interval
      tree of sync.RWMutexes.
    * The structure became timestamp-aware and concurrent access of non-causal
      read and write pairs was permitted. The effect of this was that reads no
      longer waited for writes at higher timestamps and writes no longer waited
      for reads at lower timestamps. Conceptually, the structure became an interval
      tree of timestamp-aware sync.RWMutexes.

*/
package spanlatch
