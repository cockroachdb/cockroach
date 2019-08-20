// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package spanlatch provides a latch management structure for serializing access
to keys and key ranges. Latch acquisitions affecting keys or key ranges must wait
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
