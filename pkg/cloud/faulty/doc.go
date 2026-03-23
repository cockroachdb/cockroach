// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package faulty provides utilities for fault injection testing.
//
// # Terminology
//
// Fault handling: A system handles a fault when it can automatically recover
// from the fault transparently (e.g., through automatic retries). Tests of
// fault handling should enable faults metamorphically since the test logic
// should be unaware of transient failures.
//
// Fault tolerance: A system tolerates a fault when the fault may cause an
// operation to fail, but leaves the system in a healthy state with no impact
// on future operations. Testing fault tolerance usually requires its own test
// since operations may fail when non-recoverable errors are injected.
//
// # Usage Pattern
//
// This package focuses on testing fault tolerance. To write a fault tolerance test:
//  1. Enable fault injection with an appropriate probability/configuration.
//  2. Run the operation under test in a test level retry loop.
//  3. Verify the system remains in a consistent state despite failures
//
// See TestBackupRestore_FlakyStorage for an example fault tolerance test.
package faulty
