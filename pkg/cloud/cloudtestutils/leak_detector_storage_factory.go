// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloudtestutils

import (
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/closetest"
)

type StorageLeakDetector struct {
	tracker *closetest.AllocationTracker
}

func NewStorageLeakDetector() *StorageLeakDetector {
	return &StorageLeakDetector{closetest.NewTracker("cloud.ExternalStorage")}
}

func (s *StorageLeakDetector) Check() error {
	return s.tracker.CheckLeaks()
}

func (s *StorageLeakDetector) Wrap(storage cloud.ExternalStorage) cloud.ExternalStorage {
	if storage == nil {
		return nil
	}
	return &wrappedStorage{
		storage,
		s.tracker.TrackAllocation(3),
	}
}

// TODO(jeffswenson): wrappedStorage should also wrap the returned readers and
// writers. The wrapped readers and writers should assert that they are closed
// before the wrappedStorage is closed.
type wrappedStorage struct {
	cloud.ExternalStorage
	allocation *closetest.Allocation
}

// Close implements cloud.ExternalStorage.
func (w *wrappedStorage) Close() error {
	w.allocation.Close()
	return w.ExternalStorage.Close()
}
