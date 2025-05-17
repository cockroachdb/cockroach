package cloudtestutils

import (
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/closetest"
)

type StorageLeakDetector struct {
	tracker *closetest.AllocationTracker
}

func NewStorageLeakDetector() *StorageLeakDetector {
	return &StorageLeakDetector { closetest.NewTracker("cloud.ExternalStorage") }
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

type wrappedStorage struct {
	cloud.ExternalStorage
	allocation *closetest.Allocation
}

// Close implements cloud.ExternalStorage.
func (w *wrappedStorage) Close() error {
	w.allocation.Close()
	return w.ExternalStorage.Close()
}
