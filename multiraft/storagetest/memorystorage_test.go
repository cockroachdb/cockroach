// Copyright 2014 Square, Inc
// Author: Ben Darnell (bdarnell@)

package storagetest

import (
	"testing"

	"github.com/coreos/etcd/raft"

	// TODO(bdarnell): this is necessary to run this test with cockroach's "make test";
	// remove it when moving to coreos repo.
	_ "github.com/cockroachdb/cockroach/util/log"
)

func setUp(*testing.T) WriteableStorage {
	return raft.NewMemoryStorage()
}

func tearDown(*testing.T, WriteableStorage) {
}

// TestMemoryStorage runs the storage tests on raft.MemoryStorage.
func TestMemoryStorage(t *testing.T) {
	RunTests(t, setUp, tearDown)
}
