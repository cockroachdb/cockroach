// Copyright 2014 Square, Inc
// Author: Ben Darnell (bdarnell@)

/*
Package storagetest is a test suite for raft.Storage implementations.

The otherwise read-only storage interface is augmented with write
methods for use in the tests.
*/
package storagetest

import (
	"testing"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

type testFunc func(*testing.T, WriteableStorage)

var testFuncs = []testFunc{
	testEmptyLog,
}

// WriteableStorage is a raft.Storage with some additional write methods
// for testing.
type WriteableStorage interface {
	raft.Storage
	Append(entries []raftpb.Entry) error
	SetHardState(st raftpb.HardState) error
}

// RunTests runs the test suite. The setUp and tearDown functions will be called
// at the beginning and end of each test case.
func RunTests(t *testing.T, setUp func(*testing.T) WriteableStorage,
	tearDown func(*testing.T, WriteableStorage)) {
	for _, f := range testFuncs {
		// Use a nested function to create an inline defer scope.
		func() {
			s := setUp(t)
			defer tearDown(t, s)
			f(t, s)
		}()
	}
}

// testEmptyLog calls all the read methods on an empty log and verifies the expected
// state.
func testEmptyLog(t *testing.T, s WriteableStorage) {
	hs, _, err := s.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if !raft.IsEmptyHardState(hs) {
		t.Errorf("expected empty HardState, got %v", hs)
	}

	ents, err := s.Entries(1, 2)
	if err != raft.ErrUnavailable {
		t.Errorf("expected ErrUnavailable, got %v, %v", ents, err)
	}

	term, err := s.Term(0)
	if err != nil {
		t.Fatal(err)
	}
	if term != 0 {
		t.Errorf("expected Term(0) to be 0, got %d", term)
	}

	// FirstIndex starts at 1; LastIndex starts at 0.
	// This is because both indices are inclusive so
	// len == 1 + last - first
	firstIndex, err := s.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if firstIndex != 1 {
		t.Errorf("expected FirstIndex to be 1, got %d", firstIndex)
	}

	lastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if lastIndex != 0 {
		t.Errorf("expected LastIndex to be 0, got %d", lastIndex)
	}
}
