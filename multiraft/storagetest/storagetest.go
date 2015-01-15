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
// state. Note that an empty log need not start from index zero.
func testEmptyLog(t *testing.T, s WriteableStorage) {
	hs, _, err := s.InitialState()
	if err != nil {
		t.Fatal(err)
	}

	// When the log is empty, FirstIndex = LastIndex + 1 (typically 1 and 0).
	// This is because both indices are inclusive so len == 1 + last - first
	firstIndex, err := s.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if firstIndex < 1 {
		t.Errorf("expected FirstIndex to be >= 1, got %d", firstIndex)
	}

	lastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if lastIndex != firstIndex-1 {
		t.Errorf("expected LastIndex to be firstIndex - 1 (%d), got %d", firstIndex-1, lastIndex)
	}

	ents, err := s.Entries(firstIndex, firstIndex+1)
	if err != raft.ErrUnavailable {
		t.Errorf("expected ErrUnavailable, got %v, %v", ents, err)
	}

	term, err := s.Term(firstIndex - 1)
	if err != nil {
		t.Fatal(err)
	}
	if term != hs.Term {
		t.Errorf("expected Term(firstIndex) to be hs.Term (%d), got %d", hs.Term, term)
	}

}
