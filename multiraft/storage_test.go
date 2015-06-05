// Copyright 2014 Square, Inc
// Author: Ben Darnell (bdarnell@)

package multiraft

import (
	"sync"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/coreos/etcd/raft/raftpb"
)

// BlockableStorage is an implementation of Storage that can be blocked for testing
// to simulate slow storage devices.
type BlockableStorage struct {
	storage Storage
	mu      sync.Mutex
}

// Assert implementation of the storage interface.
var _ Storage = &BlockableStorage{}

// wait until the storage is unblocked.
func (b *BlockableStorage) wait() {
	b.mu.Lock()
	defer b.mu.Unlock()
}

// Block causes all operations on this storage to block until Unblock is called.
func (b *BlockableStorage) Block() {
	b.mu.Lock()
}

// Unblock undoes the effect of Block() and allows blocked operations to proceed.
func (b *BlockableStorage) Unblock() {
	b.mu.Unlock()
}

func (b *BlockableStorage) GroupStorage(g proto.RaftID) WriteableGroupStorage {
	return &blockableGroupStorage{b, b.storage.GroupStorage(g)}
}

type blockableGroupStorage struct {
	b *BlockableStorage
	s WriteableGroupStorage
}

func (b *blockableGroupStorage) Append(entries []raftpb.Entry) error {
	b.b.wait()
	return b.s.Append(entries)
}

func (b *blockableGroupStorage) ApplySnapshot(snap raftpb.Snapshot) error {
	b.b.wait()
	return b.s.ApplySnapshot(snap)
}

func (b *blockableGroupStorage) SetHardState(st raftpb.HardState) error {
	b.b.wait()
	return b.s.SetHardState(st)
}

func (b *blockableGroupStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	b.b.wait()
	return b.s.InitialState()
}

func (b *blockableGroupStorage) Entries(lo, hi, maxBytes uint64) ([]raftpb.Entry, error) {
	b.b.wait()
	return b.s.Entries(lo, hi, maxBytes)
}

func (b *blockableGroupStorage) Term(i uint64) (uint64, error) {
	b.b.wait()
	return b.s.Term(i)
}

func (b *blockableGroupStorage) LastIndex() (uint64, error) {
	b.b.wait()
	return b.s.LastIndex()
}

func (b *blockableGroupStorage) FirstIndex() (uint64, error) {
	b.b.wait()
	return b.s.FirstIndex()
}

func (b *blockableGroupStorage) Snapshot() (raftpb.Snapshot, error) {
	b.b.wait()
	return b.s.Snapshot()
}
