// Copyright 2014 Square, Inc
// Author: Ben Darnell (bdarnell@)

package multiraft

import (
	"sync"

	"github.com/cockroachdb/cockroach/roachpb"
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

func (b *BlockableStorage) GroupStorage(g roachpb.RangeID, r roachpb.ReplicaID) (WriteableGroupStorage, error) {
	gs, err := b.storage.GroupStorage(g, r)
	return &blockableGroupStorage{b, gs}, err
}

func (b *BlockableStorage) ReplicaDescriptor(groupID roachpb.RangeID, replicaID roachpb.ReplicaID) (roachpb.ReplicaDescriptor, error) {
	return b.storage.ReplicaDescriptor(groupID, replicaID)
}

func (b *BlockableStorage) ReplicaIDForStore(groupID roachpb.RangeID, storeID roachpb.StoreID) (roachpb.ReplicaID, error) {
	return b.storage.ReplicaIDForStore(groupID, storeID)
}

func (b *BlockableStorage) ReplicasFromSnapshot(snap raftpb.Snapshot) ([]roachpb.ReplicaDescriptor, error) {
	return b.storage.ReplicasFromSnapshot(snap)
}

func (b *BlockableStorage) CanApplySnapshot(groupID roachpb.RangeID, snap raftpb.Snapshot) bool {
	return b.storage.CanApplySnapshot(groupID, snap)
}

func (b *BlockableStorage) AppliedIndex(groupID roachpb.RangeID) (uint64, error) {
	return b.storage.AppliedIndex(groupID)
}

func (b *BlockableStorage) RaftLocker() sync.Locker {
	return b.storage.RaftLocker()
}

type blockableGroupStorage struct {
	b *BlockableStorage
	s WriteableGroupStorage
}

func (b *blockableGroupStorage) Append(entries []raftpb.Entry) error {
	b.b.wait()
	return b.s.Append(entries)
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
