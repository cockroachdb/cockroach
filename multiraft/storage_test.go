// Copyright 2014 Square, Inc
// Author: Ben Darnell (bdarnell@)

package multiraft

import "sync"

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

func (b *BlockableStorage) LoadGroups() <-chan *GroupPersistentState {
	b.wait()
	return b.storage.LoadGroups()
}

func (b *BlockableStorage) SetGroupElectionState(groupID GroupID,
	electionState *GroupElectionState) error {
	b.wait()
	return b.storage.SetGroupElectionState(groupID, electionState)
}

func (b *BlockableStorage) AppendLogEntries(groupID GroupID, entries []*LogEntry) error {
	b.wait()
	return b.storage.AppendLogEntries(groupID, entries)
}

func (b *BlockableStorage) TruncateLog(groupID GroupID, lastIndex int) error {
	b.wait()
	return b.storage.TruncateLog(groupID, lastIndex)
}

func (b *BlockableStorage) GetLogEntry(groupID GroupID, index int) (*LogEntry, error) {
	b.wait()
	return b.storage.GetLogEntry(groupID, index)
}

func (b *BlockableStorage) GetLogEntries(groupID GroupID, firstIndex, lastIndex int,
	ch chan<- *LogEntryState) {
	b.wait()
	b.storage.GetLogEntries(groupID, firstIndex, lastIndex, ch)
}
