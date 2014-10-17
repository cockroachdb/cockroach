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

/*func (b *BlockableStorage) LoadGroups() <-chan *GroupPersistentState {
	b.wait()
	return b.storage.LoadGroups()
}*/

func (b *BlockableStorage) SetGroupState(groupID uint64,
	state *GroupPersistentState) error {
	b.wait()
	return b.storage.SetGroupState(groupID, state)
}

func (b *BlockableStorage) AppendLogEntries(groupID uint64, entries []*LogEntry) error {
	b.wait()
	return b.storage.AppendLogEntries(groupID, entries)
}

/*func (b *BlockableStorage) TruncateLog(groupID uint64, lastIndex int) error {
	b.wait()
	return b.storage.TruncateLog(groupID, lastIndex)
}

func (b *BlockableStorage) GetLogEntry(groupID uint64, index int) (*LogEntry, error) {
	b.wait()
	return b.storage.GetLogEntry(groupID, index)
}

func (b *BlockableStorage) GetLogEntries(groupID uint64, firstIndex, lastIndex int,
	ch chan<- *LogEntryState) {
	b.wait()
	b.storage.GetLogEntries(groupID, firstIndex, lastIndex, ch)
}
*/
