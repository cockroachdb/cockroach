// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package enginepb

// ProcessBatch processes a batch of updates to the file registry.
func (r *FileRegistry) ProcessBatch(batch *RegistryUpdateBatch) {
	for _, update := range batch.Updates {
		r.ProcessUpdate(update)
	}
}

// ProcessUpdate processes a single update to the file registry.
func (r *FileRegistry) ProcessUpdate(update *RegistryUpdate) {
	if update.Entry == nil {
		delete(r.Files, update.Filename)
	} else {
		if r.Files == nil {
			r.Files = make(map[string]*FileEntry)
		}
		r.Files[update.Filename] = update.Entry
	}
}

// SetVersion updates the version of the file registry. This function will
// panic if the provided version is lower than the current version.
func (r *FileRegistry) SetVersion(version RegistryVersion) {
	if version < r.Version {
		panic("illegal downgrade of file registry version")
	}
	r.Version = version
}

// Empty returns whether a batch is empty.
func (b *RegistryUpdateBatch) Empty() bool {
	return len(b.Updates) == 0
}

// PutEntry adds an update to the batch corresponding to the addition of a new
// file entry to the registry. The entry should not be nil.
func (b *RegistryUpdateBatch) PutEntry(filename string, entry *FileEntry) {
	b.Updates = append(b.Updates, &RegistryUpdate{Filename: filename, Entry: entry})
}

// DeleteEntry adds an update to the batch corresponding to the deletion of a
// file entry from the registry.
func (b *RegistryUpdateBatch) DeleteEntry(filename string) {
	b.Updates = append(b.Updates, &RegistryUpdate{Filename: filename, Entry: nil})
}
