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

// SetOp constructs an update to the file registry setting the provided
// file path with the given FileEntry.
func SetOp(filename string, entry *FileEntry) *RegistryUpdate {
	return &RegistryUpdate{
		Operation: &RegistryUpdate_Set{
			Set: &RegistryOpSet{
				Filename: filename,
				Entry:    entry,
			},
		},
	}
}

// RemoveOp constructs an update to the file registry removing the entry
// associated with the provided file path if any exists.
func RemoveOp(filenames ...string) *RegistryUpdate {
	return &RegistryUpdate{
		Operation: &RegistryUpdate_Remove{
			Remove: &RegistryOpRemove{
				Filenames: filenames,
			},
		},
	}
}

// RenameOp constructs an update to the file registry moving the entry
// associated with the srcFilename to the dstFilename.
func RenameOp(srcFilename, dstFilename string) *RegistryUpdate {
	return &RegistryUpdate{
		Operation: &RegistryUpdate_Rename{
			Rename: &RegistryOpRename{
				SrcFilename: srcFilename,
				DstFilename: dstFilename,
			},
		},
	}
}

// SnapshotOp constructs an update to the file registry replacing the
// existing state with the provided filename, FileEntry mapping.
func SnapshotOp(files map[string]*FileEntry) *RegistryUpdate {
	return &RegistryUpdate{
		Operation: &RegistryUpdate_Snapshot{
			Snapshot: &RegistryOpSnapshot{
				Files: files,
			},
		},
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

// Apply applies an update to the file registry.
func (r *FileRegistry) Apply(update *RegistryUpdate) {
	if r.Files == nil {
		r.Files = make(map[string]*FileEntry)
	}
	switch x := update.Operation.(type) {
	case *RegistryUpdate_Set:
		if x.Set.Entry == nil {
			delete(r.Files, x.Set.Filename)
		} else {
			r.Files[x.Set.Filename] = x.Set.Entry
		}
	case *RegistryUpdate_Remove:
		for _, filename := range x.Remove.Filenames {
			delete(r.Files, filename)
		}
	case *RegistryUpdate_Rename:
		if e := r.Files[x.Rename.SrcFilename]; e == nil {
			delete(r.Files, x.Rename.DstFilename)
		} else {
			r.Files[x.Rename.DstFilename] = e
		}
		delete(r.Files, x.Rename.SrcFilename)
	case *RegistryUpdate_Snapshot:
		r.Files = x.Snapshot.Files
	default:
		panic("unreachable")
	}
}
