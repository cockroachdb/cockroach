// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package base

// TempStorageConfig contains the details that can be specified in the cli
// pertaining to temp storage flags, specifically --temp-dir and
// --max-disk-temp-storage.
type TempStorageConfig struct {
	// InMemory specifies whether the temporary storage will remain
	// in-memory or occupy a temporary subdirectory on-disk.
	InMemory bool
	// ParentDir is the path to the parent directory where the temporary
	// subdirectory for temp storage will be created.
	// If InMemory is set to true, ParentDir is ignored.
	ParentDir string
	// MaxSizeBytes is the space budget allocated for temp storage. This
	// will either be a limit in-memory (if InMemory is true) or on-disk.
	// If the budget is exceeded, no further allocations on temp storage
	// will be permitted unless space is freed from previous allocations.
	MaxSizeBytes int64
	// RecordDir is the path to the directory of the record file which
	// records the path of the temporary directory created on node startup.
	// Previous temporary directories that could not be cleaned up may
	// also be recorded. The record file will be read on startup and these
	// abandoned temporary directories will be cleaned up.
	// If InMemory is set to true, nothing will be written to the record
	// file.
	RecordDir string
}

// MakeTempStorageConfig initializes TempStorage with the invoked default parameters.
func MakeTempStorageConfig(
	inMem bool, maxSizeBytes int64, firstStorePath string,
) TempStorageConfig {
	return TempStorageConfig{
		InMemory: inMem,
		// Initialize the parent directory of the temp storage to the
		// first store's path. This may be updated later on (e.g. by a
		// CLI flag).
		ParentDir:    firstStorePath,
		MaxSizeBytes: maxSizeBytes,
		RecordDir:    firstStorePath,
	}
}

// Reinitialize returns the potentially modified TempStorageConfig that
// aligns with the first store's spec.
// That is, the temp storage is configured to be in-memory if the first store
// is in-memory. If a ParentDir is not specified, it is defaulted to the
// first store's path. Finally, the RecordDir is defaulted to the first store's
// path.
func (ts TempStorageConfig) Reinitialize(firstStore StoreSpec) TempStorageConfig {
	// In-memory setting of temp storage must match that of the first
	// store's.
	ts.InMemory = firstStore.InMemory
	// If no parent directory was set, default it to the first store's path.
	if ts.ParentDir == "" {
		ts.ParentDir = firstStore.Path
	}
	// The record directory is always defaulted to the first store's path
	// such that subsequent node startups can retrieve the records of
	// abandoned temporary subdirectories.
	ts.RecordDir = firstStore.Path
	return ts
}
