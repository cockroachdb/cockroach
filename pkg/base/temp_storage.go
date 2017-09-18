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

// TempStorage contains the details that can be specified in the cli pertaining
// to temp storage flags, specifically --temp-dir and --max-disk-temp-storage.
type TempStorage struct {
	InMemory bool
	// ParentDir contains the path to the parent directory which will
	// be used to generate the temporary directory containing the
	// temporary storage.
	ParentDir string
	// MaxSizeBytes is the limit on the disk capacity to be used for temp
	// storage.
	MaxSizeBytes int64
	// RecordPath contains the path to the record file which records
	// all the temporary directories created for a given node.
	RecordPath string
}

// NewTempStorage initializes TempStorage with the invoked default parameters.
func NewTempStorage(maxSizeBytes, maxSizeBytesInMemStore int64, firstStore StoreSpec) TempStorage {
	if firstStore.InMemory {
		return TempStorage{InMemory: true, MaxSizeBytes: maxSizeBytesInMemStore}
	}
	return TempStorage{
		ParentDir:    firstStore.Path,
		MaxSizeBytes: maxSizeBytes,
		RecordPath:   firstStore.Path,
	}
}

// Reinitialize resets the settings under TempStorage depending on the
// firstStore StoreSpec (which may have been re-initialized from cli flags).
// If firstStore is in-memory, then TempStorage is re-initialized to in-memory
// as well.
// If ParentDir is initially unspecified, then the firstStore's path is used
// as the parent.
// RecordPath is also set to the first store's path.
func (ts TempStorage) Reinitialize(firstStore StoreSpec) (TempStorage, error) {
	if firstStore.InMemory {
		ts.InMemory = true
		return ts, nil
	}

	// No parent directory specified in current TempStorage ts:
	// default parent directory as firstStore's path.
	if ts.ParentDir == "" {
		ts.ParentDir = firstStore.Path
	}

	ts.RecordPath = firstStore.Path
	return ts, nil
}
