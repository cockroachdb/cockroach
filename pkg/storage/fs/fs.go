// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"io"

	"github.com/cockroachdb/pebble/vfs"
)

// CreateWithSync creates a file wrapped with logic to periodically sync
// whenever more than bytesPerSync bytes accumulate. This syncing does not
// provide any persistency guarantees, but can prevent latency spikes.
func CreateWithSync(fs vfs.FS, name string, bytesPerSync int) (vfs.File, error) {
	f, err := fs.Create(name)
	if err != nil {
		return nil, err
	}
	return vfs.NewSyncingFile(f, vfs.SyncingFileOptions{BytesPerSync: bytesPerSync}), nil
}

// WriteFile writes data to a file named by filename.
func WriteFile(fs vfs.FS, filename string, data []byte) error {
	f, err := fs.Create(filename)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

// ReadFile reads data from a file named by filename.
func ReadFile(fs vfs.FS, filename string) ([]byte, error) {
	file, err := fs.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(file)
}
