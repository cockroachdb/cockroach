// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import "github.com/cockroachdb/pebble/vfs"

type BlockingWriteFSForTesting struct {
	vfs.FS
	block chan struct{}
}

type blockingFile struct {
	vfs.File
	fs *BlockingWriteFSForTesting
}

func (fs *BlockingWriteFSForTesting) Block() {
	fs.block = make(chan struct{})
}

func (fs *BlockingWriteFSForTesting) WaitForBlockAndUnblock() {
	fs.block <- struct{}{}
	close(fs.block)
}

func (fs *BlockingWriteFSForTesting) Create(
	name string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	f, err := fs.FS.Create(name, category)
	if err != nil {
		return nil, err
	}
	return blockingFile{File: f, fs: fs}, nil
}

func (f blockingFile) Write(p []byte) (n int, err error) {
	if f.fs.block != nil {
		<-f.fs.block
	}
	return f.File.Write(p)
}
