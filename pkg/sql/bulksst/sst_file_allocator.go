// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/vfs"
)

// FileAllocator is used to allocate new files for SSTs ingested via the Writer.
type FileAllocator interface {
	// AddFile creates a new file and stores the URI for tracking.
	AddFile(
		ctx context.Context, fileIndex int,
	) (objstorage.Writable, func(), error)

	// GetFileList gets all the files created by this file allocator.
	GetFileList() []string
}

// VFSFileAllocator allocates local files for storing SSTs.
type VFSFileAllocator struct {
	baseName string
	fileList []string
	storage  vfs.FS
}

// NewVFSFileAllocator creates a new file allocator with baseName and a VFS.
func NewVFSFileAllocator(baseName string, storage vfs.FS) FileAllocator {
	return &VFSFileAllocator{
		baseName: baseName,
		storage:  storage,
	}
}

// AddFile creates a new file and stores the URI for tracking.
func (f *VFSFileAllocator) AddFile(
	ctx context.Context, fileIndex int,
) (objstorage.Writable, func(), error) {
	fileName := fmt.Sprintf("%s_%d", f.baseName, fileIndex)
	writer, err := f.storage.Create(fileName, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, nil, err
	}
	remoteWritable := objstorageprovider.NewRemoteWritable(writer)
	f.fileList = append(f.fileList, fileName)
	return remoteWritable, func() { writer.Close() }, nil
}

// GetFileList gets all the files created by this file allocator.
func (f *VFSFileAllocator) GetFileList() []string {
	return f.fileList
}

// ExternalFileAllocator allocates external files for SSTs.
type ExternalFileAllocator struct {
	es       cloud.ExternalStorage
	baseURI  string
	fileList []string
}

func NewExternalFileAllocator(es cloud.ExternalStorage, baseURI string) FileAllocator {
	return &ExternalFileAllocator{
		es:      es,
		baseURI: baseURI,
	}
}

// AddFile creates a new file and stores the URI for tracking.
func (e *ExternalFileAllocator) AddFile(
	ctx context.Context, fileIndex int,
) (objstorage.Writable, func(), error) {
	fileName := fmt.Sprintf("%s_%d", e.baseURI, fileIndex)
	writer, err := e.es.Writer(ctx, fileName)
	if err != nil {
		return nil, nil, err
	}
	remoteWritable := objstorageprovider.NewRemoteWritable(writer)
	e.fileList = append(e.fileList, fileName)
	return remoteWritable, func() { writer.Close() }, nil
}

// GetFileList gets all the files created by this file allocator.
func (e *ExternalFileAllocator) GetFileList() []string {
	return e.fileList
}
