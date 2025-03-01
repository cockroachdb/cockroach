// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/vfs"
)

// FileAllocator is used to allocate new files for SSTs ingested via the Writer.
type FileAllocator interface {
	// AddFile creates a new file and stores the URI for tracking.
	//
	// TODO(jeffswenson): rework the file allocator interface so that the caller
	// tracks the files instead of the allocator. This is imporant because the
	// non-sorting flusher doesn't know stats about the file when they are first
	// created.
	AddFile(
		ctx context.Context, fileIndex int, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
	) (objstorage.Writable, func(), error)

	// GetFileList gets all the files created by this file allocator.
	GetFileList() *SSTFiles
}

// fileAllocatorBase helps track metadata for created SST files.
type fileAllocatorBase struct {
	fileInfo SSTFiles
}

// GetFileList gets all the files created by this file allocator.
func (f *fileAllocatorBase) GetFileList() *SSTFiles {
	return &f.fileInfo
}

// addFile helps track metadata for created SST files.
func (f *fileAllocatorBase) addFile(
	uri string, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
) {
	f.fileInfo.SST = append(f.fileInfo.SST, &SSTFileInfo{
		URI:      uri,
		StartKey: span.Key,
		EndKey:   span.EndKey,
		FileSize: fileSize,
	})
	f.fileInfo.TotalSize += fileSize
	// TODO(fqazi): Downsample this becomes too large.
	f.fileInfo.RowSamples = append(f.fileInfo.RowSamples, string(rowSample))
}

// VFSFileAllocator allocates local files for storing SSTs.
type VFSFileAllocator struct {
	fileAllocatorBase
	baseName string
	storage  vfs.FS
}

// NewVFSFileAllocator creates a new file allocator with baseName and a VFS.
func NewVFSFileAllocator(baseName string, storage vfs.FS) FileAllocator {
	return &VFSFileAllocator{
		baseName:          baseName,
		storage:           storage,
		fileAllocatorBase: fileAllocatorBase{},
	}
}

// AddFile creates a new file and stores the URI for tracking.
func (f *VFSFileAllocator) AddFile(
	ctx context.Context, fileIndex int, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
) (objstorage.Writable, func(), error) {
	fileName := fmt.Sprintf("%s_%d", f.baseName, fileIndex)
	writer, err := f.storage.Create(fileName, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, nil, err
	}
	remoteWritable := objstorageprovider.NewRemoteWritable(writer)
	f.fileAllocatorBase.addFile(fileName, span, rowSample, fileSize)
	return remoteWritable, func() { writer.Close() }, nil
}

// ExternalFileAllocator allocates external files for SSTs.
type ExternalFileAllocator struct {
	es      cloud.ExternalStorage
	baseURI string
	fileAllocatorBase
}

// TODO(jeffswenson): rework this so it takes a storage factory and a uri.
func NewExternalFileAllocator(es cloud.ExternalStorage, baseURI string) FileAllocator {
	return &ExternalFileAllocator{
		es:                es,
		baseURI:           baseURI,
		fileAllocatorBase: fileAllocatorBase{},
	}
}

// AddFile creates a new file and stores the URI for tracking.
func (e *ExternalFileAllocator) AddFile(
	ctx context.Context, fileIndex int, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
) (objstorage.Writable, func(), error) {
	fileName := fmt.Sprintf("%d.sst", fileIndex)
	writer, err := e.es.Writer(ctx, fileName)
	if err != nil {
		return nil, nil, err
	}
	remoteWritable := objstorageprovider.NewRemoteWritable(writer)
	e.fileAllocatorBase.addFile(e.baseURI+fileName, span, rowSample, fileSize)
	return remoteWritable, func() { writer.Close() }, nil
}
