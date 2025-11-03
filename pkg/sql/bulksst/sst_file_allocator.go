// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	maxRowSamples = 1024
)

// FileAllocator is used to allocate new files for SSTs ingested via the Writer.
type FileAllocator interface {
	// AddFile creates a new file and stores the URI for tracking.
	AddFile(
		ctx context.Context, fileIndex int, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
	) (objstorage.Writable, func(), string, error)

	// CommitFile records metadata for a successfully written SST file.
	CommitFile(uri string, span roachpb.Span, rowSample roachpb.Key, fileSize uint64)

	// GetFileList gets all the files created by this file allocator.
	GetFileList() *SSTFiles
}

// fileAllocatorBase helps track metadata for created SST files.
type fileAllocatorBase struct {
	fileInfo        SSTFiles
	rowSampleRand   *rand.Rand
	totalRowSamples int
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
	f.recordRowSample(rowSample)
}

func (f *fileAllocatorBase) recordRowSample(rowSample roachpb.Key) {
	if len(rowSample) == 0 {
		return
	}
	f.totalRowSamples++
	if len(f.fileInfo.RowSamples) < maxRowSamples {
		f.fileInfo.RowSamples = append(f.fileInfo.RowSamples, string(rowSample))
		return
	}
	if f.rowSampleRand == nil {
		rng, _ := randutil.NewLockedPseudoRand()
		f.rowSampleRand = rng
	}
	// Reservoir sampling: replace an existing sample with probability maxRowSamples / totalRowSamples.
	idx := f.rowSampleRand.Intn(f.totalRowSamples)
	if idx < maxRowSamples {
		f.fileInfo.RowSamples[idx] = string(rowSample)
	}
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
) (objstorage.Writable, func(), string, error) {
	fileName := fmt.Sprintf("%s_%d", f.baseName, fileIndex)
	writer, err := f.storage.Create(fileName, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, nil, "", err
	}
	remoteWritable := objstorageprovider.NewRemoteWritable(writer)
	return remoteWritable, func() { writer.Close() }, fileName, nil
}

// CommitFile records metadata for a successfully written SST file.
func (f *VFSFileAllocator) CommitFile(
	uri string, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
) {
	f.fileAllocatorBase.addFile(uri, span, rowSample, fileSize)
}

// ExternalFileAllocator allocates external files for SSTs.
type ExternalFileAllocator struct {
	es      cloud.ExternalStorage
	baseURI string
	fileAllocatorBase
}

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
) (objstorage.Writable, func(), string, error) {
	fileName := fmt.Sprintf("%d.sst", fileIndex)
	writer, err := e.es.Writer(ctx, fileName)
	if err != nil {
		return nil, nil, "", err
	}
	remoteWritable := objstorageprovider.NewRemoteWritable(writer)
	return remoteWritable, func() { writer.Close() }, e.baseURI + fileName, nil
}

// CommitFile records metadata for a successfully written SST file.
func (e *ExternalFileAllocator) CommitFile(
	uri string, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
) {
	e.fileAllocatorBase.addFile(uri, span, rowSample, fileSize)
}
