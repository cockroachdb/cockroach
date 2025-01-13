// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupinfo

import (
	"context"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

const (
	// BackupMetadataFilesListPath is the name of the SST file containing the
	// BackupManifest_Files of the backup. This file is always written in
	// conjunction with the `BACKUP_METADATA`.
	BackupMetadataFilesListPath = "filelist.sst"
	sstFilesPrefix              = "file/"
)

// WriteFilesListSST is responsible for constructing and writing the
// filePathInfo to dest. This file contains the `BackupManifest_Files` of the
// backup.
func WriteFilesListSST(
	ctx context.Context,
	dest cloud.ExternalStorage,
	enc *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	manifest *backuppb.BackupManifest,
	filePathInfo string,
) error {
	return writeFilesSST(ctx, manifest, dest, enc, kmsEnv, filePathInfo)
}

func writeFilesSST(
	ctx context.Context,
	m *backuppb.BackupManifest,
	dest cloud.ExternalStorage,
	enc *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	fileInfoPath string,
) error {
	w, err := makeWriter(ctx, dest, fileInfoPath, enc, kmsEnv)
	if err != nil {
		return err
	}
	defer w.Close()
	fileSST := storage.MakeTransportSSTWriter(ctx, dest.Settings(), w)
	defer fileSST.Close()

	// Sort and write all of the files into a single file info SST.
	sort.Slice(m.Files, func(i, j int) bool {
		return FileCmp(m.Files[i], m.Files[j]) < 0
	})

	for i := range m.Files {
		file := m.Files[i]
		b, err := protoutil.Marshal(&file)
		if err != nil {
			return err
		}
		if err := fileSST.PutUnversioned(encodeFileSSTKey(file.Span.Key, file.Path), b); err != nil {
			return err
		}
	}

	err = fileSST.Finish()
	if err != nil {
		return err
	}
	return w.Close()
}

func encodeFileSSTKey(spanStart roachpb.Key, filename string) roachpb.Key {
	buf := make([]byte, 0)
	buf = encoding.EncodeBytesAscending(buf, spanStart)
	return roachpb.Key(encoding.EncodeStringAscending(buf, filename))
}

// FileCmp gives an ordering to two backuppb.BackupManifest_File.
func FileCmp(left backuppb.BackupManifest_File, right backuppb.BackupManifest_File) int {
	if cmp := left.Span.Key.Compare(right.Span.Key); cmp != 0 {
		return cmp
	}

	return strings.Compare(left.Path, right.Path)
}

// NewFileSSTIter creates a new FileIterator to iterate over the storeFile.
// It is the caller's responsibility to Close() the returned iterator.
func NewFileSSTIter(
	ctx context.Context, storeFile storageccl.StoreFile, encOpts *kvpb.FileEncryptionOptions,
) (*FileIterator, error) {
	return newFileSSTIter(ctx, []storageccl.StoreFile{storeFile}, encOpts)
}

// FileIterator is a simple iterator to iterate over backuppb.BackupManifest_File.
type FileIterator struct {
	mergedIterator storage.SimpleMVCCIterator
	err            error
	file           *backuppb.BackupManifest_File
}

func newFileSSTIter(
	ctx context.Context, storeFiles []storageccl.StoreFile, encOpts *kvpb.FileEncryptionOptions,
) (*FileIterator, error) {
	iter, err := storageccl.ExternalSSTReader(ctx, storeFiles, encOpts, iterOpts)
	if err != nil {
		return nil, err
	}
	iter.SeekGE(storage.MVCCKey{})
	fi := &FileIterator{mergedIterator: iter}
	fi.Next()
	return fi, nil
}

// Close closes the iterator.
func (fi *FileIterator) Close() {
	fi.mergedIterator.Close()
}

// Valid indicates whether or not the iterator is pointing to a valid value.
func (fi *FileIterator) Valid() (bool, error) {
	if fi.err != nil {
		return false, fi.err
	}

	return fi.file != nil, nil
}

// Value implements the Iterator interface.
func (fi *FileIterator) Value() *backuppb.BackupManifest_File {
	return fi.file
}

// Next implements the Iterator interface.
func (fi *FileIterator) Next() {
	if fi.err != nil {
		return
	}

	if ok, err := fi.mergedIterator.Valid(); !ok {
		if err != nil {
			fi.err = err
		}
		fi.file = nil
		return
	}

	v, err := fi.mergedIterator.UnsafeValue()
	if err != nil {
		fi.err = err
		return
	}

	file := &backuppb.BackupManifest_File{}
	err = protoutil.Unmarshal(v, file)
	if err != nil {
		fi.err = err
		return
	}

	fi.file = file
	fi.mergedIterator.Next()
}
