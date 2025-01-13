// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupinfo

import (
	"bytes"
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/backup/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func makeWriter(
	ctx context.Context,
	dest cloud.ExternalStorage,
	filename string,
	enc *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (io.WriteCloser, error) {
	w, err := dest.Writer(ctx, filename)
	if err != nil {
		return nil, err
	}

	if enc != nil {
		key, err := backupencryption.GetEncryptionKey(ctx, enc, kmsEnv)
		if err != nil {
			return nil, err
		}
		encW, err := storageccl.EncryptingWriter(w, key)
		if err != nil {
			return nil, err
		}
		w = encW
	}
	return w, nil
}

var iterOpts = storage.IterOptions{
	KeyTypes:   storage.IterKeyTypePointsOnly,
	LowerBound: keys.LocalMax,
	UpperBound: keys.MaxKey,
}

type sliceIterator[T any] struct {
	backingSlice []T
	idx          int
}

func newSlicePointerIterator[T any](backing []T) *sliceIterator[T] {
	return &sliceIterator[T]{
		backingSlice: backing,
	}
}

func (s *sliceIterator[T]) Valid() (bool, error) {
	return s.idx < len(s.backingSlice), nil
}

func (s *sliceIterator[T]) Value() *T {
	if s.idx < len(s.backingSlice) {
		return &s.backingSlice[s.idx]
	}

	return nil
}

func (s *sliceIterator[T]) Next() {
	s.idx++
}

func (s *sliceIterator[T]) Close() {
}

type bytesIter struct {
	Iter storage.SimpleMVCCIterator

	prefix      []byte
	useMVCCNext bool
	iterError   error
}

type resultWrapper struct {
	key   storage.MVCCKey
	value []byte
}

func makeBytesIter(
	ctx context.Context,
	store cloud.ExternalStorage,
	path string,
	prefix []byte,
	enc *jobspb.BackupEncryptionOptions,
	useMVCCNext bool,
	kmsEnv cloud.KMSEnv,
) bytesIter {
	var encOpts *kvpb.FileEncryptionOptions
	if enc != nil {
		key, err := backupencryption.GetEncryptionKey(ctx, enc, kmsEnv)
		if err != nil {
			return bytesIter{iterError: err}
		}
		encOpts = &kvpb.FileEncryptionOptions{Key: key}
	}

	iter, err := storageccl.ExternalSSTReader(ctx, []storageccl.StoreFile{{Store: store,
		FilePath: path}}, encOpts, iterOpts)
	if err != nil {
		return bytesIter{iterError: err}
	}

	iter.SeekGE(storage.MakeMVCCMetadataKey(prefix))
	return bytesIter{
		Iter:        iter,
		prefix:      prefix,
		useMVCCNext: useMVCCNext,
	}
}

func (bi *bytesIter) next(resWrapper *resultWrapper) bool {
	if bi.iterError != nil {
		return false
	}

	valid, err := bi.Iter.Valid()
	if err != nil || !valid || !bytes.HasPrefix(bi.Iter.UnsafeKey().Key, bi.prefix) {
		bi.iterError = err
		return false
	}

	key := bi.Iter.UnsafeKey()
	resWrapper.key.Key = key.Key.Clone()
	resWrapper.key.Timestamp = key.Timestamp
	resWrapper.value = resWrapper.value[:0]
	v, err := bi.Iter.UnsafeValue()
	if err != nil {
		bi.close()
		bi.iterError = err
		return false
	}
	resWrapper.value = append(resWrapper.value, v...)

	if bi.useMVCCNext {
		bi.Iter.NextKey()
	} else {
		bi.Iter.Next()
	}
	return true
}

func (bi *bytesIter) err() error {
	return bi.iterError
}

func (bi *bytesIter) close() {
	if bi.Iter != nil {
		bi.Iter.Close()
		bi.Iter = nil
	}
}
