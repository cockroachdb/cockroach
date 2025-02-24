// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/pebble/objstorage"
)

// SSTFileAllocator allocates a new file on disk for ingesting. The fileID can
// be used to uniquely identify this file.
type SSTFileAllocator func(ctx context.Context, fileID int) (writable objstorage.Writable, closer func(), err error)

type Writer struct {
	kvData        []byte
	kv            []storage.MVCCKeyValue
	fileID        int
	fileAllocator SSTFileAllocator
	settings      *cluster.Settings
}

var BatchSize = settings.RegisterByteSizeSetting(settings.ApplicationLevel,
	"bulkio.sst_writer.batch_size",
	"Writer in memory batch size",
	128*1024*1024)

var BatchKeyCount = settings.RegisterIntSetting(settings.ApplicationLevel,
	"bulkio.sst_writer.batch_key_count",
	"Writer in memory batch key count",
	1024*10)

// NewUnsortedSSTBatcher creates a new SST batcher, a file allocator must be
// provided which will be used to create new files either locally or remotely
// to write SST data into.
func NewUnsortedSSTBatcher(settings *cluster.Settings, allocator SSTFileAllocator) Writer {
	return Writer{
		kvData:        make([]byte, 0, BatchSize.Get(&settings.SV)),
		kv:            make([]storage.MVCCKeyValue, 0, BatchKeyCount.Get(&settings.SV)),
		fileAllocator: allocator,
		settings:      settings,
	}
}

// flushSST allocates a new file and flushes all KVs. The caller is supposed
// to sort the input.
func (s *Writer) flushSST(ctx context.Context) error {
	// Allocate a new file in storage with the next ID.
	sstFile, closer, err := s.fileAllocator(ctx, s.fileID)
	if err != nil {
		return err
	}
	fileClosed := false
	// If something goes wrong and the file isn't closed,
	// then clean up here.
	defer func() {
		if fileClosed {
			return
		}
		closer()
	}()
	s.fileID += 1
	// Ingest the KVs into the file.
	sstWriter := storage.MakeIngestionSSTWriter(ctx, s.settings, sstFile)
	for _, kv := range s.kv {
		err = sstWriter.PutRawMVCC(kv.Key, kv.Value)
		if err != nil {
			return err
		}
	}
	err = sstWriter.Finish()
	if err != nil {
		return err
	}
	sstWriter.Close()
	fileClosed = true
	return nil
}

// flushBuffer flushes internally cached KV values after sorting them. After
// this call the SST is written into storage.
func (s *Writer) flushBuffer(ctx context.Context) error {
	// If nothing is cached in memory were done.
	if len(s.kv) == 0 {
		return nil
	}
	// Sort the data first before flushing it.
	sort.Slice(s.kv, func(i, j int) bool {
		return s.kv[i].Key.Compare(s.kv[j].Key) < 0
	})
	// Add a SSTable based on this data.
	if err := s.flushSST(ctx); err != nil {
		return err
	}
	// Reset the buffer for re-use.
	s.kv = s.kv[:0]
	s.kvData = s.kvData[:0]
	return nil
}

// AddMVCCKey adds key value in memory, and possibly flushed if the buffer
// is full.
func (s *Writer) AddMVCCKey(ctx context.Context, key storage.MVCCKey, value []byte) error {
	// If we are going to exceed the capacity then flush this SST.
	bytesRequired := len(key.Key) + len(value)
	if bytesRequired+len(s.kvData) > cap(s.kvData) || len(s.kv) >= cap(s.kv) {
		if err := s.flushBuffer(ctx); err != nil {
			return err
		}
	}
	// Append the key / values into your byte buffer.
	keyStartOffset := len(s.kvData)
	s.kvData = append(s.kvData, key.Key...)
	valueStartOffset := len(s.kvData)
	s.kvData = append(s.kvData, value...)
	s.kv = append(s.kv, storage.MVCCKeyValue{
		Key:   storage.MVCCKey{Key: s.kvData[keyStartOffset : keyStartOffset+len(key.Key)], Timestamp: key.Timestamp},
		Value: s.kvData[valueStartOffset : valueStartOffset+len(value)],
	})
	return nil
}

// Close flushes everything left in memory.
func (s *Writer) Close(ctx context.Context) error {
	return s.flushBuffer(ctx)
}
