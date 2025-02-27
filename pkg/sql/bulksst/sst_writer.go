// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/pebble/objstorage"
)

// SSTFileAllocator allocates a new file on disk for ingesting. The fileID can
// be used to uniquely identify this file.
type SSTFileAllocator func(ctx context.Context, fileID int) (writable objstorage.Writable, closer func(), err error)

type Writer struct {
	mu            sync.Mutex
	kvData        []byte
	kv            []storage.MVCCKeyValue
	rowPicker     *rand.Rand
	fileID        int
	fileAllocator FileAllocator
	settings      *cluster.Settings
	onFlush       func(summary kvpb.BulkOpSummary)
	totalSummary  kvpb.BulkOpSummary
	writeTS       hlc.Timestamp
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
func NewUnsortedSSTBatcher(settings *cluster.Settings, allocator FileAllocator) *Writer {
	return &Writer{
		kvData:        make([]byte, 0, BatchSize.Get(&settings.SV)),
		kv:            make([]storage.MVCCKeyValue, 0, BatchKeyCount.Get(&settings.SV)),
		fileAllocator: allocator,
		settings:      settings,
		rowPicker:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SetWriteTS sets the write timestamp for Add.
func (s *Writer) SetWriteTS(ts hlc.Timestamp) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeTS = ts
}

// flushSST allocates a new file and flushes all KVs. The caller is supposed
// to sort the input.
func (s *Writer) flushSST(ctx context.Context, span roachpb.Span, rowSample roachpb.Key) error {
	// Allocate a new file in storage with the next ID.
	sstFile, closer, err := s.fileAllocator.AddFile(ctx, s.fileID, span, rowSample, uint64(len(s.kvData)))
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
	// Pick a random key to sample.
	rowSample := s.kv[s.rowPicker.Intn(len(s.kv))].Key
	start := s.kv[0].Key.Clone()
	end := s.kv[len(s.kv)-1].Key.Next().Clone()
	span := roachpb.Span{Key: start.Key, EndKey: end.Key}
	if err := s.flushSST(ctx, span, rowSample.Key); err != nil {
		return err
	}
	// If the user has set a callback, call it with the summary.
	if s.onFlush != nil {
		summary := s.getCurrentBufferSummary()
		s.totalSummary.Add(summary)
		s.onFlush(summary)
	}
	// Reset the buffer for re-use.
	s.kv = s.kv[:0]
	s.kvData = s.kvData[:0]
	return nil
}

// AddMVCCKey adds key value in memory, and possibly flushed if the buffer
// is full.
func (s *Writer) AddMVCCKey(ctx context.Context, key storage.MVCCKey, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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

// Add implements kvservebase.BulkAdder.
func (s *Writer) Add(ctx context.Context, key roachpb.Key, value []byte) error {
	return s.AddMVCCKey(ctx, storage.MVCCKey{Key: key, Timestamp: s.writeTS}, value)
}

// Flush implements kvservebase.BulkAdder.
func (s *Writer) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushBuffer(ctx)
}

// IsEmpty implements kvservebase.BulkAdder.
func (s *Writer) IsEmpty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.kv) == 0
}

// CurrentBufferFill implements kvservebase.BulkAdder.
func (s *Writer) CurrentBufferFill() float32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return float32(len(s.kvData)) / float32(cap(s.kvData))
}

func (s *Writer) getCurrentBufferSummary() kvpb.BulkOpSummary {
	return kvpb.BulkOpSummary{
		DataSize:    int64(len(s.kvData)),
		SSTDataSize: int64(len(s.kvData)),
		EntryCounts: make(map[uint64]int64),
	}
}

// GetSummary implements kvservebase.BulkAdder.
func (s *Writer) GetSummary() kvpb.BulkOpSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalSummary
}

// SetOnFlush implements kvservebase.BulkAdder.
func (s *Writer) SetOnFlush(f func(summary kvpb.BulkOpSummary)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onFlush = f
}

// CloseWithError flushes everything left in memory.
func (s *Writer) CloseWithError(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushBuffer(ctx)
}

// Close implements kvservebase.BulkAdder.
func (s *Writer) Close(ctx context.Context) {
	if err := s.CloseWithError(ctx); err != nil {
		// TODO(fqazi): Remove panic and find a nicer way to
		//  surface this error.
		panic(err)
	}
}

var _ kvserverbase.BulkAdder = &Writer{}
