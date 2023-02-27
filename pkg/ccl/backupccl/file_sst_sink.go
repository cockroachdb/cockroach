// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	io "io"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/storage"
	hlc "github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
	"github.com/kr/pretty"
)

type sstQueue []returnedSST

// Len implements the sort.Interface.
func (s sstQueue) Len() int {
	return len(s)
}

// Less implements the sort.Interface.
func (s sstQueue) Less(i, j int) bool {
	cmp := s[i].f.Span.Key.Compare(s[j].f.Span.Key)
	// If two backup files have the same start key, it implies we have
	// paginated in the middle of a key's revisions and those revisions
	// straddle the file boundary. We must ensure that these files are sorted
	// in decreasing order of the key's revision timestamps so that they are
	// added in-order to the underlying SSTWriter.
	if cmp == 0 {
		// Since the start key of the files are equal we compare the EndKeys.
		endKeyCmp := s[i].f.Span.EndKey.Compare(s[j].f.Span.EndKey)
		// If the end keys are also equal, we know that both files only contain
		// revisions of the same key, and so it is safe to sort in descending
		// order of the timestamp of the last revision in each file.
		if endKeyCmp == 0 {
			return s[j].endKeyTS.Less(s[i].endKeyTS)
		}
		// Otherwise, we want to sort the files in increasing order of their
		// EndKeys.
		// This ensures that a queue such as
		// `{[a@3, a@2, b@6] , [a@6, a@5, a@4]}`
		// will have its files in an order that will write to the underlying
		// SSTWriter in-order.
		// `{[a@6, a@5, a@4], [a@3, a@2, b@6]}`
		return endKeyCmp < 0
	}

	return cmp < 0
}

// Swap implements the sort.Interface.
func (s sstQueue) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

var _ sort.Interface = &sstQueue{}

type sstSinkConf struct {
	progCh   chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	enc      *roachpb.FileEncryptionOptions
	id       base.SQLInstanceID
	settings *settings.Values
}

type fileSSTSink struct {
	dest cloud.ExternalStorage
	conf sstSinkConf

	queue sstQueue
	// queueCap is the maximum byte size that the queue can grow to.
	queueCap int64
	// queueSize is the current byte size of the queue.
	queueSize int

	sst     storage.SSTWriter
	ctx     context.Context
	cancel  func()
	out     io.WriteCloser
	outName string

	flushedFiles    []backuppb.BackupManifest_File
	flushedSize     int64
	flushedRevStart hlc.Timestamp
	completedSpans  int32

	stats struct {
		files       int
		flushes     int
		oooFlushes  int
		sizeFlushes int
		spanGrows   int
	}

	memAcc struct {
		ba            *mon.BoundAccount
		reservedBytes int64
	}
}

func makeFileSSTSink(
	ctx context.Context, conf sstSinkConf, dest cloud.ExternalStorage, backupMem *mon.BoundAccount,
) (*fileSSTSink, error) {
	s := &fileSSTSink{conf: conf, dest: dest}
	s.memAcc.ba = backupMem

	// Reserve memory for the file buffer. Incrementally reserve memory in chunks
	// upto a maximum of the `SmallFileBuffer` cluster setting value. If we fail
	// to grow the bound account at any stage, use the buffer size we arrived at
	// prior to the error.
	incrementSize := int64(32 << 20)
	maxSize := backupbase.SmallFileBuffer.Get(s.conf.settings)
	for {
		if s.queueCap >= maxSize {
			break
		}

		if incrementSize > maxSize-s.queueCap {
			incrementSize = maxSize - s.queueCap
		}

		if err := s.memAcc.ba.Grow(ctx, incrementSize); err != nil {
			log.Infof(ctx, "failed to grow file queue by %d bytes, running backup with queue size %d bytes: %+v", incrementSize, s.queueCap, err)
			break
		}
		s.queueCap += incrementSize
	}
	if s.queueCap == 0 {
		return nil, errors.New("failed to reserve memory for fileSSTSink queue")
	}

	s.memAcc.reservedBytes += s.queueCap
	return s, nil
}

func (s *fileSSTSink) Close() error {
	if log.V(1) && s.ctx != nil {
		log.Infof(s.ctx, "backup sst sink recv'd %d files, wrote %d (%d due to size, %d due to re-ordering), %d recv files extended prior span",
			s.stats.files, s.stats.flushes, s.stats.sizeFlushes, s.stats.oooFlushes, s.stats.spanGrows)
	}
	if s.cancel != nil {
		s.cancel()
	}

	// Release the memory reserved for the file buffer.
	s.memAcc.ba.Shrink(s.ctx, s.memAcc.reservedBytes)
	s.memAcc.reservedBytes = 0
	if s.out != nil {
		return s.out.Close()
	}
	return nil
}

// push pushes one returned backup file into the sink. Returned files can arrive
// out of order, but must be written to an underlying file in-order or else a
// new underlying file has to be opened. The queue allows buffering up files and
// sorting them before pushing them to the underlying file to try to avoid this.
// When the queue length or sum of the data sizes in it exceeds thresholds the
// queue is sorted and the first half is flushed.
func (s *fileSSTSink) push(ctx context.Context, resp returnedSST) error {
	s.queue = append(s.queue, resp)
	s.queueSize += len(resp.dataSST)

	if s.queueSize >= int(s.queueCap) {
		sort.Sort(s.queue)
		// Drain the first half.
		drain := len(s.queue) / 2
		if drain < 1 {
			drain = 1
		}
		for i := range s.queue[:drain] {
			if err := s.write(ctx, s.queue[i]); err != nil {
				return err
			}
			s.queueSize -= len(s.queue[i].dataSST)
		}

		// Shift down the remainder of the queue and slice off the tail.
		copy(s.queue, s.queue[drain:])
		s.queue = s.queue[:len(s.queue)-drain]
	}
	return nil
}

func (s *fileSSTSink) flush(ctx context.Context) error {
	sort.Sort(s.queue)
	for i := range s.queue {
		if err := s.write(ctx, s.queue[i]); err != nil {
			return err
		}
	}
	s.queue = nil
	return s.flushFile(ctx)
}

func (s *fileSSTSink) flushFile(ctx context.Context) error {
	if s.out == nil {
		return nil
	}
	s.stats.flushes++

	if err := s.sst.Finish(); err != nil {
		return err
	}
	if err := s.out.Close(); err != nil {
		log.Warningf(ctx, "failed to close write in fileSSTSink: % #v", pretty.Formatter(err))
		return errors.Wrap(err, "writing SST")
	}
	s.outName = ""
	s.out = nil

	progDetails := backuppb.BackupManifest_Progress{
		RevStartTime:   s.flushedRevStart,
		Files:          s.flushedFiles,
		CompletedSpans: s.completedSpans,
	}
	var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	details, err := gogotypes.MarshalAny(&progDetails)
	if err != nil {
		return err
	}
	prog.ProgressDetails = *details
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.conf.progCh <- prog:
	}

	s.flushedFiles = nil
	s.flushedSize = 0
	s.flushedRevStart.Reset()
	s.completedSpans = 0

	return nil
}

func (s *fileSSTSink) open(ctx context.Context) error {
	s.outName = generateUniqueSSTName(s.conf.id)
	if s.ctx == nil {
		s.ctx, s.cancel = context.WithCancel(ctx)
	}
	w, err := s.dest.Writer(s.ctx, s.outName)
	if err != nil {
		return err
	}
	if s.conf.enc != nil {
		var err error
		w, err = storageccl.EncryptingWriter(w, s.conf.enc.Key)
		if err != nil {
			return err
		}
	}
	s.out = w
	s.sst = storage.MakeBackupSSTWriter(ctx, s.dest.Settings(), s.out)

	return nil
}

func (s *fileSSTSink) write(ctx context.Context, resp returnedSST) error {
	s.stats.files++

	span := resp.f.Span

	// If this span starts before the last buffered span ended, we need to flush
	// since it overlaps but SSTWriter demands writes in-order.
	if len(s.flushedFiles) > 0 {
		last := s.flushedFiles[len(s.flushedFiles)-1].Span.EndKey
		if span.Key.Compare(last) < 0 {
			log.VEventf(ctx, 1, "flushing backup file %s of size %d because span %s cannot append before %s",
				s.outName, s.flushedSize, span, last,
			)
			s.stats.oooFlushes++
			if err := s.flushFile(ctx); err != nil {
				return err
			}
		}
	}

	// Initialize the writer if needed.
	if s.out == nil {
		if err := s.open(ctx); err != nil {
			return err
		}
	}

	log.VEventf(ctx, 2, "writing %s to backup file %s", span, s.outName)

	// To speed up SST reading, surface all the point keys first, flush,
	// then surface all the range keys and flush.
	//
	// TODO(msbutler): investigate using single a single iterator that surfaces
	// all point keys first and then all range keys
	if err := s.copyPointKeys(resp.dataSST); err != nil {
		return err
	}
	if err := s.copyRangeKeys(resp.dataSST); err != nil {
		return err
	}

	// If this span extended the last span added -- that is, picked up where it
	// ended and has the same time-bounds -- then we can simply extend that span
	// and add to its entry counts. Otherwise we need to record it separately.
	if l := len(s.flushedFiles) - 1; l > 0 && s.flushedFiles[l].Span.EndKey.Equal(span.Key) &&
		s.flushedFiles[l].EndTime.EqOrdering(resp.f.EndTime) &&
		s.flushedFiles[l].StartTime.EqOrdering(resp.f.StartTime) {
		s.flushedFiles[l].Span.EndKey = span.EndKey
		s.flushedFiles[l].EntryCounts.Add(resp.f.EntryCounts)
		s.stats.spanGrows++
	} else {
		f := resp.f
		f.Path = s.outName
		s.flushedFiles = append(s.flushedFiles, f)
	}
	s.flushedRevStart.Forward(resp.revStart)
	s.completedSpans += resp.completedSpans
	s.flushedSize += int64(len(resp.dataSST))

	// If our accumulated SST is now big enough, and we are positioned at the end
	// of a range flush it.
	if s.flushedSize > targetFileSize.Get(s.conf.settings) && resp.atKeyBoundary {
		s.stats.sizeFlushes++
		log.VEventf(ctx, 2, "flushing backup file %s with size %d", s.outName, s.flushedSize)
		if err := s.flushFile(ctx); err != nil {
			return err
		}
	} else {
		log.VEventf(ctx, 3, "continuing to write to backup file %s of size %d", s.outName, s.flushedSize)
	}
	return nil
}

func (s *fileSSTSink) copyPointKeys(dataSST []byte) error {
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}
	iter, err := storage.NewMemSSTIterator(dataSST, false, iterOpts)
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.SeekGE(storage.MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if valid, err := iter.Valid(); !valid || err != nil {
			if err != nil {
				return err
			}
			break
		}
		k := iter.UnsafeKey()
		if k.Timestamp.IsEmpty() {
			if err := s.sst.PutUnversioned(k.Key, iter.UnsafeValue()); err != nil {
				return err
			}
		} else {
			if err := s.sst.PutRawMVCC(iter.UnsafeKey(), iter.UnsafeValue()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *fileSSTSink) copyRangeKeys(dataSST []byte) error {
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypeRangesOnly,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}
	iter, err := storage.NewMemSSTIterator(dataSST, false, iterOpts)
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.SeekGE(storage.MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		rangeKeys := iter.RangeKeys()
		for _, v := range rangeKeys.Versions {
			if err := s.sst.PutRawMVCCRangeKey(rangeKeys.AsRangeKey(v), v.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

func generateUniqueSSTName(nodeID base.SQLInstanceID) string {
	// The data/ prefix, including a /, is intended to group SSTs in most of the
	// common file/bucket browse UIs.
	return fmt.Sprintf("data/%d.sst", builtins.GenerateUniqueInt(nodeID))
}
