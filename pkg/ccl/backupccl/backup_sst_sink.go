// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package backupccl

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
)

var (
	targetFileSize = settings.RegisterByteSizeSetting(
		"bulkio.backup.file_size",
		"target file size",
		128<<20,
	)
	smallFileBuffer = settings.RegisterByteSizeSetting(
		"bulkio.backup.merge_file_buffer_size",
		"size limit used when buffering backup files before merging them",
		16<<20,
		settings.NonNegativeInt,
	)
	smallFileMaxQueueSize = settings.RegisterIntSetting(
		"bulkio.backup.merge_file_max_queue_size",
		"max size of the queue when buffering backup files before merging them",
		24,
		settings.NonNegativeInt,
	)
)

type sstSinkConf struct {
	progCh   chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	enc      *roachpb.FileEncryptionOptions
	id       base.SQLInstanceID
	settings *settings.Values
}

type sstSink struct {
	csvDest cloud.ExternalStorage
	dest    cloud.ExternalStorage
	conf    sstSinkConf

	queue     []returnedSST
	queueSize int

	sst       storage.SSTWriter
	ctx       context.Context
	cancel    func()
	out       io.WriteCloser
	csvWriter *csv.Writer
	outName   string

	flushedFiles    []BackupManifest_File
	flushedSize     int64
	flushedRevStart hlc.Timestamp
	completedSpans  int32

	stats struct {
		files       int
		flushes     int
		oooFlushes  int
		sizeFlushes int
		spanGrows   int

		flushesBecauseOfMaxQueueSize int
		numberOfOtherTableIDs        int
	}
}

func (s *sstSink) Close() error {
	if log.V(1) && s.ctx != nil {
		log.Infof(s.ctx, "backup sst sink recv'd %d files, wrote %d (%d due to size, %d due to re-ordering), %d recv files extended prior span",
			s.stats.files, s.stats.flushes, s.stats.sizeFlushes, s.stats.oooFlushes, s.stats.spanGrows)
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.csvWriter != nil {
		s.csvWriter.Flush()
		if s.csvWriter.Error() != nil {
			return s.csvWriter.Error()
		}
	}
	if s.out != nil {
		return s.closeSinkWriterWithRetry()
	}
	return nil
}

func (s *sstSink) closeSinkWriterWithRetry() error {
	retryOpts := retry.Options{
		MaxBackoff: 1 * time.Second,
		MaxRetries: 5,
	}
	for r := retry.StartWithCtx(s.ctx, retryOpts); r.Next(); {
		if err := s.out.Close(); err != nil {
			if strings.Contains(err.Error(), "got HTTP response code 503") {
				log.Warningf(s.ctx, "retrying Close() on receiving error %v", err)
				continue
			}
			return err
		}
	}
	return nil
}

// push pushes one returned backup file into the sink. Returned files can arrive
// out of order, but must be written to an underlying file in-order or else a
// new underlying file has to be opened. The queue allows buffering up files and
// sorting them before pushing them to the underlying file to try to avoid this.
// When the queue length or sum of the data sizes in it exceeds thresholds the
// queue is sorted and the first half is flushed.
func (s *sstSink) push(ctx context.Context, resp returnedSST) error {
	if s.csvWriter != nil {
		startKey := base64.StdEncoding.EncodeToString(resp.f.Span.Key)
		endKey := base64.StdEncoding.EncodeToString(resp.f.Span.EndKey)
		byteSize := strconv.Itoa(len(resp.sst))
		if err := s.csvWriter.Write([]string{startKey, endKey, byteSize}); err != nil {
			return err
		}
	}

	s.queue = append(s.queue, resp)
	s.queueSize += len(resp.sst)

	maxSinkQueueFiles := int(smallFileMaxQueueSize.Get(s.conf.settings))
	if len(s.queue) >= maxSinkQueueFiles {
		s.stats.flushesBecauseOfMaxQueueSize++
	}

	if len(s.queue) >= maxSinkQueueFiles || s.queueSize >= int(smallFileBuffer.Get(s.conf.settings)) {
		sort.Slice(s.queue, func(i, j int) bool { return s.queue[i].f.Span.Key.Compare(s.queue[j].f.Span.Key) < 0 })

		// Drain the first half.
		drain := len(s.queue) / 2
		if drain < 1 {
			drain = 1
		}
		for i := range s.queue[:drain] {
			if err := s.write(ctx, s.queue[i]); err != nil {
				return err
			}
			s.queueSize -= len(s.queue[i].sst)
		}

		// Shift down the remainder of the queue and slice off the tail.
		copy(s.queue, s.queue[drain:])
		s.queue = s.queue[:len(s.queue)-drain]
	}
	return nil
}

func (s *sstSink) flush(ctx context.Context) error {
	for i := range s.queue {
		if err := s.write(ctx, s.queue[i]); err != nil {
			return err
		}
	}
	s.queue = nil
	return s.flushFile(ctx)
}

func (s *sstSink) flushFile(ctx context.Context) error {
	if s.out == nil {
		return nil
	}
	s.stats.flushes++

	if err := s.sst.Finish(); err != nil {
		return err
	}
	if err := s.closeSinkWriterWithRetry(); err != nil {
		return errors.Wrap(err, "writing SST")
	}
	s.outName = ""
	s.out = nil

	progDetails := BackupManifest_Progress{
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

func (s *sstSink) open(ctx context.Context) error {
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
	s.sst = storage.MakeBackupSSTWriter(s.out)
	return nil
}

func (s *sstSink) write(ctx context.Context, resp returnedSST) error {
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
			_, lastTableID, _, err := keys.DecodeTableIDIndexID(last)
			if err != nil {
				return err
			}
			_, spanTableID, _, err := keys.DecodeTableIDIndexID(span.Key)
			if err != nil {
				return err
			}
			if lastTableID != spanTableID {
				s.stats.numberOfOtherTableIDs++
			}
			s.stats.oooFlushes++
			if err := s.flushFile(ctx); err != nil {
				return err
			}
		}
	}

	// Initialize the writer if needed.
	if s.out == nil && !resp.skipWrite {
		if err := s.open(ctx); err != nil {
			return err
		}
	}

	log.VEventf(ctx, 2, "writing %s to backup file %s", span, s.outName)

	if !resp.skipWrite {
		// Copy SST content.
		sst, err := storage.NewMemSSTIterator(resp.sst, false)
		if err != nil {
			return err
		}
		defer sst.Close()

		sst.SeekGE(storage.MVCCKey{Key: keys.MinKey})
		for {
			if valid, err := sst.Valid(); !valid || err != nil {
				if err != nil {
					return err
				}
				break
			}
			k := sst.UnsafeKey()
			if k.Timestamp.IsEmpty() {
				if err := s.sst.PutUnversioned(k.Key, sst.UnsafeValue()); err != nil {
					return err
				}
			} else {
				if err := s.sst.PutMVCC(sst.UnsafeKey(), sst.UnsafeValue()); err != nil {
					return err
				}
			}
			sst.Next()
		}
	}

	// If this span extended the last span added -- that is, picked up where it
	// ended and has the same time-bounds -- then we can simply extend that span
	// and add to its entry counts. Otherwise we need to record it separately.
	if l := len(s.flushedFiles) - 1; l > 0 && s.flushedFiles[l].Span.EndKey.Equal(span.Key) &&
		s.flushedFiles[l].EndTime.EqOrdering(resp.f.EndTime) &&
		s.flushedFiles[l].StartTime.EqOrdering(resp.f.StartTime) {
		s.flushedFiles[l].Span.EndKey = span.EndKey
		s.flushedFiles[l].EntryCounts.add(resp.f.EntryCounts)
		s.stats.spanGrows++
	} else {
		f := resp.f
		f.Path = s.outName
		s.flushedFiles = append(s.flushedFiles, f)
	}
	s.flushedRevStart.Forward(resp.revStart)
	s.completedSpans += resp.completedSpans
	s.flushedSize += int64(len(resp.sst))

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

func generateUniqueSSTName(nodeID base.SQLInstanceID) string {
	// The data/ prefix, including a /, is intended to group SSTs in most of the
	// common file/bucket browse UIs.
	return fmt.Sprintf("data/%d.sst", builtins.GenerateUniqueInt(nodeID))
}
