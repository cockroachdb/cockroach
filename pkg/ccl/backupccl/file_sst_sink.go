// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupccl

import (
	"context"
	"fmt"
	io "io"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/storage"
	hlc "github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
	"github.com/kr/pretty"
)

type sstSinkConf struct {
	progCh   chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	enc      *kvpb.FileEncryptionOptions
	id       base.SQLInstanceID
	settings *settings.Values
}

type fileSSTSink struct {
	dest cloud.ExternalStorage
	conf sstSinkConf

	sst     storage.SSTWriter
	ctx     context.Context
	cancel  func()
	out     io.WriteCloser
	outName string

	flushedFiles []backuppb.BackupManifest_File
	flushedSize  int64

	// flushedRevStart is the earliest start time of the export responses
	// written to this sink since the last flush. Resets on each flush.
	flushedRevStart hlc.Timestamp

	// completedSpans contain the number of completed spans since the last
	// flush. This counter resets on each flush.
	completedSpans int32

	// stats contain statistics about the actions of the fileSSTSink over its
	// entire lifespan.
	stats struct {
		files       int // number of files created.
		flushes     int // number of flushes.
		oooFlushes  int // number of out of order flushes.
		sizeFlushes int // number of flushes due to file exceeding targetFileSize.
		spanGrows   int // number of times a span was extended.
	}
}

func makeFileSSTSink(conf sstSinkConf, dest cloud.ExternalStorage) *fileSSTSink {
	return &fileSSTSink{conf: conf, dest: dest}
}

func (s *fileSSTSink) Close() error {
	if log.V(1) && s.ctx != nil {
		log.Infof(s.ctx, "backup sst sink recv'd %d files, wrote %d (%d due to size, %d due to re-ordering), %d recv files extended prior span",
			s.stats.files, s.stats.flushes, s.stats.sizeFlushes, s.stats.oooFlushes, s.stats.spanGrows)
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.out != nil {
		return s.out.Close()
	}
	s.sst.Close()
	return nil
}

func (s *fileSSTSink) flush(ctx context.Context) error {
	return s.flushFile(ctx)
}

func (s *fileSSTSink) flushFile(ctx context.Context) error {
	if s.out == nil {
		// If the writer was not initialized but the sink has reported completed
		// spans then there were empty ExportRequests that were processed by the
		// owner of this sink. These still need to reported to the coordinator as
		// progress updates.
		if s.completedSpans != 0 {
			progDetails := backuppb.BackupManifest_Progress{
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
			s.completedSpans = 0
		}
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
	wroteSize := s.sst.Meta.Size
	s.outName = ""
	s.out = nil

	for i := range s.flushedFiles {
		s.flushedFiles[i].BackingFileSize = wroteSize
	}

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
	s.out = w
	if s.conf.enc != nil {
		e, err := storageccl.EncryptingWriter(w, s.conf.enc.Key)
		if err != nil {
			return err
		}
		s.out = e
	}
	s.sst = storage.MakeBackupSSTWriter(ctx, s.dest.Settings(), s.out)

	return nil
}

func (s *fileSSTSink) writeWithNoData(resp exportedSpan) {
	s.completedSpans += resp.completedSpans
}

func (s *fileSSTSink) write(ctx context.Context, resp exportedSpan) error {
	s.stats.files++

	span := resp.metadata.Span

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
	if l := len(s.flushedFiles) - 1; l >= 0 && s.flushedFiles[l].Span.EndKey.Equal(span.Key) &&
		s.flushedFiles[l].EndTime.EqOrdering(resp.metadata.EndTime) &&
		s.flushedFiles[l].StartTime.EqOrdering(resp.metadata.StartTime) {
		s.flushedFiles[l].Span.EndKey = span.EndKey
		s.flushedFiles[l].EntryCounts.Add(resp.metadata.EntryCounts)
		s.stats.spanGrows++
	} else {
		f := resp.metadata
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
		v, err := iter.UnsafeValue()
		if err != nil {
			return err
		}
		if k.Timestamp.IsEmpty() {
			if err := s.sst.PutUnversioned(k.Key, v); err != nil {
				return err
			}
		} else {
			if err := s.sst.PutRawMVCC(iter.UnsafeKey(), v); err != nil {
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
	return fmt.Sprintf("data/%d.sst",
		builtins.GenerateUniqueInt(builtins.ProcessUniqueID(nodeID)))
}
