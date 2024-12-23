// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package backupsink

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
)

type FileSSTSinkExportedSpanWriter struct {
	fileSSTSink
}

var _ FileSSTSinkWriter = &FileSSTSinkExportedSpanWriter{}

func NewFileSSTSinkExportedSpanWriter(
	conf SSTSinkConf, dest cloud.ExternalStorage, pacer *admission.Pacer,
) *FileSSTSinkExportedSpanWriter {
	return &FileSSTSinkExportedSpanWriter{
		fileSSTSink: *makeFileSSTSink(conf, dest, pacer),
	}
}

func (s *FileSSTSinkExportedSpanWriter) Write(
	ctx context.Context, resp ExportedSpan,
) (roachpb.Key, error) {
	s.stats.files++

	span := resp.Metadata.Span

	spanPrefix, err := ElidedPrefix(span.Key, s.conf.ElideMode)
	if err != nil {
		return nil, err
	}

	// If this span starts before the last buffered span ended, we need to flush
	// since it overlaps but SSTWriter demands writes in-order.
	if len(s.flushedFiles) > 0 {
		last := s.flushedFiles[len(s.flushedFiles)-1].Span.EndKey
		if span.Key.Compare(last) < 0 || !bytes.Equal(spanPrefix, s.elidePrefix) {
			log.VEventf(ctx, 1, "flushing backup file %s of size %d because span %s cannot append before %s",
				s.outName, s.flushedSize, span, last,
			)
			s.stats.oooFlushes++
			if err := s.flush(ctx); err != nil {
				return nil, err
			}
		}
	}

	// Initialize the writer if needed.
	if s.out == nil {
		if err := s.open(ctx); err != nil {
			return nil, err
		}
	}
	s.elidePrefix = append(s.elidePrefix[:0], spanPrefix...)

	log.VEventf(ctx, 2, "writing %s to backup file %s", span, s.outName)

	// To speed up SST reading, surface all the point keys first, flush,
	// then surface all the range keys and flush.
	//
	// TODO(msbutler): investigate using single a single iterator that surfaces
	// all point keys first and then all range keys
	maxKey, err := s.copyPointKeys(ctx, resp.DataSST)
	if err != nil {
		return nil, err
	}

	maxRange, err := s.copyRangeKeys(resp.DataSST)
	if err != nil {
		return nil, err
	}
	hasRangeKeys := maxRange != nil

	// extend determines if the new span should be added to the last span. This
	// will occur if the previous span ended mid row, or if the new span is a
	// continuation of the previous span (i.e. the new span picks up where the
	// previous one ended and has the same time bounds).
	var extend bool
	if s.midRow {
		extend = true
	} else if len(s.flushedFiles) > 0 {
		last := s.flushedFiles[len(s.flushedFiles)-1]
		extend = last.Span.EndKey.Equal(span.Key) &&
			last.EndTime == resp.Metadata.EndTime &&
			last.StartTime == resp.Metadata.StartTime &&
			last.EntryCounts.DataSize < fileSpanByteLimit
	}

	if len(resp.ResumeKey) > 0 {
		span.EndKey, s.midRow = adjustFileEndKey(span.EndKey, maxKey, maxRange)
		// Update the resume key to be the adjusted end key so that start key of the
		// next file is also clean.
		resp.ResumeKey = span.EndKey
	} else {
		s.midRow = false
	}

	if extend {
		if len(s.flushedFiles) == 0 {
			return nil, errors.AssertionFailedf("cannot extend an empty file sink")
		}
		l := len(s.flushedFiles) - 1
		s.flushedFiles[l].Span.EndKey = span.EndKey
		s.flushedFiles[l].EntryCounts.Add(resp.Metadata.EntryCounts)
		s.flushedFiles[l].ApproximatePhysicalSize += resp.Metadata.ApproximatePhysicalSize
		s.flushedFiles[l].HasRangeKeys = s.flushedFiles[l].HasRangeKeys || hasRangeKeys
		s.stats.spanGrows++
	} else {
		f := resp.Metadata
		f.Path = s.outName
		f.Span.EndKey = span.EndKey
		f.HasRangeKeys = hasRangeKeys
		s.flushedFiles = append(s.flushedFiles, f)
	}

	s.flushedRevStart.Forward(resp.RevStart)
	s.completedSpans += resp.CompletedSpans
	s.flushedSize += int64(len(resp.DataSST))

	// If our accumulated SST is now big enough, and we are positioned at the end
	// of a range flush it.
	if s.flushedSize > targetFileSize.Get(s.conf.Settings) && !s.midRow {
		s.stats.sizeFlushes++
		log.VEventf(ctx, 2, "flushing backup file %s with size %d", s.outName, s.flushedSize)
		if err := s.flush(ctx); err != nil {
			return nil, err
		}
	} else {
		log.VEventf(ctx, 3, "continuing to write to backup file %s of size %d", s.outName, s.flushedSize)
	}
	return resp.ResumeKey, err
}

func (s *FileSSTSinkExportedSpanWriter) WriteWithNoData(resp ExportedSpan) {
	s.completedSpans += resp.CompletedSpans
	s.midRow = false
}

func (s *FileSSTSinkExportedSpanWriter) Flush(ctx context.Context) error {
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
			case s.conf.ProgCh <- prog:
			}
			s.completedSpans = 0
		}
		return nil
	}
	return s.flush(ctx)
}

func (s *FileSSTSinkExportedSpanWriter) Close() error {
	return s.close()
}
