// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupsink

import (
	"bytes"
	"context"
	io "io"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	hlc "github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
	"github.com/kr/pretty"
)

var (
	targetFileSize = settings.RegisterByteSizeSetting(
		settings.ApplicationLevel,
		"bulkio.backup.file_size",
		"target size for individual data files produced during BACKUP",
		128<<20,
		settings.WithPublic)
)

type ExportedSpan struct {
	Metadata       backuppb.BackupManifest_File
	DataSST        []byte
	RevStart       hlc.Timestamp
	CompletedSpans int32
	ResumeKey      roachpb.Key
}

type SSTSinkConf struct {
	ProgCh    chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	Enc       *kvpb.FileEncryptionOptions
	ID        base.SQLInstanceID
	Settings  *settings.Values
	ElideMode execinfrapb.ElidePrefix
}

type FileSSTSink struct {
	dest  cloud.ExternalStorage
	conf  SSTSinkConf
	pacer *admission.Pacer

	sst     storage.SSTWriter
	ctx     context.Context
	cancel  func()
	out     io.WriteCloser
	outName string

	flushedFiles []backuppb.BackupManifest_File
	flushedSize  int64

	// midRow is true if the last batch added to the sink ended mid-row, which can
	// be the case if it ended between column families or revisions of a family.
	midRow bool

	// flushedRevStart is the earliest start time of the export responses
	// written to this sink since the last flush. Resets on each flush.
	flushedRevStart hlc.Timestamp

	// completedSpans contain the number of completed spans since the last
	// flush. This counter resets on each flush.
	completedSpans int32

	// elidedPrefix represents the elided prefix of the last exportSpan/key written to the sink.
	// This resets on each flush.
	elidedPrefix roachpb.Key

	// stats contain statistics about the actions of the FileSSTSink over its
	// entire lifespan.
	stats struct {
		files       int // number of files created.
		flushes     int // number of flushes.
		oooFlushes  int // number of out of order flushes.
		sizeFlushes int // number of flushes due to file exceeding targetFileSize.
		spanGrows   int // number of times a span was extended.
	}
}

// fileSpanByteLimit is the maximum size of a file span that can be extended.
var fileSpanByteLimit int64 = 64 << 20

func MakeFileSSTSink(
	conf SSTSinkConf, dest cloud.ExternalStorage, pacer *admission.Pacer,
) *FileSSTSink {
	return &FileSSTSink{
		conf:  conf,
		dest:  dest,
		pacer: pacer,
	}
}

func (s *FileSSTSink) Write(ctx context.Context, resp ExportedSpan) (roachpb.Key, error) {
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
		if span.Key.Compare(last) < 0 || !bytes.Equal(spanPrefix, s.elidedPrefix) {
			log.VEventf(ctx, 1, "flushing backup file %s of size %d because span %s cannot append before %s",
				s.outName, s.flushedSize, span, last,
			)
			s.stats.oooFlushes++
			if err := s.Flush(ctx); err != nil {
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
	s.elidedPrefix = append(s.elidedPrefix[:0], spanPrefix...)

	log.VEventf(ctx, 2, "writing %s to backup file %s", span, s.outName)

	// To speed up SST reading, surface all the point keys first, flush,
	// then surface all the range keys and flush.
	//
	// TODO(msbutler): investigate using single a single iterator that surfaces
	// all point keys first and then all range keys.
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
		if err := s.Flush(ctx); err != nil {
			return nil, err
		}
	} else {
		log.VEventf(ctx, 3, "continuing to write to backup file %s of size %d", s.outName, s.flushedSize)
	}
	return resp.ResumeKey, err
}

func (s *FileSSTSink) WriteWithNoData(resp ExportedSpan) {
	s.completedSpans += resp.CompletedSpans
	s.midRow = false
}

func (s *FileSSTSink) Close() error {
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

func (s *FileSSTSink) Flush(ctx context.Context) error {
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

	if s.midRow {
		var lastKey roachpb.Key
		if len(s.flushedFiles) > 0 {
			lastKey = s.flushedFiles[len(s.flushedFiles)-1].Span.EndKey
		}
		return errors.AssertionFailedf("backup closed file ending mid-key in %q", lastKey)
	}

	s.stats.flushes++

	if err := s.sst.Finish(); err != nil {
		return err
	}
	if err := s.out.Close(); err != nil {
		log.Warningf(ctx, "failed to close write in FileSSTSink: % #v", pretty.Formatter(err))
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
	case s.conf.ProgCh <- prog:
	}

	s.flushedFiles = nil
	s.elidedPrefix = s.elidedPrefix[:0]
	s.flushedSize = 0
	s.flushedRevStart.Reset()
	s.completedSpans = 0

	return nil
}

func (s *FileSSTSink) open(ctx context.Context) error {
	s.outName = generateUniqueSSTName(s.conf.ID)
	if s.ctx == nil {
		s.ctx, s.cancel = context.WithCancel(ctx)
	}
	w, err := s.dest.Writer(s.ctx, s.outName)
	if err != nil {
		return err
	}
	s.out = w
	if s.conf.Enc != nil {
		e, err := storageccl.EncryptingWriter(w, s.conf.Enc.Key)
		if err != nil {
			return err
		}
		s.out = e
	}
	// TODO(dt): make ExternalStorage.Writer return objstorage.Writable.
	//
	// Value blocks are disabled since such SSTs can be huge (e.g. 750MB in the
	// mixed_version_backup.go roachtest), which can cause OOMs due to value
	// block buffering.
	s.sst = storage.MakeIngestionSSTWriterWithOverrides(
		ctx, s.dest.Settings(), storage.NoopFinishAbortWritable(s.out),
		storage.WithValueBlocksDisabled,
		storage.WithCompressionFromClusterSetting(
			ctx, s.dest.Settings(), storage.CompressionAlgorithmBackupStorage,
		),
	)

	return nil
}

func (s *FileSSTSink) copyPointKeys(ctx context.Context, dataSST []byte) (roachpb.Key, error) {
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}
	iter, err := storage.NewMemSSTIterator(dataSST, false, iterOpts)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var valueBuf []byte

	empty := true
	for iter.SeekGE(storage.MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if err := s.pacer.Pace(ctx); err != nil {
			return nil, err
		}
		if valid, err := iter.Valid(); !valid || err != nil {
			if err != nil {
				return nil, err
			}
			break
		}
		k := iter.UnsafeKey()
		suffix, ok := bytes.CutPrefix(k.Key, s.elidedPrefix)
		if !ok {
			return nil, errors.AssertionFailedf("prefix mismatch %q does not have %q", k.Key, s.elidedPrefix)
		}
		k.Key = suffix

		raw, err := iter.UnsafeValue()
		if err != nil {
			return nil, err
		}

		valueBuf = append(valueBuf[:0], raw...)
		v, err := storage.DecodeValueFromMVCCValue(valueBuf)
		if err != nil {
			return nil, errors.Wrapf(err, "decoding mvcc value %s", k)
		}

		// Checksums include the key, but *exported* keys no longer live at that key
		// once they are exported, and could be restored as some other key, so zero
		// out the checksum.
		v.ClearChecksum()

		// NB: DecodeValueFromMVCCValue does not decode the MVCCValueHeader, which
		// we need to back up. In other words, if we passed v.RawBytes to the put
		// call below, we would lose data. By putting valueBuf, we pass the value
		// header and the cleared checksum.
		//
		// TODO(msbutler): create a ClearChecksum() method that can act on raw value
		// bytes, and remove this hacky code.
		if k.Timestamp.IsEmpty() {
			if err := s.sst.PutUnversioned(k.Key, valueBuf); err != nil {
				return nil, err
			}
		} else {
			if err := s.sst.PutRawMVCC(k, valueBuf); err != nil {
				return nil, err
			}
		}
		empty = false
	}
	if empty {
		return nil, nil
	}
	iter.Prev()
	ok, err := iter.Valid()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.AssertionFailedf("failed to find last key of non-empty file")
	}
	return iter.UnsafeKey().Key.Clone(), nil
}

// copyRangeKeys copies all range keys from the dataSST into the buffer and
// returns the max range key observed.
func (s *FileSSTSink) copyRangeKeys(dataSST []byte) (roachpb.Key, error) {
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypeRangesOnly,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}
	iter, err := storage.NewMemSSTIterator(dataSST, false, iterOpts)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var maxKey roachpb.Key
	for iter.SeekGE(storage.MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return nil, err
		} else if !ok {
			break
		}
		rangeKeys := iter.RangeKeys()
		for _, v := range rangeKeys.Versions {
			rk := rangeKeys.AsRangeKey(v)
			if rk.EndKey.Compare(maxKey) > 0 {
				maxKey = append(maxKey[:0], rk.EndKey...)
			}
			var ok bool
			if rk.StartKey, ok = bytes.CutPrefix(rk.StartKey, s.elidedPrefix); !ok {
				return nil, errors.AssertionFailedf("prefix mismatch %q does not have %q", rk.StartKey, s.elidedPrefix)
			}
			if rk.EndKey, ok = bytes.CutPrefix(rk.EndKey, s.elidedPrefix); !ok {
				return nil, errors.AssertionFailedf("prefix mismatch %q does not have %q", rk.EndKey, s.elidedPrefix)
			}
			if err := s.sst.PutRawMVCCRangeKey(rk, v.Value); err != nil {
				return nil, err
			}
		}
	}
	return maxKey, nil
}
