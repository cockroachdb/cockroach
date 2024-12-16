// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupsink

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type SSTSinkKeyWriter struct {
	FileSSTSink
	// prevKey represents the last key written using WriteKey. When writing a new
	// key, this helps determine if the last key written was mid-row. This resets
	// on each flush.
	prevKey roachpb.Key
}

func MakeSSTSinkKeyWriter(
	conf SSTSinkConf, dest cloud.ExternalStorage, pacer *admission.Pacer,
) *SSTSinkKeyWriter {
	return &SSTSinkKeyWriter{
		FileSSTSink: *MakeFileSSTSink(conf, dest, pacer),
	}
}

// WriteKey writes a single key to the SST file. The key should be the full key.
// span refers to the span that the key belongs to. start and end are the time
// bounds of the span being backed up. The writing of each key could
// potentially split up the span of the previously written key (if both keys
// were part of the same span and the span had to be split). As a consequence
// the span that is recorded for the new key is returned, as it may be a subspan
// of the span passed in for the key.
//
// flush should be called after the last key is written to ensure that the SST
// is written to the destination.
func (s *SSTSinkKeyWriter) WriteKey(
	ctx context.Context,
	key storage.MVCCKey,
	value []byte,
	span roachpb.Span,
	startTime hlc.Timestamp,
	endTime hlc.Timestamp,
) (roachpb.Span, error) {
	if key.Key.Compare(span.Key) < 0 || key.Key.Compare(span.EndKey) >= 0 {
		return roachpb.Span{}, errors.AssertionFailedf("key %s outside of span %v", key.Key, span)
	}
	if err := s.maybeFlushOOOSpans(ctx, span, startTime, endTime, true /* skipSameSpan */); err != nil {
		return roachpb.Span{}, err
	}
	if err := s.maybeFlushOOOKey(ctx, key.Key); err != nil {
		return roachpb.Span{}, err
	}
	s.setMidRowForPrevKey(key.Key)

	span, err := s.maybeDoSizeFlushWithNextKey(ctx, key.Key, span, startTime, endTime)
	if err != nil {
		return roachpb.Span{}, err
	}

	// At this point, we can make the following assumptions about the new key and
	// the span it belongs to:
	// - The new key comes after the previous key.
	// - The new span comes after the previous span, although it may not
	// 	 contiguously extend the previous span.
	if s.out == nil {
		if err := s.open(ctx); err != nil {
			return roachpb.Span{}, err
		}
	}

	elidedKey, prefix, err := elideMVCCKeyPrefix(key, s.conf.ElideMode)
	if err != nil {
		return roachpb.Span{}, err
	}
	s.elidedPrefix = append(s.elidedPrefix[:0], prefix...)
	if err := s.sst.PutRawMVCC(elidedKey, value); err != nil {
		return roachpb.Span{}, err
	}

	extend, err := s.shouldExtendLastFile(span, startTime, endTime, true /* extendSameSpan */)
	if err != nil {
		return roachpb.Span{}, err
	}
	// TODO (kev-cao): Should add some method of tracking `Rows` (and potentially
	// `IndexEntries`).
	keyAsRowCount := roachpb.RowCount{
		DataSize: int64(len(key.Key)) + int64(len(value)),
	}

	var lastFile *backuppb.BackupManifest_File
	if len(s.flushedFiles) > 0 {
		lastFile = &s.flushedFiles[len(s.flushedFiles)-1]
	}

	if extend {
		lastFile.Span.EndKey = span.EndKey
		span = lastFile.Span
		lastFile.EntryCounts.Add(keyAsRowCount)
		s.stats.spanGrows++
	} else {
		if lastFile != nil && sameAsFileSpan(span, startTime, endTime, lastFile) {
			// The new span covers the same span as the last BackupManifest_File, yet
			// we are not extending. That means we are splitting due to the
			// fileSpanByteLimit. As such, the current key can serve as an exclusive
			// end key for the last file and an inclusive start key for the new file.
			lastFile.Span.EndKey = key.Key
			span.Key = key.Key
		}
		f := backuppb.BackupManifest_File{
			Span: span,
			EntryCounts: roachpb.RowCount{
				DataSize: int64(len(key.Key)) + int64(len(value)),
			},
			StartTime: startTime,
			EndTime:   endTime,
			Path:      s.outName,
		}
		s.flushedFiles = append(s.flushedFiles, f)
		s.completedSpans++
	}

	s.flushedSize += keyAsRowCount.DataSize
	s.prevKey = append(s.prevKey[:0], key.Key...)

	return span, nil
}

func (s *SSTSinkKeyWriter) Flush(ctx context.Context) error {
	if len(s.flushedFiles) > 0 {
		lastFile := s.flushedFiles[len(s.flushedFiles)-1]
		// If WriteKey was used, it is possible that the last written key was
		// mid-row, but because no keys were written after, s.midRow was not updated.
		// To ensure this, we update midRow using the end of the span that the last
		// key belonged to.
		s.setMidRowForPrevKey(lastFile.Span.EndKey)
	}
	err := s.FileSSTSink.Flush(ctx)
	s.prevKey = nil
	return err
}

// TODO (kev-cao): Most of these `maybeFlushX` functions can be generalized to
// other SSTSink implementations. When we get around to refactoring out the
// other implementation, we should consider moving these functions into the base
// FileSSTSink implementation.

// maybeFlushOOOSpans checks if the new span's startKey precedes the last spans'
// endKey. If it does, the files are flushed as the underlying SST writer
// expects keys in order. If `skipSameSpan` is true, the files are not flushed
// if the new span is the same as the last span with the same time bounds.
func (s *SSTSinkKeyWriter) maybeFlushOOOSpans(
	ctx context.Context, newSpan roachpb.Span, startTime, endTime hlc.Timestamp, skipSameSpan bool,
) error {
	if len(s.flushedFiles) == 0 {
		return nil
	}
	lastFile := &s.flushedFiles[len(s.flushedFiles)-1]
	if skipSameSpan && sameAsFileSpan(newSpan, startTime, endTime, lastFile) {
		return nil
	}
	if newSpan.Key.Compare(lastFile.Span.EndKey) < 0 {
		log.VEventf(
			ctx, 1, "flushing backup file %s of size %d because span %v overlaps with %v",
			s.outName, s.flushedSize, newSpan, lastFile.Span,
		)
		s.stats.oooFlushes++
		if err := s.Flush(s.ctx); err != nil {
			return err
		}
	}
	return nil
}

// maybeFlushOOOKey checks if the new key is out of order with respect to the
// last key written using WriteKey to the SST file. If it is, the file is
// flushed and a new one is created.
func (s *SSTSinkKeyWriter) maybeFlushOOOKey(ctx context.Context, newKey roachpb.Key) error {
	if s.prevKey == nil {
		return nil
	}
	keyPrefix, err := ElidedPrefix(newKey, s.conf.ElideMode)
	if err != nil {
		return err
	}
	if newKey.Compare(s.prevKey) < 0 || !bytes.Equal(keyPrefix, s.elidedPrefix) {
		log.VEventf(
			ctx, 1, "flushing backup file %s of size %d because key %s cannot append before %s",
			s.outName, s.flushedSize, newKey, s.prevKey,
		)
		s.stats.oooFlushes++
		if err := s.Flush(s.ctx); err != nil {
			return err
		}
	}
	return nil
}

// maybeDoSizeFlushWithNextKey is checks if a flush is needed and info
// about the next key to be written is provided. This is used to determine if
// the next key can act as an exclusive end key for the last BackupManifest_File.
// The updated span of the next key is returned, as it may be set to contiguously
// extend the last span. This function should only be called after s.midRow has
// been updated for the last key/span written.
func (s *SSTSinkKeyWriter) maybeDoSizeFlushWithNextKey(
	ctx context.Context, nextKey roachpb.Key, nextSpan roachpb.Span, startTime, endTime hlc.Timestamp,
) (roachpb.Span, error) {
	if len(s.flushedFiles) == 0 || !s.shouldDoSizeFlush() {
		log.VEventf(ctx, 3, "continuing to write to backup file %s of size %d", s.outName, s.flushedSize)
		return nextSpan, nil
	}
	// If the span of the next key to be written covers the same span as the last
	// BackupManifest_File, then the next key can act as an exclusive end key for
	// the last file's span. Additionally, the span of the nextKey will be updated
	// so that it contiguously extends with the last span.
	lastFile := &s.flushedFiles[len(s.flushedFiles)-1]
	if sameAsFileSpan(nextSpan, startTime, endTime, lastFile) {
		lastFile.Span.EndKey = nextKey
		nextSpan.Key = nextKey
	}
	return nextSpan, s.doSizeFlush(ctx)
}

// shouldDoSizeFlush returns true if the current backup file exceeds the
// targetFileSize and we are currently not mid-row. Should only be called after
// s.midRow has been updated for the last key/span written.
func (s *SSTSinkKeyWriter) shouldDoSizeFlush() bool {
	return s.flushedSize > targetFileSize.Get(s.conf.Settings) && !s.midRow
}

// doSizeFlush flushes the buffered files with size exceeded as the reason.
func (s *SSTSinkKeyWriter) doSizeFlush(ctx context.Context) error {
	s.stats.sizeFlushes++
	log.VEventf(ctx, 2, "flushing backup file %s with size %d", s.outName, s.flushedSize)
	return s.Flush(ctx)
}

// shouldExtendLastFile returns true if the new span can be added to the last
// span in the flushedFiles slice. If the last written key was mid-row, the new
// span is always considered to extend the last span. If the new span is a
// contiguous extension of the last span, it is also considered to extend the
// last span. extendSameSpan determines if an identical span should be considered
// an extension of the last span. This should only be called after s.midRow has
// been updated for the last key/span written.
func (s *SSTSinkKeyWriter) shouldExtendLastFile(
	span roachpb.Span, startTime, endTime hlc.Timestamp, extendSameSpan bool,
) (bool, error) {
	if len(s.flushedFiles) == 0 {
		return false, nil
	}
	lastFile := &s.flushedFiles[len(s.flushedFiles)-1]
	canExtendFile := extendsFileSpan(span, startTime, endTime, lastFile) ||
		(extendSameSpan && sameAsFileSpan(span, startTime, endTime, lastFile))
	if s.midRow {
		if !canExtendFile {
			return false, errors.AssertionFailedf(
				"last key was mid-row, but new span %v does not extend last span %v", span, lastFile.Span,
			)
		}
		return true, nil
	}
	return canExtendFile && lastFile.EntryCounts.DataSize < fileSpanByteLimit, nil
}

// setMidRowForPrevKey checks if the last key written using WriteKey was mid-row
// by using the next key to be written or e end key of its span and sets s.midRow
// accordingly. If no key was previously written using WriteKey, this is a no-op.
func (s *SSTSinkKeyWriter) setMidRowForPrevKey(endKey roachpb.Key) {
	if s.prevKey == nil {
		return
	}
	endRowKey, err := keys.EnsureSafeSplitKey(endKey)
	if err != nil {
		// If the key does not parse a family key, it must be from reaching the end
		// of a range and be a range boundary.
		return
	}
	s.midRow = s.prevKey.Compare(endRowKey) > 0
}
