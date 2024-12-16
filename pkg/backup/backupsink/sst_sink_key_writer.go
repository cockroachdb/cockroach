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
// Flush should be called after the last key is written to ensure that the SST
// is written to the destination.
func (s *SSTSinkKeyWriter) WriteKey(ctx context.Context, key storage.MVCCKey, value []byte) error {
	if len(s.flushedFiles) == 0 {
		return errors.AssertionFailedf(
			"no BackupManifest_File to write key to, call Reset before WriteKey",
		)
	}
	if err := s.maybeFlushOOOKey(ctx, key.Key); err != nil {
		return err
	}
	s.setMidRowForPrevKey(key.Key)
	err := s.maybeDoSizeFlushWithNextKey(ctx, key.Key)
	if err != nil {
		return err
	}

	// At this point, because of the Reset invariant, we can make the following
	// assumptions about the new key:
	// - The new key comes after the previous key.
	// - This new key belongs to the span of the last BackupManifest_File in
	//   s.flushedFiles.
	if len(s.flushedFiles) == 0 {
		panic(
			"unexpected missing BackupManifest_File after flushes when writing key",
		)
	}
	elidedKey, prefix, err := elideMVCCKeyPrefix(key, s.conf.ElideMode)
	if err != nil {
		return err
	}
	s.elidedPrefix = append(s.elidedPrefix[:0], prefix...)
	if err := s.sst.PutRawMVCC(elidedKey, value); err != nil {
		return err
	}

	// TODO (kev-cao): Should add some method of tracking `Rows` (and potentially
	// `IndexEntries`).
	keyAsRowCount := roachpb.RowCount{
		DataSize: int64(len(key.Key)) + int64(len(value)),
	}
	lastFile := &s.flushedFiles[len(s.flushedFiles)-1]
	if lastFile.EntryCounts.DataSize >= fileSpanByteLimit {
		// As we are splitting due to the fileSpanByteLimit, the new span will need
		// to contiguously extend the last span. We can use the current key as an
		// exclusive end key for the last file and an inclusive start key for the
		// new file.
		newSpan := roachpb.Span{
			Key:    key.Key,
			EndKey: lastFile.Span.EndKey,
		}
		lastFile.Span.EndKey = newSpan.Key
		if err := s.Reset(ctx, newSpan); err != nil {
			return err
		}
		lastFile = &s.flushedFiles[len(s.flushedFiles)-1]
	}

	lastFile.EntryCounts.Add(keyAsRowCount)
	s.flushedSize += keyAsRowCount.DataSize
	s.prevKey = append(s.prevKey[:0], key.Key...)

	return nil
}

// Reset resets the SSTSinkKeyWriter to write to a new span. It ensures that an
// open BackupManifest_File exists for the new span.
// Any time a new span is being written, Reset MUST be called prior to any
// WriteKey calls.
func (s *SSTSinkKeyWriter) Reset(ctx context.Context, newSpan roachpb.Span) error {
	if err := s.maybeFlushOOOSpans(ctx, newSpan); err != nil {
		return err
	}
	if s.out == nil {
		if err := s.open(ctx); err != nil {
			return err
		}
	}
	var lastFile *backuppb.BackupManifest_File
	if len(s.flushedFiles) > 0 {
		lastFile = &s.flushedFiles[len(s.flushedFiles)-1]
	}

	var isContiguous, isSame bool
	if lastFile != nil {
		s.setMidRowForPrevKey(lastFile.Span.EndKey)
		isSame = lastFile.Span.Equal(newSpan)
		isContiguous = isContiguousSpan(lastFile.Span, newSpan)
		if isSame {
			return nil
		} else if isContiguous &&
			(s.midRow || lastFile.EntryCounts.DataSize < fileSpanByteLimit) {
			s.stats.spanGrows++
			lastFile.Span.EndKey = newSpan.EndKey
			return nil
		}
	}

	// If we are resetting to a new span that is not contiguous with the
	// last span, we need to ensure that we are not mid-row.
	if lastFile != nil && !isContiguous && s.midRow {
		if s.midRow {
			return errors.AssertionFailedf(
				"backup closed file ending mid-key in %v", lastFile.Span.EndKey,
			)
		}
	}
	s.flushedFiles = append(
		s.flushedFiles,
		backuppb.BackupManifest_File{
			Span: newSpan,
			Path: s.outName,
		},
	)
	return nil
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
// endKey or if the two spans do not share the same prefix based on the sink's
// ElideMode. If either of these conditions are met, the current backup file is
// flushed and a new one is created.
func (s *SSTSinkKeyWriter) maybeFlushOOOSpans(ctx context.Context, newSpan roachpb.Span) error {
	if len(s.flushedFiles) == 0 {
		return nil
	}
	lastFile := &s.flushedFiles[len(s.flushedFiles)-1]
	if newSpan.Equal(lastFile.Span) {
		return nil
	}
	samePrefix, err := sameElidedPrefix(newSpan.Key, lastFile.Span.EndKey, s.conf.ElideMode)
	if err != nil {
		return err
	}
	isOverlapping := newSpan.Key.Compare(lastFile.Span.EndKey) < 0
	if !samePrefix || isOverlapping {
		if isOverlapping {
			log.VEventf(
				ctx, 1, "flushing backup file %s of size %d because span %v overlaps with %v",
				s.outName, s.flushedSize, newSpan, lastFile.Span,
			)
		} else {
			log.VEventf(
				ctx, 1,
				"flushing backup file %s of size %d because span %v has a different prefix than %v",
				s.outName, s.flushedSize, newSpan, lastFile.Span,
			)
		}
		s.stats.oooFlushes++
		if err := s.Flush(s.ctx); err != nil {
			return err
		}
		return nil
	}
	return nil
}

// maybeFlushOOOKey checks if the new key is out of order with respect to the
// last key written using WriteKey to the SST file or if they do not share the
// same prefix based on the sink's ElideMode. If either of these are true, the
// file is flushed and a new one is created.
func (s *SSTSinkKeyWriter) maybeFlushOOOKey(ctx context.Context, newKey roachpb.Key) error {
	if s.prevKey == nil {
		return nil
	}
	if len(s.flushedFiles) == 0 {
		panic("unexpected empty flushedFiles despite prevKey being set")
	}
	lastSpan := s.flushedFiles[len(s.flushedFiles)-1].Span
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
		// After a flush, we need to reset the span for the new key.
		return s.Reset(ctx, lastSpan)
	}
	return nil
}

// maybeDoSizeFlushWithNextKey is checks if a flush is needed and info
// about the next key to be written is provided. This is used to determine if
// the next key can act as an exclusive end key for the last BackupManifest_File.
// If a flush is performed, then a new span will be created with Reset to
// contiguously extend the last span. This function should only be called after
// s.midRow has been updated for the last key/span written.
func (s *SSTSinkKeyWriter) maybeDoSizeFlushWithNextKey(
	ctx context.Context, nextKey roachpb.Key,
) error {
	if len(s.flushedFiles) == 0 || !s.shouldDoSizeFlush() {
		log.VEventf(ctx, 3, "continuing to write to backup file %s of size %d", s.outName, s.flushedSize)
		return nil
	}
	// If the span of the next key to be written covers the same span as the last
	// BackupManifest_File, then the next key can act as an exclusive end key for
	// the last file's span. Additionally, the span of the nextKey will be updated
	// so that it contiguously extends with the last span.
	lastFile := &s.flushedFiles[len(s.flushedFiles)-1]
	newSpan := roachpb.Span{
		Key:    nextKey,
		EndKey: lastFile.Span.EndKey,
	}
	lastFile.Span.EndKey = newSpan.Key
	if err := s.doSizeFlush(ctx); err != nil {
		return err
	}
	// After a flush, we need to reset the span for the new key.
	return s.Reset(ctx, newSpan)
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
