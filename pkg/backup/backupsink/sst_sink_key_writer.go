// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupsink

import (
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

	// Caching the targetFileSize from the cluster settings to avoid multiple
	// lookups during writes.
	targetFileSize int64
}

func MakeSSTSinkKeyWriter(
	conf SSTSinkConf, dest cloud.ExternalStorage, pacer *admission.Pacer,
) *SSTSinkKeyWriter {
	return &SSTSinkKeyWriter{
		FileSSTSink:    *MakeFileSSTSink(conf, dest, pacer),
		targetFileSize: targetFileSize.Get(conf.Settings),
	}
}

// WriteKey writes a single key to the SST file. The key should be the full key,
// including the prefix. Reset needs to be called prior to WriteKey whenever
// writing keys from a new span. Keys must also be written in order.
//
// Flush should be called after the last key is written to ensure that the SST
// is written to the destination.
func (s *SSTSinkKeyWriter) WriteKey(ctx context.Context, key storage.MVCCKey, value []byte) error {
	if len(s.flushedFiles) == 0 {
		return errors.AssertionFailedf(
			"no BackupManifest_File to write key to, call Reset before WriteKey",
		)
	}
	lastFile := &s.flushedFiles[len(s.flushedFiles)-1]
	if !lastFile.Span.ContainsKey(key.Key) {
		return errors.AssertionFailedf(
			"key %s not in span %s, call Reset on the new span", key.Key, lastFile.Span,
		)
	}
	if s.prevKey != nil && s.prevKey.Compare(key.Key) >= 0 {
		return errors.AssertionFailedf("key %s must be greater than previous key %s", key.Key, s.prevKey)
	}
	// At this point, because of the Reset invariant, we can make the following
	// assumptions about the new key:
	// - The new key comes after the previous key.
	// - This new key belongs to the span of the last BackupManifest_File in
	//   s.flushedFiles.
	lastFile, err := s.maybeDoSizeFlush(ctx, key.Key)
	if err != nil {
		return err
	}

	elidedKey, prefix, err := elideMVCCKeyPrefix(key, s.conf.ElideMode)
	if err != nil {
		return err
	}
	s.elidedPrefix = append(s.elidedPrefix[:0], prefix...)
	// TODO (kev-cao): Should add some method of tracking `Rows` (and potentially
	// `IndexEntries`).
	keyAsRowCount := roachpb.RowCount{
		DataSize: int64(len(key.Key)) + int64(len(value)),
	}

	if err := s.sst.PutRawMVCC(elidedKey, value); err != nil {
		return err
	}

	lastFile.EntryCounts.Add(keyAsRowCount)
	s.flushedSize += keyAsRowCount.DataSize
	s.prevKey = append(s.prevKey[:0], key.Key...)

	return nil
}

// Reset resets the SSTSinkKeyWriter to write to a new span. It ensures that an
// open BackupManifest_File exists for the new span. It will either open a new
// BackupManifest_File, or if the new span contiguously extends the last span,
// it will extend it if it remains under the fileSpanByteLimit. The caller is
// responsible for ensuring that spans that are reset on do not end mid-row.
// Any time a new span is being written, Reset MUST be called prior to any
// WriteKey calls.
func (s *SSTSinkKeyWriter) Reset(ctx context.Context, newSpan roachpb.Span) error {
	log.VEventf(ctx, 2, "resetting sink to span %s", newSpan)
	if err := s.maybeFlushNonExtendableSpans(newSpan); err != nil {
		return err
	}
	if s.out == nil {
		if err := s.open(ctx); err != nil {
			return err
		}
	}
	// At this point, we can assume the new span is either contiguous with the
	// last span or comes after the last span (if it exists).
	var lastFile *backuppb.BackupManifest_File
	if len(s.flushedFiles) > 0 {
		lastFile = &s.flushedFiles[len(s.flushedFiles)-1]
	}

	if lastFile != nil {
		s.setMidRowForPrevKey(lastFile.Span.EndKey)
		if isContiguousSpan(lastFile.Span, newSpan) &&
			(s.midRow || lastFile.EntryCounts.DataSize < fileSpanByteLimit) {
			log.VEventf(ctx, 2, "extending span %s to %s", lastFile.Span, newSpan)
			s.stats.spanGrows++
			lastFile.Span.EndKey = newSpan.EndKey
			return nil
		}
	}

	s.flushedFiles = append(
		s.flushedFiles,
		backuppb.BackupManifest_File{
			Span: newSpan,
			Path: s.outName,
		},
	)
	s.prevKey = nil
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
	return err
}

// maybeFlushNonExtendableSpans checks if the new span's startKey precedes the
// last spans' endKey or if the two spans do not share the same prefix based on
// the sink's ElideMode. If either of these conditions are met, the current
// backup file is flushed and a new one is created.
func (s *SSTSinkKeyWriter) maybeFlushNonExtendableSpans(newSpan roachpb.Span) error {
	if len(s.flushedFiles) == 0 {
		return nil
	}
	lastFile := &s.flushedFiles[len(s.flushedFiles)-1]
	samePrefix, err := sameElidedPrefix(newSpan.Key, lastFile.Span.EndKey, s.conf.ElideMode)
	if err != nil {
		return err
	}
	if !samePrefix || newSpan.Key.Compare(lastFile.Span.EndKey) < 0 {
		log.VEventf(
			s.ctx, 1, "flushing backup file %s of size %d because new span %s is not contiguous with last span %s",
			s.outName, s.flushedSize, newSpan, lastFile.Span,
		)
		s.stats.oooFlushes++
		if err := s.Flush(s.ctx); err != nil {
			return err
		}
		return nil
	}
	return nil
}

// maybeDoSizeFlush checks if either a BackupManifest_File needs to be flushed
// due to reaching the fileSpanByteLimit or if the entire SST needs to be flushed
// due to reaching targetFileSize. If either type of flush is performed, a new
// BackupManifest_File is created for the nextKey, so that is returned.
func (s *SSTSinkKeyWriter) maybeDoSizeFlush(
	ctx context.Context, nextKey roachpb.Key,
) (*backuppb.BackupManifest_File, error) {
	if len(s.flushedFiles) == 0 {
		return nil, errors.AssertionFailedf("maybeDoSizeFlush with empty flushedFiles")
	}
	s.setMidRowForPrevKey(nextKey)
	lastFile := &s.flushedFiles[len(s.flushedFiles)-1]
	if s.midRow {
		return lastFile, nil
	}
	var hardFlush = s.flushedSize >= s.targetFileSize
	if !hardFlush && lastFile.EntryCounts.DataSize < fileSpanByteLimit {
		return lastFile, nil
	}
	newSpan := roachpb.Span{
		Key:    nextKey,
		EndKey: lastFile.Span.EndKey,
	}
	lastFile.Span.EndKey = newSpan.Key
	if hardFlush {
		log.VEventf(ctx, 1, "flushing backup file %s with size %d", s.outName, s.flushedSize)
		s.stats.sizeFlushes++
		if err := s.Flush(ctx); err != nil {
			return nil, err
		}
	}
	// A new span is created for the new BackupManifest_File, so a Reset is needed.
	if err := s.Reset(ctx, newSpan); err != nil {
		return nil, err
	}
	return &s.flushedFiles[len(s.flushedFiles)-1], nil
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

	// If the end key parses as a family key but truncating to the row key does
	// _not_ produce a row key greater than every key in the file, then one of two
	// things has happened: we *did* stop at family key mid-row, so we copied some
	// families after the row key but have more to get in the next file -- so we
	// must *not* flush now -- or the file ended at a range boundary that _looks_
	// like a family key due to a numeric suffix, so the (nonsense) truncated key
	// is now some prefix less than the last copied key. The latter is unfortunate
	// but should be rare given range-sized export requests.
	s.midRow = endRowKey.Compare(s.prevKey) <= 0
}
