// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

type MergeWriter struct {
	allocator FileAllocator
	files     SSTFiles
	settings  *cluster.Settings

	targetSize int64
	fileCount  int

	scratch []byte

	writer      *storage.SSTWriter
	writerInfo  SSTFileInfo
	fileCleanup func()

	firstKey []byte
	lastKey  []byte
}

func NewInorderWriter(
	allocator FileAllocator, targetSize int64, settings *cluster.Settings,
) *MergeWriter {
	return &MergeWriter{
		allocator:  allocator,
		targetSize: targetSize,
		settings:   settings,
	}
}

func (m *MergeWriter) PutRawMVCCValue(
	ctx context.Context, key storage.MVCCKey, value []byte,
) error {
	if m.writer != nil && m.targetSize <= m.writer.DataSize {
		if err := m.flushCurrentWriter(); err != nil {
			return errors.Wrap(err, "failed to flush SST writer")
		}
	}

	if m.writer == nil {
		if err := m.openNewWriter(ctx); err != nil {
			return errors.Wrap(err, "failed to open new SST writer")
		}
	}

	if len(m.firstKey) == 0 {
		m.firstKey = append(m.firstKey[:0], key.Key...)
	}
	m.lastKey = append(m.lastKey[:0], key.Key...)

	return m.writer.PutRawMVCC(key, value)
}

func (m *MergeWriter) Flush(ctx context.Context) (SSTFiles, error) {
	if m.writer != nil {
		if err := m.flushCurrentWriter(); err != nil {
			return SSTFiles{}, errors.Wrap(err, "failed to flush SST writer")
		}
	}

	files := m.files
	m.files = SSTFiles{}

	return files, nil
}

func (m *MergeWriter) Close() {
	if m.writer != nil {
		m.writer.Close()
		m.writer = nil
	}
	if m.fileCleanup != nil {
		m.fileCleanup()
		m.fileCleanup = nil
	}
}

func (m *MergeWriter) openNewWriter(ctx context.Context) error {
	m.fileCount++
	objectWriter, cleanup, err := m.allocator.AddFile(ctx, m.fileCount, roachpb.Span{}, nil, 0)
	if err != nil {
		return errors.Wrap(err, "failed to open new SST writer")
	}

	sstWriter := storage.MakeIngestionSSTWriter(ctx, m.settings, objectWriter)
	m.writer = &sstWriter
	m.fileCleanup = cleanup

	files := m.allocator.GetFileList()
	m.writerInfo = *files.SST[len(files.SST)-1]

	return nil
}

func (m *MergeWriter) flushCurrentWriter() error {
	if err := m.writer.Finish(); err != nil {
		return errors.Wrap(err, "failed to finish SST writer")
	}
	m.writer.Close()
	m.writer = nil

	m.fileCleanup()
	m.fileCleanup = nil

	// TODO(jeffswenson): can we get the first and last key from the write
	// instead of explicitly tracking it?
	m.files.SST = append(m.files.SST, &SSTFileInfo{
		URI:      m.writerInfo.URI,
		StartKey: roachpb.Key(m.firstKey).Clone(),
		EndKey:   roachpb.Key(m.lastKey).Clone(),
		FileSize: m.writerInfo.FileSize,
	})
	m.firstKey = m.firstKey[:0]
	m.lastKey = m.lastKey[:0]

	return nil
}
