// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupsink

import (
	"context"
	io "io"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
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

type FileSSTSinkWriter interface {
	io.Closer
	Flush(context.Context) error
}

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

type fileSSTSink struct {
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

	elidePrefix roachpb.Key

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

// fileSpanByteLimit is the maximum size of a file span that can be extended.
const fileSpanByteLimit = 64 << 20

func makeFileSSTSink(
	conf SSTSinkConf, dest cloud.ExternalStorage, pacer *admission.Pacer,
) *fileSSTSink {
	return &fileSSTSink{conf: conf, dest: dest, pacer: pacer}
}

func (s *fileSSTSink) close() error {
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
	if s.out == nil {
		return errors.AssertionFailedf("flush called with no open file")
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
	case s.conf.ProgCh <- prog:
	}

	s.flushedFiles = nil
	s.elidePrefix = s.elidePrefix[:0]
	s.flushedSize = 0
	s.flushedRevStart.Reset()
	s.completedSpans = 0

	return nil
}

func (s *fileSSTSink) open(ctx context.Context) error {
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
