// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// sstIndexBackfillSink writes index backfill output to sorted SST files backed by
// ExternalStorage. Each flush returns the metadata for newly produced files so
// the coordinator can persist them for downstream merge/ingest stages.
type sstIndexBackfillSink struct {
	writer           *bulksst.Writer
	allocator        bulksst.FileAllocator
	es               cloud.ExternalStorage
	writeTS          hlc.Timestamp
	emittedFileCount int
	onFlush          func(summary kvpb.BulkOpSummary)
	pendingManifests []jobspb.IndexBackfillSSTManifest
}

var _ indexBackfillSink = (*sstIndexBackfillSink)(nil)

func newSSTIndexBackfillSink(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	distributedMergeFilePrefix string,
	writeAsOf hlc.Timestamp,
	processorID int32,
) (indexBackfillSink, error) {
	if distributedMergeFilePrefix == "" {
		return nil, errors.AssertionFailedf("distributed merge sink requires file prefix")
	}
	if flowCtx.Cfg.ExternalStorageFromURI == nil {
		return nil, errors.AssertionFailedf("external storage factory must be configured")
	}

	prefix := fmt.Sprintf("%s/proc-%d/", strings.TrimRight(distributedMergeFilePrefix, "/"), processorID)

	es, err := flowCtx.Cfg.ExternalStorageFromURI(ctx, prefix, username.NodeUserName())
	if err != nil {
		return nil, err
	}

	fileAllocator := bulksst.NewExternalFileAllocator(es, prefix, flowCtx.Cfg.DB.KV().Clock())
	writer := bulksst.NewUnsortedSSTBatcher(flowCtx.Cfg.Settings, fileAllocator)
	writer.SetWriteTS(writeAsOf)

	return &sstIndexBackfillSink{
		writer:    writer,
		allocator: fileAllocator,
		es:        es,
		writeTS:   writeAsOf,
	}, nil
}

// Add implements the indexBackfillSink interface.
func (s *sstIndexBackfillSink) Add(ctx context.Context, key roachpb.Key, value []byte) error {
	return s.writer.Add(ctx, key, value)
}

// Flush implements the indexBackfillSink interface.
func (s *sstIndexBackfillSink) Flush(ctx context.Context) error {
	// Callers must use SetOnFlush before calling Flush in order to properly track
	// the pending manifests.
	if s.onFlush == nil {
		return errors.AssertionFailedf("SetOnFlush must be called before Flush")
	}
	return s.writer.Flush(ctx)
}

// SetOnFlush implements the indexBackfillSink interface.
func (s *sstIndexBackfillSink) SetOnFlush(fn func(summary kvpb.BulkOpSummary)) {
	s.onFlush = fn
	s.writer.SetOnFlush(func(summary kvpb.BulkOpSummary) {
		s.pendingManifests = append(s.pendingManifests, s.collectNewManifests()...)
		if s.onFlush != nil {
			s.onFlush(summary)
		}
	})
}

// ComsumeFlushManifests implements the indexBackfillSink interface.
func (s *sstIndexBackfillSink) ConsumeFlushManifests() []jobspb.IndexBackfillSSTManifest {
	if len(s.pendingManifests) == 0 {
		return nil
	}
	out := s.pendingManifests
	s.pendingManifests = nil
	return out
}

func (s *sstIndexBackfillSink) collectNewManifests() []jobspb.IndexBackfillSSTManifest {
	files := s.allocator.GetFileList()
	if len(files.SST) <= s.emittedFileCount {
		return nil
	}
	outputs := make([]jobspb.IndexBackfillSSTManifest, 0, len(files.SST)-s.emittedFileCount)
	for _, f := range files.SST[s.emittedFileCount:] {
		span := &roachpb.Span{
			Key:    append(roachpb.Key(nil), f.StartKey...),
			EndKey: append(roachpb.Key(nil), f.EndKey...),
		}
		ts := s.writeTS
		output := jobspb.IndexBackfillSSTManifest{
			URI:            f.URI,
			Span:           span,
			FileSize:       f.FileSize,
			RowSample:      append(roachpb.Key(nil), f.RowSample...),
			WriteTimestamp: &ts,
		}
		outputs = append(outputs, output)
	}
	s.emittedFileCount = len(files.SST)
	return outputs
}

func (s *sstIndexBackfillSink) Close(ctx context.Context) {
	if err := s.writer.CloseWithError(ctx); err != nil {
		log.Dev.Warningf(ctx, "closing SST sink writer: %v", err)
	}
	if err := s.es.Close(); err != nil {
		log.Dev.Warningf(ctx, "closing SST sink storage: %v", err)
	}
}
