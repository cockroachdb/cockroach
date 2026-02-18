// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/iterator"
)

const (
	// gcsFlushInterval is how often the sink flushes buffered entries to GCS.
	gcsFlushInterval = 1 * time.Second
	// gcsFlushThreshold is the number of entries that triggers an immediate flush.
	gcsFlushThreshold = 100
	// gcsCompleteMarker is the name of the sentinel object written when the
	// sink is closed, indicating no more chunks will be written.
	gcsCompleteMarker = "_complete"
	// gcsWriteTimeout bounds individual GCS write operations to prevent
	// a slow or unavailable GCS from blocking task output ingestion
	// indefinitely. Each chunk is at most gcsFlushThreshold entries
	// (a few KB), so 10 seconds is generous.
	gcsWriteTimeout = 10 * time.Second
)

// GCSLogStore stores log entries as chunked JSONL objects in Google Cloud Storage.
// Safe for concurrent use.
//
// Storage layout (where pathPrefix defaults to "tasks"):
//
//	<bucket>/<pathPrefix>/<task-id>/logs/chunk-000000.jsonl
//	<bucket>/<pathPrefix>/<task-id>/logs/chunk-000001.jsonl
//	...
//	<bucket>/<pathPrefix>/<task-id>/logs/_complete
//
// Each chunk file contains newline-delimited JSON LogEntry objects. A new chunk
// is created either every gcsFlushInterval or when the buffer reaches
// gcsFlushThreshold entries, whichever comes first.
type GCSLogStore struct {
	client     *storage.Client
	bucket     string
	pathPrefix string
}

var _ ILogStore = (*GCSLogStore)(nil)

// NewGCSLogStore creates a new GCS-backed log store.
// pathPrefix is prepended to all object paths within the bucket.
func NewGCSLogStore(client *storage.Client, bucket, pathPrefix string) *GCSLogStore {
	return &GCSLogStore{
		client:     client,
		bucket:     bucket,
		pathPrefix: pathPrefix,
	}
}

// taskPrefix returns the GCS object prefix for a given task's logs.
func (s *GCSLogStore) taskPrefix(taskID uuid.UUID) string {
	return fmt.Sprintf("%s/%s/logs/", s.pathPrefix, taskID.String())
}

// NewSink creates a LogSink that writes chunked JSONL to GCS.
func (s *GCSLogStore) NewSink(taskID uuid.UUID) logger.LogSink {
	sink := &gcsSink{
		store:  s,
		taskID: taskID,
		prefix: s.taskPrefix(taskID),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	sink.startFlusher()
	return sink
}

// ReadLogs returns log entries for a task starting from the given offset.
//
// TODO(golgeek): This implementation re-reads all chunks on every call and
// applies the offset after collecting all entries, making each poll O(total
// entries) and the cumulative cost over a stream's lifetime O(N²). This is
// acceptable for the current workload (short-lived tasks producing hundreds
// of log lines), but will become a bottleneck for long-running tasks with
// thousands of entries due to increasing GCS egress, CPU, and memory
// pressure.
//
// To fix this, translate the global entry offset into a (chunk index, intra-
// chunk index) pair so that already-consumed chunks can be skipped entirely.
// This requires either:
//   - storing per-chunk entry counts in object metadata (set at write time), or
//   - using a fixed entries-per-chunk guarantee (currently gcsFlushThreshold
//     provides an upper bound but not an exact count because the timer-based
//     flusher can produce smaller chunks).
//
// With chunk-level skipping, each poll becomes O(new entries) and the
// cumulative cost drops to O(N).
func (s *GCSLogStore) ReadLogs(
	ctx context.Context, taskID uuid.UUID, offset int,
) ([]logger.LogEntry, int, bool, error) {
	prefix := s.taskPrefix(taskID)
	bkt := s.client.Bucket(s.bucket)

	// List all objects under the task prefix.
	it := bkt.Objects(ctx, &storage.Query{Prefix: prefix})
	var chunkNames []string
	done := false

	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, offset, false, errors.Wrap(err, "list log chunks")
		}

		name := strings.TrimPrefix(attrs.Name, prefix)
		if name == gcsCompleteMarker {
			done = true
			continue
		}
		if strings.HasPrefix(name, "chunk-") {
			chunkNames = append(chunkNames, attrs.Name)
		}
	}

	// Sort chunks by name (lexicographic order matches numeric order
	// because of zero-padded sequence numbers).
	sort.Strings(chunkNames)

	// Read all chunks and collect entries.
	var allEntries []logger.LogEntry
	for _, name := range chunkNames {
		reader, err := bkt.Object(name).NewReader(ctx)
		if err != nil {
			return nil, offset, false, errors.Wrapf(err, "read chunk %s", name)
		}

		data, err := io.ReadAll(reader)
		if closeErr := reader.Close(); closeErr != nil {
			return nil, offset, false, errors.Wrapf(closeErr, "close chunk reader %s", name)
		}
		if err != nil {
			return nil, offset, false, errors.Wrapf(err, "read chunk data %s", name)
		}

		dec := json.NewDecoder(bytes.NewReader(data))
		for dec.More() {
			var entry logger.LogEntry
			if err := dec.Decode(&entry); err != nil {
				return nil, offset, false, errors.Wrapf(err, "decode entry in %s", name)
			}
			allEntries = append(allEntries, entry)
		}
	}

	// Apply offset.
	if offset >= len(allEntries) {
		return nil, offset, done, nil
	}
	result := make([]logger.LogEntry, len(allEntries)-offset)
	copy(result, allEntries[offset:])
	return result, len(allEntries), done, nil
}

// DeleteLogs removes all stored log data for the given task.
func (s *GCSLogStore) DeleteLogs(ctx context.Context, taskID uuid.UUID) error {
	prefix := s.taskPrefix(taskID)
	bkt := s.client.Bucket(s.bucket)

	it := bkt.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return errors.Wrap(err, "list objects for deletion")
		}
		if err := bkt.Object(attrs.Name).Delete(ctx); err != nil {
			return errors.Wrapf(err, "delete object %s", attrs.Name)
		}
	}
	return nil
}

// gcsSink buffers log entries and periodically flushes them as JSONL chunks
// to GCS. A background goroutine runs the flush timer.
type gcsSink struct {
	store  *GCSLogStore
	taskID uuid.UUID
	prefix string

	mu       sync.Mutex
	entries  []logger.LogEntry
	chunkSeq int

	stopCh chan struct{}
	doneCh chan struct{}
}

// startFlusher spawns a background goroutine that flushes entries to GCS
// on a fixed interval.
func (s *gcsSink) startFlusher() {
	go func() {
		defer close(s.doneCh)

		ticker := time.NewTicker(gcsFlushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					_ = s.flushLocked()
				}()
			}
		}
	}()
}

// WriteEntry buffers a log entry and flushes if the threshold is reached.
func (s *gcsSink) WriteEntry(entry logger.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = append(s.entries, entry)
	if len(s.entries) >= gcsFlushThreshold {
		return s.flushLocked()
	}
	return nil
}

// flushLocked writes buffered entries as a JSONL chunk to GCS.
// Must be called with s.mu held.
func (s *gcsSink) flushLocked() error {
	if len(s.entries) == 0 {
		return nil
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, entry := range s.entries {
		if err := enc.Encode(entry); err != nil {
			return errors.Wrap(err, "encode log entry")
		}
	}

	objectName := fmt.Sprintf("%schunk-%06d.jsonl", s.prefix, s.chunkSeq)
	ctx, cancel := context.WithTimeout(context.Background(), gcsWriteTimeout)
	defer cancel()
	w := s.store.client.Bucket(s.store.bucket).Object(objectName).NewWriter(ctx)
	w.ContentType = "application/x-ndjson"

	if _, err := w.Write(buf.Bytes()); err != nil {
		_ = w.Close()
		return errors.Wrapf(err, "write chunk %s", objectName)
	}
	if err := w.Close(); err != nil {
		return errors.Wrapf(err, "close chunk writer %s", objectName)
	}

	s.chunkSeq++
	s.entries = s.entries[:0]
	return nil
}

// Close stops the flusher, performs a final flush, and writes a completion marker.
func (s *gcsSink) Close() error {
	// Signal flusher goroutine to stop.
	close(s.stopCh)
	// Wait for flusher to exit so we don't race on s.mu.
	<-s.doneCh

	// Final flush of remaining entries.
	var err error
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		err = s.flushLocked()
	}()
	if err != nil {
		return err
	}

	// Write completion marker.
	markerName := s.prefix + gcsCompleteMarker
	ctx, cancel := context.WithTimeout(context.Background(), gcsWriteTimeout)
	defer cancel()
	w := s.store.client.Bucket(s.store.bucket).Object(markerName).NewWriter(ctx)
	if err := w.Close(); err != nil {
		return errors.Wrapf(err, "write complete marker %s", markerName)
	}

	return nil
}
