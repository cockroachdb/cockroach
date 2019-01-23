// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

// cloudStorageFormatBucket formats times as YYYYMMDDHHMMSSNNNNNNNNN.
func cloudStorageFormatBucket(t time.Time) string {
	// TODO(dan): Instead do the minimal thing necessary to differentiate times
	// truncated to some bucket size.
	const f = `20060102150405`
	return fmt.Sprintf(`%s%09d`, t.Format(f), t.Nanosecond())
}

type cloudStorageSinkKey struct {
	Bucket   time.Time
	Topic    string
	SchemaID sqlbase.DescriptorVersion
	SinkID   string
	Ext      string
}

func (k cloudStorageSinkKey) Filename() string {
	return fmt.Sprintf(`%s-%s-%d-%s%s`,
		cloudStorageFormatBucket(k.Bucket), k.Topic, k.SchemaID, k.SinkID, k.Ext)
}

// cloudStorageSink emits to files on cloud storage.
//
// The data files are named `<timestamp>_<topic>_<schema_id>_<uniquer>.<ext>`.
//
// `<timestamp>` is truncated to some bucket size, specified by the required
// sink param `bucket_size`. Bucket size is a tradeoff between number of files
// and the end-to-end latency of data being resolved.
//
// `<topic>` corresponds to one SQL table.
//
// `<schema_id>` changes whenever the SQL table schema changes, which allows us
// to guarantee to users that _all entries in a given file have the same
// schema_.
//
// `<uniquer>` is used to keep nodes in a cluster from overwriting each other's
// data and should be ignored by external users. It also keeps a single node
// from overwriting its own data if there are multiple changefeeds, or if a
// changefeed gets canceled/restarted.
//
// `<ext>` implies the format of the file: currently the only option is
// `ndjson`, which means a text file conforming to the "Newline Delimited JSON"
// spec.
//
// Each record in the data files is a value, keys are not included, so the
// `envelope` option must be set to `row`, which is the default. Within a file,
// records are not guaranteed to be sorted by timestamp. A duplicate of some
// record might exist in a different file or even in the same file.
//
// The resolved timestamp files are named `<timestamp>.RESOLVED`. This is
// carefully done so that we can offer the following external guarantee: At any
// given time, if the the files are iterated in lexicographic filename order,
// then encountering any filename containing `RESOLVED` means that everything
// before it is finalized (and thus can be ingested into some other system and
// deleted, included in hive queries, etc). A typical user of cloudStorageSink
// would periodically do exactly this.
//
// Still TODO is writing out data schemas, Avro support, bounding memory usage.
// Eliminating duplicates would be great, but may not be immediately practical.
type cloudStorageSink struct {
	base       *url.URL
	bucketSize time.Duration
	settings   *cluster.Settings
	sinkID     string

	ext           string
	recordDelimFn func(io.Writer) error

	files           map[cloudStorageSinkKey]*bytes.Buffer
	localResolvedTs hlc.Timestamp
}

func makeCloudStorageSink(
	baseURI string, bucketSize time.Duration, settings *cluster.Settings, opts map[string]string,
) (Sink, error) {
	base, err := url.Parse(baseURI)
	if err != nil {
		return nil, err
	}
	// TODO(dan): Each sink needs a unique id for the reasons described in the
	// above docs, but this is a pretty ugly way to do it.
	sinkID := uuid.MakeV4().String()
	s := &cloudStorageSink{
		base:       base,
		bucketSize: bucketSize,
		settings:   settings,
		sinkID:     sinkID,
		files:      make(map[cloudStorageSinkKey]*bytes.Buffer),
	}

	switch formatType(opts[optFormat]) {
	case optFormatJSON:
		// TODO(dan): It seems like these should be on the encoder, but that
		// seems to require a bit of refactoring.
		s.ext = `.ndjson`
		s.recordDelimFn = func(w io.Writer) error {
			_, err := w.Write([]byte{'\n'})
			return err
		}
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			optFormat, opts[optFormat])
	}

	switch envelopeType(opts[optEnvelope]) {
	case optEnvelopeValueOnly:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			optEnvelope, opts[optEnvelope])
	}

	{
		// Sanity check that we can connect.
		ctx := context.Background()
		es, err := storageccl.ExportStorageFromURI(ctx, s.base.String(), settings)
		if err != nil {
			return nil, err
		}
		if err := es.Close(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// EmitRow implements the Sink interface.
func (s *cloudStorageSink) EmitRow(
	_ context.Context, table *sqlbase.TableDescriptor, _, value []byte, updated hlc.Timestamp,
) error {
	if s.files == nil {
		return errors.New(`cannot EmitRow on a closed sink`)
	}

	// localResolvedTs is a guarantee that any rows <= to it are duplicates and
	// we can drop them.
	//
	// TODO(dan): We could actually move this higher up the changefeed stack and
	// do it for all sinks.
	if !s.localResolvedTs.Less(updated) {
		return nil
	}

	// Intentionally throw away the logical part of the timestamp for bucketing.
	key := cloudStorageSinkKey{
		Bucket:   updated.GoTime().Truncate(s.bucketSize),
		Topic:    table.Name,
		SchemaID: table.Version,
		SinkID:   s.sinkID,
		Ext:      s.ext,
	}
	file := s.files[key]
	if file == nil {
		// We could pool the bytes.Buffers if necessary, but we'd need to be
		// careful to bound the size of the memory held by the pool.
		file = &bytes.Buffer{}
		s.files[key] = file
	}

	// TODO(dan): Memory monitoring for this
	if _, err := file.Write(value); err != nil {
		return err
	}
	return s.recordDelimFn(file)
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *cloudStorageSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	if s.files == nil {
		return errors.New(`cannot EmitRow on a closed sink`)
	}

	var noTopic string
	payload, err := encoder.EncodeResolvedTimestamp(noTopic, resolved)
	if err != nil {
		return err
	}
	// Don't need to copy payload because we never buffer it anywhere.

	es, err := storageccl.ExportStorageFromURI(ctx, s.base.String(), s.settings)
	if err != nil {
		return err
	}
	defer func() {
		if err := es.Close(); err != nil {
			log.Warningf(ctx, `failed to close %s, resources may have leaked: %s`, s.base.String(), err)
		}
	}()

	// resolving some given time means that every in the _previous_ bucket is
	// finished.
	resolvedBucket := resolved.GoTime().Truncate(s.bucketSize).Add(-time.Nanosecond)
	name := cloudStorageFormatBucket(resolvedBucket) + `.RESOLVED`
	if log.V(1) {
		log.Info(ctx, "writing ", name)
	}

	return es.WriteFile(ctx, name, bytes.NewReader(payload))
}

// Flush implements the Sink interface.
func (s *cloudStorageSink) Flush(ctx context.Context, ts hlc.Timestamp) error {
	if s.files == nil {
		return errors.New(`cannot Flush on a closed sink`)
	}
	if s.localResolvedTs.Less(ts) {
		s.localResolvedTs = ts
	}

	var gcKeys []cloudStorageSinkKey
	for key, file := range s.files {
		// Any files where the bucket begin is `>= ts` don't need to be flushed
		// because of the Flush contract w.r.t. `ts`. (Bucket begin time is
		// exclusive and end time is inclusive).
		if !key.Bucket.Before(ts.GoTime()) {
			continue
		}

		// TODO(dan): These files should be further subdivided for three
		// reasons. 1) we could always gc anything we flush and later write a
		// followup bucket subdivion if needed 2) very large bucket sizes could
		// mean very large files, which are unwieldy once written 3) smooth
		// and/or control memory usage of the sink.
		filename := key.Filename()
		if log.V(1) {
			log.Info(ctx, "writing ", filename)
		}
		if err := s.writeFile(ctx, filename, file); err != nil {
			return err
		}

		// If the bucket end is `<= ts`, we'll never see another _previously
		// unseen_ row for this bucket. We drop any future such rows so that it
		// can be cleaned up.
		if end := key.Bucket.Add(s.bucketSize); ts.GoTime().After(end) {
			gcKeys = append(gcKeys, key)
		} else {
			if log.V(2) {
				log.Infof(ctx, "wrote %s but was not eligible for gc", filename)
			}
		}
	}
	for _, key := range gcKeys {
		delete(s.files, key)
	}

	return nil
}

func (s *cloudStorageSink) writeFile(
	ctx context.Context, name string, contents *bytes.Buffer,
) error {
	u := *s.base
	u.Path = filepath.Join(u.Path, name)
	es, err := storageccl.ExportStorageFromURI(ctx, u.String(), s.settings)
	if err != nil {
		return err
	}
	defer func() {
		if err := es.Close(); err != nil {
			log.Warningf(ctx, `failed to close %s, resources may have leaked: %s`, name, err)
		}
	}()
	r := bytes.NewReader(contents.Bytes())
	return es.WriteFile(ctx, ``, r)
}

// Close implements the Sink interface.
func (s *cloudStorageSink) Close() error {
	s.files = nil
	return nil
}
