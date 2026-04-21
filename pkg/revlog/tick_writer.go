// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"context"
	"encoding/binary"
	"hash/crc32"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
)

// crc32cTable is the Castagnoli polynomial table used for the trailing
// checksum of manifest objects.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// TickWriter writes one data file in a tick. It opens an SSTable
// stream into external storage at log/data/<tick-end>/<file_id>.sst
// and accepts entries via Add in strictly ascending
// (user_key, mvcc_ts) order. Close finalizes the SSTable and returns
// the revlogpb.File descriptor that the caller will eventually list
// in the tick's manifest, plus a revlogpb.Stats describing the file
// (key_count, logical bytes, on-disk SST size).
//
// TickWriter is single-use: a Close-d writer cannot be reused.
type TickWriter struct {
	tickEnd    hlc.Timestamp
	fileID     int64
	flushOrder int32

	sst *sstable.Writer

	// stats accumulates as Add is called. sst_bytes is filled in by
	// Close from the underlying Pebble writer's metadata.
	stats  revlogpb.Stats
	closed bool
}

// NewTickWriter opens a fresh data file in es and prepares it for
// writing. The caller is responsible for assigning a file_id that is
// unique within the tick and the appropriate flush_order
// (per-producer-per-tick monotonic counter).
func NewTickWriter(
	ctx context.Context,
	es cloud.ExternalStorage,
	tickEnd hlc.Timestamp,
	fileID int64,
	flushOrder int32,
) (*TickWriter, error) {
	name := DataFilePath(tickEnd, fileID)
	wc, err := es.Writer(ctx, name)
	if err != nil {
		return nil, errors.Wrapf(err, "opening %s", name)
	}
	// Pebble's default WriterOptions use bytes.Compare and
	// TableFormatMinSupported. Both are appropriate for revlog data
	// files: keys are pre-encoded for ascending byte ordering
	// (encoding.go) and the format is widely supported by readers.
	opts := sstable.WriterOptions{}
	return &TickWriter{
		tickEnd:    tickEnd,
		fileID:     fileID,
		flushOrder: flushOrder,
		sst:        sstable.NewWriter(objstorageprovider.NewRemoteWritable(wc), opts),
	}, nil
}

// Add appends one entry. Keys must be added in strictly ascending
// (user_key, mvcc_ts) order; the underlying SSTable writer enforces
// this and returns an error otherwise.
func (w *TickWriter) Add(userKey roachpb.Key, ts hlc.Timestamp, value, prevValue []byte) error {
	if w.closed {
		return errors.AssertionFailedf("revlog: Add on closed TickWriter")
	}
	frame := revlogpb.ValueFrame{Value: value, PrevValue: prevValue}
	val, err := protoutil.Marshal(&frame)
	if err != nil {
		return errors.Wrap(err, "marshaling value frame")
	}
	if err := w.sst.Set(EncodeKey(userKey, ts), val); err != nil {
		return err
	}
	w.stats.KeyCount++
	w.stats.LogicalBytes += int64(len(userKey) + len(value) + len(prevValue))
	return nil
}

// Close finalizes the SSTable, flushes it to external storage, and
// returns the revlogpb.File descriptor for the tick's manifest plus
// a revlogpb.Stats describing what was written. Stats.SSTBytes is
// populated from the underlying Pebble writer's metadata; if that
// query fails the rest of the stats are returned with SSTBytes=0.
func (w *TickWriter) Close() (revlogpb.File, revlogpb.Stats, error) {
	if w.closed {
		return revlogpb.File{}, revlogpb.Stats{}, errors.AssertionFailedf("revlog: TickWriter already closed")
	}
	w.closed = true
	if err := w.sst.Close(); err != nil {
		return revlogpb.File{}, revlogpb.Stats{}, errors.Wrap(err, "closing sstable")
	}
	if md, err := w.sst.Metadata(); err == nil {
		w.stats.SstBytes = int64(md.Size)
	}
	return revlogpb.File{FileID: w.fileID, FlushOrder: w.flushOrder}, w.stats, nil
}

// WriteTickManifest writes (and seals) the close marker for one tick.
// The marker layout on disk is a 4-byte little-endian CRC32C of the
// marshaled body, followed by the marshaled Manifest proto. The
// leading-checksum layout lets readers verify with a single GET — no
// separate Size() call to locate a trailer.
//
// The manifest's TickStart and TickEnd must both be set, defining
// the tick's coverage as (TickStart, TickEnd]. Files must be the
// complete list of data files contributed to the tick. Once
// written, the tick is considered authoritative: any data file
// later PUT under log/data/<tick-end>/ is ignored by readers.
func WriteTickManifest(
	ctx context.Context, es cloud.ExternalStorage, manifest revlogpb.Manifest,
) error {
	if manifest.TickEnd.IsEmpty() {
		return errors.AssertionFailedf("revlog: Manifest.TickEnd must be set")
	}
	if manifest.TickStart.IsEmpty() {
		return errors.AssertionFailedf("revlog: Manifest.TickStart must be set")
	}
	if !manifest.TickStart.Less(manifest.TickEnd) {
		return errors.AssertionFailedf(
			"revlog: Manifest.TickStart (%s) must be < TickEnd (%s)",
			manifest.TickStart, manifest.TickEnd)
	}
	body, err := protoutil.Marshal(&manifest)
	if err != nil {
		return errors.Wrap(err, "marshaling manifest")
	}
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], crc32.Checksum(body, crc32cTable))

	name := MarkerPath(manifest.TickEnd)
	wc, err := es.Writer(ctx, name)
	if err != nil {
		return errors.Wrapf(err, "opening marker %s", name)
	}
	if _, err := wc.Write(header[:]); err != nil {
		_ = wc.Close()
		return errors.Wrap(err, "writing manifest header")
	}
	if _, err := wc.Write(body); err != nil {
		_ = wc.Close()
		return errors.Wrap(err, "writing manifest body")
	}
	return wc.Close()
}
