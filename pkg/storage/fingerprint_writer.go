// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"hash"
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
)

// fingerprintWriter hashes every key/timestamp and value for point keys, and
// combines their hashes via a XOR into a running aggregate.
//
// Range keys are not fingerprinted but instead written to a pebble SST that is
// returned to the caller. This is because range keys do not have a stable,
// discrete identity and so it is up to the caller to define a deterministic
// fingerprinting scheme across all returned range keys.
//
// The caller must Finish() and Close() the fingerprintWriter to finalize the
// writes to the underlying pebble SST.
type fingerprintWriter struct {
	hasher       hash.Hash64
	timestampBuf []byte

	sstWriter *SSTWriter
	xorAgg    *uintXorAggregate
}

// makeFingerprintWriter creates a new fingerprintWriter.
func makeFingerprintWriter(
	ctx context.Context, hasher hash.Hash64, cs *cluster.Settings, f io.Writer,
) fingerprintWriter {
	// TODO(adityamaru,dt): Once
	// https://github.com/cockroachdb/cockroach/issues/90450 has been addressed we
	// should write to a kvBuf instead of a Backup SST writer.
	sstWriter := MakeBackupSSTWriter(ctx, cs, f)
	return fingerprintWriter{
		sstWriter: &sstWriter,
		hasher:    hasher,
		xorAgg:    &uintXorAggregate{},
	}
}

type uintXorAggregate struct {
	sum uint64
}

// add inserts one value into the running xor.
func (a *uintXorAggregate) add(x uint64) {
	a.sum = a.sum ^ x
}

// result returns the xor.
func (a *uintXorAggregate) result() uint64 {
	return a.sum
}

// Finish finalizes the underlying SSTWriter, and returns the aggregated
// fingerprint for point keys.
func (f *fingerprintWriter) Finish() (uint64, error) {
	// If no records were added to the sstable, skip completing it.
	if f.sstWriter.DataSize != 0 {
		if err := f.sstWriter.Finish(); err != nil {
			return 0, err
		}
	}
	return f.xorAgg.result(), nil
}

// Close finishes and frees memory and other resources. Close is idempotent.
func (f *fingerprintWriter) Close() {
	if f.sstWriter == nil {
		return
	}
	f.sstWriter.Close()
	f.hasher.Reset()
	f.xorAgg = nil
	f.sstWriter = nil
}

var _ ExportWriter = &fingerprintWriter{}

// PutRawMVCCRangeKey implements the Writer interface.
func (f *fingerprintWriter) PutRawMVCCRangeKey(key MVCCRangeKey, bytes []byte) error {
	// We do not fingerprint range keys, instead, we write them to a Pebble SST.
	// This is because range keys do not have a stable, discrete identity and so
	// it is up to the caller to define a deterministic fingerprinting scheme
	// across all returned range keys.
	return f.sstWriter.PutRawMVCCRangeKey(key, bytes)
}

// PutRawMVCC implements the Writer interface.
func (f *fingerprintWriter) PutRawMVCC(key MVCCKey, value []byte) error {
	defer f.hasher.Reset()

	// Hash the key/timestamp and value of the RawMVCC.
	if err := f.hash(key.Key); err != nil {
		return err
	}
	f.timestampBuf = EncodeMVCCTimestampToBuf(f.timestampBuf, key.Timestamp)
	if err := f.hash(f.timestampBuf); err != nil {
		return err
	}
	if err := f.hash(value); err != nil {
		return err
	}
	f.xorAgg.add(f.hasher.Sum64())
	return nil
}

// PutUnversioned implements the Writer interface.
func (f *fingerprintWriter) PutUnversioned(key roachpb.Key, value []byte) error {
	defer f.hasher.Reset()

	// Hash the key and value in the absence of a timestamp.
	if err := f.hash(key); err != nil {
		return err
	}
	if err := f.hash(value); err != nil {
		return err
	}

	f.xorAgg.add(f.hasher.Sum64())
	return nil
}

func (f *fingerprintWriter) hash(data []byte) error {
	if _, err := f.hasher.Write(data); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err,
			`"It never returns an error." -- https://golang.org/pkg/hash: %T`, f)
	}

	return nil
}
