// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/klauspost/compress/gzip"
)

const bundleChunkSize = 1 << 20 // 1 MiB
const finalChunkSuffix = "#_final"

// WriteChunkedFileToJobInfo will break up data into chunks of a fixed size, and
// gzip compress them before writing them to the job_info table. This method
// clears any existing chunks with the same filename before writing the new
// chunks and so if the caller wishes to preserve history they must use a
// unique filename.
func WriteChunkedFileToJobInfo(
	ctx context.Context, filename string, data []byte, txn isql.Txn, jobID jobspb.JobID,
) error {
	finalChunkName := filename + finalChunkSuffix
	jobInfo := InfoStorageForJob(txn, jobID)

	// Clear any existing chunks with the same filename before writing new chunks.
	// We clear all rows that with info keys in [filename, filename#_final~). The
	// trailing "~" makes the exclusive end-key inclusive of all possible chunks
	// as "~" sorts after all digit.
	if err := jobInfo.DeleteRange(ctx, filename, finalChunkName+"~", 0); err != nil {
		return err
	}

	var chunkCounter int
	for len(data) > 0 {
		if chunkCounter > 9999 {
			return errors.AssertionFailedf(
				"too many chunks for file %s; chunk name will break lexicographic ordering", filename,
			)
		}
		chunk := data
		if len(chunk) > bundleChunkSize {
			chunk = chunk[:bundleChunkSize]
		}
		data = data[len(chunk):]

		// Determine the chunk index: use -1 for the final chunk so that
		// writeCompressedChunk assigns the sentinel #_final suffix.
		chunkIndex := chunkCounter
		if len(data) == 0 {
			chunkIndex = -1
		}
		if err := writeCompressedChunk(ctx, jobInfo, filename, chunkIndex, chunk); err != nil {
			return err
		}
		chunkCounter++
	}
	return nil
}

// ReadChunkedFileToJobInfo will stitch together all the chunks corresponding to
// the filename and return the uncompressed data of the file.
func ReadChunkedFileToJobInfo(
	ctx context.Context, filename string, txn isql.Txn, jobID jobspb.JobID,
) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	// Iterate over all the chunks of the requested file and return the unzipped
	// chunks of data.
	jobInfo := InfoStorageForJob(txn, jobID)
	var lastInfoKey string
	if err := jobInfo.Iterate(ctx, filename,
		func(infoKey string, value []byte) error {
			lastInfoKey = infoKey
			r, err := gzip.NewReader(bytes.NewBuffer(value))
			if err != nil {
				return err
			}
			decompressed, err := io.ReadAll(r)
			if err != nil {
				return err
			}
			buf.Write(decompressed)
			return nil
		}); err != nil {
		return nil, errors.Wrapf(err, "failed to iterate over chunks for file %s", filename)
	}

	if lastInfoKey != "" && !strings.Contains(lastInfoKey, finalChunkSuffix) {
		return nil, errors.Newf("failed to read all chunks for file %s, last info key read was %s", filename, lastInfoKey)
	}

	return buf.Bytes(), nil
}

// WriteUint64 writes a uint64 value to the info storage.
func (i InfoStorage) WriteUint64(ctx context.Context, infoKey string, value uint64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, value)
	return i.Write(ctx, infoKey, buf)
}

// GetUint64 retrieves a uint64 value from the info storage.
// Returns (value, found, error).
func (i InfoStorage) GetUint64(ctx context.Context, infoKey string) (uint64, bool, error) {
	data, found, err := i.Get(ctx, "get-uint64", infoKey)
	if err != nil || !found {
		return 0, found, err
	}
	if len(data) != 8 {
		return 0, false, errors.Newf("invalid uint64 data length: %d", len(data))
	}
	return binary.LittleEndian.Uint64(data), true, nil
}

// WriteProto writes a protobuf message to the info storage.
func (i InfoStorage) WriteProto(ctx context.Context, infoKey string, msg protoutil.Message) error {
	data, err := protoutil.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "failed to marshal proto")
	}
	return i.Write(ctx, infoKey, data)
}

// GetProto retrieves a protobuf message from the info storage.
// Returns (found, error). If found, the message is unmarshaled into msg.
func (i InfoStorage) GetProto(
	ctx context.Context, infoKey string, msg protoutil.Message,
) (bool, error) {
	data, found, err := i.Get(ctx, "get-proto", infoKey)
	if err != nil || !found {
		return found, err
	}
	if err := protoutil.Unmarshal(data, msg); err != nil {
		return false, errors.Wrap(err, "failed to unmarshal proto")
	}
	return true, nil
}

// DeleteChunkedFile deletes all chunks associated with the given filename from
// the job_info table. Chunks are stored with info keys in [filename,
// filename#_final~), and this function removes all of them.
func DeleteChunkedFile(
	ctx context.Context, filename string, txn isql.Txn, jobID jobspb.JobID,
) error {
	finalChunkName := filename + finalChunkSuffix
	return InfoStorageForJob(txn, jobID).DeleteRange(
		ctx, filename, finalChunkName+"~", 0,
	)
}

// WriteChunkedProtos marshals a slice of protobuf messages and writes them as
// a chunked file to the job_info table. Each entry is stored with a 4-byte
// big-endian length prefix followed by its marshaled bytes. Chunks are flushed
// to the job_info table as they fill up, so only one chunk's worth of
// marshaled data is buffered in memory at a time. Chunk boundaries are always
// aligned to proto entry boundaries, so each chunk can be independently
// decompressed and parsed by ReadChunkedProtos.
func WriteChunkedProtos[T any, PT interface {
	*T
	protoutil.Message
}](ctx context.Context, filename string, msgs []T, txn isql.Txn, jobID jobspb.JobID) error {
	if err := DeleteChunkedFile(ctx, filename, txn, jobID); err != nil {
		return err
	}
	jobInfo := InfoStorageForJob(txn, jobID)
	var buf bytes.Buffer
	var chunkCounter int
	for i := range msgs {
		data, err := protoutil.Marshal(PT(&msgs[i]))
		if err != nil {
			return errors.Wrap(err, "marshaling proto entry")
		}
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
		buf.Write(lenBuf[:])
		buf.Write(data)
		if buf.Len() >= bundleChunkSize {
			if err := writeCompressedChunk(ctx, jobInfo, filename, chunkCounter, buf.Bytes()); err != nil {
				return err
			}
			buf.Reset()
			chunkCounter++
			if chunkCounter > 9999 {
				return errors.AssertionFailedf(
					"too many chunks for file %s; chunk name will break lexicographic ordering", filename,
				)
			}
		}
	}
	// Write the final chunk (may be empty if all protos fit evenly into
	// previous chunks, but we always write a final chunk so the reader can
	// verify completeness).
	return writeCompressedChunk(ctx, jobInfo, filename, -1 /* final */, buf.Bytes())
}

// writeCompressedChunk gzip-compresses data and writes it as a single chunk.
// A chunkIndex of -1 indicates the final chunk.
func writeCompressedChunk(
	ctx context.Context, jobInfo InfoStorage, filename string, chunkIndex int, data []byte,
) error {
	var chunkName string
	if chunkIndex < 0 {
		chunkName = filename + finalChunkSuffix
	} else {
		chunkName = fmt.Sprintf("%s#%04d", filename, chunkIndex)
	}
	compressed, err := compressChunk(data)
	if err != nil {
		return errors.Wrapf(err, "compressing chunk for file %s", filename)
	}
	return jobInfo.Write(ctx, chunkName, compressed)
}

// ReadChunkedProtos reads length-prefixed proto entries from a chunked file in
// the job_info table into the provided slice. Each chunk is independently
// decompressed and its entries are parsed one at a time, so only one chunk's
// worth of serialized data is in memory at a time. Returns true if any entries
// were found.
//
// T is the concrete proto type (e.g. jobspb.BulkSSTManifest) and PT is its
// pointer type which must implement protoutil.Message.
func ReadChunkedProtos[T any, PT interface {
	*T
	protoutil.Message
}](ctx context.Context, filename string, txn isql.Txn, jobID jobspb.JobID, out *[]T) (bool, error) {
	jobInfo := InfoStorageForJob(txn, jobID)
	var lastInfoKey string
	var found bool
	if err := jobInfo.Iterate(ctx, filename,
		func(infoKey string, value []byte) error {
			lastInfoKey = infoKey
			r, err := gzip.NewReader(bytes.NewBuffer(value))
			if err != nil {
				return err
			}
			data, err := io.ReadAll(r)
			if err != nil {
				return err
			}
			for len(data) > 0 {
				if len(data) < 4 {
					return errors.New("truncated length prefix in chunked protos")
				}
				n := binary.BigEndian.Uint32(data[:4])
				data = data[4:]
				if uint32(len(data)) < n {
					return errors.Newf(
						"truncated proto entry: expected %d bytes, got %d", n, len(data),
					)
				}
				var msg T
				if err := protoutil.Unmarshal(data[:n], PT(&msg)); err != nil {
					return errors.Wrap(err, "unmarshaling proto entry")
				}
				*out = append(*out, msg)
				found = true
				data = data[n:]
			}
			return nil
		}); err != nil {
		return false, errors.Wrapf(err, "reading chunked protos for file %s", filename)
	}
	if lastInfoKey != "" && !strings.Contains(lastInfoKey, finalChunkSuffix) {
		return false, errors.Newf(
			"failed to read all chunks for file %s, last info key read was %s",
			filename, lastInfoKey,
		)
	}
	return found, nil
}
