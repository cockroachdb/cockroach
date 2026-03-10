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

// DeleteChunkedFile deletes all chunks associated with the given filename from
// the job_info table. Chunks are stored with info keys in [filename,
// filename#_final~), and this function removes all of them.
func DeleteChunkedFile(
	ctx context.Context, filename string, txn isql.Txn, jobID jobspb.JobID,
) error {
	finalChunkName := filename + finalChunkSuffix
	return InfoStorageForJob(txn, jobID).DeleteRange(ctx, filename, finalChunkName+"~", 0)
}

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

	// Clear any existing chunks with the same filename before writing new ones.
	if err := DeleteChunkedFile(ctx, filename, txn, jobID); err != nil {
		return err
	}

	var chunkCounter int
	var chunkName string
	for len(data) > 0 {
		if chunkCounter > 9999 {
			return errors.AssertionFailedf("too many chunks for file %s; chunk name will break lexicographic ordering", filename)
		}
		chunkSize := bundleChunkSize
		chunk := data
		if len(chunk) > chunkSize {
			chunkName = fmt.Sprintf("%s#%04d", filename, chunkCounter)
			chunk = chunk[:chunkSize]
		} else {
			// This is the last chunk we will write, assign it a sentinel file name.
			chunkName = finalChunkName
		}
		data = data[len(chunk):]
		var err error
		chunk, err = compressChunk(chunk)
		if err != nil {
			return errors.Wrapf(err, "failed to compress chunk for file %s", filename)
		}

		// On listing we want the info_key of each chunk to sort after the
		// previous chunk of the same file so that the chunks can be reassembled
		// on download. For this reason we use a monotonically increasing
		// chunk counter as the suffix.
		err = jobInfo.Write(ctx, chunkName, chunk)
		if err != nil {
			return errors.Wrapf(err, "failed to write chunk for file %s", filename)
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

// WriteChunkedProto marshals a protobuf message and writes it as a chunked
// file to the job_info table.
func WriteChunkedProto(
	ctx context.Context, filename string, msg protoutil.Message, txn isql.Txn, jobID jobspb.JobID,
) error {
	data, err := protoutil.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "failed to marshal proto")
	}
	return WriteChunkedFileToJobInfo(ctx, filename, data, txn, jobID)
}

// ReadChunkedProto reads a chunked file from the job_info table and unmarshals
// it into the provided protobuf message. Returns true if data was found and
// unmarshaled successfully.
func ReadChunkedProto(
	ctx context.Context, filename string, txn isql.Txn, jobID jobspb.JobID, msg protoutil.Message,
) (bool, error) {
	data, err := ReadChunkedFileToJobInfo(ctx, filename, txn, jobID)
	if err != nil {
		return false, err
	}
	if len(data) == 0 {
		return false, nil
	}
	if err := protoutil.Unmarshal(data, msg); err != nil {
		return false, errors.Wrap(err, "failed to unmarshal proto")
	}
	return true, nil
}
