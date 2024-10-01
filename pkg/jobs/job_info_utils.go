// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
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
