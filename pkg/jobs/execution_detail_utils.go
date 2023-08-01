// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/gzip"
)

const bundleChunkSize = 1 << 20 // 1 MiB
const finalChunkSuffix = "#_final"

func compressChunk(chunkBuf []byte) ([]byte, error) {
	gzipBuf := bytes.NewBuffer([]byte{})
	gz := gzip.NewWriter(gzipBuf)
	if _, err := gz.Write(chunkBuf); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return gzipBuf.Bytes(), nil
}

// WriteProtobinExecutionDetailFile writes a `binpb` file of the form
// `filename~<proto.MessageName>.binpb` to the system.job_info table, with the
// contents of the passed in protobuf message.
func WriteProtobinExecutionDetailFile(
	ctx context.Context, filename string, msg protoutil.Message, db isql.DB, jobID jobspb.JobID,
) error {
	name := fmt.Sprintf("%s~%s.binpb", filename, proto.MessageName(msg))
	b, err := protoutil.Marshal(msg)
	if err != nil {
		return err
	}
	return WriteExecutionDetailFile(ctx, name, b, db, jobID)
}

// WriteExecutionDetailFile will break up data into chunks of a fixed size, and
// gzip compress them before writing them to the job_info table.
func WriteExecutionDetailFile(
	ctx context.Context, filename string, data []byte, db isql.DB, jobID jobspb.JobID,
) error {
	return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Take a copy of the data to operate on inside the txn closure.
		chunkData := data[:]
		jobInfo := InfoStorageForJob(txn, jobID)

		var chunkCounter int
		var chunkName string
		for len(chunkData) > 0 {
			chunkSize := bundleChunkSize
			chunk := chunkData
			if len(chunk) > chunkSize {
				chunkName = fmt.Sprintf("%s#%04d", filename, chunkCounter)
				chunk = chunk[:chunkSize]
			} else {
				// This is the last chunk we will write, assign it a sentinel file name.
				chunkName = filename + finalChunkSuffix
			}
			chunkData = chunkData[len(chunk):]
			var err error
			chunk, err = compressChunk(chunk)
			if err != nil {
				return errors.Wrapf(err, "failed to compress chunk for file %s", filename)
			}

			// On listing we want the info_key of each chunk to sort after the
			// previous chunk of the same file so that the chunks can be reassembled
			// on download. For this reason we use a monotonically increasing
			// chunk counter as the suffix.
			err = jobInfo.Write(ctx, profilerconstants.MakeProfilerExecutionDetailsChunkKey(chunkName), chunk)
			if err != nil {
				return errors.Wrapf(err, "failed to write chunk for file %s", filename)
			}
			chunkCounter++
		}
		return nil
	})
}

// ProtobinExecutionDetailFile interface encapsulates the methods that must be
// implemented by protobuf messages that are collected as part of a job's
// execution details.
type ProtobinExecutionDetailFile interface {
	// ToText returns the human-readable text representation of the protobuf
	// execution detail file.
	ToText() []byte
}

func stringifyProtobinFile(filename string, fileContents []byte) ([]byte, error) {
	// A `binpb` execution detail file is expected to have its fully qualified
	// proto name after the last `~` in the filename. See
	// `WriteProtobinExecutionDetailFile` for details.
	msg, err := protoreflect.DecodeMessage(strings.TrimSuffix(
		filename[strings.LastIndex(filename, "~")+1:], ".binpb"), fileContents)
	if err != nil {
		return nil, err
	}
	f, ok := msg.(ProtobinExecutionDetailFile)
	if !ok {
		return nil, errors.Newf("protobuf in file %s is not a ProtobinExecutionDetailFile", filename)
	}
	return f.ToText(), err
}

// ReadExecutionDetailFile will stitch together all the chunks corresponding to the
// filename and return the uncompressed data of the file.
func ReadExecutionDetailFile(
	ctx context.Context, filename string, db isql.DB, jobID jobspb.JobID,
) ([]byte, error) {
	// TODO(adityamaru): If filename=all add logic to zip up all the files corresponding
	// to the job's execution details and return the zipped bundle instead.

	buf := bytes.NewBuffer([]byte{})
	fetchFileContent := func(file string) error {
		return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			// Reset the buf inside the txn closure to guard against txn retries.
			buf.Reset()
			jobInfo := InfoStorageForJob(txn, jobID)

			// Iterate over all the chunks of the requested file and return the unzipped
			// chunks of data.
			var lastInfoKey string
			if err := jobInfo.Iterate(ctx, profilerconstants.MakeProfilerExecutionDetailsChunkKeyPrefix(file),
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
				return errors.Wrapf(err, "failed to iterate over chunks for job %d", jobID)
			}

			if lastInfoKey != "" && !strings.Contains(lastInfoKey, finalChunkSuffix) {
				return errors.Newf("failed to read all chunks for file %s, last info key read was %s", file, lastInfoKey)
			}

			return nil
		})
	}

	// If the file requested is the `binpb.txt` format of a `binpb` file, we must
	// fetch the `binpb` version of the file and stringify the contents before
	// returning the response.
	if strings.HasSuffix(filename, "binpb.txt") {
		trimmedFilename := strings.TrimSuffix(filename, ".txt")
		if err := fetchFileContent(trimmedFilename); err != nil {
			return nil, err
		}
		return stringifyProtobinFile(trimmedFilename, buf.Bytes())
	}

	if err := fetchFileContent(filename); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ListExecutionDetailFiles lists all the files that have been generated as part
// of a job's execution details.
func ListExecutionDetailFiles(
	ctx context.Context, db isql.DB, jobID jobspb.JobID,
) ([]string, error) {
	var res []string
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		jobInfo := InfoStorageForJob(txn, jobID)

		// Iterate over all the files that have been stored as part of the job's
		// execution details.
		files := make([]string, 0)
		if err := jobInfo.Iterate(ctx, profilerconstants.ExecutionDetailsChunkKeyPrefix,
			func(infoKey string, value []byte) error {
				// Look for the final chunk of each file to find the unique file name.
				if strings.HasSuffix(infoKey, finalChunkSuffix) {
					filename := strings.TrimPrefix(strings.TrimSuffix(infoKey, finalChunkSuffix), profilerconstants.ExecutionDetailsChunkKeyPrefix)
					// If we see a `.binpb` file we also want to make the string version of
					// the file available for consumption.
					if strings.HasSuffix(filename, ".binpb") {
						files = append(files, filename+".txt")
					}
					files = append(files, filename)
				}
				return nil
			}); err != nil {
			return errors.Wrapf(err, "failed to iterate over execution detail files for job %d", jobID)
		}
		res = files
		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}
