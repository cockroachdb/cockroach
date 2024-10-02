// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"bytes"
	"context"
	"fmt"
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
// `~profiler/filename~<proto.MessageName>.binpb` to the system.job_info table,
// with the contents of the passed in protobuf message.
//
// Files written using this method will show up in their `binpb` and `binpb.txt`
// forms in the list of files displayed on the jobs' Advanced Debugging
// DBConsole page.
func WriteProtobinExecutionDetailFile(
	ctx context.Context, filename string, msg protoutil.Message, txn isql.Txn, jobID jobspb.JobID,
) error {
	name := fmt.Sprintf("%s~%s.binpb", filename, proto.MessageName(msg))
	b, err := protoutil.Marshal(msg)
	if err != nil {
		return err
	}
	return WriteExecutionDetailFile(ctx, name, b, txn, jobID)
}

// WriteExecutionDetailFile will chunk and write to the job_info table under the
// `~profiler/` prefix. Files written using this method can be read using the
// `ReadExecutionDetailFile` and will show up in the list of files displayed on
// the jobs' Advanced Debugging DBConsole page.
//
// This method clears any existing file with the same filename before writing a
// new one.
func WriteExecutionDetailFile(
	ctx context.Context, filename string, data []byte, txn isql.Txn, jobID jobspb.JobID,
) error {
	return WriteChunkedFileToJobInfo(ctx,
		profilerconstants.MakeProfilerExecutionDetailsChunkKeyPrefix(filename), data, txn, jobID)
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
	ctx context.Context, filename string, txn isql.Txn, jobID jobspb.JobID,
) ([]byte, error) {
	// TODO(adityamaru): If filename=all add logic to zip up all the files corresponding
	// to the job's execution details and return the zipped bundle instead.

	// If the file requested is the `binpb.txt` format of a `binpb` file, we must
	// fetch the `binpb` version of the file and stringify the contents before
	// returning the response.
	if strings.HasSuffix(filename, "binpb.txt") {
		trimmedFilename := strings.TrimSuffix(filename, ".txt")
		fileBytes, err := ReadChunkedFileToJobInfo(
			ctx,
			profilerconstants.MakeProfilerExecutionDetailsChunkKeyPrefix(trimmedFilename),
			txn,
			jobID,
		)
		if err != nil {
			return nil, err
		}
		return stringifyProtobinFile(trimmedFilename, fileBytes)
	}

	return ReadChunkedFileToJobInfo(
		ctx,
		profilerconstants.MakeProfilerExecutionDetailsChunkKeyPrefix(filename),
		txn,
		jobID,
	)
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
