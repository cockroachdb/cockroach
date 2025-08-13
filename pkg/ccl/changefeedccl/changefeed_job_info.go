// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// Filename prefix/extension for all changefeed-related job info files.
const (
	jobInfoFilenamePrefix    = "~changefeed/"
	jobInfoFilenameExtension = ".binpb"
)

const (
	// resolvedTablesFilename: ~changefeed/resolved_tables.binpb
	resolvedTablesFilename = jobInfoFilenamePrefix + "resolved_tables" + jobInfoFilenameExtension
)

// writeChangefeedJobInfo writes a changefeed job info protobuf to the
// job_info table. A changefeed-specific filename is required.
func writeChangefeedJobInfo(
	ctx context.Context, filename string, info protoutil.Message, txn isql.Txn, jobID jobspb.JobID,
) error {
	if !strings.HasPrefix(filename, jobInfoFilenamePrefix) {
		return errors.AssertionFailedf("filename %q must start with prefix %q", filename, jobInfoFilenamePrefix)
	}
	if !strings.HasSuffix(filename, jobInfoFilenameExtension) {
		return errors.AssertionFailedf("filename %q must end with extension %q", filename, jobInfoFilenameExtension)
	}
	data, err := protoutil.Marshal(info)
	if err != nil {
		return errors.Wrapf(err, "error marshaling %s for job %d", filename, jobID)
	}
	return jobs.WriteChunkedFileToJobInfo(ctx, filename, data, txn, jobID)
}

// readChangefeedJobInfo reads a changefeed job info protobuf from the
// job_info table. A changefeed-specific filename is required.
// TODO(#148119): Use this function to read.
func readChangefeedJobInfo(
	ctx context.Context, filename string, info protoutil.Message, txn isql.Txn, jobID jobspb.JobID,
) error {
	if !strings.HasPrefix(filename, jobInfoFilenamePrefix) {
		return errors.AssertionFailedf("filename %q must start with prefix %q", filename, jobInfoFilenamePrefix)
	}
	if !strings.HasSuffix(filename, jobInfoFilenameExtension) {
		return errors.AssertionFailedf("filename %q must end with extension %q", filename, jobInfoFilenameExtension)
	}
	data, err := jobs.ReadChunkedFileToJobInfo(ctx, filename, txn, jobID)
	if err != nil {
		return err
	}
	if err := protoutil.Unmarshal(data, info); err != nil {
		return errors.Wrapf(err, "error unmarshaling %s for job %d", filename, jobID)
	}
	return nil
}
