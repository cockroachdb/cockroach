// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { cockroach } from "src/js/protos";
import { HighwaterTimestamp } from "src/views/jobs/highwaterTimestamp";
import { JobStatus } from "./jobStatus";
import Job = cockroach.server.serverpb.IJobResponse;

export interface JobStatusCellProps {
  job: Job;
  lineWidth?: number;
  compact?: boolean;
}

export const JobStatusCell: React.FC<JobStatusCellProps> = ({
  job,
  lineWidth,
  compact = false,
}) => {
  if (job.highwater_timestamp) {
    return (
      <HighwaterTimestamp
        highwater={job.highwater_timestamp}
        tooltip={job.highwater_decimal}
      />
    );
  }
  return <JobStatus job={job} lineWidth={lineWidth} compact={compact} />;
};
