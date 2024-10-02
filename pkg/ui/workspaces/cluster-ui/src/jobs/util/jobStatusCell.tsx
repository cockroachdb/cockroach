// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import React from "react";

import { JobStatus } from "./jobStatus";

type Job = cockroach.server.serverpb.IJobResponse;

export interface JobStatusCellProps {
  job: Job;
  lineWidth?: number;
  compact?: boolean;
  hideDuration?: boolean;
}

export const JobStatusCell: React.FC<JobStatusCellProps> = ({
  job,
  lineWidth,
  compact = false,
  hideDuration = false,
}) => (
  <JobStatus
    job={job}
    lineWidth={lineWidth}
    compact={compact}
    hideDuration={hideDuration}
  />
);
