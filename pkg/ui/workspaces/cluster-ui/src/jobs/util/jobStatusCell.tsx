// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
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
