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
import { JobStatus } from "./jobStatus";
import { isRetrying } from "src/views/jobs/jobStatusOptions";
import { util } from "@cockroachlabs/cluster-ui";
import { DATE_FORMAT_24_UTC } from "src/util/format";
import { Tooltip } from "@cockroachlabs/ui-components";
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
  const jobStatus = (
    <JobStatus job={job} lineWidth={lineWidth} compact={compact} />
  );
  if (isRetrying(job.status)) {
    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            Next Execution Time:
            <br />
            {util.TimestampToMoment(job.next_run).format(DATE_FORMAT_24_UTC)}
          </>
        }
      >
        {jobStatus}
      </Tooltip>
    );
  }
  return jobStatus;
};
