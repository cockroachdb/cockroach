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
import { Tooltip } from "@cockroachlabs/ui-components";
import React from "react";
import { TimestampToMoment } from "src/util";
import { DATE_FORMAT_24_UTC } from "src/util/format";

import { JobStatus } from "./jobStatus";
import { isRetrying } from "./jobOptions";

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
}) => {
  const jobStatus = (
    <JobStatus
      job={job}
      lineWidth={lineWidth}
      compact={compact}
      hideDuration={hideDuration}
    />
  );
  if (isRetrying(job.status)) {
    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            Next Planned Execution Time:
            <br />
            {TimestampToMoment(job.next_run).format(DATE_FORMAT_24_UTC)}
          </>
        }
      >
        {jobStatus}
      </Tooltip>
    );
  }
  return jobStatus;
};
