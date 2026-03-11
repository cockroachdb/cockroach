// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { RouteComponentProps, withRouter } from "react-router-dom";

import {
  listExecutionDetailFiles,
  collectExecutionDetails,
  getExecutionDetailFile,
} from "src/api";

import { JobDetailsV2 } from "./jobDetails";

const JobDetailsPageInner: React.FC<RouteComponentProps> = props => {
  return (
    <JobDetailsV2
      {...props}
      onFetchExecutionDetailFiles={listExecutionDetailFiles}
      onCollectExecutionDetails={collectExecutionDetails}
      onDownloadExecutionFile={getExecutionDetailFile}
    />
  );
};

export const JobDetailsPageConnected = withRouter(JobDetailsPageInner);
