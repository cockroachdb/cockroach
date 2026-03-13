// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import {
  listExecutionDetailFiles,
  collectExecutionDetails,
  getExecutionDetailFile,
} from "src/api";

import { JobDetailsV2 } from "./jobDetails";

export const JobDetailsPageConnected: React.FC = () => {
  return (
    <JobDetailsV2
      onFetchExecutionDetailFiles={listExecutionDetailFiles}
      onCollectExecutionDetails={collectExecutionDetails}
      onDownloadExecutionFile={getExecutionDetailFile}
    />
  );
};
