// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { api as clusterUiApi, JobDetailsV2 } from "@cockroachlabs/cluster-ui";
import React from "react";

import { listExecutionDetailFiles } from "src/util/api";

const JobDetailsPage: React.FC = () => {
  return (
    <JobDetailsV2
      onFetchExecutionDetailFiles={listExecutionDetailFiles}
      onCollectExecutionDetails={clusterUiApi.collectExecutionDetails}
      onDownloadExecutionFile={clusterUiApi.getExecutionDetailFile}
    />
  );
};

export default JobDetailsPage;
