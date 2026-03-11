// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { api as clusterUiApi, JobDetailsV2 } from "@cockroachlabs/cluster-ui";
import React from "react";
import { StaticContext } from "react-router";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { listExecutionDetailFiles } from "src/util/api";

export default withRouter(
  (
    props: React.PropsWithChildren<
      RouteComponentProps<any, StaticContext, unknown>
    >,
  ) => {
    return (
      <JobDetailsV2
        {...props}
        onFetchExecutionDetailFiles={listExecutionDetailFiles}
        onCollectExecutionDetails={clusterUiApi.collectExecutionDetails}
        onDownloadExecutionFile={clusterUiApi.getExecutionDetailFile}
      />
    );
  },
);
