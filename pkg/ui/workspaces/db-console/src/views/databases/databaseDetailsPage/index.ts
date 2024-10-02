// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { DatabaseDetailsPage } from "@cockroachlabs/cluster-ui";
import { connect, ReactReduxContext } from "react-redux";
import { withRouter } from "react-router-dom";

import { mapStateToProps, mapDispatchToProps } from "./redux";

const connected = withRouter(
  connect(mapStateToProps, mapDispatchToProps, null, {
    context: ReactReduxContext,
  })(DatabaseDetailsPage),
);

export { connected as DatabaseDetailsPageLegacy };
