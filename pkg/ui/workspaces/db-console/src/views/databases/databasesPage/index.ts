// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import {
  DatabasesPage,
  DatabasesPageData,
  DatabasesPageActions,
} from "@cockroachlabs/cluster-ui";

import { mapStateToProps, mapDispatchToProps } from "./redux";

const connected = withRouter(
  connect<DatabasesPageData, DatabasesPageActions, RouteComponentProps>(
    mapStateToProps,
    mapDispatchToProps,
  )(DatabasesPage),
);

export { connected as DatabasesPage };
