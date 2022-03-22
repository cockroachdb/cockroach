// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
