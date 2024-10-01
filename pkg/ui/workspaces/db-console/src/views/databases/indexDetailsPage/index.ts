// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { IndexDetailsPage } from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { withRouter } from "react-router-dom";

import { mapStateToProps, mapDispatchToProps } from "./redux";

const connected = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(IndexDetailsPage),
);

export { connected as IndexDetailsPage };
