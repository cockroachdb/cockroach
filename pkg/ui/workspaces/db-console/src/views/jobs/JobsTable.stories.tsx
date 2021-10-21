// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import React from "react";
import { storiesOf } from "@storybook/react";
import { withRouterDecorator } from "src/util/decorators";
import { connect } from "react-redux";

// import JobsTable from "./index";
import {
  JobsTable,
  sortSetting,
  typeSetting,
  statusSetting,
  showSetting,
} from "./index";
import { jobsTablePropsFixture } from "./jobsTable.fixture";

function mapStateToProps(state: AdminUIState) {
  return {
    sort: sortSetting.selector(state),
    status: statusSetting.selector(state),
    show: showSetting.selector(state),
    type: typeSetting.selector(state),
    ...jobsTablePropsFixture,
  };
}

export const mapDispatchToProps = {
  setSort: sortSetting.set,
  setStatus: statusSetting.set,
  setShow: showSetting.set,
  setType: typeSetting.set,
  // refreshJobs: () => {},
};

const ConnectedJobsTable = connect(
  mapStateToProps,
  mapDispatchToProps,
)(JobsTable);
storiesOf("JobsTable", module)
  .addDecorator(withRouterDecorator)
  .add("with data", () => <ConnectedJobsTable />);
