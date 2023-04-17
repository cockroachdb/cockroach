// Copyright 2022 The Cockroach Authors.
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

import { AppState } from "src/store";
import {
  selectShowSetting,
  selectSortSetting,
  selectTypeSetting,
  selectStatusSetting,
  selectColumns,
  initialState,
  actions as jobsActions,
} from "src/store/jobs";
import {
  JobsPageStateProps,
  JobsPageDispatchProps,
  JobsPage,
} from "./jobsPage";
import { JobsRequest } from "src/api/jobsApi";
import { actions as localStorageActions } from "../../store/localStorage";
import { Dispatch } from "redux";
import { SortSetting } from "../../sortedtable";
import { actions as analyticsActions } from "../../store/analytics";

const mapStateToProps = (
  state: AppState,
  _: RouteComponentProps,
): JobsPageStateProps => {
  const sort = selectSortSetting(state);
  const status = selectStatusSetting(state);
  const show = selectShowSetting(state);
  const type = selectTypeSetting(state);
  const columns = selectColumns(state);
  return {
    sort,
    status,
    show,
    type,
    columns,
    jobsResponse: state.adminUI?.jobs ?? initialState,
  };
};

const mapDispatchToProps = (dispatch: Dispatch): JobsPageDispatchProps => ({
  setShow: (showValue: string) => {
    dispatch(jobsActions.invalidated());
    dispatch(
      localStorageActions.update({
        key: "showSetting/JobsPage",
        value: showValue,
      }),
    );
  },
  setSort: (ss: SortSetting) => {
    dispatch(
      localStorageActions.update({
        key: "sortSetting/JobsPage",
        value: ss,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Column Sorted",
        page: "Jobs",
        tableName: "Jobs Table",
        columnName: ss.columnTitle,
      }),
    );
  },
  setStatus: (statusValue: string) => {
    dispatch(jobsActions.invalidated());
    dispatch(
      localStorageActions.update({
        key: "statusSetting/JobsPage",
        value: statusValue,
      }),
    );
  },
  setType: (jobValue: number) => {
    dispatch(jobsActions.invalidated());
    dispatch(
      localStorageActions.update({
        key: "typeSetting/JobsPage",
        value: jobValue,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Job Type Selected",
        page: "Jobs",
        value: jobValue.toString(),
      }),
    );
  },
  onColumnsChange: (selectedColumns: string[]) => {
    const columns =
      selectedColumns.length === 0 ? " " : selectedColumns.join(",");
    dispatch(
      localStorageActions.update({
        key: "showColumns/JobsPage",
        value: columns,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Columns Selected change",
        page: "Jobs",
        value: columns,
      }),
    );
  },
  refreshJobs: (req: JobsRequest) => dispatch(jobsActions.refresh(req)),
});

export const JobsPageConnected = withRouter(
  connect<JobsPageStateProps, JobsPageDispatchProps, RouteComponentProps>(
    mapStateToProps,
    mapDispatchToProps,
  )(JobsPage),
);
