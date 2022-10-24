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
  selectJobsState,
  selectShowSetting,
  selectSortSetting,
  selectTypeSetting,
  selectStatusSetting,
  selectColumns,
} from "../../store/jobs/jobs.selectors";
import {
  JobsPageStateProps,
  JobsPageDispatchProps,
  JobsPage,
} from "./jobsPage";
import { JobsRequest } from "src/api/jobsApi";
import { actions as jobsActions } from "src/store/jobs";
import { actions as localStorageActions } from "../../store/localStorage";
import { Dispatch } from "redux";
import { SortSetting } from "../../sortedtable";

const mapStateToProps = (
  state: AppState,
  _: RouteComponentProps,
): JobsPageStateProps => {
  const sort = selectSortSetting(state);
  const status = selectStatusSetting(state);
  const show = selectShowSetting(state);
  const type = selectTypeSetting(state);
  const columns = selectColumns(state);
  const jobsState = selectJobsState(state);
  const jobs = jobsState ? jobsState.data : null;
  const jobsLoading = jobsState ? jobsState.inFlight : false;
  const jobsError = jobsState ? jobsState.lastError : null;
  return {
    sort,
    status,
    show,
    type,
    columns,
    jobs,
    jobsLoading,
    jobsError,
  };
};

const mapDispatchToProps = (dispatch: Dispatch): JobsPageDispatchProps => ({
  setShow: (showValue: string) => {
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
  },
  setStatus: (statusValue: string) => {
    dispatch(
      localStorageActions.update({
        key: "statusSetting/JobsPage",
        value: statusValue,
      }),
    );
  },
  setType: (jobValue: number) => {
    dispatch(
      localStorageActions.update({
        key: "typeSetting/JobsPage",
        value: jobValue,
      }),
    );
  },
  onColumnsChange: (selectedColumns: string[]) =>
    dispatch(
      localStorageActions.update({
        key: "showColumns/JobsPage",
        value: selectedColumns.length === 0 ? " " : selectedColumns.join(","),
      }),
    ),
  refreshJobs: (req: JobsRequest) => dispatch(jobsActions.refresh(req)),
});

export const JobsPageConnected = withRouter(
  connect<JobsPageStateProps, JobsPageDispatchProps, RouteComponentProps>(
    mapStateToProps,
    mapDispatchToProps,
  )(JobsPage),
);
