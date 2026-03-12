// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { useDispatch, useSelector } from "react-redux";

import { SortSetting } from "../../sortedtable";
import { actions as analyticsActions } from "../../store/analytics";
import {
  selectShowSetting,
  selectSortSetting,
  selectTypeSetting,
  selectStatusSetting,
  selectColumns,
} from "../../store/jobs/jobs.selectors";
import { actions as localStorageActions } from "../../store/localStorage";

import { JobsPage } from "./jobsPage";

export const JobsPageConnected: React.FC = () => {
  const dispatch = useDispatch();
  const sort = useSelector(selectSortSetting);
  const status = useSelector(selectStatusSetting);
  const show = useSelector(selectShowSetting);
  const type = useSelector(selectTypeSetting);
  const columns = useSelector(selectColumns);

  const setShow = (showValue: string) => {
    dispatch(
      localStorageActions.update({
        key: "showSetting/JobsPage",
        value: showValue,
      }),
    );
  };

  const setSort = (ss: SortSetting) => {
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
  };

  const setStatus = (statusValue: string) => {
    dispatch(
      localStorageActions.update({
        key: "statusSetting/JobsPage",
        value: statusValue,
      }),
    );
  };

  const setType = (jobValue: number) => {
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
  };

  const onColumnsChange = (selectedColumns: string[]) => {
    const cols =
      selectedColumns.length === 0 ? " " : selectedColumns.join(",");
    dispatch(
      localStorageActions.update({
        key: "showColumns/JobsPage",
        value: cols,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Columns Selected change",
        page: "Jobs",
        value: cols,
      }),
    );
  };

  return (
    <JobsPage
      sort={sort}
      status={status}
      show={show}
      type={type}
      columns={columns}
      setSort={setSort}
      setStatus={setStatus}
      setShow={setShow}
      setType={setType}
      onColumnsChange={onColumnsChange}
    />
  );
};
