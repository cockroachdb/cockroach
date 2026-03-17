// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useCallback } from "react";
import { useDispatch, useSelector } from "react-redux";

import { InsightEnumToLabel } from "src/insights";
import { SortSetting } from "src/sortedtable";
import { actions as localStorageActions } from "src/store/localStorage";
import { localStorageSelector } from "src/store/utils/selectors";

import { actions as analyticsActions } from "../../store/analytics";
import { actions as sqlActions } from "../../store/sqlStats";
import { selectTimeScale } from "../../store/utils/selectors";
import { TimeScale } from "../../timeScaleDropdown";
import { WorkloadInsightEventFilters } from "../types";

import { WorkloadInsightsRootControl } from "./workloadInsightRootControl";

const selectInsightTypes = (): string[] => {
  const insights: string[] = [];
  InsightEnumToLabel.forEach(insight => {
    insights.push(insight);
  });
  return insights;
};

export const WorkloadInsightsPageConnected: React.FC = () => {
  const dispatch = useDispatch();
  const timeScale = useSelector(selectTimeScale);
  const localStorage = useSelector(localStorageSelector);
  const filters = localStorage["filters/InsightsPage"];
  const sortSetting = localStorage["sortSetting/InsightsPage"];
  const selectedColumnNames = localStorage["showColumns/StatementInsightsPage"]
    ? localStorage["showColumns/StatementInsightsPage"]?.split(",")
    : null;

  // --- Transaction tab handlers ---

  const txnOnFiltersChange = useCallback(
    (f: WorkloadInsightEventFilters) => {
      dispatch(
        localStorageActions.update({
          key: "filters/InsightsPage",
          value: f,
        }),
      );
      dispatch(
        analyticsActions.track({
          name: "Filter Clicked",
          page: "Workload Insights - Transaction",
          filterName: "filters",
          value: f.toString(),
        }),
      );
    },
    [dispatch],
  );

  const txnOnSortChange = useCallback(
    (ss: SortSetting) => {
      dispatch(
        localStorageActions.update({
          key: "sortSetting/InsightsPage",
          value: ss,
        }),
      );
      dispatch(
        analyticsActions.track({
          name: "Column Sorted",
          page: "Workload Insights - Transaction",
          tableName: "Workload Transaction Insights Table",
          columnName: ss.columnTitle,
        }),
      );
    },
    [dispatch],
  );

  const txnSetTimeScale = useCallback(
    (ts: TimeScale) => {
      dispatch(sqlActions.updateTimeScale({ ts }));
      dispatch(
        analyticsActions.track({
          name: "TimeScale changed",
          page: "Workload Insights - Transaction",
          value: ts.key,
        }),
      );
    },
    [dispatch],
  );

  // --- Statement tab handlers ---

  const stmtOnFiltersChange = useCallback(
    (f: WorkloadInsightEventFilters) => {
      dispatch(
        localStorageActions.update({
          key: "filters/InsightsPage",
          value: f,
        }),
      );
      dispatch(
        analyticsActions.track({
          name: "Filter Clicked",
          page: "Workload Insights - Statement",
          filterName: "filters",
          value: f.toString(),
        }),
      );
    },
    [dispatch],
  );

  const stmtOnSortChange = useCallback(
    (ss: SortSetting) => {
      dispatch(
        localStorageActions.update({
          key: "sortSetting/InsightsPage",
          value: ss,
        }),
      );
      dispatch(
        analyticsActions.track({
          name: "Column Sorted",
          page: "Workload Insights - Statement",
          tableName: "Workload Statement Insights Table",
          columnName: ss.columnTitle,
        }),
      );
    },
    [dispatch],
  );

  const stmtSetTimeScale = useCallback(
    (ts: TimeScale) => {
      dispatch(sqlActions.updateTimeScale({ ts }));
      dispatch(
        analyticsActions.track({
          name: "TimeScale changed",
          page: "Workload Insights - Statement",
          value: ts.key,
        }),
      );
    },
    [dispatch],
  );

  const onColumnsChange = useCallback(
    (value: string[]) => {
      const columns = value.length === 0 ? " " : value.join(",");
      dispatch(
        localStorageActions.update({
          key: "showColumns/StatementInsightsPage",
          value: columns,
        }),
      );
      dispatch(
        analyticsActions.track({
          name: "Columns Selected change",
          page: "Workload Insights - Statement",
          value: columns,
        }),
      );
    },
    [dispatch],
  );

  const insightTypes = selectInsightTypes();

  return (
    <WorkloadInsightsRootControl
      transactionInsightsViewProps={{
        insightTypes,
        filters,
        sortSetting,
        timeScale,
        onFiltersChange: txnOnFiltersChange,
        onSortChange: txnOnSortChange,
        setTimeScale: txnSetTimeScale,
      }}
      statementInsightsViewProps={{
        insightTypes,
        filters,
        sortSetting,
        selectedColumnNames,
        timeScale,
        onFiltersChange: stmtOnFiltersChange,
        onSortChange: stmtOnSortChange,
        onColumnsChange,
        setTimeScale: stmtSetTimeScale,
      }}
    />
  );
};
