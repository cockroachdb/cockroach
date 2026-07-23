// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  WorkloadInsightEventFilters,
  SortSetting,
  WorkloadInsightsRootControl,
  InsightEnumToLabel,
  TimeScale,
  defaultFilters,
} from "@cockroachlabs/cluster-ui";
import React, { useCallback } from "react";
import { useDispatch, useSelector } from "react-redux";

import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";

export const insightStatementColumnsLocalSetting = new LocalSetting<
  AdminUIState,
  string | null
>(
  "columns/StatementsInsightsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

const filtersLocalSetting = new LocalSetting<
  AdminUIState,
  WorkloadInsightEventFilters
>("filters/InsightsPage", (state: AdminUIState) => state.localSettings, {
  app: defaultFilters.app,
  workloadInsightType: defaultFilters.workloadInsightType,
});

const sortSettingLocalSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/InsightsPage",
  (state: AdminUIState) => state.localSettings,
  {
    ascending: false,
    columnTitle: "startTime",
  },
);

const selectInsightTypes = (): string[] => {
  const insights: string[] = [];
  InsightEnumToLabel.forEach(insight => {
    insights.push(insight);
  });
  return insights;
};

const WorkloadInsightsPage: React.FC = () => {
  const dispatch = useDispatch();
  const timeScale = useSelector(selectTimeScale);
  const filters = useSelector(filtersLocalSetting.selector);
  const sortSetting = useSelector(sortSettingLocalSetting.selector);
  const selectedColumnNames = useSelector(
    insightStatementColumnsLocalSetting.selectorToArray,
  );

  const onFiltersChange = useCallback(
    (f: WorkloadInsightEventFilters) => {
      dispatch(filtersLocalSetting.set(f));
    },
    [dispatch],
  );

  const onSortChange = useCallback(
    (ss: SortSetting) => {
      dispatch(sortSettingLocalSetting.set(ss));
    },
    [dispatch],
  );

  const setTimeScale = useCallback(
    (ts: TimeScale) => {
      dispatch(setGlobalTimeScaleAction(ts));
    },
    [dispatch],
  );

  const onColumnsChange = useCallback(
    (value: string[]) => {
      dispatch(insightStatementColumnsLocalSetting.set(value.join(",")));
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
        onFiltersChange,
        onSortChange,
        setTimeScale,
      }}
      statementInsightsViewProps={{
        insightTypes,
        filters,
        sortSetting,
        selectedColumnNames,
        timeScale,
        onFiltersChange,
        onSortChange,
        onColumnsChange,
        setTimeScale,
      }}
    />
  );
};

export default WorkloadInsightsPage;
