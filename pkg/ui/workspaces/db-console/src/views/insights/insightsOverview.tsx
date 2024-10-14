// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// All changes made on this file, should also be done on the equivalent
// file on managed-service repo.

import { commonStyles, util } from "@cockroachlabs/cluster-ui";
import { Tabs } from "antd";
import React, { useState } from "react";
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";

import { tabAttr, viewAttr } from "src/util/constants";

import SchemaInsightsPage from "./schemaInsightsPage";
import WorkloadInsightsPage from "./workloadInsightsPage";

const { TabPane } = Tabs;

export enum InsightsTabType {
  WORKLOAD_INSIGHTS = "Workload Insights",
}

export const INSIGHTS_DEFAULT_TAB: InsightsTabType =
  InsightsTabType.WORKLOAD_INSIGHTS;

const InsightsOverviewPage = (props: RouteComponentProps) => {
  const currentTab =
    util.queryByName(props.location, tabAttr) ||
    InsightsTabType.WORKLOAD_INSIGHTS;
  const currentView = util.queryByName(props.location, viewAttr);
  const [restoreSqlViewParam, setRestoreSqlViewParam] = useState<string | null>(
    currentView,
  );

  const onTabChange = (tabId: string): void => {
    const params = new URLSearchParams({ tab: tabId });
    if (tabId !== InsightsTabType.WORKLOAD_INSIGHTS) {
      setRestoreSqlViewParam(currentView);
    } else if (currentView || restoreSqlViewParam) {
      params.set("view", currentView ?? restoreSqlViewParam ?? "");
    }
    props.history.push({
      search: params.toString(),
    });
  };

  return (
    <div>
      <Helmet title={currentTab} />
      <h3 className={commonStyles("base-heading")}>Insights</h3>
      <Tabs
        defaultActiveKey={INSIGHTS_DEFAULT_TAB}
        className={commonStyles("cockroach--tabs")}
        onChange={onTabChange}
        activeKey={currentTab}
        destroyInactiveTabPane
      >
        <TabPane tab="Workload Insights" key="Workload Insights">
          <WorkloadInsightsPage />
        </TabPane>
        <TabPane tab="Schema Insights" key="Schema Insights">
          <SchemaInsightsPage />
        </TabPane>
      </Tabs>
    </div>
  );
};

export default InsightsOverviewPage;
