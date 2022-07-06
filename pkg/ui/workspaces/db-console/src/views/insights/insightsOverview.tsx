// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// All changes made on this file, should also be done on the equivalent
// file on managed-service repo.

import React, { useState } from "react";
import Helmet from "react-helmet";
import { Tabs } from "antd";
import "antd/lib/tabs/style";
import { commonStyles, util } from "@cockroachlabs/cluster-ui";
import { RouteComponentProps } from "react-router-dom";
import { tabAttr, viewAttr } from "src/util/constants";
import SqlInsightsPageConnected from "src/views/insights/sqlInsightsOverview";

const { TabPane } = Tabs;

export enum InsightsTabType {
  SQL_INSIGHTS = "SQL Insights",
}

export const INSIGHTS_DEFAULT_TAB: InsightsTabType =
  InsightsTabType.SQL_INSIGHTS;

const InsightsOverviewPage = (props: RouteComponentProps) => {
  const currentTab =
    util.queryByName(props.location, tabAttr) || InsightsTabType.SQL_INSIGHTS;
  const currentView = util.queryByName(props.location, viewAttr);
  const [restoreSqlViewParam, setRestoreSqlViewParam] = useState<string | null>(
    currentView,
  );

  const onTabChange = (tabId: string): void => {
    const params = new URLSearchParams({ tab: tabId });
    if (tabId !== InsightsTabType.SQL_INSIGHTS) {
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
        <TabPane tab="SQL Insights" key="SQL Insights">
          <SqlInsightsPageConnected />
        </TabPane>
      </Tabs>
    </div>
  );
};

export default InsightsOverviewPage;
