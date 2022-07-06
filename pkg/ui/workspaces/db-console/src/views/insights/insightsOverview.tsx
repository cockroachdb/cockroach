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
import InsightsTransactionsPageConnected from "src/views/insights/insightsTransactions";

const { TabPane } = Tabs;

export enum InsightsTabType {
  Transactions = "Transactions",
}

export const INSIGHTS_DEFAULT_TAB: InsightsTabType =
  InsightsTabType.Transactions;

const InsightsOverviewPage = (props: RouteComponentProps) => {
  const currentTab =
    util.queryByName(props.location, tabAttr) || InsightsTabType.Transactions;
  const currentView = util.queryByName(props.location, viewAttr);
  const [restoreStmtsViewParam, setRestoreStmtsViewParam] = useState<
    string | null
  >(currentView);

  const onTabChange = (tabId: string): void => {
    const params = new URLSearchParams({ tab: tabId });
    if (tabId !== InsightsTabType.Transactions) {
      setRestoreStmtsViewParam(currentView);
    } else if (currentView || restoreStmtsViewParam) {
      params.set("view", currentView ?? restoreStmtsViewParam ?? "");
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
        <TabPane tab="Transactions" key="Transactions">
          <InsightsTransactionsPageConnected />
        </TabPane>
      </Tabs>
    </div>
  );
};

export default InsightsOverviewPage;
