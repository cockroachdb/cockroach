// Copyright 2021 The Cockroach Authors.
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
import SessionsPageConnected from "src/views/sessions/sessionsPage";
import TransactionsPageConnected from "src/views/transactions/transactionsPage";
import StatementsPageConnected from "src/views/statements/statementsPage";
import { commonStyles, util } from "@cockroachlabs/cluster-ui";
import { RouteComponentProps } from "react-router-dom";
import { tabAttr, viewAttr } from "src/util/constants";

const { TabPane } = Tabs;

export enum SQLActivityTabType {
  Statements = "Statements",
  Sessions = "Sessions",
  Transactions = "Transactions",
}

export const SQL_ACTIVITY_DEFAULT_TAB: SQLActivityTabType =
  SQLActivityTabType.Statements;

const SQLActivityPage = (props: RouteComponentProps) => {
  const currentTab =
    util.queryByName(props.location, tabAttr) || SQLActivityTabType.Statements;
  const currentView = util.queryByName(props.location, viewAttr);
  const [restoreStmtsViewParam, setRestoreStmtsViewParam] = useState<
    string | null
  >(currentView);

  const onTabChange = (tabId: string): void => {
    const params = new URLSearchParams({ tab: tabId });
    if (tabId === "Sessions") {
      setRestoreStmtsViewParam(currentView);
    } else if (currentView || restoreStmtsViewParam) {
      // We want to persist the view (fingerprints or active executions)
      // for statement and transactions pages, and also restore the value
      // when coming from sessions tab.
      params.set("view", currentView ?? restoreStmtsViewParam ?? "");
    }
    props.history.push({
      search: params.toString(),
    });
  };

  return (
    <div>
      <Helmet title={currentTab} />
      <h3 className={commonStyles("base-heading")}>SQL Activity</h3>
      <Tabs
        defaultActiveKey={SQL_ACTIVITY_DEFAULT_TAB}
        className={commonStyles("cockroach--tabs")}
        onChange={onTabChange}
        activeKey={currentTab}
        destroyInactiveTabPane
      >
        <TabPane tab="Statements" key="Statements">
          <StatementsPageConnected />
        </TabPane>
        <TabPane tab="Transactions" key="Transactions">
          <TransactionsPageConnected />
        </TabPane>
        <TabPane tab="Sessions" key="Sessions">
          <SessionsPageConnected />
        </TabPane>
      </Tabs>
    </div>
  );
};

export default SQLActivityPage;
