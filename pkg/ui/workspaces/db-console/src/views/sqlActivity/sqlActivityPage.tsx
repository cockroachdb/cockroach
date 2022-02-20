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

import React, { useState, useEffect } from "react";
import Helmet from "react-helmet";
import { Tabs } from "antd";
import { commonStyles, util } from "@cockroachlabs/cluster-ui";
import SessionsPageConnected from "src/views/sessions/sessionsPage";
import TransactionsPageConnected from "src/views/transactions/transactionsPage";
import StatementsPageConnected from "src/views/statements/statementsPage";
import {
  StatementsPageRoot,
  TransactionsPageRoot,
} from "@cockroachlabs/cluster-ui";
import { RouteComponentProps } from "react-router-dom";
import ActiveStatementsPage from "../statements/activeStatementsPage";
import ActiveTransactionsPage from "../transactions/activeTransactionsPage";

const { TabPane } = Tabs;

const SQLActivityPage = (props: RouteComponentProps) => {
  const defaultTab = util.queryByName(props.location, "tab") || "Statements";
  const [currentTab, setCurrentTab] = useState(defaultTab);

  const onTabChange = (tabId: string): void => {
    setCurrentTab(tabId);
    props.history.location.search = "";
    util.syncHistory({ tab: tabId }, props.history, true);
  };

  useEffect(() => {
    const queryTab = util.queryByName(props.location, "tab") || "Statements";
    if (queryTab !== currentTab) {
      setCurrentTab(queryTab);
    }
  }, [props.location, currentTab]);

  return (
    <div>
      <Helmet title={defaultTab} />
      <h3 className={commonStyles("base-heading")}>SQL Activity</h3>
      <Tabs
        defaultActiveKey={defaultTab}
        className={commonStyles("cockroach--tabs")}
        onChange={onTabChange}
        activeKey={currentTab}
      >
        <TabPane tab="Statements" key="Statements">
          <StatementsPageRoot
            activeQueriesView={ActiveStatementsPage}
            fingerprintsView={StatementsPageConnected}
          />
        </TabPane>
        <TabPane tab="Transactions" key="Transactions">
          <TransactionsPageRoot
            activeTransactionsView={ActiveTransactionsPage}
            fingerprintsView={TransactionsPageConnected}
          />
        </TabPane>
        <TabPane tab="Sessions" key="Sessions">
          <SessionsPageConnected />
        </TabPane>
      </Tabs>
    </div>
  );
};

export default SQLActivityPage;
