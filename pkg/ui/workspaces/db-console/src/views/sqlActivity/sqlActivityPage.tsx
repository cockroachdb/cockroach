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
import { RouteComponentProps } from "react-router-dom";

const { TabPane } = Tabs;

const SQLActivityPage = (props: RouteComponentProps) => {
  const defaultTab = util.queryByName(props.location, "tab") || "Sessions";
  const [currentTab, setCurrentTab] = useState(defaultTab);

  const onTabChange = (tabId: string): void => {
    setCurrentTab(tabId);
    props.history.location.search = "";
    util.syncHistory({ tab: tabId }, props.history, true);
  };

  useEffect(() => {
    const queryTab = util.queryByName(props.location, "tab") || "Sessions";
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
        <TabPane tab="Sessions" key="Sessions">
          <SessionsPageConnected />
        </TabPane>
        <TabPane tab="Transactions" key="Transactions">
          <TransactionsPageConnected />
        </TabPane>
        <TabPane tab="Statements" key="Statements">
          <StatementsPageConnected />
        </TabPane>
      </Tabs>
    </div>
  );
};

export default SQLActivityPage;
