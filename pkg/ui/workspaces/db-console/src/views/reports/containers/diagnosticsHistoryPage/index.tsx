// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { commonStyles, util } from "@cockroachlabs/cluster-ui";
import { Tabs } from "antd";
import React from "react";
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";

import { tabAttr } from "src/util/constants";
import StatementDiagnosticsHistoryView from "src/views/reports/containers/statementDiagnosticsHistory";

const { TabPane } = Tabs;

export enum DiagnosticsHistoryTabType {
  Statements = "Statements",
  Transactions = "Transactions",
}

export const DIAGNOSTICS_HISTORY_DEFAULT_TAB: DiagnosticsHistoryTabType =
  DiagnosticsHistoryTabType.Statements;

// Placeholder component for transactions tab
const TransactionDiagnosticsPlaceholder: React.FC = () => (
  <div style={{ padding: "24px", textAlign: "center" }}>
    <h3>Transaction Diagnostics</h3>
    <p>
      Transaction diagnostics history will be available here in a future
      release.
    </p>
  </div>
);

const DiagnosticsHistoryPage: React.FC<RouteComponentProps> = props => {
  const currentTab =
    util.queryByName(props.location, tabAttr) ||
    DiagnosticsHistoryTabType.Statements;

  const onTabChange = (tabId: string): void => {
    const params = new URLSearchParams({ tab: tabId });
    props.history.push({
      search: params.toString(),
    });
  };

  return (
    <div>
      <Helmet title="Diagnostics history | Debug" />
      <h3 className={commonStyles("base-heading")}>Diagnostics History</h3>
      <Tabs
        defaultActiveKey={DIAGNOSTICS_HISTORY_DEFAULT_TAB}
        className={commonStyles("cockroach--tabs")}
        onChange={onTabChange}
        activeKey={currentTab}
        destroyInactiveTabPane
      >
        <TabPane tab="Statements" key="Statements">
          <StatementDiagnosticsHistoryView />
        </TabPane>
        <TabPane tab="Transactions" key="Transactions">
          <TransactionDiagnosticsPlaceholder />
        </TabPane>
      </Tabs>
    </div>
  );
};

export default DiagnosticsHistoryPage;
