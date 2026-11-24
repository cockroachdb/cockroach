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
import TransactionDiagnosticsHistoryView from "src/views/reports/containers/transactionDiagnosticsHistory";

import type { TabsProps } from "antd";

export enum DiagnosticsHistoryTabType {
  Statements = "Statements",
  Transactions = "Transactions",
}

export const DIAGNOSTICS_HISTORY_DEFAULT_TAB: DiagnosticsHistoryTabType =
  DiagnosticsHistoryTabType.Statements;

const DiagnosticsHistoryPage: React.FC<RouteComponentProps> = props => {
  const tabFromUrl = util.queryByName(props.location, tabAttr);
  const currentTab = Object.values(DiagnosticsHistoryTabType).includes(
    tabFromUrl as DiagnosticsHistoryTabType,
  )
    ? (tabFromUrl as DiagnosticsHistoryTabType)
    : DiagnosticsHistoryTabType.Statements;

  const onTabChange = (tabId: string): void => {
    const params = new URLSearchParams({ tab: tabId });
    props.history.replace({
      search: params.toString(),
    });
  };

  const tabItems: TabsProps["items"] = [
    {
      key: DiagnosticsHistoryTabType.Statements,
      label: "Statements",
      children: <StatementDiagnosticsHistoryView />,
    },
    {
      key: DiagnosticsHistoryTabType.Transactions,
      label: "Transactions",
      children: <TransactionDiagnosticsHistoryView />,
    },
  ];

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
        items={tabItems}
      />
    </div>
  );
};

export default DiagnosticsHistoryPage;
