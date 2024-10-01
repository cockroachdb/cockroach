// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tabs } from "antd";
import React from "react";
import { useHistory, useLocation } from "react-router";

import { commonStyles } from "src/common";
import { PageLayout } from "src/layouts";
import { PageHeader } from "src/sharedFromCloud/pageHeader";

import { queryByName, tabAttr } from "../util";

import { DbGrantsView } from "./dbGrantsView";
import { TablesPageV2 } from "./tablesView";

enum TabKeys {
  TABLES = "tables",
  GRANTS = "grants",
}
export const DatabaseDetailsPageV2 = () => {
  const history = useHistory();
  const location = useLocation();
  const tab = queryByName(location, tabAttr) ?? TabKeys.TABLES;

  const onTabChange = (key: string) => {
    if (tab === key) {
      return;
    }
    const searchParams = new URLSearchParams();
    if (key) {
      searchParams.set(tabAttr, key);
    }
    history.push({
      search: searchParams.toString(),
    });
  };

  // TODO (xinhaoz) #131119 - Populate db name here.
  const tabItems = [
    {
      key: TabKeys.TABLES,
      label: "Tables",
      children: <TablesPageV2 />,
    },
    {
      key: TabKeys.GRANTS,
      label: "Grants",
      children: <DbGrantsView />,
    },
  ];

  return (
    <PageLayout>
      <PageHeader title="myDB" />
      <Tabs
        defaultActiveKey={TabKeys.TABLES}
        className={commonStyles("cockroach--tabs")}
        onChange={onTabChange}
        activeKey={tab}
        destroyInactiveTabPane
        items={tabItems}
      />
    </PageLayout>
  );
};
