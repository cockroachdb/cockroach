// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Skeleton, Tabs } from "antd";
import React from "react";
import { useHistory, useLocation } from "react-router-dom";

import { useDatabaseMetadataByID } from "src/api/databases/getDatabaseMetadataApi";
import { commonStyles } from "src/common";
import { useRouteParams } from "src/hooks/useRouteParams";
import { PageLayout } from "src/layouts";
import { PageHeader } from "src/sharedFromCloud/pageHeader";
import { queryByName, tabAttr } from "src/util";

import { DB_PAGE_PATH } from "../util/routes";

import { DbGrantsView } from "./dbGrantsView";
import { TablesPageV2 } from "./tablesView";

enum TabKeys {
  TABLES = "tables",
  GRANTS = "grants",
}
export const DatabaseDetailsPageV2 = () => {
  const { dbID: dbIdRouteParam } = useRouteParams();
  const dbId = parseInt(dbIdRouteParam, 10);
  const { data, isLoading } = useDatabaseMetadataByID(dbId);
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

  const dbName = isLoading ? (
    <Skeleton paragraph={false} title={{ width: 100 }} />
  ) : (
    data?.metadata.dbName ?? "Database Not Found"
  );

  const breadCrumbItems = [
    { name: "Databases", link: DB_PAGE_PATH },
    {
      name: <>Database: {dbName}</>,
      link: "",
    },
  ];

  return (
    <PageLayout>
      <PageHeader breadcrumbItems={breadCrumbItems} title={dbName} />
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
