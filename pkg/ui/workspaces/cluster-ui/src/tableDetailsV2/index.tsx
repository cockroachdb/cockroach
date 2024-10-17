// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tabs } from "antd";
import React, { useState } from "react";

import { useTableDetails } from "src/api/databases/getTableMetadataApi";
import { commonStyles } from "src/common";
import { useRouteParams } from "src/hooks/useRouteParams";
import { PageLayout } from "src/layouts";
import { Loading } from "src/loading";
import { PageHeader } from "src/sharedFromCloud/pageHeader";

import { TableGrantsView } from "./tableGrantsView";
import { TableIndexesView } from "./tableIndexesView";
import { TableOverview } from "./tableOverview";

enum TabKeys {
  OVERVIEW = "overview",
  GRANTS = "grants",
  INDEXES = "indexes",
}

export const TableDetailsPageV2 = () => {
  const [currentTab, setCurrentTab] = useState(TabKeys.OVERVIEW);
  const { tableID } = useRouteParams();
  const { data, error, isLoading } = useTableDetails({
    tableId: parseInt(tableID, 10),
  });
  // The table name is undefined if the table does not exist.
  const tableNotFound = error?.status === 404;

  const partiallyQualifiedTableName = !tableNotFound
    ? data
      ? data.metadata.schemaName + "." + data.metadata.tableName
      : ""
    : "Table not found";

  const breadCrumbItems = [
    { link: `/databases`, name: "Databases" },
    {
      link: `/databases/${data?.metadata.dbId}`,
      name: data?.metadata.dbName,
    },
    {
      link: null,
      name: partiallyQualifiedTableName,
    },
  ].filter(item => item.name);

  const tabItems = [
    {
      key: TabKeys.OVERVIEW,
      label: "Overview",
      children: (
        <Loading error={error} page="TableDetailsOverview" loading={isLoading}>
          {data && <TableOverview tableDetails={data} />}
        </Loading>
      ),
    },
    { key: TabKeys.GRANTS, label: "Grants", children: <TableGrantsView /> },
    {
      key: TabKeys.INDEXES,
      label: "Indexes",
      children: (
        <TableIndexesView
          dbName={data?.metadata.dbName}
          schemaName={data?.metadata.schemaName}
          tableName={data?.metadata.tableName}
        />
      ),
    },
  ];

  return (
    <PageLayout>
      <PageHeader
        breadcrumbItems={breadCrumbItems}
        title={partiallyQualifiedTableName}
      />
      <Tabs
        defaultActiveKey={TabKeys.OVERVIEW}
        className={commonStyles("cockroach--tabs")}
        onChange={setCurrentTab}
        activeKey={currentTab}
        items={tabItems}
        destroyInactiveTabPane
      />
    </PageLayout>
  );
};
