// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Tabs } from "antd";
import React, { useState } from "react";

import { useTableDetails } from "src/api/databases/getTableMetadataApi";
import { commonStyles } from "src/common";
import { useRouteParams } from "src/hooks/useRouteParams";
import { PageLayout } from "src/layouts";
import { Loading } from "src/loading";
import { PageHeader } from "src/sharedFromCloud/pageHeader";

import { TableGrantsView } from "./tableGrantsView";
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
      ? data.metadata.schema_name + "." + data.metadata.table_name
      : ""
    : "Table not found";

  const breadCrumbItems = [
    { link: `/databases`, name: "Databases" },
    {
      link: `/databases/${data?.metadata.db_id}`,
      name: data?.metadata.db_name,
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
        <Loading page="TableDetailsOverview" loading={isLoading}>
          <TableOverview tableDetails={data} />
        </Loading>
      ),
    },
    { key: TabKeys.GRANTS, label: "Grants", children: <TableGrantsView /> },
    { key: TabKeys.INDEXES, label: "Indexes" },
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
