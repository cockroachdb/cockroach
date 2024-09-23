// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";

import {
  DatabaseGrantsRow,
  useDatabaseGrants,
} from "src/api/databaseGrantsApi";
import { PageSection } from "src/layouts";
import { Table, TableColumnProps } from "src/sharedFromCloud/table";

type GrantsViewProps = {
  dbName: string;
};

const COLUMNS: TableColumnProps<DatabaseGrantsRow>[] = [
  {
    title: "User",
    render: grant => grant.user,
  },
  {
    title: "Privileges",
    render: grant => grant.privileges.join(", "),
  },
];

export const DbGrantsView: React.FC<GrantsViewProps> = ({ dbName }) => {
  const { databaseGrants, isLoading, error } = useDatabaseGrants(dbName);

  const dataWithKey = databaseGrants?.map((grant, idx) => ({
    ...grant,
    key: idx.toString(),
  }));

  return (
    <PageSection heading={"Grants"}>
      <Table
        error={error}
        loading={isLoading}
        dataSource={dataWithKey}
        columns={COLUMNS}
      />
    </PageSection>
  );
};
