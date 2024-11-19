// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useState } from "react";

import { GrantsSortOptions } from "src/api/databases/grantsApi";
import PageCount from "src/sharedFromCloud/pageCount";
import {
  TableColumnProps,
  Table,
  TableChangeFn,
} from "src/sharedFromCloud/table";

import { PageSection } from "../../layouts";

// This type is used by data source for the table.
export type GrantsByUser = {
  key: string;
  grantee: string;
  privileges: string[];
};

const COLUMNS: (TableColumnProps<GrantsByUser> & {
  sortKey: GrantsSortOptions;
})[] = [
  {
    title: "Grantee",
    sorter: (a, b) => a.grantee.localeCompare(b.grantee),
    sortKey: GrantsSortOptions.GRANTEE,
    render: grant => grant.grantee,
  },
  {
    title: "Privileges",
    sortKey: GrantsSortOptions.PRIVILEGE,
    render: grant => grant.privileges.join(", "),
  },
];

const PAGE_SIZE = 20;

type GrantsTableProps = {
  data: GrantsByUser[];
  error?: Error;
  loading?: boolean;
};

export const GrantsTable: React.FC<GrantsTableProps> = ({
  data,
  error,
  loading,
}) => {
  const [currentPage, setCurrentPage] = useState(1);

  const onTableChange: TableChangeFn<GrantsByUser> = pagination => {
    if (pagination.current) {
      setCurrentPage(pagination.current);
    }
  };

  return (
    <>
      <PageSection>
        <PageCount
          page={currentPage}
          pageSize={PAGE_SIZE}
          total={data.length}
          entity="grants"
        />
      </PageSection>
      <Table
        error={error}
        loading={loading}
        dataSource={data}
        columns={COLUMNS}
        pagination={{
          size: "small",
          current: currentPage,
          pageSize: PAGE_SIZE,
          showSizeChanger: false,
          position: ["bottomCenter"],
          total: data.length,
        }}
        onChange={onTableChange}
      />
    </>
  );
};
