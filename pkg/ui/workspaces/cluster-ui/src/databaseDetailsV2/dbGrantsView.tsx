// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useMemo, useState } from "react";

import {
  GrantsSortOptions,
  useDatabaseGrantsImmutable,
} from "src/api/databases/grantsApi";
import { useRouteParams } from "src/hooks/useRouteParams";
import { PageSection } from "src/layouts";
import PageCount from "src/sharedFromCloud/pageCount";
import {
  Table,
  TableChangeFn,
  TableColumnProps,
} from "src/sharedFromCloud/table";

// This type is used by data source for the table.
type GrantsByUser = {
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

const pageSize = 20;

export const DbGrantsView: React.FC = () => {
  const { dbID } = useRouteParams();
  const [currentPage, setCurrentPage] = useState(1);

  const {
    databaseGrants,
    pagination: paginationRes,
    isLoading,
    error,
  } = useDatabaseGrantsImmutable({
    dbId: parseInt(dbID, 10),
    pagination: {
      pageSize: 0, // Get all.
      pageNum: 0,
    },
  });

  const dataWithKey: GrantsByUser[] = useMemo(() => {
    if (!databaseGrants) {
      return [];
    }
    const grantsByUser = {} as Record<string, string[]>;
    databaseGrants.forEach(grant => {
      if (!grantsByUser[grant.grantee]) {
        grantsByUser[grant.grantee] = [];
      }
      grantsByUser[grant.grantee].push(grant.privilege);
    });

    return Object.entries(grantsByUser).map(([grantee, privileges]) => ({
      key: grantee,
      grantee,
      privileges,
    }));
  }, [databaseGrants]);

  const onTableChange: TableChangeFn<GrantsByUser> = pagination => {
    if (pagination.current) {
      setCurrentPage(pagination.current);
    }
  };

  return (
    <PageSection heading={"Grants"}>
      <PageCount
        page={currentPage}
        pageSize={pageSize}
        total={paginationRes?.total_results ?? 0}
        entity="grants"
      />
      <Table
        error={error}
        loading={isLoading}
        dataSource={dataWithKey ?? []}
        columns={COLUMNS}
        pagination={{
          size: "small",
          current: currentPage,
          pageSize,
          showSizeChanger: false,
          position: ["bottomCenter"],
          total: paginationRes?.total_results,
        }}
        onChange={onTableChange}
      />
    </PageSection>
  );
};
