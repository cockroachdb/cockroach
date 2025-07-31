// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useMemo } from "react";

import { useTableGrantsImmutable } from "src/api/databases/grantsApi";
import { GrantsByUser, GrantsTable } from "src/components/grantsTable";
import { groupGrantsByGrantee } from "src/components/grantsTable/util";
import { useRouteParams } from "src/hooks/useRouteParams";
import { PageSection } from "src/layouts";

export const TableGrantsView: React.FC = () => {
  const { tableID } = useRouteParams();

  const { tableGrants, isLoading, error } = useTableGrantsImmutable({
    tableId: parseInt(tableID, 10),
    pagination: {
      pageSize: 0, // Get all.
      pageNum: 0,
    },
  });

  const dataWithKey: GrantsByUser[] = useMemo(() => {
    return groupGrantsByGrantee(tableGrants);
  }, [tableGrants]);

  return (
    <PageSection>
      <GrantsTable error={error} loading={isLoading} data={dataWithKey ?? []} />
    </PageSection>
  );
};
