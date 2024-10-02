// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useMemo } from "react";

import { useTableGrantsImmutable } from "src/api/databases/grantsApi";
import { GrantsByUser, GrantsTable } from "src/components/grantsTable";
import { groupGrantsByGrantee } from "src/components/grantsTable/util";
import { useRouteParams } from "src/hooks/useRouteParams";
import { PageSection } from "src/layouts";
import { Loading } from "src/loading";

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
    <PageSection heading={"Grants"}>
      <Loading page="Database Grants" loading={isLoading} error={error}>
        <GrantsTable data={dataWithKey ?? []} />
      </Loading>
    </PageSection>
  );
};
