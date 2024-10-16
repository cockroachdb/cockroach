// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useMemo } from "react";

import { useDatabaseGrantsImmutable } from "src/api/databases/grantsApi";
import { GrantsByUser, GrantsTable } from "src/components/grantsTable";
import { groupGrantsByGrantee } from "src/components/grantsTable/util";
import { useRouteParams } from "src/hooks/useRouteParams";
import { PageSection } from "src/layouts";

export const DbGrantsView: React.FC = () => {
  const { dbID } = useRouteParams();

  const { databaseGrants, isLoading, error } = useDatabaseGrantsImmutable({
    dbId: parseInt(dbID, 10),
    pagination: {
      pageSize: 0, // Get all.
      pageNum: 0,
    },
  });

  const dataWithKey: GrantsByUser[] = useMemo(() => {
    return groupGrantsByGrantee(databaseGrants);
  }, [databaseGrants]);

  return (
    <PageSection>
      <GrantsTable data={dataWithKey ?? []} loading={isLoading} error={error} />
    </PageSection>
  );
};
