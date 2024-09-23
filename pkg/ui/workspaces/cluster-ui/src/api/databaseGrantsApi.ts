// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import useSWR from "swr";

import { Format, Identifier } from "./safesql";
import { executeInternalSql } from "./sqlApi";

const createGrantStmt = (db: string) => {
  return Format(
    `
  SELECT grantee as user, array_agg(privilege_type) as privileges
  FROM
    [SHOW GRANTS ON DATABASE %1]
  GROUP BY grantee`,
    [new Identifier(db)],
  );
};

export type DatabaseGrantsRow = {
  user: string;
  privileges: string[];
};

export const useDatabaseGrants = (databaseName: string) => {
  const { data, isLoading, error } = useSWR(`dbGrants/${databaseName}`, () =>
    executeInternalSql<DatabaseGrantsRow>({
      statements: [{ sql: createGrantStmt(databaseName) }],
      execute: true,
    }),
  );

  const txnResults =
    data?.execution?.txn_results?.length && data.execution.txn_results[0]?.rows;

  return {
    databaseGrants: txnResults,
    isLoading,
    error: error,
  };
};
