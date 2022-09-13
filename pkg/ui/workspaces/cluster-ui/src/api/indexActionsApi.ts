// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { executeInternalSql, SqlExecutionRequest } from "./sqlApi";

type IndexAction = {
  status: "SUCCESS" | "FAILED";
  error?: string;
};

export type IndexActionResponse = IndexAction[];

export function executeIndexRecAction(
  stmts: string,
  databaseName: string,
): Promise<IndexActionResponse> {
  const statements = stmts
    .split(";")
    .filter(stmt => stmt.trim().length != 0)
    .map(stmt => {
      return { sql: stmt.trim() };
    });

  const request: SqlExecutionRequest = {
    statements: statements,
    database: databaseName,
    execute: true,
  };
  return executeInternalSql<IndexActionResponse>(request)
    .then(result => {
      const res: IndexActionResponse = [];
      if (result.error) {
        res.push({ error: result.error.message, status: "FAILED" });
        return res;
      }
      for (let i = 0; i < result.num_statements; i++) {
        res.push({
          status: "SUCCESS",
        });
      }
      return res;
    })
    .catch(result => {
      const res: IndexActionResponse = [
        { error: result.toString(), status: "FAILED" },
      ];
      return res;
    });
}
