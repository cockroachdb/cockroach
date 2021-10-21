// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import { shortStatement } from "../../statementsTable";

export interface StatementSummary {
  statement?: string;
  table?: string;
  error?: string;
}

const keywords: { [key: string]: RegExp } = {
  update: /^update\s+(\S+)/i,
  select: /^select.+from\s+(\S+)/i,
  insert: /^insert\s+into\s+([^ \t(]+)/i,
  upsert: /^upsert\s+into\s+([^ \t(]+)/i,
  delete: /^delete\s+from\s+(\S+)/i,
  create: /^create\s+table\s+(\S+)/i,
  set: /^set\s+((cluster\s+setting\s+)?\S+)/i,
};

// summarize takes a string SQL statement and produces a structured summary
// of the query.
export function summarize(statement: string): StatementSummary {
  for (const keyword in keywords) {
    if (_.startsWith(_.toLower(statement), _.toLower(keyword))) {
      const tablePattern = keywords[keyword];
      const tableMatch = tablePattern.exec(statement);
      if (!tableMatch) {
        return {
          error: "unable to find table for " + keyword + " statement",
        };
      }

      let table = tableMatch[1];
      if (table[0] === '"' && table[table.length - 1] === '"') {
        table = table.slice(1, table.length - 1);
      }

      return {
        statement: keyword,
        table,
      };
    }
  }

  return {
    error: "unimplemented",
  };
}

export function getStmtSummary(
  statement: string,
  statementSummary: string,
): string {
  // previous way of summarizing statements with regex.
  const oldSummary = summarize(statement);

  // current statements that we support summaries for from the backend.
  const summarizedStmts = new Set(["select", "insert", "upsert", "update"]);

  return statementSummary && summarizedStmts.has(oldSummary.statement)
    ? statementSummary
    : shortStatement(oldSummary, statement);
}
