// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import startsWith from "lodash/startsWith";
import toLower from "lodash/toLower";

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
    if (startsWith(toLower(statement), toLower(keyword))) {
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

// Check if a valid statement summary exists and is supported, or else
// compute summary using regex.
export function computeOrUseStmtSummary(
  statement: string,
  statementSummary: string,
): string {
  // Statement summary computed with regex.
  const regexSummary = summarize(statement);

  // Current statements that we support summaries for from the backend.
  const summarizedStmts = new Set(["select", "insert", "upsert", "update"]);

  return statementSummary && summarizedStmts.has(regexSummary.statement)
    ? statementSummary
    : shortStatement(regexSummary, statement);
}
