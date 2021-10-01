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

export interface StatementSummary {
  statement?: string;
  columns?: string;
  table?: string;
  error?: string;
}

const keywords: { [key: string]: RegExp } = {
  update: /^update\s+(\S+)/i,
  select: /^select.+from\s+(\S+)/i,
  insert: /^insert\s+into\s+([^ \t(]+)/i,
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

// detailedSummarize takes a string SQL statement and produces a
// structured summary of the query, with columns and join/nested table info.
export function detailedSummarize(statement: string): StatementSummary {
  const statementTypes: { [key: string]: RegExp } = {
    select: /^select\s+(?<columns>.+)\s+from\s+(?<table>.+)/i,
  };

  const sqlKeywords = new Set(["ON", "WHERE", "ORDER BY"]);

  function simplifySubquery(subquery: string) {
    const innerSelectRegex = /^\(select.+from\s+(?<table>.+)\)(\s+as\s+(?<alias>\S+))?/i;
    const match = subquery.match(innerSelectRegex);
    if (match && match.groups.table) {
      subquery = "(SELECT FROM " + match.groups.table + ")";
      if (match.groups.alias) {
        subquery += " AS " + match.groups.alias;
      }
    }
    return subquery;
  }

  for (const [statementType, pattern] of Object.entries(statementTypes)) {
    const statementMatch = statement.match(pattern);
    if (
      statementMatch &&
      statementMatch.groups.columns &&
      statementMatch.groups.table
    ) {
      // get columns from regex.
      let columns = statementMatch.groups.columns
        .split(", ")
        .map(col => simplifySubquery(col)) // simplify nested selects
        .join(", ");

      const columnLimit = 15;
      if (columns.length > columnLimit) {
        columns = columns.slice(0, columnLimit) + "...";
      }

      // get tables from regex.
      let table = statementMatch.groups.table;
      for (const keyword of sqlKeywords) {
        const keywordIndex = table.indexOf(keyword);
        if (keywordIndex !== -1) {
          table = table.slice(0, keywordIndex - 1);
        }
      }
      table = table
        .split(", ")
        .map(table => simplifySubquery(table)) // simplify nested selects
        .join(", ");

      const tableLimit = 30;
      if (table.length > tableLimit) {
        table = table.slice(0, tableLimit) + "...";
      }

      return {
        statement: statementType,
        columns,
        table,
      };
    }
  }

  return summarize(statement);
}
