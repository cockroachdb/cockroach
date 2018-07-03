import _ from "lodash";

export interface StatementSummary {
  statement?: string;
  table?: string;
  error?: string;
}

const keywords: { [key: string]: RegExp } = {
  update: /^update\s+(\S+)/i,
  select: /^select(?:fro[^m]|fr[^o]|f[^r]|[^f])+from\s+(\S+)/i,
  insert: /^insert\s+into\s+([^ \t(]+)/i,
  delete: /^delete\s+from\s+(\S+)/i,
  create: /^create\s+table\s+(\S+)/i,
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
      if (table[0] === "\"" && table[table.length - 1] === "\"") {
        table = table.slice(1, table.length - 1);
      }

      return {
        statement: keyword,
        table: table,
      };
    }
  }

  return {
    error: "unimplemented",
  };
}
